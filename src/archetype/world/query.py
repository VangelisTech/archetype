# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Durable world reads.

This module intentionally has no application or audit dependency.  Every
physical read resolves through ``StorageService`` and captures a manifest
visibility snapshot before constructing the lazy result.
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable

from daft import DataFrame, col, lit

from archetype.core.aio import AsyncQueryManager
from archetype.core.archetype import Archetype
from archetype.core.component import Component
from archetype.core.config import StorageConfig
from archetype.core.interfaces import ArchetypeSignature, iAsyncStore
from archetype.core.lineage import load_lineage
from archetype.storage.catalog import (
    CatalogSchemaMismatchError,
    SignatureRecord,
    schema_fingerprint,
)
from archetype.storage.service import StorageService
from archetype.storage.signatures import match_signature_records

logger = logging.getLogger(__name__)


async def _querier_for(
    storage: StorageService,
    storage_config: StorageConfig | None,
) -> tuple[StorageConfig, iAsyncStore, AsyncQueryManager]:
    effective = storage_config or StorageConfig()
    store = await storage.get_or_create_store(effective, None)
    return effective, store, AsyncQueryManager(store=store)


async def query_archetype(
    storage: StorageService,
    sig: ArchetypeSignature,
    world_id: str,
    run_id: str,
    storage_config: StorageConfig | None = None,
    *,
    ticks: list[int] | None = None,
    entity_ids: list[int] | None = None,
    components: list[type[Component]] | None = None,
    lineage: list[tuple[str, str, int]] | None = None,
) -> DataFrame:
    """Read one signature through exact manifest visibility snapshots."""
    effective, store, querier = await _querier_for(storage, storage_config)
    canonical_sig = tuple(sorted(sig, key=lambda component: component.__name__))
    table_id = Archetype.get_name(canonical_sig)
    catalog_record = next(
        (
            record
            for record in await _signature_records(storage, effective)
            if record.table_id == table_id
        ),
        None,
    )

    async def _segment(
        segment_world: str,
        segment_run: str,
        segment_ticks: list[int] | None,
    ) -> DataFrame:
        tokens = await _visible_tokens(
            storage,
            effective,
            segment_world,
            segment_run,
            segment_ticks,
        )
        if catalog_record is None:
            return await querier.query_archetype(
                sig=canonical_sig,
                world_id=segment_world,
                run_id=segment_run,
                ticks=segment_ticks,
                entity_ids=entity_ids,
                components=components,
                commit_tokens=tokens,
            )

        physical = await store.get_existing_table_schema(catalog_record.table_id)
        if catalog_record.fingerprint != schema_fingerprint(physical):
            raise CatalogSchemaMismatchError(
                f"catalog descriptor for table {catalog_record.table_id} does not "
                "match the physical schema; refusing to read (fail closed)"
            )
        frame = await store.get_existing_table_df(
            catalog_record.table_id,
            segment_world,
            segment_run,
            ticks=segment_ticks,
            entity_ids=entity_ids,
            active_only=True,
        )
        if tokens is not None and "commit_token" in frame.column_names:
            visible = frame["commit_token"].is_in(tokens) if tokens else lit(False)
            frame = frame.where(visible)
        if components:
            frame = frame.select(*Archetype.projection_columns(components))
        return frame

    result = await _segment(str(world_id), str(run_id), ticks)
    return await _union_lineage(result, lineage, ticks, _segment)


async def query_components(
    storage: StorageService,
    components: list[type[Component]],
    world_id: str,
    run_id: str,
    storage_config: StorageConfig | None = None,
    *,
    ticks: list[int] | None = None,
    entity_ids: list[int] | None = None,
    lineage: list[tuple[str, str, int]] | None = None,
    visibility_tokens: list[str] | None = None,
) -> DataFrame:
    """Read every signature containing ``components`` through pinned visibility."""
    if visibility_tokens is not None and lineage:
        raise ValueError("visibility_tokens cannot be combined with lineage")
    effective, store, querier = await _querier_for(storage, storage_config)
    catalog_records = await _catalog_candidates(storage, effective, components)
    pinned_tokens = list(visibility_tokens) if visibility_tokens is not None else None

    async def _read(
        segment_world: str,
        segment_run: str,
        segment_ticks: list[int] | None,
    ) -> DataFrame:
        tokens = (
            pinned_tokens
            if visibility_tokens is not None
            else await _visible_tokens(
                storage,
                effective,
                segment_world,
                segment_run,
                segment_ticks,
            )
        )
        return await _components_frame(
            querier,
            store,
            catalog_records,
            components,
            segment_world,
            segment_run,
            ticks=segment_ticks,
            entity_ids=entity_ids,
            commit_tokens=tokens,
        )

    result = await _read(str(world_id), str(run_id), ticks)
    return await _union_lineage(result, lineage, ticks, _read)


async def _signature_records(
    storage: StorageService,
    storage_config: StorageConfig,
) -> list[SignatureRecord]:
    """Return durable discovery records, degrading only discovery on failure."""
    try:
        catalog = storage.get_control_catalog(storage_config)
        return await catalog.list_signatures()
    except Exception:
        logger.exception("control catalog unavailable for durable signature discovery")
        return []


async def _catalog_candidates(
    storage: StorageService,
    storage_config: StorageConfig,
    components: list[type[Component]],
) -> list[SignatureRecord]:
    requested = {component.__name__ for component in components}
    records = await _signature_records(storage, storage_config)
    return [record for record in records if requested.issubset(record.component_names)]


async def _visible_tokens(
    storage: StorageService,
    storage_config: StorageConfig,
    world_id: str,
    run_id: str,
    ticks: list[int] | None,
) -> list[str] | None:
    """Pin one immutable manifest allowlist for a world/run segment."""
    max_tick = max(ticks) if ticks else None
    visibility = await storage.pin_visibility(
        storage_config,
        str(world_id),
        run_id=str(run_id),
        max_tick=max_tick,
    )
    if visibility.visibility_tokens is None:
        return None
    return list(visibility.visibility_tokens)


async def _components_frame(
    querier: AsyncQueryManager,
    store: iAsyncStore,
    catalog_records: list[SignatureRecord],
    components: list[type[Component]],
    world_id: str,
    run_id: str,
    *,
    ticks: list[int] | None,
    entity_ids: list[int] | None,
    commit_tokens: list[str] | None,
) -> DataFrame:
    """Union process-local signatures with catalog-discovered physical tables."""
    import daft
    import pyarrow as pa

    output_sig = tuple(sorted(components, key=lambda component: component.__name__))
    projection_columns = Archetype.projection_columns(list(output_sig))
    full_schema = Archetype.get_archetype_schema(output_sig)
    schema = pa.schema([full_schema.field(name) for name in projection_columns])

    written_signatures = await querier.list_committed_signatures()
    written_tables = {Archetype.get_name(signature) for signature in written_signatures}
    extra_records = [record for record in catalog_records if record.table_id not in written_tables]

    try:
        result = await querier.query_components(
            components=components,
            world_id=world_id,
            run_id=run_id,
            ticks=ticks,
            entity_ids=entity_ids,
            commit_tokens=commit_tokens,
        )
    except KeyError:
        if not extra_records:
            raise
        result = daft.from_arrow(pa.Table.from_batches([], schema=schema))

    for record in extra_records:
        physical = await store.get_existing_table_schema(record.table_id)
        if record.fingerprint != schema_fingerprint(physical):
            raise CatalogSchemaMismatchError(
                f"catalog descriptor for table {record.table_id} does not match "
                "the physical schema; refusing to read (fail closed)"
            )
        frame = await store.get_existing_table_df(
            record.table_id,
            world_id,
            run_id,
            ticks=ticks,
            entity_ids=entity_ids,
            active_only=True,
        )
        if commit_tokens is not None and "commit_token" in frame.column_names:
            visible = frame["commit_token"].is_in(commit_tokens) if commit_tokens else lit(False)
            frame = frame.where(visible)
        result = result.concat(frame.select(*projection_columns))
    return result


async def _union_lineage(
    result: DataFrame,
    lineage: list[tuple[str, str, int]] | None,
    ticks: list[int] | None,
    segment_query: Callable[[str, str, list[int] | None], Awaitable[DataFrame]],
) -> DataFrame:
    """Union each ancestor's owned tick interval into a child read."""
    previous_up_to = -1
    for ancestor_world, ancestor_run, up_to_tick in lineage or ():
        if ticks is not None:
            segment_ticks = [tick for tick in ticks if previous_up_to < tick <= up_to_tick]
            if not segment_ticks:
                previous_up_to = up_to_tick
                continue
        else:
            segment_ticks = None
        frame = await segment_query(
            str(ancestor_world),
            str(ancestor_run),
            segment_ticks,
        )
        if ticks is None:
            lower = col("tick") > previous_up_to  # ty: ignore[unsupported-operator]
            upper = col("tick") <= up_to_tick  # ty: ignore[unsupported-operator]
            frame = frame.where(lower & upper)
        result = result.concat(frame)
        previous_up_to = up_to_tick
    return result


async def get_lineage(
    storage: StorageService,
    world_id: str,
    run_id: str,
    storage_config: StorageConfig | None = None,
) -> list[tuple[str, str, int]] | None:
    """Recover append-only fork ancestry, including for a closed world."""
    _effective, store, _querier = await _querier_for(storage, storage_config)
    return await load_lineage(store, world_id=str(world_id), run_id=str(run_id))


async def list_signatures(
    storage: StorageService,
    storage_config: StorageConfig | None = None,
) -> list[ArchetypeSignature]:
    """Resolve process-local and durably cataloged signatures."""
    effective, _store, querier = await _querier_for(storage, storage_config)
    signatures = {
        Archetype.get_name(signature): signature for signature in await querier.list_signatures()
    }
    records = await _signature_records(storage, effective)
    discovered, problems = match_signature_records(records)
    if problems:
        detail = "; ".join(
            f"{table_id}: {message}" for table_id, message in sorted(problems.items())
        )
        logger.warning(
            "list_signatures skipped %d unresolved durable record(s): %s",
            len(problems),
            detail,
        )
    signatures = discovered | signatures
    return [signature for _table_id, signature in sorted(signatures.items())]
