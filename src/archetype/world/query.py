# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Durable world reads.

This module intentionally has no application or audit dependency.  Every
physical read resolves through ``iStorageService`` and captures a manifest
visibility snapshot before constructing the lazy result.
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from daft import DataFrame, col, lit

from archetype.core.aio import AsyncQueryManager
from archetype.core.archetype import Archetype
from archetype.core.component import Component
from archetype.core.config import StorageConfig
from archetype.core.interfaces import ArchetypeSignature, CommittedTickReceipt, iAsyncStore
from archetype.core.lineage import load_lineage
from archetype.storage.catalog import (
    CatalogSchemaMismatchError,
    SignatureRecord,
    schema_fingerprint,
)
from archetype.storage.interfaces import iStorageService
from archetype.storage.signatures import match_signature_records

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class PinnedQuerySegment:
    """One immutable world/run visibility segment in a logical world read."""

    world_id: str
    run_id: str
    up_to_tick: int | None
    head_tick: int | None
    head_tokens: tuple[str, ...]
    visibility_tokens: tuple[str, ...] | None


@dataclass(frozen=True, slots=True)
class PinnedWorldQuerySnapshot:
    """Exact current-run and inherited visibility for one logical world."""

    world_id: str
    run_id: str
    head_tick: int | None
    head_tokens: tuple[str, ...]
    current: PinnedQuerySegment
    lineage: tuple[PinnedQuerySegment, ...]

    @property
    def effective_lineage(self) -> tuple[PinnedQuerySegment, ...]:
        """Return only ancestor segments that own a non-empty tick interval."""

        effective: list[PinnedQuerySegment] = []
        previous_up_to = -1
        for segment in self.lineage:
            assert segment.up_to_tick is not None
            if segment.up_to_tick > previous_up_to:
                effective.append(segment)
            previous_up_to = segment.up_to_tick
        return tuple(effective)


async def _querier_for(
    storage: iStorageService,
    storage_config: StorageConfig | None,
) -> tuple[StorageConfig, iAsyncStore, AsyncQueryManager]:
    effective = storage_config or StorageConfig()
    store = await storage.get_or_create_store(effective, None)
    return effective, store, AsyncQueryManager(store=store)


async def pin_query_snapshot(
    storage: iStorageService,
    world_id: str,
    run_id: str | None = None,
    storage_config: StorageConfig | None = None,
    *,
    max_tick: int | None = None,
) -> PinnedWorldQuerySnapshot:
    """Pin every physical segment that forms one logical world snapshot."""

    effective = storage_config or StorageConfig()
    wid = str(world_id)
    if max_tick is not None and max_tick < 0:
        raise ValueError("query snapshot max_tick must be non-negative")
    catalog = storage.get_control_catalog(effective)
    record = await catalog.get_world(wid)
    if record is None:
        raise KeyError(f"world {wid} is not recorded in catalog for {effective.uri}")
    recorded_run = str(record.run_id or "")
    if not recorded_run:
        raise RuntimeError(f"world {wid} has no recorded run; visibility cannot be pinned")
    if run_id is not None and str(run_id) != recorded_run:
        raise ValueError(f"world {wid} records run {recorded_run}, not requested run {run_id}")
    current_visibility = await storage.pin_visibility(
        effective,
        wid,
        run_id=recorded_run,
        max_tick=max_tick,
    )
    rid = str(current_visibility.run_id)
    if rid != recorded_run:
        raise RuntimeError(
            f"world {wid} visibility resolved run {rid}, expected recorded run {recorded_run}"
        )
    current = PinnedQuerySegment(
        world_id=wid,
        run_id=rid,
        up_to_tick=None,
        head_tick=current_visibility.head_tick,
        head_tokens=current_visibility.head_tokens,
        visibility_tokens=current_visibility.visibility_tokens,
    )

    parent_world_id = str(record.parent_world_id) if record.parent_world_id else None
    lineage = (
        await get_lineage(storage, wid, rid, effective) if parent_world_id is not None else None
    )
    if parent_world_id is not None:
        if not lineage:
            parent_record = await catalog.get_world(parent_world_id)
            intentionally_severed = parent_record is None
            owns_from_tick_zero = current.head_tick == 0 or (
                current.head_tick is None
                and current.visibility_tokens == ()
                and getattr(record, "tick_head", None) == 0
            )
            if not intentionally_severed and current.head_tick is not None:
                origin = await storage.pin_visibility(
                    effective,
                    wid,
                    run_id=rid,
                    max_tick=0,
                )
                owns_from_tick_zero = str(origin.run_id) == rid and origin.head_tick == 0
            if not intentionally_severed and not owns_from_tick_zero:
                raise RuntimeError(
                    f"world {wid} records parent {parent_world_id} but has no "
                    "persisted lineage or child-owned tick-zero origin"
                )
        elif str(lineage[-1][0]) != parent_world_id:
            raise RuntimeError(
                f"world {wid} records parent {parent_world_id} but its direct lineage "
                f"segment names {lineage[-1][0]}"
            )
    pinned_lineage: list[PinnedQuerySegment] = []
    previous_up_to = -1
    logical_lineage_head: PinnedQuerySegment | None = None
    for ancestor_world, ancestor_run, up_to_tick in lineage or ():
        cap = int(up_to_tick)
        if cap < previous_up_to:
            raise RuntimeError(
                "fork lineage tick caps must be monotonic: "
                f"{ancestor_world}/{ancestor_run} ends at {cap} after {previous_up_to}"
            )
        widens_visible_interval = cap > previous_up_to
        visibility = await storage.pin_visibility(
            effective,
            str(ancestor_world),
            run_id=str(ancestor_run),
            max_tick=cap,
        )
        legacy_segment = visibility.head_tick is None and visibility.visibility_tokens is None
        if widens_visible_interval and visibility.head_tick != cap and not legacy_segment:
            raise RuntimeError(
                "fork lineage visibility is incomplete: "
                f"{ancestor_world}/{ancestor_run} publishes through "
                f"{visibility.head_tick}, expected {up_to_tick}"
            )
        segment = PinnedQuerySegment(
            world_id=str(ancestor_world),
            run_id=str(ancestor_run),
            up_to_tick=cap,
            head_tick=visibility.head_tick,
            head_tokens=visibility.head_tokens,
            visibility_tokens=visibility.visibility_tokens,
        )
        pinned_lineage.append(segment)
        if widens_visible_interval:
            logical_lineage_head = segment
        previous_up_to = cap

    if current.head_tick is not None and current.head_tick <= previous_up_to:
        raise RuntimeError(
            f"world {wid} current run head {current.head_tick} overlaps inherited "
            f"lineage through tick {previous_up_to}"
        )
    logical_head_tick = current.head_tick
    logical_head_tokens = current.head_tokens
    if logical_head_tick is None and logical_lineage_head is not None:
        lineage_head = logical_lineage_head
        logical_head_tick = (
            lineage_head.head_tick
            if lineage_head.head_tick is not None
            else lineage_head.up_to_tick
        )
        logical_head_tokens = lineage_head.head_tokens
    return PinnedWorldQuerySnapshot(
        world_id=wid,
        run_id=rid,
        head_tick=logical_head_tick,
        head_tokens=logical_head_tokens,
        current=current,
        lineage=tuple(pinned_lineage),
    )


async def pin_query_snapshot_for_receipt(
    storage: iStorageService,
    receipt: CommittedTickReceipt,
    storage_config: StorageConfig | None = None,
) -> PinnedWorldQuerySnapshot:
    """Pin the sole manifest snapshot named by one committed receipt.

    A world/run/tick tuple does not identify an Activity-safe snapshot by
    itself. The selected current-run head must contain exactly the receipt's
    visibility token; a later head, a sibling token, or an ambiguous
    multi-token head fails closed.
    """

    if not isinstance(receipt, CommittedTickReceipt):
        raise TypeError("receipt must be a CommittedTickReceipt")
    if receipt.visibility_token is None or not receipt.visibility_token.strip():
        raise ValueError("receipt-pinned query requires a visibility token")
    snapshot = await pin_query_snapshot(
        storage,
        str(receipt.world_id),
        str(receipt.run_id),
        storage_config,
    )
    current = snapshot.current
    if current.head_tick != receipt.committed_tick:
        raise ValueError(
            "receipt does not name the current run's committed manifest head: "
            f"expected tick {receipt.committed_tick}, found {current.head_tick}"
        )
    if current.head_tokens != (receipt.visibility_token,):
        raise ValueError("receipt visibility token is not the sole token at its committed head")
    if (
        current.visibility_tokens is None
        or receipt.visibility_token not in current.visibility_tokens
    ):
        raise ValueError("receipt visibility token is absent from the pinned visibility set")
    return snapshot


async def query_archetype(
    storage: iStorageService,
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
    storage: iStorageService,
    components: list[type[Component]],
    world_id: str,
    run_id: str,
    storage_config: StorageConfig | None = None,
    *,
    ticks: list[int] | None = None,
    entity_ids: list[int] | None = None,
    lineage: list[tuple[str, str, int]] | None = None,
    visibility_tokens: list[str] | None = None,
    snapshot: PinnedWorldQuerySnapshot | None = None,
) -> DataFrame:
    """Read every signature containing ``components`` through pinned visibility."""
    if visibility_tokens is not None and lineage:
        raise ValueError("visibility_tokens cannot be combined with lineage")
    if snapshot is not None:
        if lineage is not None or visibility_tokens is not None:
            raise ValueError("snapshot cannot be combined with lineage or visibility_tokens")
        if (str(world_id), str(run_id)) != (snapshot.world_id, snapshot.run_id):
            raise ValueError("snapshot does not identify the requested world/run")
    effective, store, querier = await _querier_for(storage, storage_config)
    catalog_records = await _catalog_candidates(storage, effective, components)
    pinned_tokens = list(visibility_tokens) if visibility_tokens is not None else None
    snapshot_tokens = (
        {
            (segment.world_id, segment.run_id): segment.visibility_tokens
            for segment in (snapshot.current, *snapshot.lineage)
        }
        if snapshot is not None
        else {}
    )

    async def _read(
        segment_world: str,
        segment_run: str,
        segment_ticks: list[int] | None,
    ) -> DataFrame:
        if snapshot is not None:
            key = (str(segment_world), str(segment_run))
            if key not in snapshot_tokens:
                raise ValueError(f"snapshot has no pinned visibility for {key[0]}/{key[1]}")
            exact_tokens = snapshot_tokens[key]
            tokens = list(exact_tokens) if exact_tokens is not None else None
        elif visibility_tokens is not None:
            tokens = pinned_tokens
        else:
            tokens = await _visible_tokens(
                storage,
                effective,
                segment_world,
                segment_run,
                segment_ticks,
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
    effective_lineage = (
        [
            (segment.world_id, segment.run_id, int(segment.up_to_tick))
            for segment in snapshot.effective_lineage
            if segment.up_to_tick is not None
        ]
        if snapshot is not None
        else lineage
    )
    return await _union_lineage(result, effective_lineage, ticks, _read)


async def _signature_records(
    storage: iStorageService,
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
    storage: iStorageService,
    storage_config: StorageConfig,
    components: list[type[Component]],
) -> list[SignatureRecord]:
    requested = {component.__name__ for component in components}
    records = await _signature_records(storage, storage_config)
    return [record for record in records if requested.issubset(record.component_names)]


async def _visible_tokens(
    storage: iStorageService,
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
    storage: iStorageService,
    world_id: str,
    run_id: str,
    storage_config: StorageConfig | None = None,
) -> list[tuple[str, str, int]] | None:
    """Recover append-only fork ancestry, including for a closed world."""
    _effective, store, _querier = await _querier_for(storage, storage_config)
    return await load_lineage(store, world_id=str(world_id), run_id=str(run_id))


async def list_signatures(
    storage: iStorageService,
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
