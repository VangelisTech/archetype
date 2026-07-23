# Copyright 2025 Vangelis Technologies Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Query Service

Direct read path to the store. Queries any world, any run, any tick —
including worlds that no longer exist in memory.
"""

from __future__ import annotations

import logging

from daft import DataFrame, col, lit

from archetype.app.storage.interfaces import iStorageService
from archetype.core.aio import AsyncQueryManager
from archetype.core.archetype import Archetype
from archetype.core.component import Component
from archetype.core.config import StorageConfig
from archetype.core.interfaces import ArchetypeSignature
from archetype.core.lineage import load_lineage
from archetype.storage.catalog import (
    CatalogSchemaMismatchError,
    SignatureRecord,
    schema_fingerprint,
)
from archetype.storage.signatures import match_signature_records

logger = logging.getLogger(__name__)


class QueryService:
    """Direct read path to storage.

    Depends only on StorageService. Resolves a store per query via
    get_or_create_store, so any historical storage location is reachable.
    """

    def __init__(
        self,
        storage_service: iStorageService,
    ) -> None:
        self._storage_service = storage_service

    async def _querier_for(self, storage_config: StorageConfig | None):
        """Resolve (config, store, querier) for one call.

        The store is per-call state — every method takes a storage_config so
        any historical location is reachable — and the pool inside
        StorageService makes repeated resolution a dict lookup. The querier
        is a stateless facade; constructing one per call is deliberate.
        """
        effective = storage_config or StorageConfig()
        store = await self._storage_service.get_or_create_store(effective, None)
        return effective, store, AsyncQueryManager(store=store)

    async def query_archetype(
        self,
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
        """Query an archetype table by signature, world, and run.

        Reads directly from the store. Works for any historical world/run,
        not just worlds that are currently live. The store is resolved via
        get_or_create_store, so repeated calls with the same config reuse
        the cached instance.

        When `lineage` is provided (a fork's ancestor segments), pre-fork
        ticks are read from the owning ancestor's run and unioned in.
        """
        effective_config, store, querier = await self._querier_for(storage_config)
        canonical_sig = tuple(sorted(sig, key=lambda component: component.__name__))
        table_id = Archetype.get_name(canonical_sig)
        catalog_record = next(
            (
                record
                for record in await self._signature_records(effective_config)
                if record.table_id == table_id
            ),
            None,
        )

        async def _segment(seg_world: str, seg_run: str, seg_ticks: list[int] | None):
            tokens = await self._visible_tokens(effective_config, seg_world, seg_run, seg_ticks)
            if catalog_record is None:
                # Preserve the core unknown-signature diagnostic for stores
                # without a durable descriptor (including legacy stores).
                return await querier.query_archetype(
                    sig=canonical_sig,
                    world_id=seg_world,
                    run_id=seg_run,
                    ticks=seg_ticks,
                    entity_ids=entity_ids,
                    components=components,
                    commit_tokens=tokens,
                )

            physical = await store.get_existing_table_schema(catalog_record.table_id)
            if catalog_record.fingerprint != schema_fingerprint(physical):
                raise CatalogSchemaMismatchError(
                    f"catalog descriptor for table {catalog_record.table_id} does not match "
                    "the physical schema; refusing to read (fail closed)"
                )
            frame = await store.get_existing_table_df(
                catalog_record.table_id,
                seg_world,
                seg_run,
                ticks=seg_ticks,
                entity_ids=entity_ids,
                active_only=True,
            )
            if tokens is not None and "commit_token" in frame.column_names:
                visible = frame["commit_token"].is_in(tokens) if tokens else lit(False)
                frame = frame.where(visible)
            if components:
                frame = frame.select(*Archetype.projection_columns(components))
            return frame

        result = await _segment(world_id, run_id, ticks)
        return await self._union_lineage(result, lineage, ticks, _segment)

    async def query_components(
        self,
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
        """Query all entities that contain the requested component types.

        Subset matching: finds all archetype signatures that contain the
        requested types, queries each, projects to requested columns, unions.

        When `lineage` is provided (a fork's ancestor segments), pre-fork
        ticks are read from the owning ancestor's run and unioned in.

        ``visibility_tokens`` is the fail-closed immutable-read seam used by
        persisted evaluations. When supplied, it replaces the moving catalog
        lookup with that exact allowlist. A single allowlist cannot describe
        multiple lineage segments, so combining the two is rejected.
        """
        if visibility_tokens is not None and lineage:
            raise ValueError("visibility_tokens cannot be combined with lineage")
        effective_config, store, querier = await self._querier_for(storage_config)
        catalog_records = await self._catalog_candidates(effective_config, components)
        pinned_tokens = list(visibility_tokens) if visibility_tokens is not None else None

        async def _read(seg_world: str, seg_run: str, seg_ticks: list[int] | None):
            tokens = (
                pinned_tokens
                if visibility_tokens is not None
                else await self._visible_tokens(effective_config, seg_world, seg_run, seg_ticks)
            )
            return await self._components_frame(
                querier,
                store,
                catalog_records,
                components,
                seg_world,
                seg_run,
                ticks=seg_ticks,
                entity_ids=entity_ids,
                commit_tokens=tokens,
            )

        result = await _read(world_id, run_id, ticks)
        return await self._union_lineage(result, lineage, ticks, _read)

    async def _signature_records(self, storage_config: StorageConfig) -> list[SignatureRecord]:
        """Return durable discovery records, degrading safely when unavailable."""
        try:
            catalog = self._storage_service.get_control_catalog(storage_config)
            return await catalog.list_signatures()
        except Exception:
            logger.exception("control catalog unavailable for durable signature discovery")
            return []

    async def _catalog_candidates(
        self, storage_config: StorageConfig, components: list[type[Component]]
    ):
        """Durable signature records whose component sets cover the request.

        The control catalog (issue #272) is the durable complement to the
        store's process-local signature registry: a fresh process discovers
        every table ever committed against this storage identity.
        """
        requested = {c.__name__ for c in components}
        records = await self._signature_records(storage_config)
        return [r for r in records if requested.issubset(set(r.component_names))]

    async def _visible_tokens(
        self,
        storage_config: StorageConfig,
        world_id: str,
        run_id: str,
        ticks: list[int] | None,
    ) -> list[str] | None:
        """Reader-side commit-token allowlist for one (world, run) segment.

        None = unfiltered (no manifests recorded: uncoordinated or pre-#273
        history — implicit epoch-0).

        Catalog errors PROPAGATE — the opposite posture from
        _catalog_candidates, deliberately. Degraded discovery returns less
        data; a degraded visibility check would return MORE (rows from
        crashed or stale commit attempts that no manifest authorized), so a
        corrupt or unreadable catalog fails the read closed. A missing
        catalog is not an error: connecting creates an empty one, which
        reports no manifests and no fence — the legacy-unfiltered case.
        """
        catalog = self._storage_service.get_control_catalog(storage_config)
        visible = await catalog.visible_tokens(str(world_id), str(run_id), ticks)
        if visible is None:
            return None
        if ticks is None:
            return sorted({token for tokens in visible.values() for token in tokens})
        return sorted({token for t in ticks if t in visible for token in visible[t]})

    async def _components_frame(
        self,
        querier: AsyncQueryManager,
        store,
        catalog_records,
        components: list[type[Component]],
        world_id: str,
        run_id: str,
        *,
        ticks: list[int] | None,
        entity_ids: list[int] | None,
        commit_tokens: list[str] | None = None,
    ) -> DataFrame:
        """Process-local subset query unioned with catalog-discovered tables.

        Catalog tables not already covered by a successfully written signature
        are read through the open-never-create store seam and
        projected from their durable schema — no Python classes required
        beyond the ones the caller asked for. A fingerprint mismatch between
        the catalog descriptor and the physical table fails closed.
        """
        import daft
        import pyarrow as pa

        from archetype.storage.catalog import CatalogSchemaMismatchError, schema_fingerprint

        output_sig = tuple(sorted(components, key=lambda t: t.__name__))
        proj_cols = Archetype.projection_columns(list(output_sig))
        full_schema = Archetype.get_archetype_schema(output_sig)
        schema = pa.schema([full_schema.field(name) for name in proj_cols])

        written_sigs = await querier.list_committed_signatures()
        written_tables = {Archetype.get_name(sig) for sig in written_sigs}
        extra_records = [r for r in catalog_records if r.table_id not in written_tables]

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
            # The process-local registry has signatures but none satisfy the request.
            # Durable tables may still: fall through to the catalog union,
            # re-raising only when the catalog cannot help either.
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
            df = await store.get_existing_table_df(
                record.table_id,
                world_id,
                run_id,
                ticks=ticks,
                entity_ids=entity_ids,
                active_only=True,
            )
            if commit_tokens is not None and "commit_token" in df.column_names:
                # (Daft stubs Expression methods as bool; these are Expressions.)
                visible = df["commit_token"].is_in(commit_tokens) if commit_tokens else lit(False)
                df = df.where(visible)
            result = result.concat(df.select(*proj_cols))

        return result

    @staticmethod
    async def _union_lineage(result, lineage, ticks, segment_query) -> DataFrame:
        """Union ancestor-segment rows into a query result.

        Each segment owns ticks in (previous_up_to, up_to]. Rows the ancestor
        wrote after the fork point are excluded so a parent that kept running
        never leaks post-fork state into the fork's history.
        """
        previous_up_to = -1
        for ancestor_world, ancestor_run, up_to_tick in lineage or []:
            if ticks is not None:
                segment_ticks = [t for t in ticks if previous_up_to < t <= up_to_tick]
                if not segment_ticks:
                    previous_up_to = up_to_tick
                    continue
            else:
                segment_ticks = None
            df = await segment_query(str(ancestor_world), str(ancestor_run), segment_ticks)
            if ticks is None:
                # Daft stubs type Expression.__gt__/__le__ as bool; these are Expressions.
                lower = col("tick") > previous_up_to  # ty: ignore[unsupported-operator]
                upper = col("tick") <= up_to_tick
                df = df.where(lower & upper)
            result = result.concat(df)
            previous_up_to = up_to_tick
        return result

    async def get_lineage(
        self,
        world_id: str,
        run_id: str,
        storage_config: StorageConfig | None = None,
    ) -> list[tuple[str, str, int]] | None:
        """Recover a world's persisted fork ancestry from the store.

        Lineage rows are append-only, so this works for destroyed worlds.
        Returns None for root worlds (nothing was recorded at fork time).
        """
        _config, store, _querier = await self._querier_for(storage_config)
        return await load_lineage(store, world_id=world_id, run_id=run_id)

    async def list_signatures(
        self,
        storage_config: StorageConfig | None = None,
    ) -> list[ArchetypeSignature]:
        """List process-local and durably cataloged archetype signatures.

        A store's Python signature registry is only a cache. A fresh process
        therefore matches control-catalog records against imported Component
        classes by both schema fingerprint and durable table identity. Durable
        matches fill gaps without replacing exact process-local class objects.
        Unrelated historical records that can no longer be resolved are
        skipped with an observable warning.
        """
        effective_config, _store, querier = await self._querier_for(storage_config)
        signatures = {
            Archetype.get_name(signature): signature
            for signature in await querier.list_signatures()
        }
        records = await self._signature_records(effective_config)
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
