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

from archetype.app.models import Command, CommandType
from archetype.app.storage_service import StorageService
from archetype.core.aio import AsyncQueryManager
from archetype.core.archetype import Archetype
from archetype.core.component import Component
from archetype.core.config import StorageConfig
from archetype.core.interfaces import ArchetypeSignature
from archetype.core.lineage import load_lineage

logger = logging.getLogger(__name__)


class QueryService:
    """Direct read path to storage.

    Depends only on StorageService. Resolves a store per query via
    get_or_create_store, so any historical storage location is reachable.
    """

    def __init__(self, storage_service: StorageService, audit=None) -> None:
        self._storage_service = storage_service
        self._audit = audit

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
        effective_config = storage_config or StorageConfig()
        store = await self._storage_service.get_or_create_store(effective_config)
        querier = AsyncQueryManager(store=store)

        async def _segment(seg_world: str, seg_run: str, seg_ticks: list[int] | None):
            tokens = await self._visible_tokens(effective_config, seg_world, seg_run, seg_ticks)
            return await querier.query_archetype(
                sig=sig,
                world_id=seg_world,
                run_id=seg_run,
                ticks=seg_ticks,
                entity_ids=entity_ids,
                components=components,
                commit_tokens=tokens,
            )

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
    ) -> DataFrame:
        """Query all entities that contain the requested component types.

        Subset matching: finds all archetype signatures that contain the
        requested types, queries each, projects to requested columns, unions.

        When `lineage` is provided (a fork's ancestor segments), pre-fork
        ticks are read from the owning ancestor's run and unioned in.
        """
        effective_config = storage_config or StorageConfig()
        store = await self._storage_service.get_or_create_store(effective_config, None)
        querier = AsyncQueryManager(store=store)
        catalog_records = await self._catalog_candidates(effective_config, components)

        async def _read(seg_world: str, seg_run: str, seg_ticks: list[int] | None):
            tokens = await self._visible_tokens(effective_config, seg_world, seg_run, seg_ticks)
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

    async def _catalog_candidates(
        self, storage_config: StorageConfig, components: list[type[Component]]
    ):
        """Durable signature records whose component sets cover the request.

        The control catalog (issue #272) is the durable complement to the
        store's process-local signature registry: a fresh process discovers
        every table ever committed against this storage identity.
        """
        requested = {c.__name__ for c in components}
        try:
            catalog = self._storage_service.get_control_catalog(storage_config)
            records = await catalog.list_signatures()
        except Exception:
            logger.exception("control catalog unavailable for %s", storage_config.uri)
            return []
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
            return sorted(set(visible.values()))
        return sorted({visible[t] for t in ticks if t in visible})

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
        """Live subset query unioned with catalog-discovered tables.

        The live path is unchanged. Catalog tables not already covered by a
        live signature are read through the open-never-create store seam and
        projected from their durable schema — no Python classes required
        beyond the ones the caller asked for. A fingerprint mismatch between
        the catalog descriptor and the physical table fails closed.
        """
        import daft
        import pyarrow as pa

        from archetype.app._catalog import CatalogSchemaMismatchError, schema_fingerprint

        output_sig = tuple(sorted(components, key=lambda t: t.__name__))
        proj_cols = Archetype.projection_columns(list(output_sig))
        full_schema = Archetype.get_archetype_schema(output_sig)
        schema = pa.schema([full_schema.field(name) for name in proj_cols])

        live_sigs = await querier.list_signatures()
        live_tables = {Archetype.get_name(sig) for sig in live_sigs}
        extra_records = [r for r in catalog_records if r.table_id not in live_tables]

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
            # The live registry has signatures but none satisfy the request.
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
        store = await self._storage_service.get_or_create_store(storage_config or StorageConfig())
        return await load_lineage(store, world_id=world_id, run_id=run_id)

    async def list_signatures(
        self,
        storage_config: StorageConfig | None = None,
    ) -> list[ArchetypeSignature]:
        """List all archetype signatures in a store."""
        store = await self._storage_service.get_or_create_store(storage_config or StorageConfig())
        querier = AsyncQueryManager(store=store)
        return await querier.list_signatures()

    async def get_command_history(
        self,
        world_id: str,
        limit: int = 100,
    ) -> list[Command]:
        """Compatibility read for pre-gate callers; queued command history only."""
        if self._audit is None:
            return []

        rows = [
            row
            for row in self._audit._rows
            if str(row.world_id) == str(world_id) and row.status == "queued"
        ][-limit:]
        result: list[Command] = []
        for row in rows:
            try:
                command_type = CommandType(row.command_type)
            except ValueError:
                command_type = CommandType.CUSTOM
            result.append(Command(id=row.command_id, type=command_type))
        return result
