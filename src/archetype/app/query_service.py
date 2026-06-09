# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Query Service

Direct read path to the store. Queries any world, any run, any tick —
including worlds that no longer exist in memory.
"""

from __future__ import annotations

from daft import DataFrame, col

from archetype.app.models import Command, CommandType
from archetype.app.storage_service import StorageService
from archetype.core.aio import AsyncQueryManager
from archetype.core.component import Component
from archetype.core.config import StorageConfig
from archetype.core.interfaces import ArchetypeSignature
from archetype.core.lineage import load_lineage


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
        store = await self._storage_service.get_or_create_store(storage_config or StorageConfig())
        querier = AsyncQueryManager(store=store)
        result = await querier.query_archetype(
            sig=sig,
            world_id=world_id,
            run_id=run_id,
            ticks=ticks,
            entity_ids=entity_ids,
            components=components,
        )

        async def _segment(seg_world: str, seg_run: str, seg_ticks: list[int] | None):
            return await querier.query_archetype(
                sig=sig,
                world_id=seg_world,
                run_id=seg_run,
                ticks=seg_ticks,
                entity_ids=entity_ids,
                components=components,
            )

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
        store = await self._storage_service.get_or_create_store(storage_config or StorageConfig())
        querier = AsyncQueryManager(store=store)
        result = await querier.query_components(
            components=components,
            world_id=world_id,
            run_id=run_id,
            ticks=ticks,
            entity_ids=entity_ids,
        )

        async def _segment(seg_world: str, seg_run: str, seg_ticks: list[int] | None):
            return await querier.query_components(
                components=components,
                world_id=seg_world,
                run_id=seg_run,
                ticks=seg_ticks,
                entity_ids=entity_ids,
            )

        return await self._union_lineage(result, lineage, ticks, _segment)

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
                df = df.where((col("tick") > previous_up_to) & (col("tick") <= up_to_tick))
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
