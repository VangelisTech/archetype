# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Query Service

Direct read path to the store. Queries any world, any run, any tick —
including worlds that no longer exist in memory.
"""

from __future__ import annotations

from daft import DataFrame

from archetype.app.models import Command, CommandType
from archetype.app.storage_service import StorageService
from archetype.core.aio import AsyncQueryManager
from archetype.core.component import Component
from archetype.core.config import StorageConfig
from archetype.core.interfaces import ArchetypeSignature


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
    ) -> DataFrame:
        """Query an archetype table by signature, world, and run.

        Reads directly from the store. Works for any historical world/run,
        not just worlds that are currently live. The store is resolved via
        get_or_create_store, so repeated calls with the same config reuse
        the cached instance.
        """
        store = await self._storage_service.get_or_create_store(storage_config or StorageConfig())
        querier = AsyncQueryManager(store=store)
        return await querier.query_archetype(
            sig=sig,
            world_id=world_id,
            run_id=run_id,
            ticks=ticks,
            entity_ids=entity_ids,
            components=components,
        )

    async def query_components(
        self,
        components: list[type[Component]],
        world_id: str,
        run_id: str,
        storage_config: StorageConfig | None = None,
        *,
        ticks: list[int] | None = None,
        entity_ids: list[int] | None = None,
    ) -> DataFrame:
        """Query all entities that contain the requested component types.

        Subset matching: finds all archetype signatures that contain the
        requested types, queries each, projects to requested columns, unions.
        """
        store = await self._storage_service.get_or_create_store(storage_config or StorageConfig())
        querier = AsyncQueryManager(store=store)
        return await querier.query_components(
            components=components,
            world_id=world_id,
            run_id=run_id,
            ticks=ticks,
            entity_ids=entity_ids,
        )

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
