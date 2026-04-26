# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Query Service

Direct read path to the store. Queries any world, any run, any tick —
including worlds that no longer exist in memory.
"""

from __future__ import annotations

from daft import DataFrame

from archetype.app.storage_service import StorageService
from archetype.core.aio import AsyncQueryManager
from archetype.core.component import Component
from archetype.core.config import StorageConfig
from archetype.core.interfaces import ArchetypeSignature


class QueryService:
    """Direct read path to storage.

    Depends only on StorageService. Internally instantiates its own
    querier per store. Does not depend on WorldService — queries are
    against the store, not against live worlds.
    """

    def __init__(self, storage_service: StorageService) -> None:
        self._storage_service = storage_service

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
        not just worlds that are currently live.
        """
        store = await self._storage_service.get_or_create_store(
            storage_config or StorageConfig()
        )
        querier = AsyncQueryManager(store=store)
        return await querier.query_archetype(
            sig=sig,
            world_id=world_id,
            run_id=run_id,
            ticks=ticks,
            entity_ids=entity_ids,
            components=components,
        )

    async def list_signatures(
        self,
        storage_config: StorageConfig | None = None,
    ) -> list[ArchetypeSignature]:
        """List all archetype signatures in a store."""
        store = await self._storage_service.get_or_create_store(
            storage_config or StorageConfig()
        )
        querier = AsyncQueryManager(store=store)
        return await querier.list_signatures()
