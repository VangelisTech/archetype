# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Mutation Service

Owns all world mutations: entities, components, and processors.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from uuid_utils import UUID

from archetype.core.aio import AsyncWorld
from archetype.core.component import Component

if TYPE_CHECKING:
    from archetype.app.world_service import WorldService


class MutationService:
    """Mutates world contents: entities, components, processors.

    Does not drive execution (that's SimulationService).
    Does not manage world lifecycle (that's WorldService).
    """

    def __init__(self, world_service: WorldService) -> None:
        self._world_service = world_service

    def _get_async_world(self, world_id: str | UUID) -> AsyncWorld:
        world = self._world_service.get_world(UUID(str(world_id)))
        if not isinstance(world, AsyncWorld):
            raise TypeError(f"MutationService requires AsyncWorld, got {type(world).__name__}")
        return world

    # ── Entities ──────────────────────────────────────────────────────────

    async def create_entity(self, world_id: str | UUID, components: list[Component]) -> int:
        """Spawn a new entity with the given components.

        Returns the entity ID immediately. The entity is registered in
        the world's entity2sig and spawn_cache, but does not materialize
        in the store until the next step() processes the cache (tick+1).
        """
        world = self._get_async_world(world_id)
        return await world.create_entity(components)

    async def remove_entity(self, world_id: str | UUID, entity_id: int) -> None:
        """Despawn an entity."""
        world = self._get_async_world(world_id)
        await world.remove_entity(entity_id)

    # ── Components ────────────────────────────────────────────────────────

    async def update_entity(
        self, world_id: str | UUID, entity_id: int, components: list[Component]
    ) -> None:
        """Overlay component values on an existing entity (same archetype)."""
        world = self._get_async_world(world_id)
        await world.update_entity(entity_id, components)

    async def add_components(
        self, world_id: str | UUID, entity_id: int, components: list[Component]
    ) -> None:
        """Add components to an existing entity (extends archetype)."""
        world = self._get_async_world(world_id)
        await world.add_components(entity_id, components)

    async def remove_components(
        self, world_id: str | UUID, entity_id: int, component_types: list[type[Component]]
    ) -> None:
        """Remove components from an existing entity."""
        world = self._get_async_world(world_id)
        await world.remove_components(entity_id, component_types)

    # ── Processors ────────────────────────────────────────────────────────

    async def add_processor(self, world_id: str | UUID, processor) -> None:
        """Add a processor to a world's system."""
        world = self._get_async_world(world_id)
        await world.add_processor(processor)

    async def remove_processor(self, world_id: str | UUID, proc_type) -> None:
        """Remove a processor from a world's system."""
        world = self._get_async_world(world_id)
        await world.remove_processor(proc_type)
