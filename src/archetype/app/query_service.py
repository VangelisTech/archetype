# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Query Service

The read path. Time-travel queries, entity state, command history.
All external reads go through here.

Strategy:
  - Current tick (tick=None or tick==world.tick): read from live in-memory state
  - Historical tick: query the store via AsyncWorld.querier
"""

from __future__ import annotations

import logging
from collections import Counter
from typing import TYPE_CHECKING, Any

from daft import col
from uuid_utils import UUID

from archetype.app.models import Command, WorldSnapshot
from archetype.core.aio.async_world import AsyncWorld
from archetype.core.component import Component

if TYPE_CHECKING:
    from archetype.app.broker import CommandBroker
    from archetype.app.world_service import WorldService

logger = logging.getLogger(__name__)


class QueryService:
    """
    Read path facade. Time-travel queries, entity state, cross-world reads.
    """

    def __init__(self, world_service: WorldService, broker: CommandBroker | None = None):
        self._world_service = world_service
        self._broker = broker

    def _get_async_world(self, world_id: UUID) -> AsyncWorld:
        """Get the world, ensuring it's an AsyncWorld for query access."""
        world = self._world_service.get_world(world_id)
        if not isinstance(world, AsyncWorld):
            raise TypeError(f"World {world_id} is not an AsyncWorld; queries require AsyncWorld.")
        return world

    async def get_world_state(
        self,
        world_id: UUID,
        tick: int | None = None,
    ) -> WorldSnapshot:
        """Get the current (or historical) world state snapshot.

        Populates entities and archetype_counts from live state (current tick)
        or from the store (historical tick).
        """
        world = self._get_async_world(world_id)
        effective_tick = tick if tick is not None else world.tick

        if tick is None or tick == world.tick:
            # Current tick: read from live in-memory state
            return self._snapshot_from_live(world, effective_tick)

        # Historical tick: query the store
        return await self._snapshot_from_store(world, effective_tick)

    def _snapshot_from_live(self, world: AsyncWorld, tick: int) -> WorldSnapshot:
        """Build a WorldSnapshot from the live in-memory state."""
        entities: dict[int, list[str]] = {}
        archetype_counts: Counter[str] = Counter()

        for entity_id, sig in world._entity2sig.items():
            component_names = [c.__name__ for c in sig]
            entities[entity_id] = component_names
            archetype_key = ",".join(sorted(component_names))
            archetype_counts[archetype_key] += 1

        return WorldSnapshot(
            world_id=world.world_id,
            tick=tick,
            entities=entities,
            archetype_counts=dict(archetype_counts),
        )

    async def _snapshot_from_store(self, world: AsyncWorld, tick: int) -> WorldSnapshot:
        """Build a WorldSnapshot by querying the store at a historical tick."""
        entities: dict[int, list[str]] = {}
        archetype_counts: Counter[str] = Counter()

        for sig in world._live.keys():
            try:
                df = await world.querier.query_archetype(
                    sig=sig,
                    world_id=str(world.world_id),
                    run_id=world.run_id,
                    ticks=[tick],
                )
                rows = df.select("entity_id").collect().to_pylist()
                component_names = [c.__name__ for c in sig]
                archetype_key = ",".join(sorted(component_names))

                for row in rows:
                    eid = row["entity_id"]
                    entities[eid] = component_names
                    archetype_counts[archetype_key] += 1
            except Exception:
                logger.debug("Failed to query archetype %s at tick %d", sig, tick, exc_info=True)

        return WorldSnapshot(
            world_id=world.world_id,
            tick=tick,
            entities=entities,
            archetype_counts=dict(archetype_counts),
        )

    async def get_entity(
        self,
        world_id: UUID,
        entity_id: int,
        tick: int | None = None,
    ) -> dict[str, Any]:
        """Get entity state, optionally at a specific tick.

        Returns a dict with world_id, entity_id, tick, components (dict of
        component_name -> field values), and component_types (list of names).
        """
        world = self._get_async_world(world_id)
        effective_tick = tick if tick is not None else world.tick

        if tick is None or tick == world.tick:
            return await self._entity_from_live(world, entity_id, effective_tick)

        return await self._entity_from_store(world, entity_id, effective_tick)

    async def _entity_from_live(
        self, world: AsyncWorld, entity_id: int, tick: int
    ) -> dict[str, Any]:
        """Read a single entity's component data from live state."""
        sig = world._entity2sig.get(entity_id)
        if sig is None:
            raise KeyError(f"Entity {entity_id} not found in world {world.world_id}")

        component_names = [c.__name__ for c in sig]
        components_data = await self._extract_entity_components(world, sig, entity_id, tick=None)

        return {
            "world_id": str(world.world_id),
            "entity_id": entity_id,
            "tick": tick,
            "component_types": component_names,
            "components": components_data,
        }

    async def _entity_from_store(
        self, world: AsyncWorld, entity_id: int, tick: int
    ) -> dict[str, Any]:
        """Read a single entity's component data from the store at a historical tick."""
        # Try all known signatures to find this entity
        for sig in world._live.keys():
            try:
                df = await world.querier.query_archetype(
                    sig=sig,
                    world_id=str(world.world_id),
                    run_id=world.run_id,
                    ticks=[tick],
                    entity_ids=[entity_id],
                )
                rows = df.collect().to_pylist()
                if rows:
                    row = rows[0]
                    component_names = [c.__name__ for c in sig]
                    components_data = self._row_to_components(row, sig)
                    return {
                        "world_id": str(world.world_id),
                        "entity_id": entity_id,
                        "tick": tick,
                        "component_types": component_names,
                        "components": components_data,
                    }
            except Exception:
                logger.debug(
                    "Failed to query entity %d in archetype %s at tick %d",
                    entity_id,
                    sig,
                    tick,
                    exc_info=True,
                )

        raise KeyError(f"Entity {entity_id} not found at tick {tick} in world {world.world_id}")

    async def _extract_entity_components(
        self,
        world: AsyncWorld,
        sig: tuple,
        entity_id: int,
        tick: int | None,
    ) -> dict[str, dict[str, Any]]:
        """Extract component field values for a single entity from live state."""
        live_df = world._live.get(sig)
        if live_df is None:
            return {}

        filtered = live_df.where(col("entity_id") == entity_id)
        rows = filtered.collect().to_pylist()
        if not rows:
            return {}

        return self._row_to_components(rows[0], sig)

    @staticmethod
    def _row_to_components(row: dict[str, Any], sig: tuple) -> dict[str, dict[str, Any]]:
        """Convert a raw DataFrame row to a component-name -> fields dict."""
        components_data: dict[str, dict[str, Any]] = {}
        for comp_type in sig:
            prefix = comp_type.__name__.lower() + "__"
            fields = {}
            for key, val in row.items():
                if key.startswith(prefix):
                    field_name = key[len(prefix) :]
                    fields[field_name] = val
            if fields:
                components_data[comp_type.__name__] = fields
        return components_data

    async def get_components(
        self,
        world_id: UUID,
        component_types: list[str],
        entity_ids: list[int] | None = None,
    ) -> dict[str, Any]:
        """Query specific component types across entities.

        Resolves component type names to classes and delegates to
        AsyncWorld.get_components().
        """
        world = self._get_async_world(world_id)

        if not component_types:
            return {
                "world_id": str(world_id),
                "component_types": [],
                "entities": [],
            }

        # Resolve string names to Component subclasses
        resolved: list[type[Component]] = []
        for name in component_types:
            try:
                resolved.append(Component.get_type_by_name(name))
            except ValueError:
                raise KeyError(f"Unknown component type: {name}") from None

        df = await world.get_components(resolved, entity_ids=entity_ids)
        rows = df.collect().to_pylist()

        return {
            "world_id": str(world_id),
            "component_types": component_types,
            "entities": rows,
        }

    async def get_command_history(
        self,
        world_id: UUID,
        limit: int = 100,
    ) -> list[Command]:
        """Get command history for a world."""
        if self._broker:
            return await self._broker.get_history(str(world_id), limit)
        return []
