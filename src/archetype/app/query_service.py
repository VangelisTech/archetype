# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Query Service

The read path. Time-travel queries, entity state, command history.
All external reads go through here.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from daft import col
from uuid_utils import UUID

from archetype.app.models import Command, WorldSnapshot
from archetype.core.archetype import Archetype
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

    async def get_world_state(
        self,
        world_id: UUID,
        tick: int | None = None,
    ) -> WorldSnapshot:
        """Get the current (or historical) world state snapshot.

        Reads from the AsyncWorld's ``_live`` snapshots (the authoritative
        in-memory cache for the most recent tick).  When *tick* is ``None``
        or matches the current live tick, ``_live`` is used directly.
        Historical tick queries fall back to the durable store via
        ``query_archetype``.
        """
        from archetype.core.aio import AsyncWorld

        world = self._world_service.get_world(world_id)
        current_tick = getattr(world, "tick", 0)
        effective_tick = tick if tick is not None else current_tick

        entities: dict[int, list[str]] = {}
        archetype_counts: dict[str, int] = {}

        if not isinstance(world, AsyncWorld):
            return WorldSnapshot(
                world_id=world_id,
                tick=effective_tick,
                entities=entities,
                archetype_counts=archetype_counts,
            )

        # Determine whether we read from _live or from the store
        use_live = (
            tick is None or tick == current_tick or (current_tick > 0 and tick == current_tick - 1)
        )

        if use_live:
            for sig, df in world._live.items():
                active_df = df.where(col("is_active"))
                rows = active_df.select("entity_id").collect().to_pylist()
                archetype_name = Archetype.get_name(sig)
                archetype_counts[archetype_name] = len(rows)
                component_names = [cls.__name__ for cls in sig]
                for row in rows:
                    entities[row["entity_id"]] = component_names
        else:
            # Historical query via the store
            for sig in world.active_signatures:
                try:
                    df = await world.query_archetype(
                        sig=sig,
                        ticks=[effective_tick],
                    )
                    active_df = df.where(col("is_active"))
                    rows = active_df.select("entity_id").collect().to_pylist()
                    archetype_name = Archetype.get_name(sig)
                    archetype_counts[archetype_name] = len(rows)
                    component_names = [cls.__name__ for cls in sig]
                    for row in rows:
                        entities[row["entity_id"]] = component_names
                except Exception:
                    logger.debug("Failed to query archetype %s at tick %s", sig, effective_tick)

        return WorldSnapshot(
            world_id=world_id,
            tick=effective_tick,
            entities=entities,
            archetype_counts=archetype_counts,
        )

    async def get_entity(
        self,
        world_id: UUID,
        entity_id: int,
        tick: int | None = None,
    ) -> dict:
        """Get entity state, optionally at a specific tick.

        Returns a dict with world_id, entity_id, tick, and component data
        keyed by component class name.
        """
        from archetype.core.aio import AsyncWorld

        world = self._world_service.get_world(world_id)
        current_tick = getattr(world, "tick", 0)
        effective_tick = tick if tick is not None else current_tick

        result: dict = {
            "world_id": str(world_id),
            "entity_id": entity_id,
            "tick": effective_tick,
            "components": {},
        }

        if not isinstance(world, AsyncWorld):
            return result

        # Determine which archetype signature this entity belongs to
        sig = world._entity2sig.get(entity_id)
        if sig is None:
            return result

        # Read from _live for current state
        if sig in world._live:
            df = world._live[sig]
            df = df.where(col("entity_id") == entity_id).where(col("is_active"))
            rows = df.collect().to_pylist()
            if rows:
                row = rows[0]
                for comp_type in sig:
                    prefix = comp_type.get_prefix()
                    comp_data = {}
                    for key, value in row.items():
                        if key.startswith(prefix):
                            field_name = key[len(prefix) :]
                            comp_data[field_name] = value
                    result["components"][comp_type.__name__] = comp_data

        return result

    async def get_components(
        self,
        world_id: UUID,
        component_types: list[str],
        entity_ids: list[int] | None = None,
    ) -> dict:
        """Query specific component types across entities.

        Resolves *component_types* (class name strings) to actual
        ``Component`` subclasses and delegates to ``AsyncWorld.get_components``
        for the real data.  Returns a dict with metadata plus a ``data`` list
        of per-entity component rows.
        """
        from archetype.core.aio import AsyncWorld

        world = self._world_service.get_world(world_id)

        result: dict = {
            "world_id": str(world_id),
            "component_types": component_types,
            "entity_ids": entity_ids,
            "data": [],
        }

        if not isinstance(world, AsyncWorld):
            return result

        # Resolve string names to Component types
        resolved_types: list[type[Component]] = []
        for name in component_types:
            try:
                resolved_types.append(Component.get_type_by_name(name))
            except ValueError:
                logger.warning("Unknown component type: %s", name)

        if not resolved_types:
            return result

        df = await world.get_components(resolved_types, entity_ids=entity_ids)
        rows = df.collect().to_pylist()

        # Structure per-entity data grouped by component name
        for row in rows:
            entity_data: dict = {"entity_id": row.get("entity_id")}
            for comp_type in resolved_types:
                prefix = comp_type.get_prefix()
                comp_data = {}
                for key, value in row.items():
                    if key.startswith(prefix):
                        field_name = key[len(prefix) :]
                        comp_data[field_name] = value
                entity_data[comp_type.__name__] = comp_data
            result["data"].append(entity_data)

        return result

    async def get_command_history(
        self,
        world_id: UUID,
        limit: int = 100,
    ) -> list[Command]:
        """Get command history for a world."""
        if self._broker:
            return await self._broker.get_history(str(world_id), limit)
        return []
