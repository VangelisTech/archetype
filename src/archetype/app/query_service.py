# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Query Service

The read path. Time-travel queries, entity state, command history.
All external reads go through here.

Strategy: prefer live data for current tick, store for historical ticks.
This mirrors the ``prefer_live_reads`` pattern in ``AsyncWorld.step()``.
"""

from __future__ import annotations

from logging import getLogger
from typing import TYPE_CHECKING, Any

import daft
import pyarrow as pa
from daft import DataFrame, col
from uuid_utils import UUID

from archetype.app.models import Command, WorldSnapshot
from archetype.core.archetype import Archetype

if TYPE_CHECKING:
    from archetype.app.broker import CommandBroker
    from archetype.app.world_service import WorldService

logger = getLogger(__name__)

# Base columns stamped by the updater — excluded from component data output.
_BASE_COLS = frozenset({"world_id", "run_id", "entity_id", "tick", "is_active"})


def _df_to_rows(df: DataFrame) -> list[dict[str, Any]]:
    """Collect a Daft DataFrame to a list of plain dicts.

    Strips base columns and un-prefixes component field names so that
    ``{"position__x": 1.0}`` becomes ``{"position": {"x": 1.0}}``.
    """
    rows = df.collect().to_pylist()
    result: list[dict[str, Any]] = []
    for row in rows:
        components: dict[str, dict[str, Any]] = {}
        entity_id = row.get("entity_id")
        tick_val = row.get("tick")
        for key, value in row.items():
            if key in _BASE_COLS:
                continue
            if "__" in key:
                comp_name, field_name = key.split("__", 1)
                components.setdefault(comp_name, {})[field_name] = value
            else:
                components.setdefault("_extra", {})[key] = value
        result.append({"entity_id": entity_id, "tick": tick_val, "components": components})
    return result


def _resolve_component_types(world: Any, type_names: list[str]) -> list[type]:
    """Resolve component type name strings to actual Component classes.

    Searches the world's known archetype signatures for matching types
    (case-insensitive).
    """
    # Build a lookup of all known component types from entity signatures
    known: dict[str, type] = {}
    for sig in set(world._entity2sig.values()):
        for comp_type in sig:
            known[comp_type.__name__.lower()] = comp_type

    resolved = []
    for name in type_names:
        comp_cls = known.get(name.lower())
        if comp_cls is None:
            raise ValueError(f"Unknown component type: {name!r}")
        resolved.append(comp_cls)
    return resolved


class QueryService:
    """
    Read path facade. Time-travel queries, entity state, cross-world reads.

    For current-tick queries, reads from ``AsyncWorld._live`` (fast, in-memory).
    For historical ticks, queries the store via ``AsyncWorld.query_archetype()``.
    """

    def __init__(self, world_service: WorldService, broker: CommandBroker | None = None):
        self._world_service = world_service
        self._broker = broker

    def _get_async_world(self, world_id: UUID) -> Any:
        """Get and validate world as AsyncWorld."""
        from archetype.core.aio import AsyncWorld

        world = self._world_service.get_world(world_id)
        if not isinstance(world, AsyncWorld):
            raise TypeError(f"QueryService requires AsyncWorld, got {type(world).__name__}")
        return world

    def _is_current_tick(self, world: Any, tick: int | None) -> bool:
        """Check if the requested tick matches the current (live) state.

        After step(), ``world.tick`` is the *next* tick to process and
        ``_live`` holds rows stamped with ``world.tick - 1``.  A ``None``
        tick always means "current state".
        """
        if tick is None:
            return True
        current_data_tick = max(world.tick - 1, 0)
        return tick == current_data_tick

    async def get_world_state(
        self,
        world_id: UUID,
        tick: int | None = None,
    ) -> WorldSnapshot:
        """Get the current (or historical) world state snapshot."""
        world = self._get_async_world(world_id)

        if self._is_current_tick(world, tick):
            # Build from live in-memory state
            entities: dict[int, list[str]] = {}
            for eid, sig in world._entity2sig.items():
                entities[eid] = [t.__name__ for t in sig]

            archetype_counts: dict[str, int] = {}
            for sig, df in world._live.items():
                name = Archetype.get_name(sig)
                archetype_counts[name] = df.where(col("is_active")).count_rows()

            return WorldSnapshot(
                world_id=world_id,
                tick=max(world.tick - 1, 0) if tick is None else tick,
                entities=entities,
                archetype_counts=archetype_counts,
            )
        else:
            # Historical tick — query the store
            entities = {}
            archetype_counts = {}
            for sig in set(world._entity2sig.values()):
                name = Archetype.get_name(sig)
                try:
                    df = await world.query_archetype(sig, ticks=[tick])
                    collected = df.select("entity_id").collect()
                    archetype_counts[name] = collected.count_rows()
                    for row in collected.to_pylist():
                        entities[row["entity_id"]] = [t.__name__ for t in sig]
                except Exception:
                    logger.warning(
                        "Failed to query archetype %s at tick %d", name, tick, exc_info=True
                    )
                    archetype_counts[name] = 0

            return WorldSnapshot(
                world_id=world_id,
                tick=tick,
                entities=entities,
                archetype_counts=archetype_counts,
            )

    async def get_entity(
        self,
        world_id: UUID,
        entity_id: int,
        tick: int | None = None,
    ) -> dict:
        """Get entity state, optionally at a specific tick."""
        world = self._get_async_world(world_id)

        sig = world._entity2sig.get(entity_id)
        if sig is None:
            raise KeyError(f"Entity {entity_id} not found in world {world_id}")

        effective_tick = max(world.tick - 1, 0) if tick is None else tick

        if self._is_current_tick(world, tick):
            # Read from live data
            live_df = world._live.get(sig)
            if live_df is None:
                raise KeyError(f"No live data for entity {entity_id}")
            df = live_df.where(col("entity_id") == entity_id)
        else:
            # Historical — query the store
            df = await world.query_archetype(sig, ticks=[tick], entity_ids=[entity_id])

        rows = _df_to_rows(df)
        if not rows:
            raise KeyError(f"Entity {entity_id} not found at tick {effective_tick}")

        row = rows[0]
        return {
            "world_id": str(world_id),
            "entity_id": entity_id,
            "tick": effective_tick,
            "components": row["components"],
        }

    async def get_components(
        self,
        world_id: UUID,
        component_types: list[str],
        entity_ids: list[int] | None = None,
        tick: int | None = None,
    ) -> dict:
        """Query specific component types across entities."""
        world = self._get_async_world(world_id)
        effective_tick = max(world.tick - 1, 0) if tick is None else tick

        if not component_types:
            return {
                "world_id": str(world_id),
                "tick": effective_tick,
                "component_types": [],
                "rows": [],
            }

        resolved = _resolve_component_types(world, component_types)

        if self._is_current_tick(world, tick):
            df = await world.get_components(resolved, entity_ids)
        else:
            # Historical — query each matching signature from the store
            required = set(resolved)
            matching_sigs = [
                sig for sig in set(world._entity2sig.values()) if required.issubset(set(sig))
            ]

            temp_sig = tuple(sorted(resolved, key=lambda t: t.__name__))
            schema = Archetype.get_archetype_schema(temp_sig)
            df = daft.from_arrow(pa.Table.from_batches([], schema=schema))

            for sig in matching_sigs:
                sig_df = await world.query_archetype(
                    sig, ticks=[tick], entity_ids=entity_ids, components=resolved
                )
                proj_cols = schema.names
                df = df.concat(sig_df.select(*proj_cols))

        rows = _df_to_rows(df)
        return {
            "world_id": str(world_id),
            "tick": effective_tick,
            "component_types": component_types,
            "rows": rows,
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
