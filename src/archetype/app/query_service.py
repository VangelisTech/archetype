# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Query Service

The read path. Time-travel queries, entity state, command history.
All external reads go through here.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from daft import col, sql_expr
from uuid_utils import UUID

from archetype.app.models import Command, WorldSnapshot
from archetype.core.component import Component

if TYPE_CHECKING:
    from archetype.app.broker import CommandBroker
    from archetype.app.world_service import WorldService


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
        """Get the current (or historical) world state snapshot."""
        world = self._world_service.get_world(world_id)
        return WorldSnapshot(
            world_id=world_id,
            tick=tick if tick is not None else getattr(world, "tick", 0),
            entities={},
            archetype_counts={},
        )

    async def get_entity(
        self,
        world_id: UUID,
        entity_id: int,
        tick: int | None = None,
    ) -> dict:
        """Get entity state, optionally at a specific tick."""
        world = self._world_service.get_world(world_id)
        return {
            "world_id": str(world_id),
            "entity_id": entity_id,
            "tick": tick if tick is not None else getattr(world, "tick", 0),
        }

    async def get_components(
        self,
        world_id: UUID,
        component_types: list[str],
        entity_ids: list[int] | None = None,
    ) -> dict:
        """Query specific component types across entities."""
        self._world_service.get_world(world_id)  # validate world exists
        return {
            "world_id": str(world_id),
            "component_types": component_types,
            "entity_ids": entity_ids,
        }

    async def query_dataframe(
        self,
        world_id: UUID,
        component_names: list[str],
        *,
        where: str | None = None,
        select: list[str] | None = None,
        sort: str | None = None,
        desc: bool = False,
        limit: int = 50,
        offset: int = 0,
        tick: int | None = None,
        count: bool = False,
    ) -> dict[str, Any]:
        """Build a lazy DataFrame query chain from component names, materialize at terminal.

        The chain stays lazy until the terminal operation (show or count).
        Returns a JSON-serializable dict: ``{columns, rows, total}`` or ``{count}``.
        """
        world = self._world_service.get_world(world_id)

        # Resolve component type names to classes
        comp_types = [Component.get_type_by_name(name) for name in component_names]

        # Base lazy DataFrame — join across matching archetype signatures
        df = await world.get_components(comp_types)

        # Optional time-travel filter (tick column exists on all archetype rows)
        if tick is not None:
            df = df.where(col("tick") == tick)

        # Where clause — parsed as SQL expression
        if where:
            df = df.where(sql_expr(where))

        # Column projection
        if select:
            df = df.select(*select)

        # Sorting
        if sort:
            df = df.sort(col(sort), desc=desc)

        # Terminal: count
        if count:
            n = df.count_rows()
            return {"count": n}

        # Pagination
        if offset > 0:
            df = df.offset(offset)
        df = df.limit(limit)

        # Materialize to rows
        rows = df.to_pylist()
        columns = df.column_names

        return {"columns": columns, "rows": rows, "total": len(rows)}

    async def get_command_history(
        self,
        world_id: UUID,
        limit: int = 100,
    ) -> list[Command]:
        """Get command history for a world."""
        if self._broker:
            return await self._broker.get_history(str(world_id), limit)
        return []
