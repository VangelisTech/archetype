# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Query Service

The read path. Time-travel queries, entity state, command history.
All external reads go through here.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from uuid_utils import UUID

from archetype.app.models import Command, WorldSnapshot
from archetype.core.aio.async_world import AsyncWorld
from archetype.core.archetype import Archetype
from archetype.core.component import Component

if TYPE_CHECKING:
    from archetype.app.broker import CommandBroker
    from archetype.app.world_service import WorldService


class QueryService:
    """
    Read path facade. Time-travel queries, entity state, cross-world reads.

    Time-travel policy (mirrors ``prefer_live_reads``):
        - ``tick is None`` or ``tick == world.tick``: read from the world's
          live snapshots (``AsyncWorld.get_components``).
        - Otherwise: read from the store via ``AsyncWorld.query_archetype(ticks=[tick])``.
    """

    def __init__(self, world_service: WorldService, broker: CommandBroker | None = None):
        self._world_service = world_service
        self._broker = broker

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _is_current_tick(world: Any, tick: int | None) -> bool:
        """True if the requested tick is the current (live) tick of the world."""
        if tick is None:
            return True
        current = getattr(world, "tick", 0)
        return tick == current

    def _archetype_name(self, sig) -> str:
        return Archetype.get_name(sig)

    async def _collect_archetype_rows(
        self,
        world: AsyncWorld,
        sig,
        tick: int | None,
        entity_ids: list[int] | None = None,
    ) -> list[dict]:
        """Return list of row dicts for a single archetype signature at the given tick.

        Uses live snapshot when tick is current; otherwise falls back to the store.
        """
        if self._is_current_tick(world, tick):
            df = world._live.get(sig)
            if df is None:
                return []
            if entity_ids is not None:
                from daft import col

                df = df.where(col("entity_id").is_in(entity_ids))
            return df.collect().to_pylist()

        # Historical: query the store for this archetype at the given tick.
        df = await world.query_archetype(
            sig=sig,
            ticks=[tick],
            entity_ids=entity_ids,
            components=None,
        )
        return df.collect().to_pylist()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def get_world_state(
        self,
        world_id: UUID,
        tick: int | None = None,
    ) -> WorldSnapshot:
        """Get the current (or historical) world state snapshot.

        Returns per-entity component-type lists and per-archetype active counts.
        """
        world = self._world_service.get_world(world_id)
        current_tick = getattr(world, "tick", 0)
        effective_tick = current_tick if tick is None else tick

        entities: dict[int, list[str]] = {}
        archetype_counts: dict[str, int] = {}

        if not isinstance(world, AsyncWorld):
            # Non-AsyncWorld instances have no introspectable live state here.
            return WorldSnapshot(
                world_id=world_id,
                tick=effective_tick,
                entities={},
                archetype_counts={},
            )

        # Enumerate all known signatures for this world. For current-tick
        # reads _live is authoritative; for historical reads we scan the same
        # set of signatures (the archetype catalog) against the store.
        known_sigs = set(world._live.keys()) | set(world._entity2sig.values())

        for sig in known_sigs:
            rows = await self._collect_archetype_rows(world, sig, tick=tick)
            # Only count active rows (live snapshots already filter, but be defensive).
            active_rows = [r for r in rows if r.get("is_active", True)]
            if not active_rows:
                continue
            name = self._archetype_name(sig)
            archetype_counts[name] = archetype_counts.get(name, 0) + len(active_rows)
            component_names = [c.__name__ for c in sig]
            for row in active_rows:
                eid = row.get("entity_id")
                if eid is not None:
                    entities[int(eid)] = component_names

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

        Returns a dict containing identity, tick, archetype, and a
        ``components`` map of ``{ComponentName: {field: value, ...}}``.
        Returns an empty ``components`` map if the entity is not present
        at the requested tick.
        """
        world = self._world_service.get_world(world_id)
        current_tick = getattr(world, "tick", 0)
        effective_tick = current_tick if tick is None else tick

        result: dict[str, Any] = {
            "world_id": str(world_id),
            "entity_id": entity_id,
            "tick": effective_tick,
            "archetype": None,
            "components": {},
        }

        if not isinstance(world, AsyncWorld):
            return result

        # Determine which signature(s) to search. For current tick, consult
        # _entity2sig directly; for historical tick we may need to scan all
        # known signatures since the entity's archetype may have changed.
        candidate_sigs = []
        if self._is_current_tick(world, tick):
            sig = world._entity2sig.get(entity_id)
            if sig is not None:
                candidate_sigs = [sig]
        else:
            candidate_sigs = list(set(world._live.keys()) | set(world._entity2sig.values()))

        for sig in candidate_sigs:
            rows = await self._collect_archetype_rows(world, sig, tick=tick, entity_ids=[entity_id])
            active = [r for r in rows if r.get("is_active", True)]
            if not active:
                continue
            # Prefer latest tick if multiple rows returned (historical scans).
            row = max(active, key=lambda r: r.get("tick", 0))
            result["archetype"] = self._archetype_name(sig)
            components: dict[str, dict[str, Any]] = {}
            for ctype in sig:
                prefix = ctype.get_prefix()
                fields = {k[len(prefix) :]: v for k, v in row.items() if k.startswith(prefix)}
                components[ctype.__name__] = fields
            result["components"] = components
            break

        return result

    async def get_components(
        self,
        world_id: UUID,
        component_types: list[str],
        entity_ids: list[int] | None = None,
        tick: int | None = None,
    ) -> dict:
        """Query specific component types across entities.

        Returns a dict with ``rows``: a list of per-entity dicts with
        ``entity_id``, ``tick``, and each requested component's fields
        nested under the component's class name.
        """
        world = self._world_service.get_world(world_id)
        current_tick = getattr(world, "tick", 0)
        effective_tick = current_tick if tick is None else tick

        response: dict[str, Any] = {
            "world_id": str(world_id),
            "component_types": component_types,
            "entity_ids": entity_ids,
            "tick": effective_tick,
            "rows": [],
        }

        if not isinstance(world, AsyncWorld):
            return response

        # Resolve component type names to classes.
        try:
            ctypes = [Component.get_type_by_name(name) for name in component_types]
        except ValueError:
            # Unknown component name => empty projection, not an error.
            return response

        required = set(ctypes)

        if self._is_current_tick(world, tick):
            # Fast path: AsyncWorld.get_components handles union + projection.
            df = await world.get_components(ctypes, entity_ids=entity_ids)
            rows = df.collect().to_pylist()
        else:
            # Historical path: union query_archetype across matching sigs.
            rows = []
            matching = [
                sig
                for sig in (set(world._live.keys()) | set(world._entity2sig.values()))
                if required.issubset(set(sig))
            ]
            for sig in matching:
                sig_rows = await self._collect_archetype_rows(
                    world, sig, tick=tick, entity_ids=entity_ids
                )
                rows.extend(r for r in sig_rows if r.get("is_active", True))

        # Shape into a stable, component-centric response.
        shaped = []
        for row in rows:
            entry: dict[str, Any] = {
                "entity_id": row.get("entity_id"),
                "tick": row.get("tick"),
            }
            for ctype in ctypes:
                prefix = ctype.get_prefix()
                entry[ctype.__name__] = {
                    k[len(prefix) :]: v for k, v in row.items() if k.startswith(prefix)
                }
            shaped.append(entry)
        response["rows"] = shaped
        return response

    async def get_command_history(
        self,
        world_id: UUID,
        limit: int = 100,
    ) -> list[Command]:
        """Get command history for a world."""
        if self._broker:
            return await self._broker.get_history(str(world_id), limit)
        return []
