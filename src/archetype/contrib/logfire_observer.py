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

"""Logfire observability hooks for Archetype simulations.

Usage:
    from archetype.contrib.logfire_observer import logfire_hooks

    world = runtime.world(
        "demo",
        processors=[Movement()],
        hooks=logfire_hooks(),
    )

This gives you per-tick spans in Logfire showing:
- Tick number
- Active entity count
- Spawn/despawn pending counts
- Per-tick duration (via PreTick → PostTick span)
"""

from __future__ import annotations

from typing import Any

import logfire

from archetype.core.hooks import (
    OnComponentAdded,
    OnComponentRemoved,
    OnDespawn,
    OnDestroy,
    OnSpawn,
    PostTick,
    PreTick,
)

# Open logfire spans keyed by world_id (LogfireSpan; Any avoids a hard
# dependency on logfire's stub types in this optional contrib module).
_tick_spans: dict[str, Any] = {}


async def _on_pre_tick(event: PreTick) -> None:
    """Start a span for this tick."""
    span = logfire.span("tick.{tick}", tick=event.tick, world_id=str(event.world_id))
    span.__enter__()
    _tick_spans[str(event.world_id)] = span


async def _on_post_tick(event: PostTick) -> None:
    """End the tick span with result metadata."""
    span = _tick_spans.pop(str(event.world_id), None)
    if span is not None:
        logfire.info(
            "tick.complete",
            tick=event.tick,
            world_id=str(event.world_id),
            archetypes_processed=len(event.results) if event.results else 0,
        )
        span.__exit__(None, None, None)


async def _on_spawn(event: OnSpawn) -> None:
    logfire.debug(
        "entity.spawn",
        entity_id=event.entity_id,
        world_id=str(event.world_id),
        components=[type(c).__name__ for c in event.components],
    )


async def _on_despawn(event: OnDespawn) -> None:
    logfire.debug(
        "entity.despawn",
        entity_id=event.entity_id,
        world_id=str(event.world_id),
    )


async def _on_component_added(event: OnComponentAdded) -> None:
    logfire.debug(
        "entity.add_components",
        entity_id=event.entity_id,
        world_id=str(event.world_id),
        components=[type(c).__name__ for c in event.components],
    )


async def _on_component_removed(event: OnComponentRemoved) -> None:
    logfire.debug(
        "entity.remove_components",
        entity_id=event.entity_id,
        world_id=str(event.world_id),
        component_types=[t.__name__ for t in event.component_types],
    )


async def _on_destroy(event: OnDestroy) -> None:
    logfire.info("world.destroy", world_id=str(event.world_id))


def logfire_hooks() -> list[tuple]:
    """Return the full set of Logfire observability hooks.

    Pass to ``runtime.world(..., hooks=logfire_hooks())``.
    """
    return [
        (PreTick, _on_pre_tick),
        (PostTick, _on_post_tick),
        (OnSpawn, _on_spawn),
        (OnDespawn, _on_despawn),
        (OnComponentAdded, _on_component_added),
        (OnComponentRemoved, _on_component_removed),
        (OnDestroy, _on_destroy),
    ]
