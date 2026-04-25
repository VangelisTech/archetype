# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Lifecycle Hooks
===============

Demonstrates hooks as observability and integration glue:

- audit entity lifecycle mutations as they are applied
- measure tick duration without changing processors
- publish per-tick metrics from PostTick results
- remove a temporary debugging hook by handle

Hooks are side effects. Keep simulation correctness in processors or commands.

Usage:
    uv run python examples/07_hooks.py
"""

import asyncio
import time
from dataclasses import dataclass

from daft import DataFrame, col

from archetype import ArchetypeRuntime, Component, StorageConfig
from archetype.core.aio.async_processor import AsyncProcessor
from archetype.core.hooks import (
    OnComponentAdded,
    OnComponentRemoved,
    OnDespawn,
    OnSpawn,
    PostTick,
    PreTick,
)


class Position(Component):
    x: float = 0.0
    y: float = 0.0


class Velocity(Component):
    vx: float = 0.0
    vy: float = 0.0


class Battery(Component):
    percent: float = 100.0


class Payload(Component):
    label: str = ""


@dataclass
class TickMetric:
    completed_tick: int
    duration_ms: float
    active_rovers: int
    low_battery_rovers: int


class RoverProcessor(AsyncProcessor):
    """Move rovers and drain a small amount of battery each tick."""

    components = (Position, Velocity, Battery)
    priority = 10

    async def process(self, df: DataFrame, **kwargs) -> DataFrame:
        return df.with_columns(
            {
                "position__x": col("position__x") + col("velocity__vx"),
                "position__y": col("position__y") + col("velocity__vy"),
                "battery__percent": col("battery__percent") - 18.0,
            }
        )


async def main() -> None:
    storage = StorageConfig(uri="./archetype_data", namespace="hooks_demo")
    audit_log: list[str] = []
    metrics: list[TickMetric] = []
    tick_started_at: dict[int, float] = {}

    async with ArchetypeRuntime() as runtime:
        world = runtime.world(
            "hooks-demo",
            storage=storage,
            processors=[RoverProcessor()],
        )

        async def audit_spawn(event: OnSpawn) -> None:
            components = ", ".join(type(component).__name__ for component in event.components)
            audit_log.append(f"spawn entity={event.entity_id} components=[{components}]")

        async def audit_despawn(event: OnDespawn) -> None:
            audit_log.append(f"despawn entity={event.entity_id}")

        async def audit_component_added(event: OnComponentAdded) -> None:
            components = ", ".join(type(component).__name__ for component in event.components)
            audit_log.append(f"add_components entity={event.entity_id} components=[{components}]")

        async def audit_component_removed(event: OnComponentRemoved) -> None:
            names = ", ".join(component_type.__name__ for component_type in event.component_types)
            audit_log.append(f"remove_components entity={event.entity_id} components=[{names}]")

        async def start_timer(event: PreTick) -> None:
            tick_started_at[event.tick] = time.perf_counter()

        async def publish_metrics(event: PostTick) -> None:
            completed_tick = event.tick - 1
            started_at = tick_started_at.pop(completed_tick, time.perf_counter())
            battery_levels: list[float] = []

            for signature, df in event.results.items():
                if Battery not in signature:
                    continue
                rows = df.select("battery__percent").collect().to_pylist()
                battery_levels.extend(row["battery__percent"] for row in rows)

            metrics.append(
                TickMetric(
                    completed_tick=completed_tick,
                    duration_ms=(time.perf_counter() - started_at) * 1000,
                    active_rovers=len(battery_levels),
                    low_battery_rovers=sum(level < 50.0 for level in battery_levels),
                )
            )

        async def temporary_debug_trace(event: PreTick) -> None:
            print(f"debug hook: tick {event.tick} is starting")

        world.add_hook(OnSpawn, audit_spawn)
        world.add_hook(OnDespawn, audit_despawn)
        world.add_hook(OnComponentAdded, audit_component_added)
        world.add_hook(OnComponentRemoved, audit_component_removed)
        world.add_hook(PreTick, start_timer)
        world.add_hook(PostTick, publish_metrics)

        debug_handle = world.add_hook(PreTick, temporary_debug_trace)

        rover_a = await world.spawn(
            Position(x=0.0, y=0.0),
            Velocity(vx=2.0, vy=0.5),
            Battery(percent=100.0),
        )
        rover_b = await world.spawn(
            Position(x=10.0, y=-2.0),
            Velocity(vx=-1.0, vy=1.0),
            Battery(percent=55.0),
        )

        await world.step()
        world.remove_hook(debug_handle)

        await world.add_components(rover_a, Payload(label="soil sample"))
        await world.run(steps=2)

        await world.remove_components(rover_a, Payload)
        await world.despawn(rover_b)
        await world.step()

        rows = (
            (await world.query(Position, Velocity, Battery))
            .select("entity_id", "position__x", "position__y", "battery__percent")
            .collect()
            .to_pylist()
        )

    print("Lifecycle audit")
    for entry in audit_log:
        print(f"  - {entry}")

    print("\nTick metrics")
    for metric in metrics:
        print(
            f"  tick={metric.completed_tick}: "
            f"{metric.duration_ms:.2f} ms, "
            f"active={metric.active_rovers}, "
            f"low_battery={metric.low_battery_rovers}"
        )

    print("\nFinal active rovers")
    for row in rows:
        print(
            f"  entity={row['entity_id']}: "
            f"position=({row['position__x']:.1f}, {row['position__y']:.1f}), "
            f"battery={row['battery__percent']:.1f}%"
        )


if __name__ == "__main__":
    asyncio.run(main())
