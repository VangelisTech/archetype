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
from typing import cast

from daft import DataFrame, col

from archetype import ArchetypeRuntime, AsyncProcessor, Component, StorageConfig
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


async def run_demo(storage_uri: str, *, verbose: bool = False) -> dict[str, object]:
    """Exercise hook ordering, isolation, removal, and final world state."""
    storage = StorageConfig(uri=storage_uri, namespace="hooks_demo")
    audit_log: list[str] = []
    metrics: list[TickMetric] = []
    tick_started_at: dict[int, float] = {}
    debug_ticks: list[int] = []
    advisory_failure_attempts = 0

    # ── Hook handlers (all async) ────────────────────────────────────────

    async def audit_spawn(event: OnSpawn) -> None:
        components = ", ".join(type(component).__name__ for component in event.components)
        audit_log.append(f"spawn:[{components}]")

    async def audit_despawn(event: OnDespawn) -> None:
        audit_log.append("despawn")

    async def audit_component_added(event: OnComponentAdded) -> None:
        components = ", ".join(type(component).__name__ for component in event.components)
        audit_log.append(f"add_components:[{components}]")

    async def audit_component_removed(event: OnComponentRemoved) -> None:
        names = ", ".join(component_type.__name__ for component_type in event.component_types)
        audit_log.append(f"remove_components:[{names}]")

    async def start_timer(event: PreTick) -> None:
        tick_started_at[event.tick] = time.perf_counter()

    async def publish_metrics(event: PostTick) -> None:
        completed_tick = event.tick - 1
        started_at = tick_started_at.pop(completed_tick, time.perf_counter())
        battery_levels: list[float] = []

        for signature, df in event.results.items():
            if Battery not in signature:
                continue
            rows = df.where(col("is_active")).select("battery__percent").collect().to_pylist()
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
        debug_ticks.append(event.tick)
        if verbose:
            print(f"debug hook: tick {event.tick} is starting")

    async def advisory_failure(_event: PreTick) -> None:
        nonlocal advisory_failure_attempts
        advisory_failure_attempts += 1
        raise RuntimeError("demonstration observer failure")

    # ── World setup with hooks at construction ───────────────────────────

    async with ArchetypeRuntime() as runtime:
        world = runtime.world(
            "hooks-demo",
            storage=storage,
            processors=[RoverProcessor()],
            hooks=[
                (OnSpawn, audit_spawn),
                (OnDespawn, audit_despawn),
                (OnComponentAdded, audit_component_added),
                (OnComponentRemoved, audit_component_removed),
                (PreTick, start_timer),
                (PostTick, publish_metrics),
            ],
        )

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

        # Advisory hooks are isolated: this failure cannot suppress the tick.
        failure_handle = await world.add_hook(PreTick, advisory_failure)

        # Add a temporary debug hook post-activation to get a removable handle.
        debug_handle = await world.add_hook(PreTick, temporary_debug_trace)

        await world.step()

        # Removed hooks do not fire on subsequent ticks.
        await world.remove_hook(failure_handle)
        await world.remove_hook(debug_handle)

        await world.add_components(rover_a, Payload(label="soil sample"))
        await world.run(steps=2)

        await world.remove_components(rover_a, Payload)
        await world.despawn(rover_b)
        await world.step()

        info = await world.info()
        rows = (
            (await world.query(Position, Velocity, Battery))
            .where(col("tick") == info.tick - 1)
            .select("entity_id", "position__x", "position__y", "battery__percent")
            .collect()
            .to_pylist()
        )

    return {
        "lifecycle_order": audit_log,
        "tick_metrics": [
            {
                "tick": metric.completed_tick,
                "active": metric.active_rovers,
                "low_battery": metric.low_battery_rovers,
            }
            for metric in metrics
        ],
        "durations_recorded": len(metrics),
        "advisory_failure_attempts": advisory_failure_attempts,
        "temporary_hook_ticks": debug_ticks,
        "final_rovers": [
            {
                "position": [row["position__x"], row["position__y"]],
                "battery": row["battery__percent"],
            }
            for row in rows
        ],
    }


async def main() -> None:
    result = await run_demo("./archetype_data", verbose=True)
    print("Lifecycle audit")
    for entry in cast(list[str], result["lifecycle_order"]):
        print(f"  - {entry}")

    print("\nTick metrics")
    for metric in cast(list[dict[str, int]], result["tick_metrics"]):
        print(
            f"  tick={metric['tick']}: "
            f"active={metric['active']}, "
            f"low_battery={metric['low_battery']}"
        )

    print("\nFinal active rovers")
    for rover in cast(list[dict[str, object]], result["final_rovers"]):
        position = cast(list[float], rover["position"])
        battery = cast(float, rover["battery"])
        print(f"  position=({position[0]:.1f}, {position[1]:.1f}), battery={battery:.1f}%")


if __name__ == "__main__":
    asyncio.run(main())
