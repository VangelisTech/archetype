# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
World Mutations
================

Demonstrates the trusted actor-free runtime mutation surface: spawn, despawn,
update, add/remove components, add/remove processors, fork, and history reads.

No external dependencies — runs entirely in-process.

Usage:
    uv run python examples/01_world_mutations.py

The script's own narration is all you see by default. To watch the
machinery underneath (gated ops, world steps, store writes), set:
    ARCHETYPE_LOG=debug uv run python examples/01_world_mutations.py
"""

import asyncio

from daft import DataFrame, col

from archetype import ArchetypeRuntime, AsyncProcessor, Component, StorageConfig


class Position(Component):
    x: float = 0.0
    y: float = 0.0


class Velocity(Component):
    vx: float = 0.0
    vy: float = 0.0


class Health(Component):
    hp: int = 100
    max_hp: int = 100


class MovementProcessor(AsyncProcessor):
    components = (Position, Velocity)
    priority = 10

    async def process(self, df: DataFrame, **kwargs) -> DataFrame:
        return df.with_columns(
            {
                "position__x": col("position__x") + col("velocity__vx"),
                "position__y": col("position__y") + col("velocity__vy"),
            }
        )


async def run_demo(storage_uri: str) -> dict[str, object]:
    """Exercise the mutation surface and return stable semantic evidence."""
    storage = StorageConfig(uri=storage_uri, namespace="world_mutations")
    async with ArchetypeRuntime() as runtime:
        world = runtime.world("mutations-demo", storage=storage)

        # Runtime scripts are trusted and call the actor-free application facade.
        await world.step()

        # ── 1. SPAWN ─────────────────────────────────────────────────────────

        scout = await world.spawn(Position(x=0.0, y=0.0))
        dummy = await world.spawn(Position(x=10.0, y=10.0))
        await world.step()

        # ── 2. UPDATE + COMPONENT MUTATIONS ───────────────────────────────────

        await world.update(scout, Position(x=2.0, y=1.0))
        await world.step()

        await world.add_components(scout, Velocity(vx=1.5, vy=0.5), Health(hp=80, max_hp=100))
        await world.step()

        info = await world.info()
        with_health = (
            (await world.query(Position, Velocity, Health))
            .where(col("tick") == info.tick - 1)
            .where(col("entity_id") == scout)
            .collect()
            .to_pylist()[0]
        )

        await world.remove_components(scout, Health)
        await world.despawn(dummy)
        await world.step()

        info2 = await world.info()
        after_removals = (
            (await world.query(Position, Velocity))
            .where(col("tick") == info2.tick - 1)
            .collect()
            .to_pylist()
        )
        health_rows_after_removal = (
            (await world.query(Position, Velocity, Health))
            .where(col("tick") == info2.tick - 1)
            .where(col("entity_id") == scout)
            .count_rows()
        )

        # ── 3. PROCESSOR MUTATIONS ────────────────────────────────────────────

        await world.add_processor(MovementProcessor())
        await world.step()
        info3 = await world.info()
        after_processor = (
            (await world.query(Position, Velocity))
            .where(col("tick") == info3.tick - 1)
            .where(col("entity_id") == scout)
            .collect()
            .to_pylist()[0]
        )
        await world.remove_processor(MovementProcessor)
        await world.step()
        info4 = await world.info()
        after_processor_removal = (
            (await world.query(Position, Velocity))
            .where(col("tick") == info4.tick - 1)
            .where(col("entity_id") == scout)
            .collect()
            .to_pylist()[0]
        )

        # ── 4. FORK ───────────────────────────────────────────────────────────

        branch = await world.fork("branch-a", storage=storage)
        branch_seed = await branch.spawn(Position(x=-5.0, y=0.0), Velocity(vx=0.5, vy=0.0))
        await branch.step()

        source_info = await world.info()
        branch_info = await branch.info()
        source_ids = {
            row["entity_id"]
            for row in (await world.query(Position))
            .where(col("tick") == source_info.tick - 1)
            .select("entity_id")
            .collect()
            .to_pylist()
        }
        branch_ids = {
            row["entity_id"]
            for row in (await branch.query(Position))
            .where(col("tick") == branch_info.tick - 1)
            .select("entity_id")
            .collect()
            .to_pylist()
        }

        # ── 5. HISTORY ────────────────────────────────────────────────────────

        history = await world.history()
        return {
            "spawned_entities": 2,
            "component_mutations": {
                "updated_position": [with_health["position__x"], with_health["position__y"]],
                "velocity": [with_health["velocity__vx"], with_health["velocity__vy"]],
                "health_added": with_health["health__hp"],
                "health_removed": health_rows_after_removal == 0,
                "dummy_despawned": {row["entity_id"] for row in after_removals} == {scout},
            },
            "processor_mutation": {
                "moved_position": [
                    after_processor["position__x"],
                    after_processor["position__y"],
                ],
                "removal_stopped_movement": (
                    after_processor_removal["position__x"] == after_processor["position__x"]
                    and after_processor_removal["position__y"] == after_processor["position__y"]
                ),
            },
            "fork": {
                "distinct_worlds": str(source_info.world_id) != str(branch_info.world_id),
                "inherited_source_entity": scout in branch_ids,
                "branch_entity_isolated": branch_seed in branch_ids
                and branch_seed not in source_ids,
            },
            "trusted_audit_rows": history.count_rows(),
        }


async def main() -> None:
    result = await run_demo("./archetype_data")
    print("1. SPAWN")
    print(f"   spawned {result['spawned_entities']} entities")
    print("\n2. UPDATE + COMPONENT MUTATIONS")
    print(f"   {result['component_mutations']}")
    print("\n3. PROCESSOR MUTATIONS")
    print(f"   {result['processor_mutation']}")
    print("\n4. FORK")
    print(f"   {result['fork']}")
    print("\n5. HISTORY")
    print(f"   {result['trusted_audit_rows']} projected audit rows for trusted runtime calls")


if __name__ == "__main__":
    asyncio.run(main())
