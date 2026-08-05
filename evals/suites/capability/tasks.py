# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Broader end-to-end repository scenarios with blocking grader outcomes.

These deterministic checks assert architectural behavior, so a missed grader
fails the repository-check command. This is an execution policy, not a product
capability model.
"""

from __future__ import annotations

import asyncio
import tempfile
from dataclasses import dataclass

from daft import col

from archetype import ArchetypeRuntime
from archetype.core.aio.async_processor import AsyncProcessor
from archetype.core.component import Component
from archetype.core.config import StorageConfig
from archetype.core.errors import TickExecutionError
from archetype.core.hooks import PostTick, PreTick
from evals.graders import exact_match, state_check
from evals.harness import EvalHarness
from evals.types import GraderResult

SUITE = "capability"


# ---------------------------------------------------------------------------
# Components & Processors
# ---------------------------------------------------------------------------


class Position(Component):
    x: int
    y: int


class Velocity(Component):
    dx: int
    dy: int


class Health(Component):
    hp: int


class Stats(Component):
    hp: int
    name: str


class Flags(Component):
    active: bool
    level: int


class ForkValue(Component):
    amount: int = 0


class TimelineValue(Component):
    amount: int = 0


class AdversarialValue(Component):
    amount: int = 0


class AdversarialMarker(Component):
    label: str = "marked"


@dataclass
class ForkStepSize:
    delta: int = 1


class ApplyVelocity(AsyncProcessor):
    """Move entities: position += velocity each step."""

    components = (Position, Velocity)
    priority = 1

    async def process(self, df, **kwargs):
        return df.with_column("position__x", col("position__x") + col("velocity__dx")).with_column(
            "position__y", col("position__y") + col("velocity__dy")
        )


class AdvanceForkValue(AsyncProcessor):
    """Advance fork state using a mutable resource shared across branches."""

    components = (ForkValue,)
    priority = 1

    async def process(self, df, resources=None, **kwargs):
        step_size = resources.get(ForkStepSize) if resources else None
        delta = step_size.delta if step_size else 0
        return df.with_column("forkvalue__amount", col("forkvalue__amount") + delta)


class AdvanceTimelineValue(AsyncProcessor):
    """Advance a durable value once per tick after its initial condition."""

    components = (TimelineValue,)
    priority = 1

    async def process(self, df, **kwargs):
        return df.with_column("timelinevalue__amount", col("timelinevalue__amount") + 1)


class IncrementAdversarialValue(AsyncProcessor):
    """Advance every adversarial value by one."""

    components = (AdversarialValue,)
    priority = 10

    async def process(self, df, **kwargs):
        return df.with_column(
            "adversarialvalue__amount",
            col("adversarialvalue__amount") + 1,
        )


class BoostMarkedAdversarialValue(AsyncProcessor):
    """Apply an additional increment only while an entity is marked."""

    components = (AdversarialValue, AdversarialMarker)
    priority = 20

    async def process(self, df, **kwargs):
        return df.with_column(
            "adversarialvalue__amount",
            col("adversarialvalue__amount") + 10,
        )


class FailMarkedArchetype(AsyncProcessor):
    """Fail one matching table so the world's compute barrier is observable."""

    components = (AdversarialMarker,)
    priority = 15

    async def process(self, df, **kwargs):
        raise RuntimeError("intentional marked-archetype failure")


# ---------------------------------------------------------------------------
# Task: Storage round-trip integrity
# ---------------------------------------------------------------------------


def task_storage_roundtrip() -> list[GraderResult]:
    """Write entities with mixed field types, flush, read back, verify."""
    return asyncio.run(_task_storage_roundtrip())


async def _task_storage_roundtrip() -> list[GraderResult]:
    with tempfile.TemporaryDirectory() as tmp:
        storage_cfg = StorageConfig(uri=f"{tmp}/store", namespace="eval_cap")

        async with ArchetypeRuntime() as runtime:
            world = runtime.world("roundtrip", storage=storage_cfg)
            expected = []
            for i in range(20):
                components: list[Component] = [
                    Stats(hp=100 + i, name=f"entity_{i}"),
                    Flags(active=i % 2 == 0, level=i),
                ]
                eid = await world.spawn(*components)
                expected.append(
                    {
                        "entity_id": eid,
                        "hp": 100 + i,
                        "name": f"entity_{i}",
                        "level": i,
                    }
                )

            # Flush to storage
            await world.run(steps=1)

            # Read back
            df = await world.query(Stats, Flags)
            rows = df.collect().to_pydict()

            # Grade: entity count
            returned_ids = set(rows.get("entity_id", []))
            expected_ids = {e["entity_id"] for e in expected}

            # Grade: field-level integrity
            field_checks = {}
            for idx, eid in enumerate(rows.get("entity_id", [])):
                exp = next((e for e in expected if e["entity_id"] == eid), None)
                if exp is None:
                    field_checks[f"entity_{eid}_found"] = False
                    continue
                field_checks[f"entity_{eid}_hp"] = rows["stats__hp"][idx] == exp["hp"]
                field_checks[f"entity_{eid}_name"] = rows["stats__name"][idx] == exp["name"]
                field_checks[f"entity_{eid}_level"] = rows["flags__level"][idx] == exp["level"]

            return [
                exact_match(returned_ids, expected_ids, name="entity_ids_match"),
                state_check(field_checks, name="field_integrity"),
            ]


# ---------------------------------------------------------------------------
# Task: Multi-step simulation correctness
# ---------------------------------------------------------------------------

N_ENTITIES = 50
N_STEPS = 3


def task_simulation_correctness() -> list[GraderResult]:
    """Spawn, run processors, verify outcome state is correct."""
    return asyncio.run(_task_simulation_correctness())


async def _task_simulation_correctness() -> list[GraderResult]:
    with tempfile.TemporaryDirectory() as tmp:
        storage_cfg = StorageConfig(uri=f"{tmp}/store", namespace="eval_sim")
        async with ArchetypeRuntime() as runtime:
            world = runtime.world(
                "sim-eval",
                storage=storage_cfg,
                processors=[ApplyVelocity()],
            )

            spawned_ids = []
            init_positions = {}
            for i in range(N_ENTITIES):
                eid = await world.spawn(
                    Position(x=i, y=i * 2),
                    Velocity(dx=1, dy=-1),
                    Health(hp=100),
                )
                spawned_ids.append(eid)
                init_positions[eid] = (i, i * 2)

            await world.run(steps=N_STEPS)

            # Runtime queries are append-only history; select the final
            # persisted snapshot for this outcome-oriented capability check.
            df = (await world.query(Position, Velocity, Health)).where(col("tick") == N_STEPS - 1)
            rows = df.collect().to_pydict()

            entity_ids = rows.get("entity_id", [])
            entity_count = len(entity_ids)

            # Grader 1: Entity preservation
            entity_checks = {
                "correct_count": entity_count == N_ENTITIES,
                "no_duplicates": len(set(entity_ids)) == entity_count,
                "all_present": set(entity_ids) == set(spawned_ids),
            }

            # Grader 2: Processor correctness (outcome verification)
            # Initial-conditions contract (spec World Contracts): the spawn tick
            # persists raw initial state and processors first apply on the
            # following tick, so N_STEPS ticks apply ApplyVelocity(dx=1, dy=-1)
            # exactly N_STEPS - 1 times:
            #   x_final = x_init + (N_STEPS - 1), y_final = y_init - (N_STEPS - 1)
            applied_ticks = N_STEPS - 1
            position_checks = {}
            for idx in range(entity_count):
                eid = entity_ids[idx]
                x_init, y_init = init_positions.get(eid, (None, None))
                if x_init is None or y_init is None:
                    position_checks[f"eid_{eid}_found"] = False
                    continue
                position_checks[f"eid_{eid}_x"] = rows["position__x"][idx] == x_init + applied_ticks
                position_checks[f"eid_{eid}_y"] = rows["position__y"][idx] == y_init - applied_ticks

            # Grader 3: Untouched data preserved (velocity, health unchanged)
            preservation_checks = {}
            for idx in range(entity_count):
                eid = entity_ids[idx]
                preservation_checks[f"eid_{eid}_hp"] = rows["health__hp"][idx] == 100
                preservation_checks[f"eid_{eid}_dx"] = rows["velocity__dx"][idx] == 1
                preservation_checks[f"eid_{eid}_dy"] = rows["velocity__dy"][idx] == -1

            # Grader 4: Query completeness
            expected_cols = {
                "entity_id",
                "tick",
                "world_id",
                "run_id",
                "is_active",
                "position__x",
                "position__y",
                "velocity__dx",
                "velocity__dy",
                "health__hp",
            }
            actual_cols = set(rows.keys())
            col_checks = {c: c in actual_cols for c in expected_cols}

            return [
                state_check(entity_checks, name="entity_preservation"),
                state_check(position_checks, name="processor_correctness"),
                state_check(preservation_checks, name="untouched_data"),
                state_check(col_checks, name="query_completeness"),
            ]


# ---------------------------------------------------------------------------
# Task: Fork-of-fork continuity and divergence
# ---------------------------------------------------------------------------


def task_fork_divergence() -> list[GraderResult]:
    """Compose fork lineage, resource sharing, and branch isolation."""
    return asyncio.run(_task_fork_divergence())


async def _task_fork_divergence() -> list[GraderResult]:
    with tempfile.TemporaryDirectory() as tmp:
        storage_cfg = StorageConfig(uri=f"{tmp}/store", namespace="eval_fork")
        step_size = ForkStepSize()

        async with ArchetypeRuntime() as runtime:
            base = runtime.world(
                "fork-base",
                storage=storage_cfg,
                processors=[AdvanceForkValue()],
                resources=[step_size],
            )
            entity_id = await base.spawn(ForkValue(amount=0))
            await base.step()  # raw initial condition at tick 0
            await base.step()  # 1 at tick 1
            base_at_fork = await base.info()

            mid = await base.fork("fork-mid")
            mid_at_fork = await mid.info()
            await mid.step()  # continue parent state: 2 at tick 2
            mid_after_first_step = await mid.info()

            leaf = await mid.fork("fork-leaf")
            leaf_at_fork = await leaf.info()
            await leaf.step()  # continue mid state: 3 at tick 3

            resource_name = f"{ForkStepSize.__module__}.{ForkStepSize.__qualname__}"
            resource_sets = [
                {resource.qualname for resource in await world.list_resources()}
                for world in (base, mid, leaf)
            ]

            # Forks intentionally share resource instances. Mutating the
            # original object must affect the next independent step in every
            # branch without changing their lineage cutoffs.
            step_size.delta = 5
            await base.step()  # 1 + 5 = 6 at tick 2
            await mid.step()  # 2 + 5 = 7 at tick 3
            await leaf.step()  # 3 + 5 = 8 at tick 4

            await base.update(entity_id, ForkValue(amount=100))
            await mid.update(entity_id, ForkValue(amount=200))
            await leaf.update(entity_id, ForkValue(amount=300))
            await base.step()
            await mid.step()
            await leaf.step()

            histories = [
                (await world.query(ForkValue, entity_ids=[entity_id])).to_pylist()
                for world in (base, mid, leaf)
            ]

        value_maps = [
            {int(row["tick"]): int(row["forkvalue__amount"]) for row in rows} for rows in histories
        ]
        owner_maps = [
            {int(row["tick"]): str(row["world_id"]) for row in rows} for rows in histories
        ]
        base_id, mid_id, leaf_id = (
            str(base_at_fork.world_id),
            str(mid_at_fork.world_id),
            str(leaf_at_fork.world_id),
        )

        expected_values = [
            {0: 0, 1: 1, 2: 6, 3: 100},
            {0: 0, 1: 1, 2: 2, 3: 7, 4: 200},
            {0: 0, 1: 1, 2: 2, 3: 3, 4: 8, 5: 300},
        ]
        expected_owners = [
            {0: base_id, 1: base_id, 2: base_id, 3: base_id},
            {0: base_id, 1: base_id, 2: mid_id, 3: mid_id, 4: mid_id},
            {
                0: base_id,
                1: base_id,
                2: mid_id,
                3: leaf_id,
                4: leaf_id,
                5: leaf_id,
            },
        ]

        return [
            state_check(
                {
                    "fresh_world_ids": len({base_id, mid_id, leaf_id}) == 3,
                    "fresh_run_ids": len(
                        {
                            str(base_at_fork.run_id),
                            str(mid_at_fork.run_id),
                            str(leaf_at_fork.run_id),
                        }
                    )
                    == 3,
                    "mid_starts_at_base_tick": mid_at_fork.tick == base_at_fork.tick == 2,
                    "leaf_starts_at_mid_tick": (
                        leaf_at_fork.tick == mid_after_first_step.tick == 3
                    ),
                },
                name="fork_identity",
            ),
            state_check(
                {
                    "resource_in_base": resource_name in resource_sets[0],
                    "resource_in_mid": resource_name in resource_sets[1],
                    "resource_in_leaf": resource_name in resource_sets[2],
                    "shared_change_reaches_base": value_maps[0].get(2) == 6,
                    "shared_change_reaches_mid": value_maps[1].get(3) == 7,
                    "shared_change_reaches_leaf": value_maps[2].get(4) == 8,
                },
                name="fork_resource_sharing",
            ),
            state_check(
                {
                    "one_row_per_base_tick": len(histories[0]) == len(value_maps[0]),
                    "one_row_per_mid_tick": len(histories[1]) == len(value_maps[1]),
                    "one_row_per_leaf_tick": len(histories[2]) == len(value_maps[2]),
                    "base_lineage_ownership": owner_maps[0] == expected_owners[0],
                    "mid_lineage_ownership": owner_maps[1] == expected_owners[1],
                    "leaf_lineage_ownership": owner_maps[2] == expected_owners[2],
                },
                name="fork_lineage_cutoffs",
            ),
            exact_match(value_maps[0], expected_values[0], name="base_branch_history"),
            exact_match(value_maps[1], expected_values[1], name="mid_branch_history"),
            exact_match(value_maps[2], expected_values[2], name="leaf_branch_history"),
        ]


# ---------------------------------------------------------------------------
# Task: Time-travel reads and run identity across fresh runtimes
# ---------------------------------------------------------------------------


def task_time_travel_and_run_id() -> list[GraderResult]:
    """Read one durable timeline live, cold, and after mutable resume."""
    return asyncio.run(_task_time_travel_and_run_id())


def _timeline_history(rows: list[dict]) -> list[tuple[str, str, int, int, int]]:
    """Normalize durable rows into an order-independent contract surface."""
    return sorted(
        (
            str(row["world_id"]),
            str(row["run_id"]),
            int(row["entity_id"]),
            int(row["tick"]),
            int(row["timelinevalue__amount"]),
        )
        for row in rows
    )


async def _task_time_travel_and_run_id() -> list[GraderResult]:
    with tempfile.TemporaryDirectory() as tmp:
        storage_cfg = StorageConfig(uri=f"{tmp}/store", namespace="eval_time_travel")

        async with ArchetypeRuntime() as writer:
            world = writer.world(
                "timeline",
                storage=storage_cfg,
                processors=[AdvanceTimelineValue()],
            )
            entity_id = await world.spawn(TimelineValue(amount=0))
            first_result = await world.run(steps=4)
            live_info = await world.info()
            live_rows = (await world.query(TimelineValue, entity_ids=[entity_id])).to_pylist()

        world_id = str(live_info.world_id)
        run_id = str(live_info.run_id)
        expected_initial = [
            (world_id, run_id, entity_id, tick, amount) for tick, amount in enumerate(range(4))
        ]

        async with ArchetypeRuntime() as reader:
            cold = reader.attach(world_id, storage=storage_cfg)
            cold_info = await cold.info()
            cold_rows = (await cold.query(TimelineValue, entity_ids=[entity_id])).to_pylist()

        async with ArchetypeRuntime() as resumer:
            resumed = await resumer.resume(world_id, storage=storage_cfg)
            resumed_before = await resumed.info()
            await resumed.add_processor(AdvanceTimelineValue())
            second_result = await resumed.run(steps=2)
            resumed_after = await resumed.info()
            resumed_rows = (await resumed.query(TimelineValue, entity_ids=[entity_id])).to_pylist()

        live_history = _timeline_history(live_rows)
        cold_history = _timeline_history(cold_rows)
        resumed_history = _timeline_history(resumed_rows)
        expected_resumed = [
            (world_id, run_id, entity_id, tick, amount) for tick, amount in enumerate(range(6))
        ]

        return [
            exact_match(live_history, expected_initial, name="live_historical_read"),
            exact_match(cold_history, live_history, name="cold_read_parity"),
            exact_match(resumed_history, expected_resumed, name="resumed_history_continuity"),
            state_check(
                {
                    "first_result_world": str(first_result.world_id) == world_id,
                    "first_result_run": str(first_result.run_id) == run_id,
                    "first_result_ticks": first_result.ticks_completed == 4,
                    "first_result_final_tick": first_result.final_tick == 4,
                    "run_id_is_present": live_info.run_id is not None
                    and run_id not in {"", "None"},
                    "live_info_run": str(live_info.run_id) == run_id,
                    "live_info_next_tick": live_info.tick == 4,
                    "cold_info_world": str(cold_info.world_id) == world_id,
                    "cold_info_run": str(cold_info.run_id) == run_id,
                },
                name="durable_read_identity",
            ),
            state_check(
                {
                    "resume_restores_next_tick": resumed_before.tick == 4,
                    "resume_preserves_run": str(resumed_before.run_id) == run_id,
                    "second_result_world": str(second_result.world_id) == world_id,
                    "second_result_run": str(second_result.run_id) == run_id,
                    "second_result_ticks": second_result.ticks_completed == 2,
                    "second_result_final_tick": second_result.final_tick == 6,
                    "resumed_info_next_tick": resumed_after.tick == 6,
                    "resumed_info_run": str(resumed_after.run_id) == run_id,
                    "one_row_per_tick": len(resumed_rows) == 6,
                },
                name="resume_run_continuity",
            ),
        ]


# ---------------------------------------------------------------------------
# Task: Processor and hook failure boundaries across archetype migration
# ---------------------------------------------------------------------------


def task_processor_adversarial() -> list[GraderResult]:
    """Compose advisory hooks, atomic processor failure, and signature matching."""
    return asyncio.run(_task_processor_adversarial())


def _adversarial_history(rows: list[dict]) -> list[tuple[str, str, int, int, bool, int]]:
    """Normalize durable value rows so a failed-step comparison is order independent."""
    return sorted(
        (
            str(row["world_id"]),
            str(row["run_id"]),
            int(row["entity_id"]),
            int(row["tick"]),
            bool(row["is_active"]),
            int(row["adversarialvalue__amount"]),
        )
        for row in rows
    )


def _active_value_timeline(rows: list[dict], entity_id: int) -> dict[int, int]:
    return {
        int(row["tick"]): int(row["adversarialvalue__amount"])
        for row in rows
        if int(row["entity_id"]) == entity_id and bool(row["is_active"])
    }


def _active_marker_ticks(rows: list[dict], entity_id: int) -> list[int]:
    return sorted(
        int(row["tick"])
        for row in rows
        if int(row["entity_id"]) == entity_id and bool(row["is_active"])
    )


async def _task_processor_adversarial() -> list[GraderResult]:
    with tempfile.TemporaryDirectory() as tmp:
        storage_cfg = StorageConfig(uri=f"{tmp}/store", namespace="eval_processor_adversarial")
        hook_counts = {"bad_pre": 0, "good_pre": 0, "bad_post": 0, "good_post": 0}

        async with ArchetypeRuntime() as runtime:
            world = runtime.world("processor-adversarial", storage=storage_cfg)
            plain_id = await world.spawn(AdversarialValue(amount=0))
            marked_id = await world.spawn(
                AdversarialValue(amount=100),
                AdversarialMarker(),
            )
            await world.step()  # persist raw initial conditions

            async def failing_pre(_event: PreTick) -> None:
                hook_counts["bad_pre"] += 1
                raise RuntimeError("intentional pre-tick hook failure")

            async def healthy_pre(_event: PreTick) -> None:
                hook_counts["good_pre"] += 1

            async def failing_post(_event: PostTick) -> None:
                hook_counts["bad_post"] += 1
                raise RuntimeError("intentional post-tick hook failure")

            async def healthy_post(_event: PostTick) -> None:
                hook_counts["good_post"] += 1

            # Register failures first: the healthy handlers prove suppression
            # is per hook rather than an early exit from the event bucket.
            hook_handles = [
                await world.add_hook(PreTick, failing_pre),
                await world.add_hook(PreTick, healthy_pre),
                await world.add_hook(PostTick, failing_post),
                await world.add_hook(PostTick, healthy_post),
            ]
            await world.add_processor(IncrementAdversarialValue())
            await world.add_processor(BoostMarkedAdversarialValue())

            await world.step()  # tick 1: plain=1, marked=111
            before_failure_info = await world.info()
            before_failure_history = _adversarial_history(
                (await world.query(AdversarialValue)).to_pylist()
            )

            await world.add_processor(FailMarkedArchetype())
            processor_error: RuntimeError | None = None
            try:
                await world.step()
            except RuntimeError as exc:
                processor_error = exc

            after_failure_info = await world.info()
            after_failure_history = _adversarial_history(
                (await world.query(AdversarialValue)).to_pylist()
            )
            hook_counts_after_failure = dict(hook_counts)

            await world.remove_processor(FailMarkedArchetype)
            await world.step()  # retry tick 2 exactly once
            retry_info = await world.info()
            retry_rows = (await world.query(AdversarialValue)).to_pylist()
            for handle in hook_handles:
                await world.remove_hook(handle)

            # Component-set changes are explicit world mutations. The carried
            # row materializes under its new signature; newly matching
            # processors first transform it on the following tick.
            await world.add_components(plain_id, AdversarialMarker(label="moved"))
            await world.step()  # tick 3: carried value remains 2
            await world.step()  # tick 4: increment + marked boost => 13
            await world.remove_components(plain_id, AdversarialMarker)
            await world.step()  # tick 5: carried value remains 13
            await world.step()  # tick 6: unmarked increment only => 14

            final_info = await world.info()
            value_rows = (await world.query(AdversarialValue)).to_pylist()
            marker_rows = (await world.query(AdversarialMarker)).to_pylist()

        active_keys = [
            (int(row["entity_id"]), int(row["tick"]))
            for row in value_rows
            if bool(row["is_active"])
        ]
        plain_timeline = _active_value_timeline(value_rows, plain_id)
        marked_timeline = _active_value_timeline(value_rows, marked_id)

        return [
            state_check(
                {
                    "failed_pre_hook_reached": hook_counts_after_failure["bad_pre"] == 2,
                    "later_pre_hook_still_ran": hook_counts_after_failure["good_pre"] == 2,
                    "failed_step_skipped_post_hooks": (
                        hook_counts_after_failure["bad_post"] == 1
                        and hook_counts_after_failure["good_post"] == 1
                    ),
                    "failed_post_hook_did_not_stop_later_hook": (
                        hook_counts["bad_post"] == hook_counts["good_post"] == 2
                    ),
                    "all_pre_hooks_observed_every_installed_attempt": (
                        hook_counts["bad_pre"] == hook_counts["good_pre"] == 3
                    ),
                },
                name="hook_failure_isolation",
            ),
            state_check(
                {
                    # #444: classify the failure by structure, not message
                    # text — the aggregate names the table; the original
                    # exception object rides in failures.
                    "processor_error_surfaced": (
                        isinstance(processor_error, TickExecutionError)
                        and processor_error.phase == "compute"
                        and any(
                            "marked-archetype failure" in str(failure.error)
                            for failure in processor_error.failures
                        )
                    ),
                    "failed_tick_did_not_advance": (
                        before_failure_info.tick == after_failure_info.tick == 2
                    ),
                    "retry_advanced_same_tick_once": retry_info.tick == 3,
                    "plain_retry_applied_once": (
                        _active_value_timeline(retry_rows, plain_id).get(2) == 2
                    ),
                    "marked_retry_applied_once": (
                        _active_value_timeline(retry_rows, marked_id).get(2) == 122
                    ),
                },
                name="whole_tick_failure_and_retry",
            ),
            exact_match(
                after_failure_history,
                before_failure_history,
                name="failed_tick_committed_nothing",
            ),
            exact_match(
                plain_timeline,
                {0: 0, 1: 1, 2: 2, 3: 2, 4: 13, 5: 13, 6: 14},
                name="widened_then_narrowed_timeline",
            ),
            exact_match(
                marked_timeline,
                {0: 100, 1: 111, 2: 122, 3: 133, 4: 144, 5: 155, 6: 166},
                name="stable_marked_timeline",
            ),
            state_check(
                {
                    "marker_matches_only_while_plain_is_wide": (
                        _active_marker_ticks(marker_rows, plain_id) == [3, 4]
                    ),
                    "original_marker_remains_matched": (
                        _active_marker_ticks(marker_rows, marked_id) == list(range(7))
                    ),
                    "one_active_row_per_entity_tick": (
                        len(active_keys) == len(set(active_keys)) == 14
                    ),
                    "world_completed_six_successful_adversarial_steps": final_info.tick == 7,
                },
                name="signature_migration_matching",
            ),
        ]


# ---------------------------------------------------------------------------
# Register all capability tasks
# ---------------------------------------------------------------------------


def register(harness: EvalHarness) -> None:
    """Register all capability tasks on the harness."""
    harness.add(
        "storage_roundtrip",
        suite=SUITE,
        fn=task_storage_roundtrip,
        desc="Write entities with mixed types, flush to storage, read back, verify field integrity",
    )
    harness.add(
        "simulation_correctness",
        suite=SUITE,
        fn=task_simulation_correctness,
        desc="Multi-step simulation: entity preservation, processor correctness, data integrity",
    )
    harness.add(
        "fork_divergence",
        suite=SUITE,
        fn=task_fork_divergence,
        desc="Fork-of-fork lineage continuity, shared resources, and isolated branch mutations",
    )
    harness.add(
        "time_travel_and_run_id",
        suite=SUITE,
        fn=task_time_travel_and_run_id,
        desc="Live and cold historical reads preserve run identity across durable resume",
    )
    harness.add(
        "processor_adversarial",
        suite=SUITE,
        fn=task_processor_adversarial,
        desc="Hook isolation, atomic processor failure, retry, and migration-aware matching",
    )
