# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Regression eval suite: tasks that must always pass (~100%).

These protect against backsliding.  A decline in score signals something
is broken.  Tasks here were once capability evals that graduated to
regression status after reaching consistent pass rates.

Graders are deterministic (code-based) since these test well-understood,
stable behavior.
"""

from __future__ import annotations

import asyncio
import tempfile

from daft import DataFrame, col
from uuid_utils import uuid7

import archetype.app.auth.guard as guard
from archetype.app.auth.guard import (
    estimate_token_cost,
    guardrail_allow,
    reset_daily_tokens,
    reset_tick_counters,
)
from archetype.app.auth.models import ActorCtx
from archetype.app.container import ServiceContainer
from archetype.app.models import Command, CommandType, EpisodeConfig
from archetype.core.aio.async_processor import AsyncProcessor
from archetype.core.archetype import Archetype
from archetype.core.component import Component
from archetype.core.config import RunConfig, StorageConfig, WorldConfig
from evals.graders import exact_match, state_check
from evals.harness import EvalHarness
from evals.types import GraderResult

# ---------------------------------------------------------------------------
# Shared test components
# ---------------------------------------------------------------------------

SUITE = "regression"


class Health(Component):
    hp: int


class Tag(Component):
    label: str


class Countdown(Component):
    """Counts up each tick; ``done`` latches once ``step`` reaches ``goal``."""

    step: int = 0
    goal: int = 1
    done: bool = False


class CountToGoal(AsyncProcessor):
    components = (Countdown,)
    priority = 10

    async def process(self, df: DataFrame, **kwargs) -> DataFrame:
        nxt = col("countdown__step") + 1
        return df.with_column("countdown__step", nxt).with_column(
            "countdown__done", (nxt >= col("countdown__goal")) | col("countdown__done")
        )


# ---------------------------------------------------------------------------
# Task: Component serialization round-trip
# ---------------------------------------------------------------------------


def task_component_serde() -> list[GraderResult]:
    """Component → row_dict → verify prefixed keys and values."""
    h = Health(hp=42)
    row = h.to_row_dict()

    return [
        exact_match(row, {"health__hp": 42}, name="row_dict_values"),
        exact_match(Health.get_prefix(), "health__", name="prefix_format"),
        exact_match(
            set(f.name for f in Health.get_prefixed_schema()),
            {"health__hp"},
            name="schema_fields",
        ),
    ]


# ---------------------------------------------------------------------------
# Task: Archetype signature stability
# ---------------------------------------------------------------------------


def task_archetype_signatures() -> list[GraderResult]:
    """Signatures are sorted, deterministic, and support set operations."""
    sig_ht = Archetype.sig_from_components([Tag(label="x"), Health(hp=0)])
    sig_th = Archetype.sig_from_components([Health(hp=0), Tag(label="x")])

    sig_h = Archetype.remove_components(sig_ht, [Tag])

    schema = Archetype.get_archetype_schema(sig_ht)
    field_names = {f.name for f in schema}

    row = Archetype.to_row_dict(
        entity_id=1,
        tick=0,
        components=[Health(hp=99), Tag(label="a")],
        world_id="w",
        run_id="r",
    )

    return [
        # Order invariance
        exact_match(sig_ht, sig_th, name="order_invariant"),
        # Sorted by class name
        exact_match([t.__name__ for t in sig_ht], ["Health", "Tag"], name="sorted_order"),
        # Remove produces correct result
        exact_match([t.__name__ for t in sig_h], ["Health"], name="remove_op"),
        # Deterministic naming
        exact_match(Archetype.get_name(sig_ht), Archetype.get_name(sig_ht), name="name_stable"),
        # Different sigs → different names
        exact_match(
            Archetype.get_name(sig_ht) != Archetype.get_name(sig_h),
            True,
            name="name_unique",
        ),
        # Schema includes base + component fields
        state_check(
            {
                "entity_id": "entity_id" in field_names,
                "tick": "tick" in field_names,
                "health__hp": "health__hp" in field_names,
                "tag__label": "tag__label" in field_names,
            },
            name="schema_completeness",
        ),
        # Row dict contains correct values
        exact_match(row["health__hp"], 99, name="row_hp"),
        exact_match(row["tag__label"], "a", name="row_label"),
        exact_match(row["entity_id"], 1, name="row_entity_id"),
    ]


# ---------------------------------------------------------------------------
# Task: RBAC permission enforcement
# ---------------------------------------------------------------------------


def task_rbac_enforcement() -> list[GraderResult]:
    """RBAC correctly allows/denies commands per role."""
    reset_tick_counters()
    reset_daily_tokens()

    admin = ActorCtx(id=uuid7(), roles={"admin"})
    viewer = ActorCtx(id=uuid7(), roles={"viewer"})
    player = ActorCtx(id=uuid7(), roles={"player"})

    results = [
        # Admin allowed everything
        exact_match(
            _is_allowed(Command(type=CommandType.SPAWN, payload={}), admin),
            True,
            name="admin_spawn",
        ),
        exact_match(
            _is_allowed(Command(type=CommandType.ADD_PROCESSOR, payload={}), admin),
            True,
            name="admin_add_processor",
        ),
        # Viewer denied mutations
        exact_match(
            _is_allowed(Command(type=CommandType.SPAWN, payload={}), viewer),
            False,
            name="viewer_denied_spawn",
        ),
        exact_match(
            _is_allowed(Command(type=CommandType.DESPAWN, payload={}), viewer),
            False,
            name="viewer_denied_despawn",
        ),
        # Player: can spawn, cannot add_processor
        exact_match(
            _is_allowed(Command(type=CommandType.SPAWN, payload={}), player),
            True,
            name="player_spawn",
        ),
    ]

    reset_tick_counters()
    reset_daily_tokens()

    results.append(
        exact_match(
            _is_allowed(Command(type=CommandType.ADD_PROCESSOR, payload={}), player),
            False,
            name="player_denied_add_processor",
        ),
    )

    reset_tick_counters()
    reset_daily_tokens()

    # Token costs are all positive
    all_positive = all(estimate_token_cost(Command(type=ct, payload={})) > 0 for ct in CommandType)
    results.append(exact_match(all_positive, True, name="token_costs_positive"))

    # Default role is viewer
    results.append(
        exact_match(ActorCtx(id=uuid7()).roles, {"viewer"}, name="default_role_viewer"),
    )

    reset_tick_counters()
    reset_daily_tokens()
    return results


def _is_allowed(cmd: Command, ctx: ActorCtx) -> bool:
    reset_tick_counters()
    reset_daily_tokens()
    try:
        guardrail_allow(cmd, ctx)
        return True
    except PermissionError:
        return False


# ---------------------------------------------------------------------------
# Task: Command ordering
# ---------------------------------------------------------------------------


def task_command_ordering() -> list[GraderResult]:
    """Commands sort by (tick, priority, seq)."""
    a = Command(type=CommandType.SPAWN, tick=0, payload={})
    b = Command(type=CommandType.SPAWN, tick=1, payload={})
    c = Command(type=CommandType.SPAWN, tick=0, priority=0, payload={})
    d = Command(type=CommandType.SPAWN, tick=0, priority=1, payload={})
    e = Command(type=CommandType.SPAWN, tick=0, priority=0, payload={})
    f = Command(type=CommandType.SPAWN, tick=0, priority=0, payload={})

    return [
        exact_match(a < b, True, name="lower_tick_first"),
        exact_match(c < d, True, name="lower_priority_first"),
        exact_match(e < f, True, name="earlier_seq_first"),
        exact_match(
            len({Command(type=CommandType.CUSTOM, payload={}).id for _ in range(50)}),
            50,
            name="unique_ids",
        ),
    ]


# ---------------------------------------------------------------------------
# Task: Command pipeline (submit → step → history)
# ---------------------------------------------------------------------------


def task_command_pipeline() -> list[GraderResult]:
    """Full command lifecycle through ServiceContainer."""
    return asyncio.run(_task_command_pipeline())


async def _task_command_pipeline() -> list[GraderResult]:
    reset_tick_counters()
    reset_daily_tokens()

    with tempfile.TemporaryDirectory() as tmp:
        container = ServiceContainer()
        try:
            storage = StorageConfig(uri=f"{tmp}/store", namespace="eval_reg")
            world = await container.world_service.create_world(
                WorldConfig(name="reg-pipeline"),
                storage,
            )
            wid = str(world.world_id)
            admin = ActorCtx(id=uuid7(), roles={"admin"})
            viewer = ActorCtx(id=uuid7(), roles={"viewer"})

            # Submit → pending count
            cmd = Command(type=CommandType.SPAWN, tick=0, payload={"components": []})
            await container.command_service.submit(wid, cmd, admin)
            pending = await container.broker.get_pending_count(wid)

            # Step → drains
            rc = RunConfig()
            applied = await container.simulation_service.step(world.world_id, rc)
            pending_after = await container.broker.get_pending_count(wid)

            # History
            history = await container.query_service.get_command_history(world.world_id)

            # RBAC at service boundary
            viewer_blocked = False
            try:
                await container.command_service.submit(
                    wid,
                    Command(type=CommandType.SPAWN, payload={}),
                    viewer,
                )
            except PermissionError:
                viewer_blocked = True

            return [
                exact_match(pending, 1, name="submit_enqueues"),
                exact_match(applied, 1, name="step_drains"),
                exact_match(pending_after, 0, name="pending_cleared"),
                exact_match(len(history), 1, name="history_recorded"),
                exact_match(history[0].type, CommandType.SPAWN, name="history_type"),
                exact_match(viewer_blocked, True, name="rbac_at_boundary"),
            ]
        finally:
            await container.shutdown()
            reset_tick_counters()
            reset_daily_tokens()


# ---------------------------------------------------------------------------
# Task: per-tick RBAC quota resets across ticks (bug B1)
# ---------------------------------------------------------------------------


def task_tick_quota_resets() -> list[GraderResult]:
    """The per-tick command quota must NOT accumulate process-wide."""
    return asyncio.run(_task_tick_quota_resets())


async def _task_tick_quota_resets() -> list[GraderResult]:
    reset_tick_counters()
    reset_daily_tokens()

    saved = guard.MAX_CMDS_PER_TICK
    guard.MAX_CMDS_PER_TICK = 4  # so a few ticks would blow a process-wide counter
    blocked = False
    with tempfile.TemporaryDirectory() as tmp:
        container = ServiceContainer()
        try:
            storage = StorageConfig(uri=f"{tmp}/store", namespace="eval_quota")
            ctx = ActorCtx(id=uuid7(), roles={"admin"})
            info = await container.command_service.create_world(
                ctx, WorldConfig(name="quota"), storage
            )
            # 8 ticks × (spawn + step) = 16 gated commands, 4× the ceiling, but
            # only 2 per tick. A per-tick quota that resets each tick never trips.
            try:
                for _ in range(8):
                    await container.command_service.create_entity(ctx, info.world_id, [Tag(label="x")])
                    await container.command_service.step(ctx, info.world_id, RunConfig())
            except Exception:
                blocked = True
        finally:
            guard.MAX_CMDS_PER_TICK = saved
            await container.shutdown()
            reset_tick_counters()
            reset_daily_tokens()

    return [exact_match(blocked, False, name="quota_resets_across_ticks")]


# ---------------------------------------------------------------------------
# Task: value-based "all done" episode termination (bug B2)
# ---------------------------------------------------------------------------


def task_episode_value_termination() -> list[GraderResult]:
    """run_episode stops on the data (terminal_field latched), not at max_steps."""
    return asyncio.run(_task_episode_value_termination())


async def _task_episode_value_termination() -> list[GraderResult]:
    reset_tick_counters()
    reset_daily_tokens()

    with tempfile.TemporaryDirectory() as tmp:
        container = ServiceContainer()
        try:
            storage = StorageConfig(uri=f"{tmp}/store", namespace="eval_term")
            world = await container.world_service.create_world(
                WorldConfig(name="term"), storage
            )
            await world.add_processor(CountToGoal())
            await world.create_entity([Countdown(goal=3)])

            result = await container.simulation_service.run_episode(
                world.world_id,
                EpisodeConfig(
                    max_steps=50,
                    terminal_component=Countdown,
                    terminal_field="done",
                    terminal_all=True,
                ),
            )
            return [
                exact_match(result.terminated, True, name="value_termination_fires"),
                state_check(
                    {
                        "stopped_before_cap": result.duration_steps < 50,
                        "ran_past_tick0": result.duration_steps > 1,
                    },
                    name="not_capped_not_structural",
                ),
            ]
        finally:
            await container.shutdown()
            reset_tick_counters()
            reset_daily_tokens()


# ---------------------------------------------------------------------------
# Register all regression tasks
# ---------------------------------------------------------------------------


def register(harness: EvalHarness) -> None:
    """Register all regression tasks on the harness."""
    harness.add(
        "component_serde",
        suite=SUITE,
        fn=task_component_serde,
        desc="Component serialization round-trip (row_dict, prefix, schema)",
    )
    harness.add(
        "archetype_signatures",
        suite=SUITE,
        fn=task_archetype_signatures,
        desc="Signature stability, set operations, naming, schema composition",
    )
    harness.add(
        "rbac_enforcement",
        suite=SUITE,
        fn=task_rbac_enforcement,
        desc="RBAC allows/denies per role, token costs, default role",
    )
    harness.add(
        "command_ordering",
        suite=SUITE,
        fn=task_command_ordering,
        desc="Command ordering by (tick, priority, seq)",
    )
    harness.add(
        "command_pipeline",
        suite=SUITE,
        fn=task_command_pipeline,
        desc="Submit → broker → step → history + RBAC at service boundary",
    )
    harness.add(
        "tick_quota_resets",
        suite=SUITE,
        fn=task_tick_quota_resets,
        desc="Per-tick RBAC command quota resets each tick, not process-wide (B1)",
    )
    harness.add(
        "episode_value_termination",
        suite=SUITE,
        fn=task_episode_value_termination,
        desc="run_episode stops on terminal_field latched, not at max_steps (B2)",
    )
