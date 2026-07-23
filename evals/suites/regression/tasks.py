# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Required repository checks for established behavior."""

from __future__ import annotations

import asyncio
import tempfile
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime

from daft import DataFrame, col
from uuid_utils import UUID, uuid7

import archetype.app.gateway.auth.guard as guard
from archetype import ArchetypeRuntime, RuntimeWorld, SyncRuntimeWorld
from archetype.app.container import ServiceContainer
from archetype.app.gateway.auth.errors import GuardrailError
from archetype.app.gateway.auth.guard import (
    estimate_token_cost,
    guardrail_allow,
    reset_daily_tokens,
    reset_tick_counters,
)
from archetype.app.gateway.auth.models import ActorCtx
from archetype.app.gateway.service import CommandGateway
from archetype.app.models import Command, CommandType
from archetype.core.aio.async_processor import AsyncProcessor
from archetype.core.archetype import Archetype
from archetype.core.component import Component
from archetype.core.config import RunConfig, StorageConfig, WorldConfig
from archetype.world.models import EpisodeConfig
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


class BlockingHealthIncrement(AsyncProcessor):
    """Hold one admitted step open so runtime shutdown ordering is observable."""

    components = (Health,)

    def __init__(self, entered: asyncio.Event, release: asyncio.Event) -> None:
        self.entered = entered
        self.release = release

    async def process(self, df: DataFrame, **kwargs) -> DataFrame:
        self.entered.set()
        await self.release.wait()
        return df.with_column("health__hp", col("health__hp") + 1)


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
# Task: Command pipeline (admit → schedule → dispatch → history)
# ---------------------------------------------------------------------------


def task_command_pipeline() -> list[GraderResult]:
    """Full durable command lifecycle through the internal composition root."""
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
            await container.command_gateway.submit(admin, wid, cmd)
            pending = await container.command_scheduler.pending_count(wid)

            # Step → drains
            rc = RunConfig()
            applied = await container.simulation_service.step(world.world_id, rc)
            pending_after = await container.command_scheduler.pending_count(wid)

            # History
            history = await container.audit_log.get_command_history(world.world_id)

            # RBAC at service boundary
            viewer_blocked = False
            try:
                await container.command_gateway.submit(
                    viewer,
                    wid,
                    Command(type=CommandType.SPAWN, payload={}),
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
# Task: cold service-layer query correctness
# ---------------------------------------------------------------------------


def task_query_correctness() -> list[GraderResult]:
    """Gated durable reads remain correct without a live AsyncWorld."""
    return asyncio.run(_task_query_correctness())


async def _task_query_correctness() -> list[GraderResult]:
    reset_tick_counters()
    reset_daily_tokens()

    with tempfile.TemporaryDirectory() as tmp:
        storage = StorageConfig(uri=f"{tmp}/store", namespace="eval_query")
        admin = ActorCtx(id=uuid7(), roles={"admin"})
        writer = ServiceContainer()
        try:
            info = await writer.command_gateway.create_world(
                admin,
                WorldConfig(name="query-correctness"),
                storage,
            )
            health_only = await writer.command_gateway.create_entity(
                admin,
                info.world_id,
                [Health(hp=10)],
            )
            tagged = await writer.command_gateway.create_entity(
                admin,
                info.world_id,
                [Health(hp=20), Tag(label="target")],
            )
            first_run = await writer.command_gateway.run(
                admin,
                info.world_id,
                RunConfig(num_steps=1),
            )
            await writer.command_gateway.update_entity(
                admin,
                info.world_id,
                health_only,
                [Health(hp=11)],
            )
            await writer.command_gateway.run(
                admin,
                info.world_id,
                RunConfig(num_steps=1),
            )
            world_id = str(info.world_id)
            run_id = str(first_run.run_id)
        finally:
            try:
                await writer.shutdown()
            finally:
                reset_tick_counters()
                reset_daily_tokens()

        # A new composition root has no live world object. Reads must cross
        # the public gate and resolve the durable QueryService path instead.
        reader = ServiceContainer()
        viewer = ActorCtx(id=uuid7(), roles={"viewer"})
        try:
            cold_reader = not reader.world_service.has_world(world_id)
            signatures = await reader.command_gateway.list_signatures(viewer, storage)
            tick_zero = (
                await reader.command_gateway.query_components(
                    viewer,
                    [Health],
                    world_id,
                    run_id,
                    storage,
                    ticks=[0],
                )
            ).to_pylist()
            selected = (
                await reader.command_gateway.query_components(
                    viewer,
                    [Health],
                    world_id,
                    run_id,
                    storage,
                    ticks=[1],
                    entity_ids=[health_only],
                )
            ).to_pylist()
            projected = (
                await reader.command_gateway.query_archetype(
                    viewer,
                    (Health, Tag),
                    world_id,
                    run_id,
                    storage,
                    ticks=[0],
                    components=[Health],
                )
            ).to_pylist()

            signature_names = {
                tuple(component.__name__ for component in signature) for signature in signatures
            }
            tick_zero_values = sorted((row["entity_id"], row["health__hp"]) for row in tick_zero)
            selected_values = [
                (row["entity_id"], row["tick"], row["health__hp"]) for row in selected
            ]
            projection = projected[0] if len(projected) == 1 else {}

            return [
                exact_match(cold_reader, True, name="no_live_world_shortcut"),
                exact_match(
                    tick_zero_values,
                    [(health_only, 10), (tagged, 20)],
                    name="subset_union_across_signatures",
                ),
                exact_match(
                    selected_values,
                    [(health_only, 1, 11)],
                    name="tick_and_entity_filters",
                ),
                state_check(
                    {
                        "one_exact_row": len(projected) == 1,
                        "exact_entity": projection.get("entity_id") == tagged,
                        "health_projected": projection.get("health__hp") == 20,
                        "tag_not_projected": "tag__label" not in projection,
                    },
                    name="exact_signature_projection",
                ),
                state_check(
                    {
                        "health_signature": ("Health",) in signature_names,
                        "health_tag_signature": ("Health", "Tag") in signature_names,
                    },
                    name="cold_signature_discovery",
                ),
            ]
        finally:
            try:
                await reader.shutdown()
            finally:
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
            info = await container.command_gateway.create_world(
                ctx, WorldConfig(name="quota"), storage
            )
            # 8 ticks × (spawn + step) = 16 gated commands, 4× the ceiling, but
            # only 2 per tick. A per-tick quota that resets each tick never trips.
            try:
                for _ in range(8):
                    await container.command_gateway.create_entity(
                        ctx, info.world_id, [Tag(label="x")]
                    )
                    await container.command_gateway.step(ctx, info.world_id, RunConfig())
            except Exception:
                blocked = True
        finally:
            guard.MAX_CMDS_PER_TICK = saved
            await container.shutdown()
            reset_tick_counters()
            reset_daily_tokens()

    return [exact_match(blocked, False, name="quota_resets_across_ticks")]


# ---------------------------------------------------------------------------
# Task: exact quota boundaries, atomic bulk accounting, and UTC rollover
# ---------------------------------------------------------------------------


def task_quota_boundaries() -> list[GraderResult]:
    """Quota accounting is exact, actor-local, atomic, and UTC-day scoped."""
    return asyncio.run(_task_quota_boundaries())


def _custom_commands(count: int) -> list[Command]:
    return [Command(type=CommandType.CUSTOM) for _ in range(count)]


async def _submit_allowed(
    service: CommandGateway,
    world_id: str | UUID,
    ctx: ActorCtx,
    count: int,
) -> bool:
    try:
        if count == 1:
            await service.submit(ctx, world_id, _custom_commands(1)[0])
        else:
            await service.submit_batch(ctx, world_id, _custom_commands(count))
    except GuardrailError:
        return False
    return True


def _guard_allowed(ctx: ActorCtx, now: datetime) -> bool:
    try:
        guardrail_allow(Command(type=CommandType.CUSTOM), ctx, now=now)
    except GuardrailError:
        return False
    return True


async def _task_quota_boundaries() -> list[GraderResult]:
    reset_tick_counters()
    reset_daily_tokens()
    saved_daily_limit = guard.MAX_TOKENS_PER_DAY

    with tempfile.TemporaryDirectory() as tmp:
        container = ServiceContainer()
        try:
            storage = StorageConfig(uri=f"{tmp}/store", namespace="eval_quota_boundaries")
            world = await container.world_service.create_world(
                WorldConfig(name="quota-boundaries"),
                storage,
            )
            actors = (
                ActorCtx(id=uuid7(), roles={"admin"}),
                ActorCtx(id=uuid7(), roles={"admin"}),
            )

            accepted_at_499 = await asyncio.gather(
                *(
                    _submit_allowed(container.command_gateway, world.world_id, actor, 499)
                    for actor in actors
                )
            )
            pending_at_499 = await container.command_scheduler.pending_count(world.world_id)

            bulk_overflow_allowed = await asyncio.gather(
                *(
                    _submit_allowed(container.command_gateway, world.world_id, actor, 2)
                    for actor in actors
                )
            )
            pending_after_bulk_rejection = await container.command_scheduler.pending_count(
                world.world_id
            )

            accepted_at_500 = await asyncio.gather(
                *(
                    _submit_allowed(container.command_gateway, world.world_id, actor, 1)
                    for actor in actors
                )
            )
            pending_at_500 = await container.command_scheduler.pending_count(world.world_id)

            command_501_allowed = await asyncio.gather(
                *(
                    _submit_allowed(container.command_gateway, world.world_id, actor, 1)
                    for actor in actors
                )
            )
            pending_after_501 = await container.command_scheduler.pending_count(world.world_id)

            reset_tick_counters()
            reset_daily_tokens()
            guard.MAX_TOKENS_PER_DAY = 20
            before_midnight = datetime(2030, 1, 1, 23, 59, 59, tzinfo=UTC)
            at_midnight = datetime(2030, 1, 2, tzinfo=UTC)
            guard._last_reset_date = before_midnight.date()
            daily_actor = ActorCtx(id=uuid7(), roles={"admin"})
            daily_peer = ActorCtx(id=uuid7(), roles={"admin"})

            daily_exact_limit = all(_guard_allowed(daily_actor, before_midnight) for _ in range(2))
            daily_over_limit_allowed = _guard_allowed(daily_actor, before_midnight)
            peer_allowed_same_day = _guard_allowed(daily_peer, before_midnight)

            actor_allowed_at_midnight = _guard_allowed(daily_actor, at_midnight)
            peer_exact_limit_after_rollover = all(
                _guard_allowed(daily_peer, at_midnight) for _ in range(2)
            )
            peer_over_limit_after_rollover = _guard_allowed(daily_peer, at_midnight)

            return [
                state_check(
                    {
                        "both_actors_accepted": all(accepted_at_499),
                        "499_each_queued": pending_at_499 == 998,
                    },
                    name="concurrent_actor_499_boundary",
                ),
                state_check(
                    {
                        "both_bulk_overflows_rejected": not any(bulk_overflow_allowed),
                        "queue_unchanged": pending_after_bulk_rejection == pending_at_499,
                        "quota_unchanged": all(accepted_at_500),
                    },
                    name="bulk_overflow_atomic",
                ),
                state_check(
                    {
                        "500_each_queued": pending_at_500 == 1000,
                        "both_501_commands_rejected": not any(command_501_allowed),
                        "rejection_did_not_enqueue": pending_after_501 == pending_at_500,
                    },
                    name="exact_500_501_boundary",
                ),
                state_check(
                    {
                        "exact_daily_budget_allowed": daily_exact_limit,
                        "next_token_cost_rejected": not daily_over_limit_allowed,
                        "peer_budget_is_independent": peer_allowed_same_day,
                    },
                    name="daily_budget_actor_isolation",
                ),
                state_check(
                    {
                        "blocked_actor_recovers_at_midnight": actor_allowed_at_midnight,
                        "peer_receives_full_new_budget": peer_exact_limit_after_rollover,
                        "new_day_budget_still_enforced": not peer_over_limit_after_rollover,
                    },
                    name="utc_midnight_rollover",
                ),
            ]
        finally:
            guard.MAX_TOKENS_PER_DAY = saved_daily_limit
            await container.shutdown()
            reset_tick_counters()
            reset_daily_tokens()


# ---------------------------------------------------------------------------
# Task: runtime activation, shutdown ordering, invalidation, and sync parity
# ---------------------------------------------------------------------------


def task_runtime_contracts() -> list[GraderResult]:
    """Compose the public runtime's activation and lifecycle boundaries."""
    reset_tick_counters()
    reset_daily_tokens()
    try:
        activation, shutdown = asyncio.run(_task_runtime_contracts())
        return [
            state_check(activation, name="lazy_single_flight_activation"),
            state_check(shutdown, name="wait_then_close_shutdown"),
            exact_match(
                _public_methods(SyncRuntimeWorld),
                _public_methods(RuntimeWorld),
                name="sync_async_world_surface",
            ),
        ]
    finally:
        reset_tick_counters()
        reset_daily_tokens()


def _public_methods(cls: type[object]) -> set[str]:
    return {name for name in dir(cls) if not name.startswith("_") and callable(getattr(cls, name))}


def _raises_runtime_error(operation: Callable[[], object]) -> bool:
    try:
        operation()
    except RuntimeError:
        return True
    return False


async def _raises_runtime_error_async(
    operation: Callable[[], Awaitable[object]],
) -> bool:
    try:
        await operation()
    except RuntimeError:
        return True
    return False


async def _task_runtime_contracts() -> tuple[dict[str, bool], dict[str, bool]]:
    with tempfile.TemporaryDirectory() as tmp:
        entered = asyncio.Event()
        release = asyncio.Event()
        runtime = ArchetypeRuntime()
        storage = StorageConfig(
            uri=f"{tmp}/store",
            namespace="eval_runtime_contracts",
        )
        world = runtime.world(
            "runtime-contracts",
            storage=storage,
        )

        pre_activation_rejected = _raises_runtime_error(lambda: world.world_id)
        entity_ids = await asyncio.gather(*(world.spawn(Health(hp=index)) for index in range(12)))
        info = await world.info()
        audit_rows = (await world.history(limit=100)).to_pylist()
        create_events = [row for row in audit_rows if row["command_type"] == "create_world"]
        discovered = await runtime.discover(storage)

        await world.step()  # persist raw initial conditions
        await world.add_processor(BlockingHealthIncrement(entered, release))
        step_task = asyncio.create_task(world.step())
        await asyncio.wait_for(entered.wait(), timeout=5)

        shutdown_task = asyncio.create_task(runtime.shutdown())
        await asyncio.sleep(0)  # shutdown stops admission before waiting on op_lock
        shutdown_started = _raises_runtime_error(lambda: runtime.world("too-late"))
        shutdown_waited = not shutdown_task.done()

        release.set()
        await asyncio.wait_for(asyncio.gather(step_task, shutdown_task), timeout=10)
        handle_invalidated = await _raises_runtime_error_async(world.info)
        await runtime.shutdown()  # idempotent after the first completion

        activation = {
            "property_rejects_before_activation": pre_activation_rejected,
            "one_durable_world": len(discovered) == 1 and discovered[0].world_id == info.world_id,
            "trusted_runtime_fabricates_no_access_audit": not create_events,
            "all_spawns_returned": len(entity_ids) == 12,
            "entity_ids_are_unique": len(set(entity_ids)) == 12,
            "handle_and_info_share_world": str(world.world_id) == str(info.world_id),
        }
        shutdown = {
            "new_handles_rejected_during_shutdown": shutdown_started,
            "shutdown_waited_for_step": shutdown_waited,
            "in_flight_step_completed": (
                step_task.done() and not step_task.cancelled() and step_task.exception() is None
            ),
            "shutdown_completed": (
                shutdown_task.done()
                and not shutdown_task.cancelled()
                and shutdown_task.exception() is None
            ),
            "existing_handle_invalidated": handle_invalidated,
        }
        return activation, shutdown


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
            world = await container.world_service.create_world(WorldConfig(name="term"), storage)
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
        desc="Authorize → admit → schedule → dispatch → history at the API boundary",
    )
    harness.add(
        "query_correctness",
        suite=SUITE,
        fn=task_query_correctness,
        desc="Cold gated component/archetype reads, filters, projection, and discovery",
    )
    harness.add(
        "tick_quota_resets",
        suite=SUITE,
        fn=task_tick_quota_resets,
        desc="Per-tick RBAC command quota resets each tick, not process-wide (B1)",
    )
    harness.add(
        "quota_boundaries",
        suite=SUITE,
        fn=task_quota_boundaries,
        desc="Exact per-tick and daily quota edges, atomic bulk debit, and actor isolation",
    )
    harness.add(
        "runtime_contracts",
        suite=SUITE,
        fn=task_runtime_contracts,
        desc="Lazy activation, wait-then-close shutdown, handle invalidation, and sync parity",
    )
    harness.add(
        "episode_value_termination",
        suite=SUITE,
        fn=task_episode_value_termination,
        desc="run_episode stops on terminal_field latched, not at max_steps (B2)",
    )
