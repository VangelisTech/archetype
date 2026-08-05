# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Required repository checks for established behavior."""

from __future__ import annotations

import asyncio
import tempfile
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime

from daft import DataFrame, col
from uuid_utils import uuid7

from archetype import ArchetypeRuntime, RuntimeWorld, SyncRuntimeWorld
from archetype.commands.models import (
    ActorCtx,
    DeferredItem,
    DurableOptions,
    PolicyRequest,
)
from archetype.commands.policy import Policy
from archetype.core.aio.async_processor import AsyncProcessor
from archetype.core.archetype import Archetype
from archetype.core.component import Component
from archetype.core.config import RunConfig, StorageConfig, WorldConfig
from archetype.world.models import (
    CreateWorld,
    Despawn,
    EpisodeConfig,
    ListSignatures,
    QueryArchetype,
    QueryComponents,
    Run,
    Spawn,
    Step,
    Update,
)
from evals.graders import exact_match, state_check
from evals.harness import EvalHarness
from evals.infra.runtime import component_refs, component_values, isolated_eval_process
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
    policy = Policy()
    admin = ActorCtx(id=uuid7(), roles={"admin"})
    viewer = ActorCtx(id=uuid7(), roles={"viewer"})
    player = ActorCtx(id=uuid7(), roles={"player"})

    results = [
        # Admin allowed everything
        exact_match(
            _is_allowed(policy, "spawn", admin),
            True,
            name="admin_spawn",
        ),
        exact_match(
            _is_allowed(policy, "add_processor", admin),
            True,
            name="admin_add_processor",
        ),
        # Viewer denied mutations
        exact_match(
            _is_allowed(policy, "spawn", viewer),
            False,
            name="viewer_denied_spawn",
        ),
        exact_match(
            _is_allowed(policy, "despawn", viewer),
            False,
            name="viewer_denied_despawn",
        ),
        # Player: can spawn, cannot add_processor
        exact_match(
            _is_allowed(policy, "spawn", player),
            True,
            name="player_spawn",
        ),
    ]

    results.append(
        exact_match(
            _is_allowed(policy, "add_processor", player),
            False,
            name="player_denied_add_processor",
        ),
    )

    with tempfile.TemporaryDirectory() as tmp:
        process = isolated_eval_process(tmp)
        try:
            all_positive = all(
                callable(spec.token_cost)
                or (
                    isinstance(spec.token_cost, int)
                    and not isinstance(spec.token_cost, bool)
                    and spec.token_cost >= 0
                )
                for spec in process.registry.specs
            )
        finally:
            asyncio.run(process.aclose())
    results.append(exact_match(all_positive, True, name="token_costs_nonnegative"))

    # Default role is viewer
    results.append(
        exact_match(ActorCtx(id=uuid7()).roles, {"viewer"}, name="default_role_viewer"),
    )

    return results


def _is_allowed(policy: Policy, permission: str, ctx: ActorCtx) -> bool:
    try:
        policy.preauthorize(ctx, permission=permission)
        return True
    except PermissionError:
        return False


# ---------------------------------------------------------------------------
# Task: Command ordering
# ---------------------------------------------------------------------------


def task_command_ordering() -> list[GraderResult]:
    """The control catalog orders by target tick, priority, then sequence."""
    return asyncio.run(_task_command_ordering())


async def _task_command_ordering() -> list[GraderResult]:
    with tempfile.TemporaryDirectory() as tmp:
        process = isolated_eval_process(tmp)
        try:
            world = await process.dispatcher.apply(
                CreateWorld(
                    config=WorldConfig(name="command-ordering"),
                    storage_config=StorageConfig(
                        uri=f"{tmp}/store",
                        namespace="eval_ordering",
                    ),
                )
            )
            actor = ActorCtx(id=uuid7(), roles={"admin"})
            command_ids = [uuid7() for _ in range(4)]
            items = (
                DeferredItem(
                    Despawn(world_id=world.world_id, entity_id=1),
                    DurableOptions(target_tick=0, priority=1),
                    command_id=command_ids[0],
                ),
                DeferredItem(
                    Despawn(world_id=world.world_id, entity_id=2),
                    DurableOptions(target_tick=1, priority=0),
                    command_id=command_ids[1],
                ),
                DeferredItem(
                    Despawn(world_id=world.world_id, entity_id=3),
                    DurableOptions(target_tick=0, priority=0),
                    command_id=command_ids[2],
                ),
                DeferredItem(
                    Despawn(world_id=world.world_id, entity_id=4),
                    DurableOptions(target_tick=0, priority=0),
                    command_id=command_ids[3],
                ),
            )
            await process.dispatcher.defer_batch_as(actor, items)
            records = await process.scheduler.records(world.world_id)
        finally:
            await process.aclose()

    by_id = {str(record.command_id): record for record in records}
    ordered = sorted(
        records,
        key=lambda record: (
            record.scheduled_tick,
            record.priority,
            record.sequence,
        ),
    )
    return [
        exact_match(
            [str(record.command_id) for record in ordered],
            [
                str(command_ids[2]),
                str(command_ids[3]),
                str(command_ids[0]),
                str(command_ids[1]),
            ],
            name="tick_priority_sequence_order",
        ),
        state_check(
            {
                "catalog_assigned_unique_sequences": len({record.sequence for record in records})
                == 4,
                "earlier_equal_key_has_lower_sequence": by_id[str(command_ids[2])].sequence
                < by_id[str(command_ids[3])].sequence,
                "caller_ids_preserved": set(by_id)
                == {str(command_id) for command_id in command_ids},
            },
            name="catalog_owned_sequence",
        ),
    ]


# ---------------------------------------------------------------------------
# Task: Command pipeline (admit → schedule → dispatch → history)
# ---------------------------------------------------------------------------


def task_command_pipeline() -> list[GraderResult]:
    """Full durable command lifecycle through the internal composition root."""
    return asyncio.run(_task_command_pipeline())


async def _task_command_pipeline() -> list[GraderResult]:
    with tempfile.TemporaryDirectory() as tmp:
        process = isolated_eval_process(tmp)
        try:
            storage = StorageConfig(uri=f"{tmp}/store", namespace="eval_reg")
            admin = ActorCtx(id=uuid7(), roles={"admin"})
            viewer = ActorCtx(id=uuid7(), roles={"viewer"})
            world = await process.dispatcher.apply_as(
                admin,
                CreateWorld(
                    config=WorldConfig(name="reg-pipeline"),
                    storage_config=storage,
                ),
            )
            wid = str(world.world_id)

            # Submit → pending count
            command_id = uuid7()
            spawn = Spawn(world_id=wid)
            await process.dispatcher.defer_as(
                admin,
                spawn,
                DurableOptions(target_tick=0),
                command_id=command_id,
            )
            pending = await process.scheduler.pending_count(wid)

            # Step → drains
            applied = await process.dispatcher.apply(Step(world_id=wid, run_config=RunConfig()))
            pending_after = await process.scheduler.pending_count(wid)

            # Durable command ledger
            records = await process.scheduler.records(world.world_id)

            # RBAC at service boundary
            viewer_blocked = False
            try:
                await process.dispatcher.defer_as(
                    viewer,
                    Spawn(world_id=wid),
                    DurableOptions(target_tick=1),
                )
            except PermissionError:
                viewer_blocked = True

            return [
                exact_match(pending, 1, name="submit_enqueues"),
                exact_match(applied, 1, name="step_drains"),
                exact_match(pending_after, 0, name="pending_cleared"),
                exact_match(len(records), 1, name="history_recorded"),
                state_check(
                    {
                        "operation_name": records[0].command_type == "spawn",
                        "terminal_status": records[0].status == "APPLIED",
                        "identity_preserved": str(records[0].command_id) == str(command_id),
                    },
                    name="history_operation_identity",
                ),
                exact_match(viewer_blocked, True, name="rbac_at_boundary"),
            ]
        finally:
            await process.aclose()


# ---------------------------------------------------------------------------
# Task: cold service-layer query correctness
# ---------------------------------------------------------------------------


def task_query_correctness() -> list[GraderResult]:
    """Trusted durable reads remain correct without a live AsyncWorld."""
    return asyncio.run(_task_query_correctness())


async def _task_query_correctness() -> list[GraderResult]:
    with tempfile.TemporaryDirectory() as tmp:
        storage = StorageConfig(uri=f"{tmp}/store", namespace="eval_query")
        admin = ActorCtx(id=uuid7(), roles={"admin"})
        writer = isolated_eval_process(tmp)
        try:
            info = await writer.dispatcher.apply_as(
                admin,
                CreateWorld(
                    config=WorldConfig(name="query-correctness"),
                    storage_config=storage,
                ),
            )
            health_only = await writer.dispatcher.apply_as(
                admin,
                Spawn.from_components(
                    world_id=info.world_id,
                    components=[Health(hp=10)],
                ),
            )
            tagged = await writer.dispatcher.apply_as(
                admin,
                Spawn.from_components(
                    world_id=info.world_id,
                    components=[Health(hp=20), Tag(label="target")],
                ),
            )
            first_run = await writer.dispatcher.apply_as(
                admin,
                Run(
                    world_id=info.world_id,
                    run_config=RunConfig(num_steps=1),
                ),
            )
            await writer.dispatcher.apply_as(
                admin,
                Update(
                    world_id=info.world_id,
                    entity_id=health_only,
                    components=component_values([Health(hp=11)]),
                ),
            )
            await writer.dispatcher.apply_as(
                admin,
                Run(
                    world_id=info.world_id,
                    run_config=RunConfig(num_steps=1),
                ),
            )
            world_id = str(info.world_id)
            run_id = str(first_run.run_id)
        finally:
            await writer.aclose()

        # A new canonical process graph has no live world object. Exact query
        # operations resolve the durable storage path instead.
        reader = isolated_eval_process(tmp)
        try:
            cold_reader = not await reader.worlds.contains(world_id)
            signatures = await reader.dispatcher.apply(ListSignatures(storage_config=storage))
            tick_zero = (
                await reader.dispatcher.apply(
                    QueryComponents(
                        components=component_refs([Health]),
                        world_id=world_id,
                        run_id=run_id,
                        storage_config=storage,
                        ticks=(0,),
                    )
                )
            ).to_pylist()
            selected = (
                await reader.dispatcher.apply(
                    QueryComponents(
                        components=component_refs([Health]),
                        world_id=world_id,
                        run_id=run_id,
                        storage_config=storage,
                        ticks=(1,),
                        entity_ids=(health_only,),
                    )
                )
            ).to_pylist()
            projected = (
                await reader.dispatcher.apply(
                    QueryArchetype(
                        signature=component_refs([Health, Tag]),
                        world_id=world_id,
                        run_id=run_id,
                        storage_config=storage,
                        ticks=(0,),
                        components=component_refs([Health]),
                    )
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
            await reader.aclose()


# ---------------------------------------------------------------------------
# Task: per-tick RBAC quota resets across ticks (bug B1)
# ---------------------------------------------------------------------------


def task_tick_quota_resets() -> list[GraderResult]:
    """Target-tick generations remain independent within one policy."""
    return _task_tick_quota_resets()


def _task_tick_quota_resets() -> list[GraderResult]:
    policy = Policy(max_commands_per_tick=4, max_tokens_per_day=1_000_000)
    actor = ActorCtx(id=uuid7(), roles={"operator"})
    blocked = False
    try:
        # Eight generations contain two commands each. A process-global
        # counter would reject this after generation two.
        for target_tick in range(8):
            policy.authorize(
                actor,
                permission="spawn",
                world_id="eval-quota",
                target_tick=target_tick,
            )
            policy.authorize(
                actor,
                permission="step",
                world_id="eval-quota",
                target_tick=target_tick,
            )
    except PermissionError:
        blocked = True

    return [exact_match(blocked, False, name="quota_resets_across_ticks")]


# ---------------------------------------------------------------------------
# Task: exact quota boundaries, atomic bulk accounting, and UTC rollover
# ---------------------------------------------------------------------------


def task_quota_boundaries() -> list[GraderResult]:
    """Quota accounting is exact, actor-local, atomic, and UTC-day scoped."""
    return _task_quota_boundaries()


def _policy_batch_allowed(
    policy: Policy,
    actor: ActorCtx,
    *,
    count: int,
) -> bool:
    requests = tuple(
        PolicyRequest(
            permission="spawn",
            world_id="eval-quota-boundaries",
            target_tick=0,
        )
        for _ in range(count)
    )
    try:
        policy.authorize_batch(actor, requests=requests)
    except PermissionError:
        return False
    return True


@dataclass(slots=True)
class _PolicyClock:
    value: datetime

    def __call__(self) -> datetime:
        return self.value


def _daily_allowed(policy: Policy, actor: ActorCtx) -> bool:
    try:
        policy.authorize(
            actor,
            permission="spawn",
            world_id="eval-daily-budget",
            target_tick=0,
            token_cost=10,
        )
    except PermissionError:
        return False
    return True


def _task_quota_boundaries() -> list[GraderResult]:
    policy = Policy(max_commands_per_tick=500, max_tokens_per_day=1_000_000)
    actors = (
        ActorCtx(id=uuid7(), roles={"admin"}),
        ActorCtx(id=uuid7(), roles={"admin"}),
    )
    accepted_counts = [0, 0]

    accepted_at_499 = [_policy_batch_allowed(policy, actor, count=499) for actor in actors]
    for index, accepted in enumerate(accepted_at_499):
        if accepted:
            accepted_counts[index] += 499
    pending_at_499 = sum(accepted_counts)

    bulk_overflow_allowed = [_policy_batch_allowed(policy, actor, count=2) for actor in actors]
    for index, accepted in enumerate(bulk_overflow_allowed):
        if accepted:
            accepted_counts[index] += 2
    pending_after_bulk_rejection = sum(accepted_counts)

    accepted_at_500 = [_policy_batch_allowed(policy, actor, count=1) for actor in actors]
    for index, accepted in enumerate(accepted_at_500):
        if accepted:
            accepted_counts[index] += 1
    pending_at_500 = sum(accepted_counts)

    command_501_allowed = [_policy_batch_allowed(policy, actor, count=1) for actor in actors]
    for index, accepted in enumerate(command_501_allowed):
        if accepted:
            accepted_counts[index] += 1
    pending_after_501 = sum(accepted_counts)

    clock = _PolicyClock(datetime(2030, 1, 1, 23, 59, 59, tzinfo=UTC))
    daily_policy = Policy(
        max_commands_per_tick=500,
        max_tokens_per_day=20,
        utcnow=clock,
    )
    daily_actor = ActorCtx(id=uuid7(), roles={"admin"})
    daily_peer = ActorCtx(id=uuid7(), roles={"admin"})

    daily_exact_limit = all(_daily_allowed(daily_policy, daily_actor) for _ in range(2))
    daily_over_limit_allowed = _daily_allowed(daily_policy, daily_actor)
    peer_allowed_same_day = _daily_allowed(daily_policy, daily_peer)

    clock.value = datetime(2030, 1, 2, tzinfo=UTC)
    actor_allowed_at_midnight = _daily_allowed(daily_policy, daily_actor)
    peer_exact_limit_after_rollover = all(
        _daily_allowed(daily_policy, daily_peer) for _ in range(2)
    )
    peer_over_limit_after_rollover = _daily_allowed(daily_policy, daily_peer)

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


# ---------------------------------------------------------------------------
# Task: runtime activation, shutdown ordering, invalidation, and sync parity
# ---------------------------------------------------------------------------


def task_runtime_contracts() -> list[GraderResult]:
    """Compose the public runtime's activation and lifecycle boundaries."""
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
    with tempfile.TemporaryDirectory() as tmp:
        async with ArchetypeRuntime() as runtime:
            storage = StorageConfig(uri=f"{tmp}/store", namespace="eval_term")
            world = runtime.world(
                "term",
                storage=storage,
                processors=[CountToGoal()],
            )
            await world.spawn(Countdown(goal=3))

            result = await world.run_episode(
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
        desc="Cold trusted component/archetype reads, filters, projection, and discovery",
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
