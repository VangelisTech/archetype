# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Idempotency eval suite.

These tasks execute the idempotency matrix in ``docs/guide/specification.md``.
They intentionally cover both sides of the contract:

- operations marked idempotent collapse, reuse, or no-op on repetition
- operations marked non-idempotent produce distinct observable effects
"""

from __future__ import annotations

import asyncio
import logging
import tempfile
from dataclasses import dataclass
from pathlib import Path

from uuid_utils import uuid7

from archetype.app.auth.guard import reset_daily_tokens, reset_tick_counters
from archetype.app.auth.models import ActorCtx
from archetype.app.broker import CommandBroker
from archetype.app.container import ServiceContainer
from archetype.app.models import Command, CommandType
from archetype.app.storage_service import StorageService
from archetype.app.world_service import WorldService
from archetype.core.archetype import Archetype
from archetype.core.component import Component
from archetype.core.config import CacheConfig, RunConfig, StorageConfig, WorldConfig
from archetype.core.hooks import OnComponentAdded, OnComponentRemoved
from archetype.core.sync import QueryManager
from evals.graders import exact_match, state_check
from evals.harness import EvalHarness
from evals.types import GraderResult

SUITE = "idempotency"
ROOT = Path(__file__).resolve().parents[2]
SPECIFICATION = ROOT / "docs" / "guide" / "specification.md"


@dataclass(frozen=True)
class IdempotencyCase:
    """One row from the normative idempotency matrix mapped to an eval task."""

    operation: str
    expected_contract: str
    task_id: str


IDEMPOTENCY_CASES: tuple[IdempotencyCase, ...] = (
    IdempotencyCase(
        operation="`StorageService.get_or_create_store(key)`",
        expected_contract=(
            "Idempotent per `(uri, namespace, backend, cache config)` within one service instance"
        ),
        task_id="idempotency.storage_pooling_and_shutdown",
    ),
    IdempotencyCase(
        operation="`WorldService.create_world(world_id=X)`",
        expected_contract="Idempotent by explicit `world_id`",
        task_id="idempotency.world_lifecycle",
    ),
    IdempotencyCase(
        operation="`WorldService.destroy_world(missing)`",
        expected_contract="Safe no-op",
        task_id="idempotency.world_lifecycle",
    ),
    IdempotencyCase(
        operation="`AsyncCachedStore.shutdown()`",
        expected_contract="Idempotent",
        task_id="idempotency.storage_pooling_and_shutdown",
    ),
    IdempotencyCase(
        operation="`CommandBroker.enqueue()`",
        expected_contract="Not idempotent; duplicate logical commands remain distinct",
        task_id="idempotency.broker_and_submit_non_idempotent",
    ),
    IdempotencyCase(
        operation="`CommandService.submit()`",
        expected_contract="Not idempotent; duplicate submits create duplicate commands",
        task_id="idempotency.broker_and_submit_non_idempotent",
    ),
    IdempotencyCase(
        operation="`CommandService.submit_spawn()`",
        expected_contract=(
            "Returns one reserved `entity_id` per successful call; repeated calls create new "
            "entities unless the caller reuses an explicit reservation"
        ),
        task_id="idempotency.submit_spawn_distinct_entities",
    ),
    IdempotencyCase(
        operation="`AsyncWorld.create_entity()`",
        expected_contract="Not idempotent; each call allocates a new world-local entity ID",
        task_id="idempotency.async_world_entity_ids_and_missing_remove",
    ),
    IdempotencyCase(
        operation="`AsyncWorld.remove_entity(missing)`",
        expected_contract="Safe no-op with observability",
        task_id="idempotency.async_world_entity_ids_and_missing_remove",
    ),
    IdempotencyCase(
        operation="`RuntimeWorld.as_actor(ctx)`",
        expected_contract=(
            "Idempotent as handle binding only; creates another alias, not another world"
        ),
        task_id="idempotency.runtime_aliases_and_history",
    ),
    IdempotencyCase(
        operation="Duplicate despawn in one tick",
        expected_contract="Idempotent collapse by entity ID",
        task_id="idempotency.same_tick_duplicate_mutations",
    ),
    IdempotencyCase(
        operation="Duplicate spawn for same entity in one tick",
        expected_contract="Deterministic last-write-wins",
        task_id="idempotency.same_tick_duplicate_mutations",
    ),
    IdempotencyCase(
        operation="`RuntimeWorld.history()`",
        expected_contract="Idempotent for fixed audit history",
        task_id="idempotency.runtime_aliases_and_history",
    ),
    IdempotencyCase(
        operation="`add_components()` with no signature change",
        expected_contract="Idempotent no-op",
        task_id="idempotency.component_signature_noops",
    ),
    IdempotencyCase(
        operation="`remove_components()` with no signature change",
        expected_contract="Idempotent no-op",
        task_id="idempotency.component_signature_noops",
    ),
    IdempotencyCase(
        operation="`world.step()`",
        expected_contract="Not idempotent; advances tick and appends new rows",
        task_id="idempotency.step_and_run_non_idempotent",
    ),
    IdempotencyCase(
        operation="`world.run()`",
        expected_contract="Not idempotent; performs multiple steps under one run contract",
        task_id="idempotency.step_and_run_non_idempotent",
    ),
    IdempotencyCase(
        operation="`QueryManager.query_archetype()`",
        expected_contract="Idempotent for fixed persisted state",
        task_id="idempotency.query_archetype_repeatable",
    ),
)


class IdemCounter(Component):
    value: int = 0


class IdemMarker(Component):
    label: str = ""


def _admin() -> ActorCtx:
    return ActorCtx(id=uuid7(), roles={"admin"})


def _pending_rows(world) -> tuple[int, int]:
    spawn_rows = sum(len(rows) for rows in world.spawn_cache.values())
    despawn_rows = sum(len(rows) for rows in world.despawn_cache.values())
    return spawn_rows, despawn_rows


def contract_map() -> list[dict[str, str]]:
    """Inspectable matrix-to-task map for this suite."""
    return [
        {
            "operation": case.operation,
            "expected_contract": case.expected_contract,
            "task_id": case.task_id,
        }
        for case in IDEMPOTENCY_CASES
    ]


def _registered_task_ids() -> set[str]:
    harness = EvalHarness()
    register(harness)
    return {task_id for task_id, _, _, _ in harness._tasks}


def task_manifest_traceability() -> list[GraderResult]:
    """Every idempotency matrix row maps to a registered eval task."""
    text = SPECIFICATION.read_text() if SPECIFICATION.exists() else ""
    registered = _registered_task_ids()
    checks: dict[str, bool] = {
        "specification_exists": SPECIFICATION.exists(),
        "all_task_ids_registered": all(case.task_id in registered for case in IDEMPOTENCY_CASES),
    }
    for case in IDEMPOTENCY_CASES:
        checks[f"{case.operation}:operation_anchor"] = case.operation in text
        checks[f"{case.operation}:contract_anchor"] = case.expected_contract in text

    mapped_operations = {case.operation for case in IDEMPOTENCY_CASES}
    checks["unique_operations"] = len(mapped_operations) == len(IDEMPOTENCY_CASES)

    return [state_check(checks, name="idempotency_manifest_traceability")]


def task_storage_pooling_and_shutdown() -> list[GraderResult]:
    """Storage pooling and cached-store shutdown are repeat-safe."""
    return asyncio.run(_task_storage_pooling_and_shutdown())


async def _task_storage_pooling_and_shutdown() -> list[GraderResult]:
    with tempfile.TemporaryDirectory() as tmp:
        service = StorageService()
        storage = StorageConfig(uri=f"{tmp}/store", namespace="idem")
        other_namespace = storage.model_copy(update={"namespace": "idem_other"})
        cache = CacheConfig(flush_rows=10_000, flush_mb=10_000, global_mb=10_000, idle_sec=3600)
        try:
            plain_a = await service.get_or_create_store(storage)
            plain_b = await service.get_or_create_store(storage)
            cached_a = await service.get_or_create_store(storage, cache)
            cached_b = await service.get_or_create_store(storage, cache)
            other = await service.get_or_create_store(other_namespace)

            # The cached store itself owns an idempotent shutdown contract.
            await cached_a.shutdown()
            await cached_a.shutdown()

            # The service wrapper should also tolerate repeated shutdowns.
            await service.shutdown()
            await service.shutdown()

            return [
                state_check(
                    {
                        "same_config_reuses_plain_store": plain_a is plain_b,
                        "same_cache_config_reuses_cached_store": cached_a is cached_b,
                        "cache_config_is_part_of_pool_key": plain_a is not cached_a,
                        "namespace_is_part_of_pool_key": plain_a is not other,
                    },
                    name="storage_pooling_contract",
                )
            ]
        finally:
            await service.shutdown()


def task_world_lifecycle_idempotency() -> list[GraderResult]:
    """Explicit world IDs collapse creates; missing and double destroys no-op."""
    return asyncio.run(_task_world_lifecycle_idempotency())


async def _task_world_lifecycle_idempotency() -> list[GraderResult]:
    with tempfile.TemporaryDirectory() as tmp:
        storage_service = StorageService()
        worlds = WorldService(storage_service)
        storage = StorageConfig(uri=f"{tmp}/store", namespace="idem_worlds")
        world_id = uuid7()
        try:
            first = await worlds.create_world(
                WorldConfig(world_id=world_id, name="idem-lifecycle"),
                storage,
            )
            second = await worlds.create_world(
                WorldConfig(world_id=world_id, name="ignored-on-repeat"),
                storage,
            )
            before_destroy = len(worlds.list_worlds())

            await worlds.destroy_world(uuid7())
            after_missing_destroy = len(worlds.list_worlds())
            await worlds.destroy_world(world_id)
            after_first_destroy = len(worlds.list_worlds())
            await worlds.destroy_world(world_id)
            after_second_destroy = len(worlds.list_worlds())

            return [
                state_check(
                    {
                        "explicit_world_id_returns_same_instance": first is second,
                        "repeat_create_does_not_insert_duplicate": before_destroy == 1,
                        "missing_destroy_is_noop": after_missing_destroy == before_destroy,
                        "destroy_removes_once": after_first_destroy == 0,
                        "double_destroy_remains_noop": after_second_destroy == 0,
                    },
                    name="world_lifecycle_idempotency",
                )
            ]
        finally:
            await worlds.shutdown()


def task_broker_and_submit_are_not_idempotent() -> list[GraderResult]:
    """Duplicate logical broker/submit commands remain distinct queued work."""
    return asyncio.run(_task_broker_and_submit_are_not_idempotent())


async def _task_broker_and_submit_are_not_idempotent() -> list[GraderResult]:
    reset_tick_counters()
    reset_daily_tokens()
    try:
        broker = CommandBroker()
        direct_a = Command(type=CommandType.SPAWN, payload={"components": []})
        direct_b = Command(type=CommandType.SPAWN, payload={"components": []})
        await broker.enqueue("direct-world", direct_a)
        await broker.enqueue("direct-world", direct_b)
        direct_pending = await broker.get_pending_count("direct-world")
        direct_history = await broker.get_history("direct-world")
        direct_dequeued = await broker.dequeue("direct-world")
        direct_pending_after = await broker.get_pending_count("direct-world")

        with tempfile.TemporaryDirectory() as tmp:
            container = ServiceContainer()
            try:
                storage = StorageConfig(uri=f"{tmp}/store", namespace="idem_submit")
                world = await container.world_service.create_world(
                    WorldConfig(name="submit-non-idempotent"),
                    storage,
                )
                admin = _admin()
                service_a = Command(type=CommandType.SPAWN, payload={"components": []})
                service_b = Command(type=CommandType.SPAWN, payload={"components": []})

                service_id_a = await container.command_service.submit(
                    admin,
                    world.world_id,
                    service_a,
                )
                service_id_b = await container.command_service.submit(
                    admin,
                    world.world_id,
                    service_b,
                )
                service_pending = await container.broker.get_pending_count(world.world_id)
                service_history = await container.broker.get_history(world.world_id)
            finally:
                await container.shutdown()

        return [
            state_check(
                {
                    "broker_keeps_two_pending_commands": direct_pending == 2,
                    "broker_history_keeps_both_commands": len(direct_history) == 2,
                    "broker_dequeues_both_commands": len(direct_dequeued) == 2,
                    "broker_drain_clears_queue": direct_pending_after == 0,
                    "submit_returns_distinct_command_ids": service_id_a != service_id_b,
                    "submit_keeps_two_pending_commands": service_pending == 2,
                    "submit_history_keeps_both_commands": len(service_history) == 2,
                },
                name="queued_commands_are_not_deduplicated",
            )
        ]
    finally:
        reset_tick_counters()
        reset_daily_tokens()


def task_submit_spawn_reserves_distinct_entities() -> list[GraderResult]:
    """Repeated submit_spawn calls reserve and materialize distinct entities."""
    return asyncio.run(_task_submit_spawn_reserves_distinct_entities())


async def _task_submit_spawn_reserves_distinct_entities() -> list[GraderResult]:
    reset_tick_counters()
    reset_daily_tokens()
    with tempfile.TemporaryDirectory() as tmp:
        container = ServiceContainer()
        try:
            storage = StorageConfig(uri=f"{tmp}/store", namespace="idem_spawn")
            world = await container.world_service.create_world(
                WorldConfig(name="submit-spawn"),
                storage,
            )
            admin = _admin()
            first = await container.command_service.submit_spawn(
                admin,
                world.world_id,
                [IdemCounter(value=1)],
            )
            second = await container.command_service.submit_spawn(
                admin,
                world.world_id,
                [IdemCounter(value=2)],
            )
            pending_before = await container.broker.get_pending_count(world.world_id)
            applied = await container.simulation_service.step(world.world_id, RunConfig())
            pending_after = await container.broker.get_pending_count(world.world_id)

            rows = (await world.query_archetype(sig=(IdemCounter,), ticks=[0])).to_pylist()
            values_by_entity = {row["entity_id"]: row["idemcounter__value"] for row in rows}

            return [
                state_check(
                    {
                        "reserved_ids_are_monotonic": [first, second] == [1, 2],
                        "reserved_ids_are_distinct": first != second,
                        "both_spawns_were_queued": pending_before == 2,
                        "both_spawns_were_applied": applied == 2,
                        "queue_drained_after_step": pending_after == 0,
                        "both_entities_materialized": set(values_by_entity) == {1, 2},
                        "entity_values_remain_distinct": values_by_entity == {1: 1, 2: 2},
                    },
                    name="submit_spawn_non_idempotency",
                )
            ]
        finally:
            await container.shutdown()
            reset_tick_counters()
            reset_daily_tokens()


def task_async_world_entity_ids_and_missing_remove() -> list[GraderResult]:
    """AsyncWorld create_entity allocates IDs; missing remove is observable no-op."""
    return asyncio.run(_task_async_world_entity_ids_and_missing_remove())


async def _task_async_world_entity_ids_and_missing_remove() -> list[GraderResult]:
    with tempfile.TemporaryDirectory() as tmp:
        storage_service = StorageService()
        worlds = WorldService(storage_service)
        storage = StorageConfig(uri=f"{tmp}/store", namespace="idem_async_world")
        records: list[str] = []

        class _ListHandler(logging.Handler):
            def emit(self, record: logging.LogRecord) -> None:
                records.append(record.getMessage())

        logger = logging.getLogger("archetype.core.aio.async_world")
        handler = _ListHandler()
        previous_level = logger.level

        try:
            world = await worlds.create_world(WorldConfig(name="async-world-ids"), storage)
            first = await world.create_entity([IdemCounter(value=11)])
            second = await world.create_entity([IdemCounter(value=22)])
            before_missing_remove = (
                dict(world.entity2sig),
                {sig: list(rows) for sig, rows in world.spawn_cache.items()},
                {sig: list(rows) for sig, rows in world.despawn_cache.items()},
            )

            logger.addHandler(handler)
            logger.setLevel(logging.WARNING)
            await world.remove_entity(999_999)
            logger.removeHandler(handler)
            logger.setLevel(previous_level)

            after_missing_remove = (
                dict(world.entity2sig),
                {sig: list(rows) for sig, rows in world.spawn_cache.items()},
                {sig: list(rows) for sig, rows in world.despawn_cache.items()},
            )
            await world.step(RunConfig())
            rows = (await world.query_archetype(sig=(IdemCounter,), ticks=[0])).to_pylist()
            values_by_entity = {row["entity_id"]: row["idemcounter__value"] for row in rows}

            return [
                state_check(
                    {
                        "create_entity_allocates_distinct_ids": [first, second] == [1, 2],
                        "missing_remove_preserves_world_state": (
                            after_missing_remove == before_missing_remove
                        ),
                        "missing_remove_logs_observability": any(
                            "Entity Removal Failed" in message and "999999" in message
                            for message in records
                        ),
                        "created_entities_materialize": values_by_entity == {1: 11, 2: 22},
                    },
                    name="async_world_entity_ids_and_missing_remove",
                )
            ]
        finally:
            logger.removeHandler(handler)
            logger.setLevel(previous_level)
            await worlds.shutdown()


def task_runtime_aliases_and_history() -> list[GraderResult]:
    """Runtime aliases bind handles without creating worlds; fixed history is stable."""
    return asyncio.run(_task_runtime_aliases_and_history())


async def _task_runtime_aliases_and_history() -> list[GraderResult]:
    from archetype.runtime import ArchetypeRuntime

    reset_tick_counters()
    reset_daily_tokens()
    try:
        with tempfile.TemporaryDirectory() as tmp:
            admin = _admin()
            sibling_ctx = ActorCtx(id=uuid7(), roles={"admin"})
            async with ArchetypeRuntime(actor_ctx=admin) as runtime:
                storage = StorageConfig(uri=f"{tmp}/store", namespace="idem_runtime")
                world = runtime.world("runtime-alias", storage=storage)
                sibling_a = world.as_actor(sibling_ctx)
                sibling_b = world.as_actor(sibling_ctx)
                aliases_before_activation = len(world._state.aliases)
                worlds_before_activation = len(runtime._container.world_service.list_worlds())

                await world.spawn(IdemCounter(value=7))
                worlds_after_activation = len(runtime._container.world_service.list_worlds())
                world_info = await world.info()
                sibling_info = await sibling_a.info()

                # Fixed empty filter: each gated read emits an audit row, but not
                # one matching this key, so the visible history remains stable.
                history_a = await sibling_a.history(idempotency_key="no-such-idempotency-key")
                history_b = await sibling_b.history(idempotency_key="no-such-idempotency-key")

                return [
                    state_check(
                        {
                            "aliases_share_state": (
                                world._state is sibling_a._state is sibling_b._state
                            ),
                            "aliases_keep_distinct_actor_contexts": (
                                world._ctx.id == admin.id and sibling_a._ctx.id == sibling_ctx.id
                            ),
                            "aliases_do_not_create_worlds_pre_activation": (
                                aliases_before_activation == 3 and worlds_before_activation == 0
                            ),
                            "aliases_create_only_one_world_on_activation": (
                                worlds_after_activation == 1
                            ),
                            "alias_resolves_same_world_id": (
                                str(world_info.world_id) == str(sibling_info.world_id)
                            ),
                            "fixed_history_first_read_empty": history_a.count_rows() == 0,
                            "fixed_history_second_read_still_empty": history_b.count_rows() == 0,
                        },
                        name="runtime_aliases_and_history",
                    )
                ]
    finally:
        reset_tick_counters()
        reset_daily_tokens()


def task_duplicate_same_tick_mutations_collapse() -> list[GraderResult]:
    """Same-entity duplicate spawn/despawn commands collapse at materialization."""
    return asyncio.run(_task_duplicate_same_tick_mutations_collapse())


async def _task_duplicate_same_tick_mutations_collapse() -> list[GraderResult]:
    reset_tick_counters()
    reset_daily_tokens()
    with tempfile.TemporaryDirectory() as tmp:
        container = ServiceContainer()
        try:
            storage = StorageConfig(uri=f"{tmp}/store", namespace="idem_dupes")
            world = await container.world_service.create_world(
                WorldConfig(name="duplicate-mutations"),
                storage,
            )
            admin = _admin()

            await container.command_service.submit(
                admin,
                world.world_id,
                Command(
                    type=CommandType.SPAWN,
                    tick=0,
                    payload={"entity_id": 77, "components": [IdemCounter(value=1)]},
                ),
            )
            await container.command_service.submit(
                admin,
                world.world_id,
                Command(
                    type=CommandType.SPAWN,
                    tick=0,
                    payload={"entity_id": 77, "components": [IdemCounter(value=9)]},
                ),
            )
            spawn_applied = await container.simulation_service.step(world.world_id, RunConfig())
            spawn_rows = (await world.query_archetype(sig=(IdemCounter,), ticks=[0])).to_pylist()

            await container.command_service.submit(
                admin,
                world.world_id,
                Command(type=CommandType.DESPAWN, tick=1, payload={"entity_id": 77}),
            )
            await container.command_service.submit(
                admin,
                world.world_id,
                Command(type=CommandType.DESPAWN, tick=1, payload={"entity_id": 77}),
            )
            despawn_applied = await container.simulation_service.step(world.world_id, RunConfig())
            store = await container.storage_service.get_or_create_store(storage)
            despawn_rows = (
                await store.get_archetype_df(
                    sig=(IdemCounter,),
                    world_id=str(world.world_id),
                    run_id=str(world.run_id),
                    ticks=[1],
                    active_only=False,
                )
            ).to_pylist()

            spawn_value = spawn_rows[0]["idemcounter__value"] if spawn_rows else None
            spawn_active = spawn_rows[0]["is_active"] if spawn_rows else None
            despawn_active = despawn_rows[0]["is_active"] if despawn_rows else None

            return [
                state_check(
                    {
                        "both_duplicate_spawns_applied": spawn_applied == 2,
                        "duplicate_spawn_materialized_once": len(spawn_rows) == 1,
                        "duplicate_spawn_last_write_wins": spawn_value == 9,
                        "spawn_row_starts_active": spawn_active is True,
                        "both_duplicate_despawns_applied": despawn_applied == 2,
                        "duplicate_despawn_materialized_once": len(despawn_rows) == 1,
                        "duplicate_despawn_marks_inactive": despawn_active is False,
                    },
                    name="same_tick_duplicate_mutation_collapse",
                )
            ]
        finally:
            await container.shutdown()
            reset_tick_counters()
            reset_daily_tokens()


def task_component_signature_noops_are_idempotent() -> list[GraderResult]:
    """Adding existing or removing absent component types is a no-op."""
    return asyncio.run(_task_component_signature_noops_are_idempotent())


async def _task_component_signature_noops_are_idempotent() -> list[GraderResult]:
    reset_tick_counters()
    reset_daily_tokens()
    with tempfile.TemporaryDirectory() as tmp:
        container = ServiceContainer()
        try:
            storage = StorageConfig(uri=f"{tmp}/store", namespace="idem_component_noops")
            world = await container.world_service.create_world(
                WorldConfig(name="component-noops"),
                storage,
            )
            admin = _admin()
            entity_id = await world.create_entity([IdemCounter(value=3)])
            await world.step(RunConfig())

            added: list[int] = []
            removed: list[int] = []

            async def on_added(event: OnComponentAdded) -> None:
                added.append(event.entity_id)

            async def on_removed(event: OnComponentRemoved) -> None:
                removed.append(event.entity_id)

            world.add_hook(OnComponentAdded, on_added)
            world.add_hook(OnComponentRemoved, on_removed)

            signature_before = world.entity2sig[entity_id]
            await container.command_service.add_components(
                admin,
                world.world_id,
                entity_id,
                [IdemCounter(value=99)],
            )
            after_add_signature = world.entity2sig[entity_id]
            after_add_pending = _pending_rows(world)

            await container.command_service.remove_components(
                admin,
                world.world_id,
                entity_id,
                [IdemMarker],
            )
            after_remove_signature = world.entity2sig[entity_id]
            after_remove_pending = _pending_rows(world)

            return [
                state_check(
                    {
                        "add_existing_component_keeps_signature": (
                            after_add_signature == signature_before
                        ),
                        "remove_absent_component_keeps_signature": (
                            after_remove_signature == signature_before
                        ),
                        "add_existing_component_stages_no_rows": after_add_pending == (0, 0),
                        "remove_absent_component_stages_no_rows": after_remove_pending == (0, 0),
                        "no_component_added_hook_fired": added == [],
                        "no_component_removed_hook_fired": removed == [],
                    },
                    name="component_signature_noops",
                )
            ]
        finally:
            await container.shutdown()
            reset_tick_counters()
            reset_daily_tokens()


def task_fixed_reads_are_idempotent() -> list[GraderResult]:
    """Repeated query/history reads over fixed persisted state are stable."""
    return asyncio.run(_task_fixed_reads_are_idempotent())


async def _task_fixed_reads_are_idempotent() -> list[GraderResult]:
    reset_tick_counters()
    reset_daily_tokens()
    with tempfile.TemporaryDirectory() as tmp:
        container = ServiceContainer()
        try:
            storage = StorageConfig(uri=f"{tmp}/store", namespace="idem_reads")
            world = await container.world_service.create_world(
                WorldConfig(name="fixed-reads"),
                storage,
            )
            admin = _admin()
            await container.command_service.submit_spawn(
                admin,
                world.world_id,
                [IdemCounter(value=5)],
            )
            await container.simulation_service.step(world.world_id, RunConfig())

            rows_a = (
                await container.query_service.query_components(
                    [IdemCounter],
                    str(world.world_id),
                    str(world.run_id),
                    storage,
                )
            ).to_pylist()
            rows_b = (
                await container.query_service.query_components(
                    [IdemCounter],
                    str(world.world_id),
                    str(world.run_id),
                    storage,
                )
            ).to_pylist()
            history_a = await container.query_service.get_command_history(str(world.world_id))
            history_b = await container.query_service.get_command_history(str(world.world_id))
            signatures_a = await container.query_service.list_signatures(storage)
            signatures_b = await container.query_service.list_signatures(storage)

            history_shape_a = [(cmd.id, cmd.type) for cmd in history_a]
            history_shape_b = [(cmd.id, cmd.type) for cmd in history_b]
            signature_names_a = sorted(tuple(c.__name__ for c in sig) for sig in signatures_a)
            signature_names_b = sorted(tuple(c.__name__ for c in sig) for sig in signatures_b)

            return [
                exact_match(rows_a, rows_b, name="query_components_repeatable"),
                exact_match(history_shape_a, history_shape_b, name="history_repeatable"),
                exact_match(signature_names_a, signature_names_b, name="signatures_repeatable"),
            ]
        finally:
            await container.shutdown()
            reset_tick_counters()
            reset_daily_tokens()


def task_query_archetype_repeatable() -> list[GraderResult]:
    """Sync QueryManager.query_archetype is stable for fixed persisted state."""
    import daft

    sig = (IdemCounter,)
    rows = [
        Archetype.to_row_dict(
            entity_id=1,
            tick=0,
            components=[IdemCounter(value=101)],
            world_id="idem-query-world",
            run_id="idem-query-run",
        ),
        {
            **Archetype.to_row_dict(
                entity_id=2,
                tick=0,
                components=[IdemCounter(value=202)],
                world_id="idem-query-world",
                run_id="idem-query-run",
            ),
            "is_active": False,
        },
    ]
    fixed_df = daft.from_pylist(rows)
    calls: list[tuple[tuple[type[Component], ...], str, str]] = []

    class _Store:
        def get_archetype_df(self, requested_sig, world_id, run_id):
            calls.append((requested_sig, world_id, run_id))
            return fixed_df

    query = QueryManager(store=_Store())
    first = (
        query.query_archetype(
            sig=sig,
            world_id="idem-query-world",
            run_id="idem-query-run",
            ticks=[0],
            entity_ids=[1],
        )
        .collect()
        .to_pylist()
    )
    second = (
        query.query_archetype(
            sig=sig,
            world_id="idem-query-world",
            run_id="idem-query-run",
            ticks=[0],
            entity_ids=[1],
        )
        .collect()
        .to_pylist()
    )

    return [
        exact_match(first, second, name="query_archetype_repeatable_rows"),
        state_check(
            {
                "one_active_filtered_row": len(first) == 1,
                "world_scoped": first[0].get("world_id") == "idem-query-world" if first else False,
                "run_scoped": first[0].get("run_id") == "idem-query-run" if first else False,
                "entity_filtered": first[0].get("entity_id") == 1 if first else False,
                "tick_filtered": first[0].get("tick") == 0 if first else False,
                "active_only": first[0].get("is_active") is True if first else False,
                "component_value_preserved": (
                    first[0].get("idemcounter__value") == 101 if first else False
                ),
            },
            name="query_archetype_fixed_filters",
        ),
        exact_match(
            calls,
            [
                (sig, "idem-query-world", "idem-query-run"),
                (sig, "idem-query-world", "idem-query-run"),
            ],
            name="query_archetype_scopes_by_world_and_run",
        ),
    ]


def task_step_and_run_are_not_idempotent() -> list[GraderResult]:
    """Repeated step/run calls advance time and append additional tick rows."""
    return asyncio.run(_task_step_and_run_are_not_idempotent())


async def _task_step_and_run_are_not_idempotent() -> list[GraderResult]:
    with tempfile.TemporaryDirectory() as tmp:
        container = ServiceContainer()
        try:
            storage = StorageConfig(uri=f"{tmp}/store", namespace="idem_step_run")
            world = await container.world_service.create_world(
                WorldConfig(name="step-run"),
                storage,
            )
            await world.create_entity([IdemCounter(value=8)])

            start_tick = world.tick
            await world.step(RunConfig())
            after_first_step = world.tick
            await world.step(RunConfig())
            after_second_step = world.tick

            run_one = await container.simulation_service.run(world.world_id, RunConfig(num_steps=1))
            run_two = await container.simulation_service.run(world.world_id, RunConfig(num_steps=1))

            tick0_rows = (await world.query_archetype(sig=(IdemCounter,), ticks=[0])).to_pylist()
            tick1_rows = (await world.query_archetype(sig=(IdemCounter,), ticks=[1])).to_pylist()
            tick2_rows = (await world.query_archetype(sig=(IdemCounter,), ticks=[2])).to_pylist()
            tick3_rows = (await world.query_archetype(sig=(IdemCounter,), ticks=[3])).to_pylist()

            return [
                state_check(
                    {
                        "first_step_advances_tick": (start_tick, after_first_step) == (0, 1),
                        "second_step_advances_again": after_second_step == 2,
                        "first_run_advances_again": run_one.final_tick == 3,
                        "second_run_advances_again": run_two.final_tick == 4,
                        "tick0_row_exists": len(tick0_rows) == 1,
                        "tick1_row_exists": len(tick1_rows) == 1,
                        "tick2_row_exists": len(tick2_rows) == 1,
                        "tick3_row_exists": len(tick3_rows) == 1,
                    },
                    name="step_and_run_non_idempotency",
                )
            ]
        finally:
            await container.shutdown()


def register(harness: EvalHarness) -> None:
    """Register all idempotency tasks on the harness."""
    harness.add(
        "idempotency.manifest_traceability",
        suite=SUITE,
        fn=task_manifest_traceability,
        desc="Idempotency matrix rows cite spec anchors and registered eval tasks.",
    )
    harness.add(
        "idempotency.storage_pooling_and_shutdown",
        suite=SUITE,
        fn=task_storage_pooling_and_shutdown,
        desc="StorageService pooling and cached-store shutdown idempotency.",
    )
    harness.add(
        "idempotency.world_lifecycle",
        suite=SUITE,
        fn=task_world_lifecycle_idempotency,
        desc="WorldService explicit-ID create and missing/double destroy idempotency.",
    )
    harness.add(
        "idempotency.broker_and_submit_non_idempotent",
        suite=SUITE,
        fn=task_broker_and_submit_are_not_idempotent,
        desc="CommandBroker.enqueue and CommandService.submit keep duplicate logical commands.",
    )
    harness.add(
        "idempotency.submit_spawn_distinct_entities",
        suite=SUITE,
        fn=task_submit_spawn_reserves_distinct_entities,
        desc="Repeated submit_spawn calls reserve and materialize distinct entity IDs.",
    )
    harness.add(
        "idempotency.async_world_entity_ids_and_missing_remove",
        suite=SUITE,
        fn=task_async_world_entity_ids_and_missing_remove,
        desc="AsyncWorld create_entity allocates IDs and missing remove is observable no-op.",
    )
    harness.add(
        "idempotency.runtime_aliases_and_history",
        suite=SUITE,
        fn=task_runtime_aliases_and_history,
        desc="RuntimeWorld.as_actor aliases handles and fixed-filter history remains stable.",
    )
    harness.add(
        "idempotency.same_tick_duplicate_mutations",
        suite=SUITE,
        fn=task_duplicate_same_tick_mutations_collapse,
        desc="Duplicate same-entity spawn/despawn commands collapse at materialization.",
    )
    harness.add(
        "idempotency.component_signature_noops",
        suite=SUITE,
        fn=task_component_signature_noops_are_idempotent,
        desc="No-signature-change add/remove component calls stage no rows and fire no hooks.",
    )
    harness.add(
        "idempotency.fixed_reads",
        suite=SUITE,
        fn=task_fixed_reads_are_idempotent,
        desc="Repeated fixed-state query, history, and signature reads are stable.",
    )
    harness.add(
        "idempotency.query_archetype_repeatable",
        suite=SUITE,
        fn=task_query_archetype_repeatable,
        desc="Sync QueryManager.query_archetype is repeatable for fixed persisted state.",
    )
    harness.add(
        "idempotency.step_and_run_non_idempotent",
        suite=SUITE,
        fn=task_step_and_run_are_not_idempotent,
        desc="Repeated step/run calls advance time and append additional rows.",
    )
