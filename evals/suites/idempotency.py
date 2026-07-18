# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Idempotency eval suite.

These tasks execute the idempotency matrix in ``docs/guide/specification.md``.
They intentionally cover repetition, concurrency, and crash-retry boundaries:

- operations marked idempotent collapse, reuse, or no-op on repetition
- operations marked non-idempotent produce distinct observable effects
- durable operations converge on one visible outcome after races or failures
- identity reuse with different content fails loudly instead of silently merging
"""

from __future__ import annotations

import asyncio
import logging
import tempfile
from dataclasses import dataclass
from pathlib import Path

from daft.io import IOConfig
from uuid_utils import uuid7

from archetype.app.container import ServiceContainer
from archetype.app.gateway.auth.guard import reset_daily_tokens, reset_tick_counters
from archetype.app.gateway.auth.models import ActorCtx
from archetype.app.models import Command, CommandType
from archetype.app.storage.service import StorageService
from archetype.app.storage.session import configure_session
from archetype.app.world.service import WorldService
from archetype.core.component import Component
from archetype.core.config import (
    CacheConfig,
    RunConfig,
    StorageBackend,
    StorageConfig,
    WorldConfig,
)
from archetype.core.hooks import OnComponentAdded, OnComponentRemoved
from archetype.core.sync import QueryManager, SyncStore, UpdateManager
from evals.graders import exact_match, state_check
from evals.harness import EvalHarness
from evals.suites.idempotency_durable import (
    task_atomic_publish_retry,
    task_durable_artifact_crash_recovery,
    task_durable_artifact_replay,
    task_durable_discovery,
    task_evaluation_receipt_replay,
    task_resume_and_writer_fencing,
)
from evals.suites.idempotency_process import (
    task_process_artifact_replay,
    task_process_crash_cold_resume,
    task_process_evaluation_replay,
    task_process_writer_fence_race,
)
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
            "Idempotent per `(uri, namespace, backend, Daft IOConfig fingerprint, cache config)` "
            "within one service instance"
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
        operation="Command admission with the same `command_id` and content",
        expected_contract="Idempotent; returns the existing durable record",
        task_id="idempotency.command_identity",
    ),
    IdempotencyCase(
        operation="Command admission with the same `command_id` and changed content",
        expected_contract="Conflicts",
        task_id="idempotency.command_identity",
    ),
    IdempotencyCase(
        operation="Command admission with a new `command_id`",
        expected_contract="Creates a distinct logical command",
        task_id="idempotency.command_identity",
    ),
    IdempotencyCase(
        operation="Deferred spawn",
        expected_contract=(
            "Returns one reserved `entity_id` per successful admission; replay of the same "
            "command converges on that reservation"
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
        operation="Duplicate despawn in one tick",
        expected_contract="Idempotent collapse by entity ID",
        task_id="idempotency.same_tick_duplicate_mutations",
    ),
    IdempotencyCase(
        operation="Duplicate staged spawn rows for the same entity in one tick",
        expected_contract="Deterministic last-write-wins at materialization",
        task_id="idempotency.staged_spawn_last_write_wins",
    ),
    IdempotencyCase(
        operation="Replay of an already-settled reserved spawn command",
        expected_contract=(
            "Returns or observes the existing terminal outcome; never materializes twice"
        ),
        task_id="idempotency.same_tick_duplicate_mutations",
    ),
    IdempotencyCase(
        operation="`RuntimeWorld.history()`",
        expected_contract="Idempotent for fixed audit history",
        task_id="idempotency.runtime_handles_and_history",
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
    IdempotencyCase(
        operation="Store `append()` replay",
        expected_contract="Not idempotent; repeating an append persists duplicate rows",
        task_id="idempotency.query_archetype_repeatable",
    ),
    IdempotencyCase(
        operation="Updater `update()` replay",
        expected_contract="Not idempotent; repeating an update appends another row version",
        task_id="idempotency.query_archetype_repeatable",
    ),
    IdempotencyCase(
        operation="Store `get_archetype_df()` replay",
        expected_contract="Idempotent for the same persisted data",
        task_id="idempotency.query_archetype_repeatable",
    ),
    IdempotencyCase(
        operation="`QueryService` fixed-state reads",
        expected_contract="Idempotent for fixed rows, history, and signature catalog",
        task_id="idempotency.fixed_reads",
    ),
    IdempotencyCase(
        operation="Catalog re-registration",
        expected_contract=(
            "Same identity and content is an idempotent no-op; different content conflicts loudly"
        ),
        task_id="idempotency.durable_discovery",
    ),
    IdempotencyCase(
        operation="Coordinated tick retry after failed publish",
        expected_contract=(
            "Unpublished attempts stay invisible; retry produces exactly one visible attempt"
        ),
        task_id="idempotency.atomic_publish_retry",
    ),
    IdempotencyCase(
        operation="Cold discovery and reads",
        expected_contract="Repeated cold discovery and reads return stable durable state",
        task_id="idempotency.durable_discovery",
    ),
    IdempotencyCase(
        operation="Fenced mutable resume",
        expected_contract=(
            "Resume continues from the last visible tick and stale-writer retries stay invisible"
        ),
        task_id="idempotency.resume_and_writer_fencing",
    ),
    IdempotencyCase(
        operation="Artifact publication replay",
        expected_contract=(
            "Identical producer identity and payload converges on one visible artifact/link; "
            "changed payload conflicts"
        ),
        task_id="idempotency.durable_artifact_replay",
    ),
    IdempotencyCase(
        operation="Artifact publication crash recovery",
        expected_contract=(
            "Lease takeover completes an appended orphan without creating a second visible "
            "publication"
        ),
        task_id="idempotency.durable_artifact_crash_recovery",
    ),
    IdempotencyCase(
        operation="`evaluate()` replay",
        expected_contract=(
            "Same evaluation identity, subject, and contract returns one receipt without re-grading"
        ),
        task_id="idempotency.evaluation_receipt_replay",
    ),
    IdempotencyCase(
        operation="Hard process crash and cold resume",
        expected_contract=(
            "Unpublished physical rows do not advance a fresh process beyond the last visible tick"
        ),
        task_id="idempotency.process_crash_cold_resume",
    ),
    IdempotencyCase(
        operation="Independent writer-process race",
        expected_contract="Exactly one fenced writer publishes the contested tick",
        task_id="idempotency.process_writer_fence_race",
    ),
    IdempotencyCase(
        operation="Independent process `publish()` replay",
        expected_contract="Concurrent processes converge on one visible external artifact",
        task_id="idempotency.process_artifact_replay",
    ),
    IdempotencyCase(
        operation="Independent process `evaluate()` replay",
        expected_contract=(
            "Concurrent processes grade once, and changed subjects conflict before grading"
        ),
        task_id="idempotency.process_evaluation_replay",
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


def _registered_tasks() -> list[tuple[str, str, object, str]]:
    harness = EvalHarness()
    register(harness)
    return list(harness.registered_tasks)


def _registered_task_ids() -> set[str]:
    return {task_id for task_id, _, _, _ in _registered_tasks()}


def _spec_matrix_rows(text: str) -> list[tuple[str, str]]:
    """Parse the normative two-column matrix without a Markdown dependency."""
    try:
        section = text.split("## Idempotency Matrix", 1)[1].split("\n## ", 1)[0]
    except IndexError:
        return []

    rows: list[tuple[str, str]] = []
    for line in section.splitlines():
        if not line.startswith("|"):
            continue
        cells = [cell.strip() for cell in line.strip().strip("|").split("|")]
        if len(cells) != 2 or cells[0] == "Operation" or set(cells[0]) == {"-"}:
            continue
        rows.append((cells[0], cells[1]))
    return rows


def traceability_checks() -> dict[str, bool]:
    """Return fast static checks used by the eval and the lint entry point."""
    text = SPECIFICATION.read_text() if SPECIFICATION.exists() else ""
    registered = _registered_task_ids()
    mapped_rows = [(case.operation, case.expected_contract) for case in IDEMPOTENCY_CASES]
    mapped_task_ids = {case.task_id for case in IDEMPOTENCY_CASES}
    parsed_rows = _spec_matrix_rows(text)
    checks: dict[str, bool] = {
        "specification_exists": SPECIFICATION.exists(),
        "matrix_is_not_empty": bool(parsed_rows),
        "matrix_rows_match_eval_manifest": parsed_rows == mapped_rows,
        "all_task_ids_registered": mapped_task_ids <= registered,
        "all_behavior_tasks_mapped": registered
        == mapped_task_ids | {"idempotency.manifest_traceability"},
        "unique_operations": len({case.operation for case in IDEMPOTENCY_CASES})
        == len(IDEMPOTENCY_CASES),
        "unique_registered_task_ids": len(registered)
        == len([task_id for task_id, _, _, _ in _registered_tasks()]),
    }
    return checks


def task_manifest_traceability() -> list[GraderResult]:
    """Every idempotency matrix row maps to a registered eval task."""
    return [state_check(traceability_checks(), name="idempotency_manifest_traceability")]


def task_storage_pooling_and_shutdown() -> list[GraderResult]:
    """Storage pooling and cached-store shutdown are repeat-safe."""
    return asyncio.run(_task_storage_pooling_and_shutdown())


async def _task_storage_pooling_and_shutdown() -> list[GraderResult]:
    with tempfile.TemporaryDirectory() as tmp:
        base_storage = StorageConfig(
            uri=f"{tmp}/store",
            namespace="idem",
            backend=StorageBackend.ICEBERG,
        )
        service = StorageService(session=configure_session(base_storage))
        io_config = IOConfig()
        storage = base_storage.model_copy(update={"io_config": io_config})
        other_namespace = storage.model_copy(update={"namespace": "idem_other"})
        equivalent_io = storage.model_copy(update={"io_config": IOConfig()})
        other_io = storage.model_copy(update={"io_config": IOConfig(disable_suffix_range=True)})
        cache = CacheConfig(flush_rows=10_000, flush_mb=10_000, global_mb=10_000, idle_sec=3600)
        try:
            plain_a = await service.get_or_create_store(storage)
            plain_b = await service.get_or_create_store(storage)
            cached_a = await service.get_or_create_store(storage, cache)
            cached_b = await service.get_or_create_store(storage, cache)
            try:
                await service.get_or_create_store(other_namespace)
                other_namespace_rejected = False
            except ValueError:
                other_namespace_rejected = True
            equivalent_credentials = await service.get_or_create_store(equivalent_io)
            different_credentials = await service.get_or_create_store(other_io)

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
                        "injected_session_rejects_other_namespace": (other_namespace_rejected),
                        "equivalent_io_config_reuses_store": (plain_a is equivalent_credentials),
                        "io_config_fingerprint_is_part_of_pool_key": (
                            plain_a is not different_credentials
                        ),
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


def task_command_identity() -> list[GraderResult]:
    """Command identity converges replays while distinct IDs remain distinct."""
    return asyncio.run(_task_command_identity())


async def _task_command_identity() -> list[GraderResult]:
    reset_tick_counters()
    reset_daily_tokens()
    try:
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

                service_id_a = await container.command_gateway.submit(
                    admin,
                    world.world_id,
                    service_a,
                )
                service_id_b = await container.command_gateway.submit(
                    admin,
                    world.world_id,
                    service_b,
                )
                replay_id = await container.command_gateway.submit(
                    admin,
                    world.world_id,
                    service_a,
                )
                changed_content_conflicted = False
                try:
                    await container.command_gateway.submit(
                        admin,
                        world.world_id,
                        Command(
                            id=service_a.id,
                            type=CommandType.SPAWN,
                            payload={"components": [], "changed": True},
                        ),
                    )
                except Exception:
                    changed_content_conflicted = True
                service_pending = await container.command_scheduler.pending_count(world.world_id)
                service_history = await container.command_scheduler.history(world.world_id)
            finally:
                await container.shutdown()

        return [
            state_check(
                {
                    "submit_returns_distinct_command_ids": service_id_a != service_id_b,
                    "identical_id_replay_converges": replay_id == service_id_a,
                    "changed_content_conflicts": changed_content_conflicted,
                    "submit_keeps_two_pending_commands": service_pending == 2,
                    "submit_history_keeps_both_commands": len(service_history) == 2,
                },
                name="durable_command_identity",
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
            first = await container.command_gateway.submit_spawn(
                admin,
                world.world_id,
                [IdemCounter(value=1)],
            )
            second = await container.command_gateway.submit_spawn(
                admin,
                world.world_id,
                [IdemCounter(value=2)],
            )
            pending_before = await container.command_scheduler.pending_count(world.world_id)
            applied = await container.simulation_service.step(world.world_id, RunConfig())
            pending_after = await container.command_scheduler.pending_count(world.world_id)

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


def task_runtime_handles_and_history() -> list[GraderResult]:
    """Attached handles preserve one world identity; fixed history is stable."""
    return asyncio.run(_task_runtime_handles_and_history())


async def _task_runtime_handles_and_history() -> list[GraderResult]:
    from archetype.runtime import ArchetypeRuntime

    with tempfile.TemporaryDirectory() as tmp:
        async with ArchetypeRuntime() as runtime:
            storage = StorageConfig(uri=f"{tmp}/store", namespace="idem_runtime")
            world = runtime.world("runtime-handles", storage=storage)
            worlds_before_activation = len(runtime._container.world_service.list_worlds())

            await world.spawn(IdemCounter(value=7))
            world_info = await world.info()
            sibling_a = runtime.attach(world_info.world_id, storage=storage)
            sibling_b = runtime.attach(world_info.world_id, storage=storage)
            worlds_after_activation = len(runtime._container.world_service.list_worlds())
            sibling_a_info = await sibling_a.info()
            sibling_b_info = await sibling_b.info()

            history_a = await sibling_a.history(idempotency_key="no-such-idempotency-key")
            history_b = await sibling_b.history(idempotency_key="no-such-idempotency-key")

            return [
                state_check(
                    {
                        "lazy_handle_creates_no_world": worlds_before_activation == 0,
                        "activation_creates_only_one_world": worlds_after_activation == 1,
                        "attached_handles_are_actor_free": all(
                            not hasattr(handle, "_ctx") for handle in (world, sibling_a, sibling_b)
                        ),
                        "attached_handles_resolve_same_world": len(
                            {
                                str(world_info.world_id),
                                str(sibling_a_info.world_id),
                                str(sibling_b_info.world_id),
                            }
                        )
                        == 1,
                        "fixed_history_first_read_empty": history_a.count_rows() == 0,
                        "fixed_history_second_read_still_empty": history_b.count_rows() == 0,
                    },
                    name="runtime_handles_and_history",
                )
            ]


def task_staged_spawn_last_write_wins() -> list[GraderResult]:
    """Duplicate raw staged rows retain the last value at materialization."""
    return asyncio.run(_task_staged_spawn_last_write_wins())


async def _task_staged_spawn_last_write_wins() -> list[GraderResult]:
    reset_tick_counters()
    reset_daily_tokens()
    with tempfile.TemporaryDirectory() as tmp:
        container = ServiceContainer()
        try:
            storage = StorageConfig(uri=f"{tmp}/store", namespace="idem_staged_spawns")
            world = await container.world_service.create_world(
                WorldConfig(name="staged-spawn-last-write-wins"),
                storage,
            )

            entity_id = await world.create_entity([IdemCounter(value=1)])
            sig = world.entity2sig[entity_id]
            duplicate_row = {
                **world.spawn_cache[sig][-1],
                "idemcounter__value": 9,
            }
            world.spawn_cache[sig].append(duplicate_row)
            staged_count = len(world.spawn_cache[sig])
            await container.simulation_service.step(world.world_id, RunConfig())
            rows = (await world.query_archetype(sig=(IdemCounter,), ticks=[0])).to_pylist()

            return [
                state_check(
                    {
                        "two_raw_rows_staged_for_same_entity": staged_count == 2,
                        "duplicate_staged_spawn_materialized_once": len(rows) == 1,
                        "duplicate_staged_spawn_last_write_wins": bool(rows)
                        and rows[0]["idemcounter__value"] == 9,
                    },
                    name="staged_spawn_last_write_wins",
                )
            ]
        finally:
            await container.shutdown()
            reset_tick_counters()
            reset_daily_tokens()


def task_duplicate_same_tick_mutations_collapse() -> list[GraderResult]:
    """Settled spawn replay converges; duplicate despawns collapse."""
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

            spawn_command = Command(
                type=CommandType.SPAWN,
                tick=0,
                payload={"entity_id": 77, "components": [IdemCounter(value=1)]},
            )
            admitted_id = await container.command_gateway.submit(
                admin,
                world.world_id,
                spawn_command,
            )
            spawn_applied = await container.simulation_service.step(world.world_id, RunConfig())
            spawn_rows = (await world.query_archetype(sig=(IdemCounter,), ticks=[0])).to_pylist()
            replay_id = await container.command_gateway.submit(
                admin,
                world.world_id,
                spawn_command,
            )
            pending_after_replay = await container.command_scheduler.pending_count(world.world_id)
            spawn_records = [
                record
                for record in await container.command_scheduler.records(world.world_id)
                if record.command_type == CommandType.SPAWN.value
            ]

            await container.command_gateway.submit(
                admin,
                world.world_id,
                Command(type=CommandType.DESPAWN, tick=1, payload={"entity_id": 77}),
            )
            await container.command_gateway.submit(
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
                        "settled_spawn_replay_returns_same_identity": replay_id
                        == admitted_id
                        == spawn_command.id,
                        "settled_spawn_replay_is_not_requeued": pending_after_replay == 0,
                        "one_terminal_spawn_record": len(spawn_records) == 1
                        and spawn_records[0].status == "APPLIED",
                        "first_spawn_applied_once": spawn_applied == 1,
                        "reserved_spawn_materialized_once": len(spawn_rows) == 1,
                        "reserved_spawn_value_preserved": spawn_value == 1,
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
            await container.command_gateway.add_components(
                admin,
                world.world_id,
                entity_id,
                [IdemCounter(value=99)],
            )
            after_add_signature = world.entity2sig[entity_id]
            after_add_pending = _pending_rows(world)

            await container.command_gateway.remove_components(
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
            await container.command_gateway.submit_spawn(
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
    """Real sync store reads repeat; updater replay deliberately appends."""
    import daft

    with tempfile.TemporaryDirectory() as tmp:
        storage = StorageConfig(uri=f"{tmp}/store", namespace="real_sync_idempotency")
        store = SyncStore(uri=str(storage.uri), session=configure_session(storage))
        updater = UpdateManager(store)
        query = QueryManager(store=store)
        sig = (IdemCounter,)
        raw = daft.from_pylist([{"entity_id": 1, "is_active": True, "idemcounter__value": 101}])

        updater.update(raw, sig, tick=0, world_id="idem-query-world", run_id="idem-query-run")
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

        updater.update(raw, sig, tick=0, world_id="idem-query-world", run_id="idem-query-run")
        after_replayed_update = (
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
        store.shutdown()

        return [
            exact_match(first, second, name="real_store_fixed_read_repeatability"),
            state_check(
                {
                    "one_active_filtered_row": len(first) == 1,
                    "world_scoped": first[0].get("world_id") == "idem-query-world"
                    if first
                    else False,
                    "run_scoped": first[0].get("run_id") == "idem-query-run" if first else False,
                    "entity_filtered": first[0].get("entity_id") == 1 if first else False,
                    "tick_filtered": first[0].get("tick") == 0 if first else False,
                    "component_value_preserved": first[0].get("idemcounter__value") == 101
                    if first
                    else False,
                    "replayed_updater_appends_again": len(after_replayed_update) == 2,
                },
                name="real_sync_store_contracts",
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
        "idempotency.command_identity",
        suite=SUITE,
        fn=task_command_identity,
        desc="Command IDs converge exact replays, reject conflicts, and preserve distinct IDs.",
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
        "idempotency.runtime_handles_and_history",
        suite=SUITE,
        fn=task_runtime_handles_and_history,
        desc="Actor-free attached handles preserve one world and fixed-filter history is stable.",
    )
    harness.add(
        "idempotency.staged_spawn_last_write_wins",
        suite=SUITE,
        fn=task_staged_spawn_last_write_wins,
        desc="Duplicate raw staged spawn rows resolve last-write-wins at materialization.",
    )
    harness.add(
        "idempotency.same_tick_duplicate_mutations",
        suite=SUITE,
        fn=task_duplicate_same_tick_mutations_collapse,
        desc="Reserved-spawn replays reject and duplicate despawns collapse.",
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
        desc="Real sync store reads repeat while updater replay appends another row.",
    )
    harness.add(
        "idempotency.step_and_run_non_idempotent",
        suite=SUITE,
        fn=task_step_and_run_are_not_idempotent,
        desc="Repeated step/run calls advance time and append additional rows.",
    )
    harness.add(
        "idempotency.atomic_publish_retry",
        suite=SUITE,
        fn=task_atomic_publish_retry,
        desc="Failed tick publication stays invisible and retry exposes one attempt.",
    )
    harness.add(
        "idempotency.durable_discovery",
        suite=SUITE,
        fn=task_durable_discovery,
        desc="Cold discovery, reads, and catalog re-registration converge on durable state.",
    )
    harness.add(
        "idempotency.resume_and_writer_fencing",
        suite=SUITE,
        fn=task_resume_and_writer_fencing,
        desc="Fenced resume owns the next tick and stale writer attempts remain invisible.",
    )
    harness.add(
        "idempotency.durable_artifact_replay",
        suite=SUITE,
        fn=task_durable_artifact_replay,
        desc="Concurrent artifact replay converges and identity-content conflicts fail loudly.",
    )
    harness.add(
        "idempotency.durable_artifact_crash_recovery",
        suite=SUITE,
        fn=task_durable_artifact_crash_recovery,
        desc="Lease takeover completes an appended orphan without a duplicate visible artifact.",
    )
    harness.add(
        "idempotency.evaluation_receipt_replay",
        suite=SUITE,
        fn=task_evaluation_receipt_replay,
        desc="Evaluation replay returns one receipt without re-running the grader.",
    )
    harness.add(
        "idempotency.process_crash_cold_resume",
        suite=SUITE,
        fn=task_process_crash_cold_resume,
        desc="A hard-killed process leaves invisible rows and cold resume retries one tick.",
    )
    harness.add(
        "idempotency.process_writer_fence_race",
        suite=SUITE,
        fn=task_process_writer_fence_race,
        desc="Two independent writer processes race and exactly one publishes.",
    )
    harness.add(
        "idempotency.process_artifact_replay",
        suite=SUITE,
        fn=task_process_artifact_replay,
        desc="Concurrent processes converge on one externally identified artifact.",
    )
    harness.add(
        "idempotency.process_evaluation_replay",
        suite=SUITE,
        fn=task_process_evaluation_replay,
        desc="Concurrent processes grade once and changed subjects conflict before grading.",
    )
