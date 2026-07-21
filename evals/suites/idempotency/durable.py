# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Durability-amendment scenarios for the idempotency eval suite.

These checks primarily use service-facing outcomes and durable query results.
The two crash scenarios patch only the documented commit boundary in order to
create a state that ordinary public calls cannot produce deterministically;
selected internal counters provide secondary diagnostics when a check fails.
"""

from __future__ import annotations

import asyncio
import tempfile
from pathlib import Path
from unittest.mock import patch

from uuid_utils import uuid7

from archetype.app.container import ServiceContainer
from archetype.app.gateway.auth.models import ActorCtx
from archetype.app.storage.catalog import (
    CatalogConflictError,
    SqliteControlCatalog,
    WorldRecord,
)
from archetype.artifacts import ArtifactSource
from archetype.core.component import Component
from archetype.core.config import RunConfig, StorageBackend, StorageConfig, WorldConfig
from archetype.core.interfaces import StaleWriterError
from archetype.evaluation.contracts import GraderContract, Outcome
from archetype.ingestion import IngestionTable
from evals.graders import exact_match, state_check
from evals.types import GraderResult


class DurableReading(Component):
    value: float = 0.0


def _actor(role: str = "operator") -> ActorCtx:
    return ActorCtx(id=uuid7(), roles={role})


async def _seed_world(container: ServiceContainer, storage: StorageConfig, *, name: str):
    world = await container.world_service.create_world(WorldConfig(name=name), storage)
    await container.mutation_service.create_entity(world.world_id, [DurableReading(value=1.0)])
    await container.simulation_service.step(world.world_id, RunConfig())
    return world


async def _visible_rows(
    container: ServiceContainer,
    component: type[Component],
    world_id: str,
    run_id: str,
    storage: StorageConfig,
    *,
    ticks: list[int] | None = None,
) -> list[dict]:
    frame = await container.query_service.query_components(
        [component], world_id, run_id, storage, ticks=ticks
    )
    return frame.to_pylist()


_EVALUATION_RESULTS = IngestionTable("evaluation_results", key_columns=("evaluation_id",))


def task_atomic_publish_retry() -> list[GraderResult]:
    """A failed publish is invisible and a retry exposes one attempt."""
    return asyncio.run(_task_atomic_publish_retry())


async def _task_atomic_publish_retry() -> list[GraderResult]:
    with tempfile.TemporaryDirectory() as tmp:
        container = ServiceContainer()
        try:
            storage = StorageConfig(uri=f"{tmp}/store", namespace="atomic-retry")
            world = await container.world_service.create_world(
                WorldConfig(name="atomic-retry"), storage
            )
            await container.mutation_service.create_entity(
                world.world_id, [DurableReading(value=7.0)]
            )
            wid, rid = str(world.world_id), str(world.run_id)

            async def crash_publish(self, *args, **kwargs):
                raise RuntimeError("injected crash before manifest publish")

            failed = False
            with patch.object(SqliteControlCatalog, "publish_manifest", crash_publish):
                try:
                    await container.simulation_service.step(wid, RunConfig())
                except RuntimeError as exc:
                    failed = "injected crash" in str(exc)

            invisible_after_failure = await _visible_rows(
                container, DurableReading, wid, rid, storage
            )
            tick_after_failure = world.tick
            caches_after_failure = sum(len(rows) for rows in world.spawn_cache.values())

            await container.simulation_service.step(wid, RunConfig())
            visible_after_retry = await _visible_rows(
                container, DurableReading, wid, rid, storage, ticks=[0]
            )
            manifests = await container.storage_service.get_control_catalog(storage).list_manifests(
                wid
            )

            return [
                state_check(
                    {
                        "fault_was_injected": failed,
                        "failed_tick_did_not_advance": tick_after_failure == 0,
                        "successful_retry_advanced_once": world.tick == 1,
                        "failed_attempt_was_invisible": invisible_after_failure == [],
                        "staged_spawn_survived_failure": caches_after_failure == 1,
                        "retry_has_one_visible_row": len(visible_after_retry) == 1,
                        "retry_preserved_payload": (
                            visible_after_retry[0]["durablereading__value"] == 7.0
                            if visible_after_retry
                            else False
                        ),
                        "one_manifest_won": len(manifests) == 1,
                        "winning_manifest_is_tick_zero": (
                            manifests[0].tick == 0 if manifests else False
                        ),
                    },
                    name="atomic_publish_retry",
                )
            ]
        finally:
            await container.shutdown()


def task_durable_discovery() -> list[GraderResult]:
    """Fresh containers repeatably discover the same durable identity and rows."""
    return asyncio.run(_task_durable_discovery())


async def _task_durable_discovery() -> list[GraderResult]:
    with tempfile.TemporaryDirectory() as tmp:
        storage = StorageConfig(uri=f"{tmp}/store", namespace="discovery")
        writer = ServiceContainer()
        try:
            world = await _seed_world(writer, storage, name="discoverable")
            wid, rid = str(world.world_id), str(world.run_id)
        finally:
            await writer.shutdown()

        reader = ServiceContainer()
        try:
            discovered_a = await reader.world_service.discover_worlds(storage)
            discovered_b = await reader.world_service.discover_worlds(storage)
            info_a = await reader.world_service.open_world_readonly(storage, wid)
            info_b = await reader.world_service.open_world_readonly(storage, wid)
            rows_a = await _visible_rows(reader, DurableReading, wid, rid, storage)
            rows_b = await _visible_rows(reader, DurableReading, wid, rid, storage)

            catalog = reader.storage_service.get_control_catalog(storage)
            record = await catalog.get_world(wid)
            await catalog.register_world(record)
            conflict_loud = False
            try:
                await catalog.register_world(
                    WorldRecord(
                        world_id=record.world_id,
                        name="different-name",
                        run_id=record.run_id,
                        parent_world_id=record.parent_world_id,
                        status=record.status,
                        tick_head=record.tick_head,
                    )
                )
            except CatalogConflictError:
                conflict_loud = True

            discovered_shape_a = [info.model_dump(mode="json") for info in discovered_a]
            discovered_shape_b = [info.model_dump(mode="json") for info in discovered_b]
            return [
                exact_match(
                    discovered_shape_a,
                    discovered_shape_b,
                    name="cold_discovery_repeatable",
                ),
                exact_match(rows_a, rows_b, name="cold_rows_repeatable"),
                state_check(
                    {
                        "world_was_discovered": [str(info.world_id) for info in discovered_a]
                        == [wid],
                        "readonly_open_is_repeatable": info_a == info_b,
                        "readonly_open_created_no_live_world": not reader.world_service.has_world(
                            wid
                        ),
                        "cold_query_returned_durable_row": len(rows_a) == 1,
                        "identical_catalog_registration_is_noop": (await catalog.get_world(wid))
                        == record,
                        "different_catalog_content_conflicts": conflict_loud,
                    },
                    name="durable_discovery_and_catalog_identity",
                ),
            ]
        finally:
            await reader.shutdown()


def task_resume_and_writer_fencing() -> list[GraderResult]:
    """Resume owns the next visible tick and fences the old writer."""
    return asyncio.run(_task_resume_and_writer_fencing())


async def _task_resume_and_writer_fencing() -> list[GraderResult]:
    with tempfile.TemporaryDirectory() as tmp:
        storage = StorageConfig(uri=f"{tmp}/store", namespace="resume")
        old = ServiceContainer()
        resumed = ServiceContainer()
        try:
            old_world = await _seed_world(old, storage, name="resumable")
            wid, rid = str(old_world.world_id), str(old_world.run_id)
            new_world = await resumed.world_service.open_world_mutable(storage, wid)
            resume_tick = new_world.tick

            stale_failed = False
            try:
                await old.simulation_service.step(wid, RunConfig())
            except StaleWriterError:
                stale_failed = True
            except RuntimeError as exc:
                stale_failed = "StaleWriter" in type(exc).__name__ or "not the" in str(exc)

            before_new_publish = await _visible_rows(
                resumed, DurableReading, wid, rid, storage, ticks=[1]
            )
            await resumed.simulation_service.step(wid, RunConfig())
            after_new_publish = await _visible_rows(
                resumed, DurableReading, wid, rid, storage, ticks=[1]
            )
            manifests = await resumed.storage_service.get_control_catalog(storage).list_manifests(
                wid
            )

            return [
                state_check(
                    {
                        "resume_started_at_next_visible_tick": resume_tick == 1,
                        "new_writer_advanced_once": new_world.tick == 2,
                        "resume_kept_run_identity": str(new_world.run_id) == rid,
                        "resume_incremented_writer_epoch": new_world.commit_coordinator.epoch == 2,
                        "old_writer_failed_closed": stale_failed,
                        "old_writer_did_not_advance": old_world.tick == 1,
                        "stale_attempt_stayed_invisible": before_new_publish == [],
                        "new_writer_published_one_row": len(after_new_publish) == 1,
                        "one_manifest_per_visible_tick": [m.tick for m in manifests] == [0, 1],
                    },
                    name="resume_and_writer_fencing",
                )
            ]
        finally:
            await old.shutdown()
            await resumed.shutdown()


def task_artifact_occurrence_identity() -> list[GraderResult]:
    """Every submission is an occurrence while equal bytes reuse one object."""
    return asyncio.run(_task_artifact_occurrence_identity())


async def _task_artifact_occurrence_identity() -> list[GraderResult]:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        container = ServiceContainer()
        try:
            storage = StorageConfig(
                uri=str(root / "store"),
                namespace="artifact-occurrences",
                backend=StorageBackend.ICEBERG,
            )
            world = await container.world_service.create_world(
                WorldConfig(name="artifact-occurrences"), storage
            )
            first = root / "first.txt"
            second = root / "second.txt"
            first.write_text("same bytes")
            second.write_text("same bytes")

            submitted = await container.artifact_service.ingest(
                str(world.world_id),
                (
                    ArtifactSource(source_uri=str(first)),
                    ArtifactSource(source_uri=str(second)),
                ),
            )
            (retry,) = await container.artifact_service.ingest(
                str(world.world_id), ArtifactSource(source_uri=str(first))
            )
            rows = (await container.artifact_service.index(str(world.world_id))).to_pylist()
            refs = (*submitted, retry)

            return [
                state_check(
                    {
                        "each_submission_has_uuidv7_identity": len(
                            {reference.artifact_id for reference in refs}
                        )
                        == 3,
                        "equal_bytes_share_one_object": len({reference.uri for reference in refs})
                        == 1,
                        "equal_bytes_share_sha256": len({reference.sha256 for reference in refs})
                        == 1,
                        "all_occurrences_are_indexed": len(rows) == 3,
                    },
                    name="artifact_occurrence_identity",
                )
            ]
        finally:
            await container.shutdown()


def task_evaluation_result_replay() -> list[GraderResult]:
    """Result replay returns the persisted evidence without re-grading."""
    return asyncio.run(_task_evaluation_result_replay())


async def _task_evaluation_result_replay() -> list[GraderResult]:
    with tempfile.TemporaryDirectory() as tmp:
        container = ServiceContainer()
        try:
            storage = StorageConfig(
                uri=f"{tmp}/store",
                namespace="evaluation-results",
                backend=StorageBackend.ICEBERG,
            )
            world = await _seed_world(container, storage, name="evaluation-world")
            wid = str(world.world_id)
            calls: list[int] = []

            def grader(frame):
                calls.append(frame.count_rows())
                return Outcome(status="pass", score=1.0)

            contract = GraderContract(
                grader_id="idempotency-eval",
                implementation_version="1",
                thresholds={"minimum": 1.0},
            )
            first = await container.command_gateway.evaluate(
                _actor(),
                wid,
                [DurableReading],
                contract=contract,
                grader=grader,
                evaluation_id="stable-evaluation",
            )
            replay = await container.command_gateway.evaluate(
                _actor(),
                wid,
                [DurableReading],
                contract=contract,
                grader=grader,
                evaluation_id="stable-evaluation",
            )
            rows = (
                await container.ingestion_service.read(
                    wid,
                    _EVALUATION_RESULTS,
                    storage_config=storage,
                )
            ).to_pylist()

            conflict_loud = False
            try:
                await container.command_gateway.evaluate(
                    _actor(),
                    wid,
                    [DurableReading],
                    contract=GraderContract(
                        grader_id="idempotency-eval",
                        implementation_version="2",
                    ),
                    grader=grader,
                    evaluation_id="stable-evaluation",
                )
            except ValueError:
                conflict_loud = True

            return [
                state_check(
                    {
                        "first_call_graded_once": calls == [1],
                        "replay_returns_persisted_result": replay == first,
                        "one_result_is_visible": len(rows) == 1,
                        "changed_contract_conflicts": conflict_loud,
                    },
                    name="evaluation_result_replay",
                )
            ]
        finally:
            await container.shutdown()
