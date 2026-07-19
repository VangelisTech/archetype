# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Fresh-process worker for real-storage idempotency scenarios.

The parent eval invokes this module with ``python -m``. Each command creates a
new ServiceContainer, so no component registry, world registry, connection,
or event-loop state is shared between scenario phases.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import time
from pathlib import Path

from uuid_utils import uuid7

from archetype.app.container import ServiceContainer
from archetype.app.evaluation.models import EvalReceipt, GraderContract, Outcome
from archetype.app.gateway.auth.models import ActorCtx
from archetype.app.storage.catalog import ClaimConflictError, SqliteControlCatalog
from archetype.artifacts.components import ArtifactMeta
from archetype.core.component import Component
from archetype.core.config import RunConfig, StorageConfig, WorldConfig
from archetype.core.interfaces import StaleWriterError


class ProcessReading(Component):
    value: float = 0.0


def _storage(args: argparse.Namespace) -> StorageConfig:
    return StorageConfig(uri=args.uri, namespace=args.namespace)


def _emit(payload: dict) -> None:
    print(json.dumps(payload, sort_keys=True), flush=True)


def _wait_for(path: str | None, timeout: float = 30.0) -> None:
    if path is None:
        return
    deadline = time.monotonic() + timeout
    marker = Path(path)
    while not marker.exists():
        if time.monotonic() >= deadline:
            raise TimeoutError(f"timed out waiting for {marker}")
        time.sleep(0.01)


def _ready(path: str | None, payload: dict | None = None) -> None:
    if path is None:
        return
    Path(path).write_text(json.dumps(payload or {"ready": True}, sort_keys=True))


def _record_grader_call(path: str) -> None:
    fd = os.open(path, os.O_CREAT | os.O_WRONLY | os.O_APPEND, 0o600)
    try:
        os.write(fd, f"{os.getpid()}\n".encode())
    finally:
        os.close(fd)


def _contract() -> GraderContract:
    return GraderContract(
        grader_id="process-integration-grader",
        implementation_version="1",
        thresholds={"minimum": 1.0},
    )


def _actor() -> ActorCtx:
    return ActorCtx(id=uuid7(), roles={"operator"})


async def seed(args: argparse.Namespace) -> None:
    container = ServiceContainer()
    try:
        storage = _storage(args)
        world = await container.world_service.create_world(WorldConfig(name=args.name), storage)
        await container.mutation_service.create_entity(
            world.world_id, [ProcessReading(value=args.value)]
        )
        await container.simulation_service.step(world.world_id, RunConfig())
        _emit(
            {
                "world_id": str(world.world_id),
                "run_id": str(world.run_id),
                "tick": world.tick,
            }
        )
    finally:
        await container.shutdown()


async def crash_publish(args: argparse.Namespace) -> None:
    """Hard-exit after durable appends but before manifest publication."""
    container = ServiceContainer()
    storage = _storage(args)
    await container.world_service.open_world_mutable(storage, args.world_id)

    async def die_before_publish(self, *publish_args, **publish_kwargs):
        os._exit(args.exit_code)

    SqliteControlCatalog.publish_manifest = die_before_publish
    await container.simulation_service.step(args.world_id, RunConfig())
    raise AssertionError("publish crash hook did not terminate the process")


async def resume_verify(args: argparse.Namespace) -> None:
    container = ServiceContainer()
    try:
        storage = _storage(args)
        world = await container.world_service.open_world_mutable(storage, args.world_id)
        resume_tick = world.tick
        await container.simulation_service.step(args.world_id, RunConfig())
        frame = await container.query_service.query_components(
            [ProcessReading], args.world_id, str(world.run_id), storage, ticks=[resume_tick]
        )
        manifests = await container.storage_service.get_control_catalog(storage).list_manifests(
            args.world_id
        )
        _emit(
            {
                "resume_tick": resume_tick,
                "final_tick": world.tick,
                "visible_rows": len(frame.to_pylist()),
                "manifest_ticks": [record.tick for record in manifests],
                "epoch": world.commit_coordinator.epoch,
            }
        )
    finally:
        await container.shutdown()


async def resume_race(args: argparse.Namespace) -> None:
    container = ServiceContainer()
    try:
        storage = _storage(args)
        world = await container.world_service.open_world_mutable(storage, args.world_id)
        _ready(args.ready, {"epoch": world.commit_coordinator.epoch})
        _wait_for(args.go)
        try:
            await container.simulation_service.step(args.world_id, RunConfig())
            status = "published"
        except StaleWriterError:
            status = "stale"
        except RuntimeError as exc:
            if "StaleWriter" not in type(exc).__name__ and "not the" not in str(exc):
                raise
            status = "stale"
        _emit({"status": status, "epoch": world.commit_coordinator.epoch, "tick": world.tick})
    finally:
        await container.shutdown()


async def query_world(args: argparse.Namespace) -> None:
    container = ServiceContainer()
    try:
        storage = _storage(args)
        info = await container.world_service.open_world_readonly(storage, args.world_id)
        rows = (
            await container.query_service.query_components(
                [ProcessReading], args.world_id, str(info.run_id), storage
            )
        ).to_pylist()
        manifests = await container.storage_service.get_control_catalog(storage).list_manifests(
            args.world_id
        )
        _emit(
            {
                "row_ticks": sorted({row["tick"] for row in rows}),
                "manifest_ticks": [record.tick for record in manifests],
                "rows": len(rows),
            }
        )
    finally:
        await container.shutdown()


async def publish(args: argparse.Namespace) -> None:
    _ready(args.ready)
    _wait_for(args.go)
    container = ServiceContainer()
    try:
        receipt = await container.artifact_service.publish(
            args.world_id,
            [ProcessReading(value=args.value)],
            external_id=args.external_id,
            producer=args.producer,
            storage_config=_storage(args),
        )
        _emit(
            {
                "duplicate": receipt.duplicate,
                "commit_token": receipt.commit_token,
                "artifact_entity_id": receipt.artifact_entity_id,
            }
        )
    finally:
        await container.shutdown()


async def query_artifacts(args: argparse.Namespace) -> None:
    container = ServiceContainer()
    try:
        storage = _storage(args)
        info = await container.world_service.open_world_readonly(storage, args.world_id)
        rows = (
            await container.query_service.query_components(
                [ArtifactMeta], args.world_id, str(info.run_id), storage
            )
        ).to_pylist()
        _emit(
            {
                "rows": len(rows),
                "external_ids": sorted(row["artifactmeta__external_id"] for row in rows),
                "commit_ids": sorted({row["artifactmeta__commit_id"] for row in rows}),
            }
        )
    finally:
        await container.shutdown()


async def evaluate(args: argparse.Namespace) -> None:
    _ready(args.ready)
    _wait_for(args.go)
    container = ServiceContainer()
    try:

        def grader(frame):
            _record_grader_call(args.grader_log)
            return Outcome(status="pass", score=float(frame.count_rows()))

        receipt = await container.command_gateway.evaluate(
            _actor(),
            args.world_id,
            [ProcessReading],
            contract=_contract(),
            grader=grader,
            evaluation_id=args.evaluation_id,
            storage_config=_storage(args),
        )
        _emit({"duplicate": receipt.duplicate, "commit_token": receipt.commit_token})
    finally:
        await container.shutdown()


async def evaluate_conflict(args: argparse.Namespace) -> None:
    container = ServiceContainer()
    try:

        def grader(frame):
            _record_grader_call(args.grader_log)
            return Outcome(status="pass", score=float(frame.count_rows()))

        try:
            await container.command_gateway.evaluate(
                _actor(),
                args.world_id,
                [ProcessReading],
                contract=_contract(),
                grader=grader,
                evaluation_id=args.evaluation_id,
                storage_config=_storage(args),
            )
            conflict = False
        except ClaimConflictError:
            conflict = True
        _emit({"conflict": conflict})
    finally:
        await container.shutdown()


async def query_receipts(args: argparse.Namespace) -> None:
    container = ServiceContainer()
    try:
        storage = _storage(args)
        info = await container.world_service.open_world_readonly(storage, args.world_id)
        rows = (
            await container.query_service.query_components(
                [EvalReceipt], args.world_id, str(info.run_id), storage
            )
        ).to_pylist()
        _emit(
            {
                "rows": len(rows),
                "evaluation_ids": sorted(row["evalreceipt__evaluation_id"] for row in rows),
            }
        )
    finally:
        await container.shutdown()


async def advance(args: argparse.Namespace) -> None:
    container = ServiceContainer()
    try:
        storage = _storage(args)
        world = await container.world_service.open_world_mutable(storage, args.world_id)
        await container.simulation_service.step(args.world_id, RunConfig())
        _emit({"tick": world.tick})
    finally:
        await container.shutdown()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "command",
        choices=(
            "seed",
            "crash-publish",
            "resume-verify",
            "resume-race",
            "query-world",
            "publish-artifact",
            "query-artifacts",
            "evaluate",
            "evaluate-conflict",
            "query-receipts",
            "advance",
        ),
    )
    parser.add_argument("--uri", required=True)
    parser.add_argument("--namespace", default="process_idempotency")
    parser.add_argument("--world-id")
    parser.add_argument("--name", default="process-world")
    parser.add_argument("--value", type=float, default=1.0)
    parser.add_argument("--exit-code", type=int, default=91)
    parser.add_argument("--ready")
    parser.add_argument("--go")
    parser.add_argument("--external-id", default="process-event")
    parser.add_argument("--producer", default="process-producer")
    parser.add_argument("--evaluation-id", default="process-evaluation")
    parser.add_argument("--grader-log")
    return parser


async def _main() -> None:
    args = build_parser().parse_args()
    commands = {
        "seed": seed,
        "crash-publish": crash_publish,
        "resume-verify": resume_verify,
        "resume-race": resume_race,
        "query-world": query_world,
        "publish-artifact": publish,
        "query-artifacts": query_artifacts,
        "evaluate": evaluate,
        "evaluate-conflict": evaluate_conflict,
        "query-receipts": query_receipts,
        "advance": advance,
    }
    await commands[args.command](args)


if __name__ == "__main__":
    asyncio.run(_main())
