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
from archetype.app.gateway.auth.models import ActorCtx
from archetype.core.aio import AsyncWorld
from archetype.core.component import Component
from archetype.core.config import RunConfig, StorageBackend, StorageConfig, WorldConfig
from archetype.core.interfaces import StaleWriterError
from archetype.evaluation.contracts import GraderContract, Outcome
from archetype.storage.catalog import SqliteControlCatalog


class ProcessReading(Component):
    value: float = 0.0


_EVALUATION_RESULTS = "evaluation_results"


def _storage(args: argparse.Namespace) -> StorageConfig:
    return StorageConfig(
        uri=args.uri,
        namespace=args.namespace,
        backend=StorageBackend(args.backend),
    )


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


async def _live_world(container: ServiceContainer, world_id: object) -> AsyncWorld:
    world = await container.world_registry.live_world(str(world_id))
    if not isinstance(world, AsyncWorld):
        raise RuntimeError(f"world {world_id} was not activated")
    return world


async def seed(args: argparse.Namespace) -> None:
    container = ServiceContainer()
    try:
        storage = _storage(args)
        info = await container.application.create_world(WorldConfig(name=args.name), storage)
        await container.application.create_entity(
            info.world_id,
            [ProcessReading(value=args.value)],
        )
        await container.application.step(info.world_id, RunConfig())
        world = await _live_world(container, info.world_id)
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
    await container.application.resume_world(storage, args.world_id)

    async def die_before_publish(self, *publish_args, **publish_kwargs):
        os._exit(args.exit_code)

    SqliteControlCatalog.publish_manifest = die_before_publish  # ty: ignore[invalid-assignment]
    await container.application.step(args.world_id, RunConfig())
    raise AssertionError("publish crash hook did not terminate the process")


async def resume_verify(args: argparse.Namespace) -> None:
    container = ServiceContainer()
    try:
        storage = _storage(args)
        await container.application.resume_world(storage, args.world_id)
        world = await _live_world(container, args.world_id)
        resume_tick = world.tick
        await container.application.step(args.world_id, RunConfig())
        frame = await container.application.query_components(
            [ProcessReading], args.world_id, str(world.run_id), storage, ticks=[resume_tick]
        )
        manifests = await container.storage_service.get_control_catalog(storage).list_manifests(
            args.world_id
        )
        writer_epoch = getattr(world.commit_coordinator, "epoch", None)
        _emit(
            {
                "resume_tick": resume_tick,
                "final_tick": world.tick,
                "visible_rows": len(frame.to_pylist()),
                "manifest_ticks": [record.tick for record in manifests],
                "epoch": writer_epoch,
            }
        )
    finally:
        await container.shutdown()


async def resume_race(args: argparse.Namespace) -> None:
    container = ServiceContainer()
    try:
        storage = _storage(args)
        await container.application.resume_world(storage, args.world_id)
        world = await _live_world(container, args.world_id)
        writer_epoch = getattr(world.commit_coordinator, "epoch", None)
        _ready(args.ready, {"epoch": writer_epoch})
        _wait_for(args.go)
        try:
            await container.application.step(args.world_id, RunConfig())
            status = "published"
        except StaleWriterError:
            status = "stale"
        except RuntimeError as exc:
            if "StaleWriter" not in type(exc).__name__ and "not the" not in str(exc):
                raise
            status = "stale"
        _emit({"status": status, "epoch": writer_epoch, "tick": world.tick})
    finally:
        await container.shutdown()


async def query_world(args: argparse.Namespace) -> None:
    container = ServiceContainer()
    try:
        storage = _storage(args)
        info = await container.application.open_world_readonly(storage, args.world_id)
        rows = (
            await container.application.query_components(
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


async def evaluate_world(args: argparse.Namespace) -> None:
    """Race a paid-style grader through a fresh service graph."""
    container = ServiceContainer()
    try:
        storage = _storage(args)
        _ready(args.ready)
        _wait_for(args.go)

        def grader(frame):
            with Path(args.grader_log).open("a") as log:
                log.write(f"{os.getpid()}\n")
            return Outcome(status="pass", score=float(frame.count_rows()))

        result = await container.command_gateway.evaluate(
            ActorCtx(id=uuid7(), roles={"operator"}),
            args.world_id,
            [ProcessReading],
            contract=GraderContract(
                grader_id="process-evaluation",
                implementation_version="1",
            ),
            grader=grader,
            evaluation_id=args.evaluation_id,
            storage_config=storage,
        )
        _emit(result.model_dump())
    finally:
        await container.shutdown()


async def query_evaluations(args: argparse.Namespace) -> None:
    container = ServiceContainer()
    try:
        rows = await container.ingestion_service.read(
            args.world_id,
            _EVALUATION_RESULTS,
            storage_config=_storage(args),
        )
        values = rows.select("evaluation_id").to_pydict()
        _emit({"rows": rows.count_rows(), "evaluation_ids": values["evaluation_id"]})
    finally:
        await container.shutdown()


async def advance(args: argparse.Namespace) -> None:
    container = ServiceContainer()
    try:
        storage = _storage(args)
        await container.application.resume_world(storage, args.world_id)
        world = await _live_world(container, args.world_id)
        await container.application.step(world.world_id, RunConfig())
        _emit({"tick": world.tick})
    finally:
        await container.shutdown()


async def evaluate_conflict(args: argparse.Namespace) -> None:
    container = ServiceContainer()
    try:
        storage = _storage(args)

        def grader(_frame):
            with Path(args.grader_log).open("a") as log:
                log.write(f"conflict:{os.getpid()}\n")
            return Outcome(status="pass", score=1.0)

        conflict = False
        try:
            await container.command_gateway.evaluate(
                ActorCtx(id=uuid7(), roles={"operator"}),
                args.world_id,
                [ProcessReading],
                contract=GraderContract(
                    grader_id="process-evaluation",
                    implementation_version="1",
                ),
                grader=grader,
                evaluation_id=args.evaluation_id,
                storage_config=storage,
            )
        except ValueError:
            conflict = True
        _emit({"conflict": conflict})
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
            "evaluate",
            "query-evaluations",
            "advance",
            "evaluate-conflict",
        ),
    )
    parser.add_argument("--uri", required=True)
    parser.add_argument("--namespace", default="process_idempotency")
    parser.add_argument(
        "--backend",
        choices=tuple(backend.value for backend in StorageBackend),
        default=StorageBackend.LANCEDB.value,
    )
    parser.add_argument("--world-id")
    parser.add_argument("--name", default="process-world")
    parser.add_argument("--value", type=float, default=1.0)
    parser.add_argument("--exit-code", type=int, default=91)
    parser.add_argument("--ready")
    parser.add_argument("--go")
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
        "evaluate": evaluate_world,
        "query-evaluations": query_evaluations,
        "advance": advance,
        "evaluate-conflict": evaluate_conflict,
    }
    await commands[args.command](args)


if __name__ == "__main__":
    asyncio.run(_main())
