# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Fresh-process worker for real-storage idempotency scenarios.

The parent eval invokes this module with ``python -m``. Each command composes a
new canonical process graph, so no component registry, world registry,
connection, or event-loop state is shared between scenario phases.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import time
from pathlib import Path

from uuid_utils import uuid7

from archetype.commands.models import ActorCtx
from archetype.core.aio import AsyncWorld
from archetype.core.component import Component
from archetype.core.config import RunConfig, StorageBackend, StorageConfig, WorldConfig
from archetype.core.interfaces import StaleWriterError
from archetype.evaluation.contracts import GraderContract, Outcome
from archetype.evaluation.models import Evaluate
from archetype.storage.catalog import SqliteControlCatalog
from archetype.world.models import (
    CreateWorld,
    OpenWorldReadonly,
    QueryComponents,
    ResumeWorld,
    Spawn,
    Step,
)
from evals.infra.runtime import EvalProcess, component_refs


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


async def _live_world(process: EvalProcess, world_id: object) -> AsyncWorld:
    world = await process.worlds.live_world(str(world_id))
    if not isinstance(world, AsyncWorld):
        raise RuntimeError(f"world {world_id} was not activated")
    return world


async def seed(args: argparse.Namespace) -> None:
    process = EvalProcess()
    try:
        storage = _storage(args)
        info = await process.dispatcher.apply(
            CreateWorld(
                config=WorldConfig(name=args.name),
                storage_config=storage,
            )
        )
        await process.dispatcher.apply(
            Spawn.from_components(
                world_id=info.world_id,
                components=[ProcessReading(value=args.value)],
            )
        )
        await process.dispatcher.apply(Step(world_id=info.world_id, run_config=RunConfig()))
        world = await _live_world(process, info.world_id)
        _emit(
            {
                "world_id": str(world.world_id),
                "run_id": str(world.run_id),
                "tick": world.tick,
            }
        )
    finally:
        await process.aclose()


async def crash_publish(args: argparse.Namespace) -> None:
    """Hard-exit after durable appends but before manifest publication."""
    process = EvalProcess()
    storage = _storage(args)
    await process.dispatcher.apply(ResumeWorld(storage_config=storage, world_id=args.world_id))

    async def die_before_publish(self, *publish_args, **publish_kwargs):
        os._exit(args.exit_code)

    SqliteControlCatalog.publish_manifest = die_before_publish  # ty: ignore[invalid-assignment]
    await process.dispatcher.apply(Step(world_id=args.world_id, run_config=RunConfig()))
    raise AssertionError("publish crash hook did not terminate the process")


async def resume_verify(args: argparse.Namespace) -> None:
    process = EvalProcess()
    try:
        storage = _storage(args)
        await process.dispatcher.apply(ResumeWorld(storage_config=storage, world_id=args.world_id))
        world = await _live_world(process, args.world_id)
        resume_tick = world.tick
        await process.dispatcher.apply(Step(world_id=args.world_id, run_config=RunConfig()))
        frame = await process.dispatcher.apply(
            QueryComponents(
                components=component_refs([ProcessReading]),
                world_id=args.world_id,
                run_id=str(world.run_id),
                storage_config=storage,
                ticks=(resume_tick,),
            )
        )
        manifests = await process.storage.get_control_catalog(storage).list_manifests(args.world_id)
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
        await process.aclose()


async def resume_race(args: argparse.Namespace) -> None:
    process = EvalProcess()
    try:
        storage = _storage(args)
        await process.dispatcher.apply(ResumeWorld(storage_config=storage, world_id=args.world_id))
        world = await _live_world(process, args.world_id)
        writer_epoch = getattr(world.commit_coordinator, "epoch", None)
        _ready(args.ready, {"epoch": writer_epoch})
        _wait_for(args.go)
        try:
            await process.dispatcher.apply(Step(world_id=args.world_id, run_config=RunConfig()))
            status = "published"
        except StaleWriterError:
            status = "stale"
        except RuntimeError as exc:
            if (
                not isinstance(exc.__cause__, StaleWriterError)
                and "StaleWriter" not in type(exc).__name__
                and "not the" not in str(exc)
            ):
                raise
            status = "stale"
        _emit({"status": status, "epoch": writer_epoch, "tick": world.tick})
    finally:
        await process.aclose()


async def query_world(args: argparse.Namespace) -> None:
    process = EvalProcess()
    try:
        storage = _storage(args)
        info = await process.dispatcher.apply(
            OpenWorldReadonly(storage_config=storage, world_id=args.world_id)
        )
        rows = (
            await process.dispatcher.apply(
                QueryComponents(
                    components=component_refs([ProcessReading]),
                    world_id=args.world_id,
                    run_id=str(info.run_id),
                    storage_config=storage,
                )
            )
        ).to_pylist()
        manifests = await process.storage.get_control_catalog(storage).list_manifests(args.world_id)
        _emit(
            {
                "row_ticks": sorted({row["tick"] for row in rows}),
                "manifest_ticks": [record.tick for record in manifests],
                "rows": len(rows),
            }
        )
    finally:
        await process.aclose()


async def evaluate_world(args: argparse.Namespace) -> None:
    """Race a paid-style grader through a fresh service graph."""
    process = EvalProcess()
    try:
        storage = _storage(args)
        _ready(args.ready)
        _wait_for(args.go)

        def grader(frame):
            with Path(args.grader_log).open("a") as log:
                log.write(f"{os.getpid()}\n")
            return Outcome(status="pass", score=float(frame.count_rows()))

        result = await process.dispatcher.apply_as(
            ActorCtx(id=uuid7(), roles={"operator"}),
            Evaluate(
                world_id=args.world_id,
                components=(ProcessReading,),
                contract=GraderContract(
                    grader_id="process-evaluation",
                    implementation_version="1",
                ),
                grader=grader,
                evaluation_id=args.evaluation_id,
                storage_config=storage,
            ),
        )
        _emit(result.model_dump())
    finally:
        await process.aclose()


async def query_evaluations(args: argparse.Namespace) -> None:
    process = EvalProcess()
    try:
        rows = await process.storage.read_world_rows(
            _storage(args),
            args.world_id,
            _EVALUATION_RESULTS,
        )
        values = rows.select("evaluation_id").to_pydict()
        _emit({"rows": rows.count_rows(), "evaluation_ids": values["evaluation_id"]})
    finally:
        await process.aclose()


async def advance(args: argparse.Namespace) -> None:
    process = EvalProcess()
    try:
        storage = _storage(args)
        await process.dispatcher.apply(ResumeWorld(storage_config=storage, world_id=args.world_id))
        world = await _live_world(process, args.world_id)
        await process.dispatcher.apply(Step(world_id=world.world_id, run_config=RunConfig()))
        _emit({"tick": world.tick})
    finally:
        await process.aclose()


async def evaluate_conflict(args: argparse.Namespace) -> None:
    process = EvalProcess()
    try:
        storage = _storage(args)

        def grader(_frame):
            with Path(args.grader_log).open("a") as log:
                log.write(f"conflict:{os.getpid()}\n")
            return Outcome(status="pass", score=1.0)

        conflict = False
        try:
            await process.dispatcher.apply_as(
                ActorCtx(id=uuid7(), roles={"operator"}),
                Evaluate(
                    world_id=args.world_id,
                    components=(ProcessReading,),
                    contract=GraderContract(
                        grader_id="process-evaluation",
                        implementation_version="1",
                    ),
                    grader=grader,
                    evaluation_id=args.evaluation_id,
                    storage_config=storage,
                ),
            )
        except ValueError:
            conflict = True
        _emit({"conflict": conflict})
    finally:
        await process.aclose()


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
