# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Review regressions for temporary RuntimeApplication command bridges."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock

import pytest
from daft import DataFrame
from uuid_utils import uuid7

from archetype import AsyncProcessor, Component
from archetype.app.container import ServiceContainer
from archetype.app.gateway._pr3_commands_bridge import PR3_BRIDGE_MODEL_LITERALS
from archetype.app.gateway.auth.models import ActorCtx
from archetype.core.config import RunConfig, StorageConfig, WorldConfig


class ReviewValue(Component):
    value: int = 0


class CaptureInputs(AsyncProcessor):
    components = (ReviewValue,)

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    async def process(self, df: DataFrame, **input_kwargs: Any) -> DataFrame:
        self.calls.append(input_kwargs)
        return df


@asynccontextmanager
async def _container_harness():
    container = ServiceContainer()
    try:
        yield container
    finally:
        for world in await container.world_registry.list_worlds():
            await container.world_lifecycle.destroy_world(world.world_id)
        await container.shutdown()


async def _call_temporary_bridge(application: Any, operation: str) -> None:
    if operation == "autoresearch":
        await application.autoresearch("world", object(), object())
    elif operation == "evaluate_physical_task":
        await application.evaluate_physical_task(object(), env_client=object())
    elif operation == "sweep_physical_instructions":
        await application.sweep_physical_instructions(
            object(),
            env_client=object(),
            policy_client=object(),
        )
    elif operation == "ingest_artifacts":
        await application.ingest_artifacts("world", ("source",))
    elif operation == "ingest_claude_transcript":
        await application.ingest_claude_transcript("world", object())
    elif operation == "query_transcript_rows":
        await application.query_transcript_rows("world")
    elif operation == "query_artifacts":
        await application.query_artifacts("world")
    elif operation == "run_graders":
        await application.run_graders(object(), (object(),))
    elif operation == "evaluate":
        await application.evaluate("world", ())
    elif operation == "query_trajectory":
        await application.query_trajectory(object(), "world", "run")
    elif operation == "grade_trajectory":
        await application.grade_trajectory(
            object(),
            "world",
            "run",
            graders=(object(),),
        )
    else:
        raise AssertionError(f"unknown temporary bridge operation {operation!r}")


_TEMPORARY_ASYNC_BRIDGES = (
    "autoresearch",
    "evaluate_physical_task",
    "sweep_physical_instructions",
    "ingest_artifacts",
    "ingest_claude_transcript",
    "query_transcript_rows",
    "query_artifacts",
    "run_graders",
    "evaluate",
    "query_trajectory",
    "grade_trajectory",
)
_MISSION_SERVICE_BRIDGES = frozenset(
    {
        "submit_mission",
        "run_mission",
        "restore_mission_sandbox",
    }
)


def test_temporary_application_bridge_inventory_is_exhaustive() -> None:
    assert frozenset(_TEMPORARY_ASYNC_BRIDGES) == (
        frozenset(PR3_BRIDGE_MODEL_LITERALS.values()) - _MISSION_SERVICE_BRIDGES
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", _TEMPORARY_ASYNC_BRIDGES)
async def test_temporary_application_bridge_rejects_before_service_effect(
    operation: str,
) -> None:
    async with _container_harness() as container:
        application = cast(Any, container.application)
        effects = [AsyncMock() for _ in range(11)]
        application._research = SimpleNamespace(run=effects[0])
        application._physical_ai = SimpleNamespace(
            evaluate_task=effects[1],
            sweep_instructions=effects[2],
        )
        application._artifacts = SimpleNamespace(
            ingest=effects[3],
            index=effects[6],
        )
        application._transcripts = SimpleNamespace(
            ingest=effects[4],
            read=effects[5],
        )
        application._evaluations = SimpleNamespace(
            run_graders=effects[7],
            evaluate=effects[8],
        )
        application._trajectories = SimpleNamespace(
            query=effects[9],
            grade=effects[10],
        )
        application._resolve_storage = AsyncMock(return_value=None)
        application._resolve_lineage = AsyncMock(return_value=None)

        await application.stop_admission()

        with pytest.raises(RuntimeError, match="not accepting work"):
            await _call_temporary_bridge(application, operation)

        assert all(effect.await_count == 0 for effect in effects)
        application._resolve_storage.assert_not_awaited()
        application._resolve_lineage.assert_not_awaited()


@pytest.mark.asyncio
async def test_stop_admission_waits_for_admitted_autoresearch() -> None:
    async with _container_harness() as container:
        application = cast(Any, container.application)
        entered = asyncio.Event()
        release = asyncio.Event()
        calls = 0

        async def blocked_run(*args: Any, **kwargs: Any) -> str:
            nonlocal calls
            del args, kwargs
            calls += 1
            entered.set()
            await release.wait()
            return "finished"

        application._research = SimpleNamespace(run=blocked_run)
        operation = asyncio.create_task(application.autoresearch("world", object(), object()))
        await asyncio.wait_for(entered.wait(), timeout=1)

        stop = asyncio.create_task(application.stop_admission())
        await asyncio.sleep(0)

        assert not stop.done()
        with pytest.raises(RuntimeError, match="not accepting work"):
            await application.autoresearch("world", object(), object())
        assert calls == 1

        release.set()
        assert await asyncio.wait_for(operation, timeout=1) == "finished"
        await asyncio.wait_for(stop, timeout=1)


async def _invoke_simulation(
    application: Any,
    operation: str,
    world_id: object,
    **input_kwargs: Any,
) -> None:
    config = RunConfig(num_steps=1)
    if operation == "step":
        await application.step(world_id, config, **input_kwargs)
    elif operation == "run":
        await application.run(world_id, config, **input_kwargs)
    else:
        raise AssertionError(f"unknown simulation operation {operation!r}")


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ("step", "run"))
async def test_trusted_direct_simulation_preserves_live_kwarg_identity(
    tmp_path,
    operation: str,
) -> None:
    async with _container_harness() as container:
        application = container.application
        info = await application.create_world(
            WorldConfig(name=f"live-{operation}"),
            StorageConfig(
                uri=str(tmp_path / operation),
                namespace="application-review-live",
            ),
        )
        processor = CaptureInputs()
        await application.create_entity(info.world_id, [ReviewValue(value=1)])
        await application.add_processor(info.world_id, processor)
        capability = object()

        await _invoke_simulation(
            application,
            operation,
            info.world_id,
            capability=capability,
        )

        assert len(processor.calls) == 1
        assert processor.calls[0]["capability"] is capability


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ("step", "run"))
async def test_trusted_direct_simulation_preserves_tuple_identity_and_shape(
    tmp_path,
    operation: str,
) -> None:
    async with _container_harness() as container:
        application = container.application
        info = await application.create_world(
            WorldConfig(name=f"tuple-{operation}"),
            StorageConfig(
                uri=str(tmp_path / operation),
                namespace="application-review-tuple",
            ),
        )
        processor = CaptureInputs()
        await application.create_entity(info.world_id, [ReviewValue(value=1)])
        await application.add_processor(info.world_id, processor)
        coordinates = ("outer", ("inner", 3))

        await _invoke_simulation(
            application,
            operation,
            info.world_id,
            coordinates=coordinates,
        )

        assert len(processor.calls) == 1
        assert processor.calls[0]["coordinates"] is coordinates
        assert processor.calls[0]["coordinates"] == ("outer", ("inner", 3))


@pytest.mark.asyncio
async def test_actor_aware_step_preserves_live_inputs_through_registered_handler(
    tmp_path,
) -> None:
    async with _container_harness() as container:
        gateway = container.command_gateway
        actor = ActorCtx(id=uuid7(), roles={"admin"})
        info = await gateway.create_world(
            actor,
            WorldConfig(name="actor-live-input"),
            StorageConfig(
                uri=str(tmp_path / "actor"),
                namespace="application-review-actor",
            ),
        )
        processor = CaptureInputs()
        await gateway.create_entity(actor, info.world_id, [ReviewValue(value=1)])
        await gateway.add_processor(actor, info.world_id, processor)
        capability = object()
        coordinates = ("outer", ("inner", 3))

        await gateway.step(
            actor,
            info.world_id,
            RunConfig(),
            capability=capability,
            coordinates=coordinates,
        )

        assert len(processor.calls) == 1
        assert processor.calls[0]["capability"] is capability
        assert processor.calls[0]["coordinates"] is coordinates
