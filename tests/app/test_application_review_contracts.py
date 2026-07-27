# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Review regressions for the canonical exact-operation boundary."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from functools import partial
from pathlib import Path
from typing import Any

import pytest
from daft import DataFrame
from uuid_utils import uuid7

from archetype import AsyncProcessor, Component
from archetype.commands.dispatch import CommandDispatcher
from archetype.commands.models import ActorCtx
from archetype.core.config import RunConfig, StorageConfig, WorldConfig
from archetype.research.models import AutoResearch, AutoResearchConfig
from archetype.world.lifecycle import WorldLifecycle
from archetype.world.models import (
    AddProcessor,
    CreateWorld,
    Run,
    Spawn,
    Step,
)
from archetype.world.registry import WorldRegistry
from tests._runtime import build_test_runtime


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
async def _runtime_harness(tmp_path: Path):
    resources = build_test_runtime(tmp_path)
    dispatcher = resources.dispatcher
    step_handler = dispatcher._registry.resolve_name("step").handler  # noqa: SLF001
    create_handler = dispatcher._registry.resolve_name("create_world").handler  # noqa: SLF001
    assert isinstance(step_handler, partial)
    assert isinstance(create_handler, partial)
    registry = step_handler.args[0]
    lifecycle = getattr(create_handler.args[0], "__self__", None)
    assert isinstance(registry, WorldRegistry)
    assert isinstance(lifecycle, WorldLifecycle)
    try:
        yield resources
    finally:
        for world in await registry.list_worlds():
            await lifecycle.destroy_world(world.world_id)
        await resources.aclose()


_PULL_FORWARD_OPERATIONS = frozenset(
    {
        "autoresearch",
        "evaluate",
        "grade_trajectory",
        "ingest_artifacts",
        "ingest_claude_transcript",
        "query_artifacts",
        "query_trajectory",
        "query_transcript_rows",
        "restore_mission_sandbox",
        "run_graders",
        "run_mission",
        "submit_mission",
        "run_hosted_episode",
    }
)


@pytest.mark.asyncio
async def test_pull_forward_inventory_is_canonical_and_direct_only(tmp_path: Path) -> None:
    async with _runtime_harness(tmp_path) as resources:
        registry = resources.dispatcher._registry  # noqa: SLF001 - exact registry oracle
        specs = {spec.name: spec for spec in registry.specs}

        assert _PULL_FORWARD_OPERATIONS <= specs.keys()
        for operation in _PULL_FORWARD_OPERATIONS:
            spec = specs[operation]
            assert spec.model.__module__.startswith("archetype.")
            assert ".app." not in spec.model.__module__
            assert spec.trusted
            assert spec.durable is None


@pytest.mark.asyncio
async def test_stop_admission_then_wait_drained_joins_admitted_autoresearch(
    tmp_path: Path,
) -> None:
    async with _runtime_harness(tmp_path) as resources:
        dispatcher = resources.dispatcher
        info = await dispatcher.apply(
            CreateWorld(
                config=WorldConfig(name="autoresearch-drain"),
                storage_config=StorageConfig(
                    uri=str(tmp_path / "autoresearch"),
                    namespace="dispatcher-review-drain",
                ),
            )
        )
        entered = asyncio.Event()
        release = asyncio.Event()
        calls = 0

        async def prepare_candidate(_context: object) -> None:
            nonlocal calls
            calls += 1
            entered.set()
            await release.wait()

        model = AutoResearch(
            world_id=info.world_id,
            config=AutoResearchConfig(
                experiment_name="drain",
                experiment_id="drain-v1",
                evaluator_id="score-v1",
                rollout_contract_id="rollout-v1",
                num_episodes=1,
                max_iterations=1,
                record_to_ledger=False,
            ),
            evaluator=lambda _rollout: 0.0,
            prepare_candidate=prepare_candidate,
        )
        operation = asyncio.create_task(dispatcher.apply(model))
        await asyncio.wait_for(entered.wait(), timeout=1)

        drain: asyncio.Task[None] | None = None
        try:
            await dispatcher.stop_admission()
            drain = asyncio.create_task(dispatcher.wait_drained())
            await asyncio.sleep(0)

            assert not drain.done()
            with pytest.raises(RuntimeError, match="not accepting work"):
                await dispatcher.apply(model)
            assert calls == 1

            release.set()
            result = await asyncio.wait_for(operation, timeout=5)
            assert result.iterations_completed == 1
            await asyncio.wait_for(drain, timeout=1)
        finally:
            release.set()
            if not operation.done():
                await operation
            if drain is not None and not drain.done():
                await drain


async def _invoke_simulation(
    dispatcher: CommandDispatcher,
    operation: str,
    world_id: object,
    actor: ActorCtx | None = None,
    **input_kwargs: Any,
) -> None:
    config = RunConfig(num_steps=1)
    model: Step | Run
    if operation == "step":
        model = Step(
            world_id=world_id,
            run_config=config,
            input_kwargs=input_kwargs,
        )
    elif operation == "run":
        model = Run(
            world_id=world_id,
            run_config=config,
            input_kwargs=input_kwargs,
        )
    else:
        raise AssertionError(f"unknown simulation operation {operation!r}")
    if actor is None:
        await dispatcher.apply(model)
    else:
        await dispatcher.apply_as(actor, model)


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ("step", "run"))
async def test_trusted_direct_simulation_preserves_live_kwarg_identity(
    tmp_path,
    operation: str,
) -> None:
    async with _runtime_harness(tmp_path) as resources:
        dispatcher = resources.dispatcher
        info = await dispatcher.apply(
            CreateWorld(
                config=WorldConfig(name=f"live-{operation}"),
                storage_config=StorageConfig(
                    uri=str(tmp_path / operation),
                    namespace="dispatcher-review-live",
                ),
            )
        )
        processor = CaptureInputs()
        await dispatcher.apply(
            Spawn.from_components(
                world_id=info.world_id,
                components=[ReviewValue(value=1)],
            )
        )
        await dispatcher.apply(AddProcessor(world_id=info.world_id, processor=processor))
        capability = object()

        await _invoke_simulation(
            dispatcher,
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
    async with _runtime_harness(tmp_path) as resources:
        dispatcher = resources.dispatcher
        info = await dispatcher.apply(
            CreateWorld(
                config=WorldConfig(name=f"tuple-{operation}"),
                storage_config=StorageConfig(
                    uri=str(tmp_path / operation),
                    namespace="dispatcher-review-tuple",
                ),
            )
        )
        processor = CaptureInputs()
        await dispatcher.apply(
            Spawn.from_components(
                world_id=info.world_id,
                components=[ReviewValue(value=1)],
            )
        )
        await dispatcher.apply(AddProcessor(world_id=info.world_id, processor=processor))
        coordinates = ("outer", ("inner", 3))

        await _invoke_simulation(
            dispatcher,
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
    async with _runtime_harness(tmp_path) as resources:
        dispatcher = resources.dispatcher
        actor = ActorCtx(id=uuid7(), roles={"admin"})
        info = await dispatcher.apply_as(
            actor,
            CreateWorld(
                config=WorldConfig(name="actor-live-input"),
                storage_config=StorageConfig(
                    uri=str(tmp_path / "actor"),
                    namespace="dispatcher-review-actor",
                ),
            ),
        )
        processor = CaptureInputs()
        await dispatcher.apply_as(
            actor,
            Spawn.from_components(
                world_id=info.world_id,
                components=[ReviewValue(value=1)],
            ),
        )
        await dispatcher.apply_as(
            actor,
            AddProcessor(world_id=info.world_id, processor=processor),
        )
        capability = object()
        coordinates = ("outer", ("inner", 3))

        await _invoke_simulation(
            dispatcher,
            "step",
            info.world_id,
            actor=actor,
            capability=capability,
            coordinates=coordinates,
        )

        assert len(processor.calls) == 1
        assert processor.calls[0]["capability"] is capability
        assert processor.calls[0]["coordinates"] is coordinates
