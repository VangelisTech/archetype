# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""AutoResearch experiment-admission contracts."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from functools import partial
from typing import Any

import pytest

from archetype.core.component import Component
from archetype.core.config import RunConfig, StorageConfig, WorldConfig
from archetype.research.handlers import AutoResearchAdmissions, handle_autoresearch
from archetype.research.models import AutoResearch, AutoResearchConfig
from archetype.world.models import CreateWorld, EpisodeConfig, Spawn, Step
from tests._runtime import build_test_runtime


class _Seed(Component):
    value: int = 0


class _TrackingAdmissions(AutoResearchAdmissions):
    """Expose deterministic attempted-admission evidence to one race test."""

    def __init__(self) -> None:
        super().__init__()
        self.attempts = 0
        self.second_attempted = asyncio.Event()

    @asynccontextmanager
    async def admit(self, experiment_id: str) -> AsyncIterator[str]:
        self.attempts += 1
        if self.attempts == 2:
            self.second_attempted.set()
        async with super().admit(experiment_id) as key:
            yield key


def _config(experiment_id: str, *, record_to_ledger: bool = True) -> AutoResearchConfig:
    return AutoResearchConfig(
        experiment_name=experiment_id,
        experiment_id=experiment_id,
        evaluator_id="admission-score-v1",
        rollout_contract_id="admission-rollout-v1",
        episode_config=EpisodeConfig(max_steps=1),
        num_episodes=1,
        max_iterations=1,
        record_to_ledger=record_to_ledger,
    )


async def _base_world(dispatcher: Any, tmp_path: Any, name: str) -> Any:
    info = await dispatcher.apply(
        CreateWorld(
            config=WorldConfig(name=name),
            storage_config=StorageConfig(
                uri=str(tmp_path / "store"),
                namespace="research-admission",
            ),
        )
    )
    await dispatcher.apply(
        Spawn.from_components(
            world_id=info.world_id,
            components=[_Seed()],
        )
    )
    await dispatcher.apply(Step(world_id=info.world_id, run_config=RunConfig()))
    return info


async def _run(
    handler: Callable[[AutoResearch], Any],
    world_id: Any,
    config: AutoResearchConfig,
    *,
    evaluator: Callable[[Any], float],
    prepare_candidate: Callable[[Any], Any],
) -> Any:
    operation = AutoResearch(
        world_id=world_id,
        config=config,
        evaluator=evaluator,
        prepare_candidate=prepare_candidate,
    )
    return await handler(operation)


def _family_handler(
    dispatcher: Any,
    admissions: AutoResearchAdmissions | None = None,
) -> Callable[[AutoResearch], Any]:
    """Bind family-private dependencies from the existing composition graph."""

    registry = dispatcher._registry
    world_registry = registry.resolve_name("step").handler.args[0]
    create_world = registry.resolve_name("create_world").handler.args[0]
    world_lifecycle = create_world.__self__
    storage = registry.resolve_name("query_archetype").handler.args[1]
    destroy_world = registry.resolve_name("destroy_world").handler.args[0]
    return partial(
        handle_autoresearch,
        admissions or AutoResearchAdmissions(),
        world_registry,
        world_lifecycle,
        storage,
        destroy_world,
    )


@pytest.mark.asyncio
async def test_same_experiment_waits_then_resumes_at_the_next_iteration(tmp_path) -> None:
    """A second admitted call must not observe the first call's active attempt."""

    resources = build_test_runtime(tmp_path)
    dispatcher = resources.dispatcher
    admissions = _TrackingAdmissions()
    handler = _family_handler(dispatcher, admissions)
    release_first = asyncio.Event()
    first_entered = asyncio.Event()
    second_entered = asyncio.Event()
    tasks: list[asyncio.Task[Any]] = []
    try:
        base = await _base_world(dispatcher, tmp_path, "same-experiment-base")
        config = _config("same-experiment")

        async def first_candidate(_context: Any) -> None:
            first_entered.set()
            await release_first.wait()

        async def second_candidate(_context: Any) -> None:
            second_entered.set()

        first = asyncio.create_task(
            _run(
                handler,
                base.world_id,
                config,
                evaluator=lambda _rollout: 1.0,
                prepare_candidate=first_candidate,
            )
        )
        tasks.append(first)
        await asyncio.wait_for(first_entered.wait(), timeout=5)

        second = asyncio.create_task(
            _run(
                handler,
                base.world_id,
                config,
                evaluator=lambda _rollout: 2.0,
                prepare_candidate=second_candidate,
            )
        )
        tasks.append(second)
        await asyncio.wait_for(admissions.second_attempted.wait(), timeout=5)

        assert not second.done()
        assert not second_entered.is_set()

        release_first.set()
        first_result, second_result = await asyncio.gather(first, second)
        assert [first_result.iterations[0].iteration, second_result.iterations[0].iteration] == [
            0,
            1,
        ]
        assert second_result.initial_score == 1.0
        assert second_result.final_score == 2.0
    finally:
        release_first.set()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        await resources.aclose()


@pytest.mark.asyncio
async def test_unrelated_experiments_enter_candidate_work_concurrently(tmp_path) -> None:
    resources = build_test_runtime(tmp_path)
    dispatcher = resources.dispatcher
    handler = _family_handler(dispatcher)
    release = asyncio.Event()
    entered = {name: asyncio.Event() for name in ("alpha", "beta")}
    tasks: list[asyncio.Task[Any]] = []
    try:
        base = await _base_world(dispatcher, tmp_path, "unrelated-experiment-base")

        def candidate(name: str) -> Callable[[Any], Any]:
            async def prepare(_context: Any) -> None:
                entered[name].set()
                await release.wait()

            return prepare

        for name in ("alpha", "beta"):
            tasks.append(
                asyncio.create_task(
                    _run(
                        handler,
                        base.world_id,
                        _config(name),
                        evaluator=lambda _rollout: 1.0,
                        prepare_candidate=candidate(name),
                    )
                )
            )

        await asyncio.wait_for(
            asyncio.gather(*(event.wait() for event in entered.values())),
            timeout=5,
        )
        assert all(not task.done() for task in tasks)

        release.set()
        results = await asyncio.gather(*tasks)
        assert [result.iterations[0].iteration for result in results] == [0, 0]
    finally:
        release.set()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        await resources.aclose()


@pytest.mark.asyncio
async def test_cancellation_releases_experiment_admission_for_resume(tmp_path) -> None:
    resources = build_test_runtime(tmp_path)
    dispatcher = resources.dispatcher
    handler = _family_handler(dispatcher)
    entered = asyncio.Event()
    release = asyncio.Event()
    first: asyncio.Task[Any] | None = None
    try:
        base = await _base_world(dispatcher, tmp_path, "cancelled-experiment-base")
        config = _config("cancelled-experiment")

        async def blocked_candidate(_context: Any) -> None:
            entered.set()
            await release.wait()

        first = asyncio.create_task(
            _run(
                handler,
                base.world_id,
                config,
                evaluator=lambda _rollout: 1.0,
                prepare_candidate=blocked_candidate,
            )
        )
        await asyncio.wait_for(entered.wait(), timeout=5)
        first.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first

        resumed = await asyncio.wait_for(
            _run(
                handler,
                base.world_id,
                config,
                evaluator=lambda _rollout: 2.0,
                prepare_candidate=lambda _context: None,
            ),
            timeout=5,
        )
        assert resumed.iterations[0].iteration == 1
        assert resumed.final_score == 2.0
    finally:
        release.set()
        if first is not None and not first.done():
            first.cancel()
            await asyncio.gather(first, return_exceptions=True)
        await resources.aclose()


@pytest.mark.asyncio
async def test_record_to_ledger_false_bypasses_keyed_admission(tmp_path) -> None:
    resources = build_test_runtime(tmp_path)
    dispatcher = resources.dispatcher
    handler = _family_handler(dispatcher)
    release = asyncio.Event()
    entered = [asyncio.Event(), asyncio.Event()]
    tasks: list[asyncio.Task[Any]] = []
    try:
        base = await _base_world(dispatcher, tmp_path, "ephemeral-experiment-base")
        config = _config("ephemeral-experiment", record_to_ledger=False)

        def candidate(index: int) -> Callable[[Any], Any]:
            async def prepare(_context: Any) -> None:
                entered[index].set()
                await release.wait()

            return prepare

        for index in range(2):
            tasks.append(
                asyncio.create_task(
                    _run(
                        handler,
                        base.world_id,
                        config,
                        evaluator=lambda _rollout: 1.0,
                        prepare_candidate=candidate(index),
                    )
                )
            )

        await asyncio.wait_for(
            asyncio.gather(*(event.wait() for event in entered)),
            timeout=5,
        )
        assert all(not task.done() for task in tasks)
    finally:
        for task in tasks:
            task.cancel()
        release.set()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        await resources.aclose()
