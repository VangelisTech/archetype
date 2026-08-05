# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Contract tests for the execution hierarchy: episode and rollout semantics.

Exercises exact episode and rollout operations through the composed runtime owners.
Verifies termination predicates, fork isolation, registry cleanup, and result shapes.
"""

import asyncio
from dataclasses import dataclass
from functools import partial
from typing import Any

import pytest
from daft import DataFrame, col

from archetype.commands.dispatch import CommandDispatcher
from archetype.commands.models import DurableOptions
from archetype.commands.scheduler import CommandScheduler
from archetype.core.aio.async_processor import AsyncProcessor
from archetype.core.component import Component
from archetype.core.config import StorageConfig, WorldConfig
from archetype.runtime_resources import RuntimeResources
from archetype.storage.service import StorageService
from archetype.world import simulation
from archetype.world.lifecycle import WorldLifecycle
from archetype.world.models import (
    AddProcessor,
    ComponentTypeRef,
    CreateWorld,
    Despawn,
    DestroyWorld,
    EpisodeConfig,
    GetWorldInfo,
    ListWorlds,
    QueryComponents,
    RolloutConfig,
    RunEpisode,
    RunRollout,
    Spawn,
)
from archetype.world.registry import WorldRegistry
from tests._runtime import build_test_runtime

_TEARDOWN_SENTINEL_ENTITY_ID = 2_147_483_647

# ---------------------------------------------------------------------------
# Test components
# ---------------------------------------------------------------------------


class Pos(Component):
    x: float = 0.0


class Terminal(Component):
    done: bool = True


class Countdown(Component):
    """Counts up each tick; ``done`` latches once ``step`` reaches ``goal``.

    Lets one episode hold entities that finish at different ticks, so the
    value-based "all" vs "any" termination reducers can be told apart.
    """

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


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class _RolloutHarness:
    resources: RuntimeResources
    dispatcher: CommandDispatcher
    registry: WorldRegistry
    storage: StorageService
    lifecycle: WorldLifecycle
    scheduler: CommandScheduler
    fork_world: Any
    destroy_world: Any

    async def close(self) -> None:
        await self.resources.aclose()


def _build_harness(tmp_path) -> _RolloutHarness:
    resources = build_test_runtime(tmp_path)
    dispatcher = resources.dispatcher

    rollout_handler = dispatcher._registry.resolve_name("run_rollout").handler
    assert isinstance(rollout_handler, partial)
    assert len(rollout_handler.args) == 4
    registry, storage, fork_world, destroy_world = rollout_handler.args
    assert isinstance(registry, WorldRegistry)
    assert isinstance(storage, StorageService)

    create_handler = dispatcher._registry.resolve_name("create_world").handler
    assert isinstance(create_handler, partial)
    assert create_handler.args
    create_world = create_handler.args[0]
    lifecycle = create_world.__self__
    assert isinstance(lifecycle, WorldLifecycle)
    scheduler = dispatcher._scheduler
    assert isinstance(scheduler, CommandScheduler)
    return _RolloutHarness(
        resources=resources,
        dispatcher=dispatcher,
        registry=registry,
        storage=storage,
        lifecycle=lifecycle,
        scheduler=scheduler,
        fork_world=fork_world,
        destroy_world=destroy_world,
    )


async def _make_world(harness: _RolloutHarness, tmp_path, name: str = "test"):
    storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
    return await harness.dispatcher.apply(
        CreateWorld(
            config=WorldConfig(name=name),
            storage_config=storage,
        )
    )


async def _run_episode(
    harness: _RolloutHarness,
    world_id: object,
    config: EpisodeConfig,
):
    return await harness.dispatcher.apply(
        RunEpisode(
            world_id=world_id,
            config=config,
        )
    )


async def _run_rollout(
    harness: _RolloutHarness,
    world_id: object,
    config: RolloutConfig,
    *,
    fork_world=None,
    destroy_world=None,
):
    if fork_world is None and destroy_world is None:
        return await harness.dispatcher.apply(
            RunRollout(
                world_id=world_id,
                config=config,
            )
        )
    # White-box failure injection still owns the exact dispatcher admission
    # lease so stop/wait assertions observe the production drain boundary.
    async with harness.dispatcher._admitted():
        return await simulation.run_rollout(
            harness.registry,
            harness.storage,
            fork_world or harness.fork_world,
            destroy_world or harness.destroy_world,
            world_id,
            config,
        )


async def _stop_admission(harness: _RolloutHarness) -> None:
    await harness.dispatcher.stop_admission()
    await harness.dispatcher.wait_drained()


async def _spawn(
    harness: _RolloutHarness,
    world_id: object,
    *components: Component,
) -> int:
    return await harness.dispatcher.apply(
        Spawn.from_components(
            world_id=world_id,
            components=list(components),
        )
    )


def _queue_future_command_on_each_fork(
    harness: _RolloutHarness,
    monkeypatch: pytest.MonkeyPatch,
) -> dict[str, object]:
    """Admit one command after fork creation and before its episode starts."""

    original_fork = harness.lifecycle.fork_world
    commands: dict[str, object] = {}

    async def fork_and_queue(*args, **kwargs):
        fork = await original_fork(*args, **kwargs)
        command_id = await harness.dispatcher.defer(
            Despawn(
                world_id=fork.world_id,
                entity_id=_TEARDOWN_SENTINEL_ENTITY_ID,
            ),
            DurableOptions(target_tick=10_000),
        )
        commands[str(fork.world_id)] = command_id
        return fork

    monkeypatch.setattr(harness.lifecycle, "fork_world", fork_and_queue)
    return commands


# ---------------------------------------------------------------------------
# Episode tests
# ---------------------------------------------------------------------------


class TestEpisode:
    @pytest.mark.asyncio
    async def test_episode_terminates_on_terminal_component(self, tmp_path):
        """Episode stops when an entity with the terminal_component is detected."""
        harness = _build_harness(tmp_path)
        try:
            world = await _make_world(harness, tmp_path)

            # Spawn an entity with Terminal before the episode starts.
            # terminal_component check happens before each step, so
            # the episode should terminate immediately with 0 steps.
            await _spawn(harness, world.world_id, Pos(x=1.0), Terminal(done=True))

            config = EpisodeConfig(
                max_steps=100,
                terminal_component=Terminal,
            )
            result = await _run_episode(harness, world.world_id, config)

            assert result.terminated is True
            assert result.duration_steps == 0
        finally:
            await harness.close()

    @pytest.mark.asyncio
    async def test_episode_terminates_on_callable(self, tmp_path):
        """Episode stops when the termination callable returns True."""
        harness = _build_harness(tmp_path)
        try:
            world = await _make_world(harness, tmp_path)

            config = EpisodeConfig(
                max_steps=100,
                termination=lambda w: w.tick >= 5,
            )
            result = await _run_episode(harness, world.world_id, config)

            assert result.terminated is True
            assert result.final_tick == 5
            assert result.duration_steps == 5
        finally:
            await harness.close()

    @pytest.mark.asyncio
    async def test_episode_caps_at_max_steps(self, tmp_path):
        """No termination predicate: episode runs exactly max_steps ticks."""
        harness = _build_harness(tmp_path)
        try:
            world = await _make_world(harness, tmp_path)

            config = EpisodeConfig(max_steps=10)
            result = await _run_episode(harness, world.world_id, config)

            assert result.duration_steps == 10
            assert result.run_id == world.run_id
            assert result.start_tick == 0
            assert result.final_tick == 10
        finally:
            await harness.close()

    @pytest.mark.asyncio
    async def test_episode_does_not_fork(self, tmp_path):
        """world_id before and after episode is the same."""
        harness = _build_harness(tmp_path)
        try:
            world = await _make_world(harness, tmp_path)
            wid_before = world.world_id

            config = EpisodeConfig(max_steps=3)
            result = await _run_episode(harness, world.world_id, config)

            assert str(result.world_id) == str(wid_before)
            assert result.run_id is not None
        finally:
            await harness.close()

    @pytest.mark.asyncio
    async def test_terminated_false_when_max_steps_cap(self, tmp_path):
        """terminated is False when the episode stopped due to max_steps, not a predicate."""
        harness = _build_harness(tmp_path)
        try:
            world = await _make_world(harness, tmp_path)

            config = EpisodeConfig(max_steps=5)
            result = await _run_episode(harness, world.world_id, config)

            assert result.terminated is False
            assert result.duration_steps == 5
        finally:
            await harness.close()


# ---------------------------------------------------------------------------
# Value-based "all done" termination (bug B2)
# ---------------------------------------------------------------------------


async def _make_countdown_world(harness: _RolloutHarness, tmp_path, name="b2"):
    world = await _make_world(harness, tmp_path, name)
    await harness.dispatcher.apply(
        AddProcessor(
            world_id=world.world_id,
            processor=CountToGoal(),
        )
    )
    return world


async def _done_by_entity(
    harness: _RolloutHarness,
    world,
) -> dict[int, bool]:
    """``done`` at the latest committed frame, keyed by entity id."""
    rows = (
        await harness.dispatcher.apply(
            QueryComponents(
                components=(ComponentTypeRef.from_type(Countdown),),
                world_id=world.world_id,
                run_id=world.run_id,
            )
        )
    ).to_pylist()
    return {r["entity_id"]: bool(r["countdown__done"]) for r in rows}


class TestValueBasedTermination:
    """``terminal_component`` + ``terminal_field`` = stop when entities latch.

    GIVEN entities whose boolean field flips True after some ticks
    WHEN run with terminal_field set
    THEN the episode stops on the data, not at max_steps — and ``terminal_all``
         decides whether it waits for every entity or stops at the first.
    """

    @pytest.mark.asyncio
    async def test_all_done_terminates_early_not_at_max_steps(self, tmp_path):
        harness = _build_harness(tmp_path)
        try:
            world = await _make_countdown_world(harness, tmp_path)
            await _spawn(harness, world.world_id, Countdown(goal=3))

            config = EpisodeConfig(
                max_steps=50,
                terminal_component=Countdown,
                terminal_field="done",
                terminal_all=True,
            )
            result = await _run_episode(harness, world.world_id, config)

            assert result.terminated is True
            assert result.duration_steps < 50  # stopped on the data, not the cap
            # The reason it stopped: the entity is genuinely done.
            assert all((await _done_by_entity(harness, world)).values())
        finally:
            await harness.close()

    @pytest.mark.asyncio
    async def test_terminal_field_suppresses_structural_check(self, tmp_path):
        """With terminal_field set, the entity merely *carrying* Countdown must
        NOT end the episode at tick 0 (that is the structural path)."""
        harness = _build_harness(tmp_path)
        try:
            world = await _make_countdown_world(harness, tmp_path)
            await _spawn(harness, world.world_id, Countdown(goal=4))

            config = EpisodeConfig(
                max_steps=50,
                terminal_component=Countdown,
                terminal_field="done",
            )
            result = await _run_episode(harness, world.world_id, config)

            assert result.terminated is True
            assert result.duration_steps > 1  # ran, did not fire structurally at tick 0
        finally:
            await harness.close()

    @pytest.mark.asyncio
    async def test_all_mode_waits_for_the_slowest_entity(self, tmp_path):
        harness = _build_harness(tmp_path)
        try:
            world = await _make_countdown_world(harness, tmp_path)
            fast = await _spawn(harness, world.world_id, Countdown(goal=2))
            slow = await _spawn(harness, world.world_id, Countdown(goal=6))

            config = EpisodeConfig(
                max_steps=50,
                terminal_component=Countdown,
                terminal_field="done",
                terminal_all=True,
            )
            result = await _run_episode(harness, world.world_id, config)

            assert result.terminated is True
            assert result.duration_steps < 50
            done = await _done_by_entity(harness, world)
            # Waited for *both* — including the slow one.
            assert done[fast] is True
            assert done[slow] is True
        finally:
            await harness.close()

    @pytest.mark.asyncio
    async def test_any_mode_stops_at_the_first_entity(self, tmp_path):
        harness = _build_harness(tmp_path)
        try:
            world = await _make_countdown_world(harness, tmp_path)
            fast = await _spawn(harness, world.world_id, Countdown(goal=2))
            slow = await _spawn(harness, world.world_id, Countdown(goal=6))

            config = EpisodeConfig(
                max_steps=50,
                terminal_component=Countdown,
                terminal_field="done",
                terminal_all=False,
            )
            result = await _run_episode(harness, world.world_id, config)

            assert result.terminated is True
            done = await _done_by_entity(harness, world)
            # Stopped at the first finisher; the slow one is still running.
            assert done[fast] is True
            assert done[slow] is False
        finally:
            await harness.close()

    @pytest.mark.asyncio
    async def test_caps_at_max_steps_when_never_done(self, tmp_path):
        harness = _build_harness(tmp_path)
        try:
            world = await _make_countdown_world(harness, tmp_path)
            await _spawn(harness, world.world_id, Countdown(goal=999))

            config = EpisodeConfig(
                max_steps=4,
                terminal_component=Countdown,
                terminal_field="done",
                terminal_all=True,
            )
            result = await _run_episode(harness, world.world_id, config)

            assert result.terminated is False
            assert result.duration_steps == 4
        finally:
            await harness.close()


# ---------------------------------------------------------------------------
# Rollout tests
# ---------------------------------------------------------------------------


class TestRollout:
    @pytest.mark.asyncio
    async def test_rollout_creates_n_forks(self, tmp_path):
        """num_episodes=3 produces 3 EpisodeResult entries."""
        harness = _build_harness(tmp_path)
        try:
            world = await _make_world(harness, tmp_path)

            config = RolloutConfig(
                num_episodes=3,
                episode_config=EpisodeConfig(max_steps=2),
            )
            result = await _run_rollout(harness, world.world_id, config)

            assert result.num_episodes == 3
            assert len(result.episodes) == 3
        finally:
            await harness.close()

    @pytest.mark.asyncio
    async def test_base_world_tick_unchanged_by_rollout(self, tmp_path):
        """Forks do the work, not the base world."""
        harness = _build_harness(tmp_path)
        try:
            world = await _make_world(harness, tmp_path)
            tick_before = world.tick

            config = RolloutConfig(
                num_episodes=2,
                episode_config=EpisodeConfig(max_steps=5),
            )
            await _run_rollout(harness, world.world_id, config)

            current = await harness.dispatcher.apply(GetWorldInfo(world_id=world.world_id))
            assert current.tick == tick_before
        finally:
            await harness.close()

    @pytest.mark.asyncio
    async def test_destroy_forks_on_complete(self, tmp_path):
        """Forks are removed from the registry after rollout when flag is set."""
        harness = _build_harness(tmp_path)
        try:
            world = await _make_world(harness, tmp_path)

            config = RolloutConfig(
                num_episodes=2,
                episode_config=EpisodeConfig(max_steps=2),
                destroy_forks_on_complete=True,
            )
            result = await _run_rollout(harness, world.world_id, config)

            # Fork worlds should no longer be in the registry
            for ep in result.episodes:
                assert not await harness.registry.contains(str(ep.world_id))

            # Base world should still be in the registry
            assert await harness.registry.contains(str(world.world_id))
        finally:
            await harness.close()

    @pytest.mark.asyncio
    async def test_auto_destroy_rejects_queued_command_before_durable_destroy(
        self,
        tmp_path,
        monkeypatch,
    ):
        """Rollout cleanup follows reconcile -> cancel -> lifecycle destroy."""
        harness = _build_harness(tmp_path)
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        try:
            world = await _make_world(harness, tmp_path, "command-cleanup")
            commands = _queue_future_command_on_each_fork(harness, monkeypatch)
            catalog = harness.storage.get_control_catalog(storage)
            set_world_status = catalog.set_world_status
            observed_before_destroy: list[tuple[str, str, str | None]] = []

            async def observe_set_world_status(world_id: str, status: str) -> None:
                if status == "destroyed" and world_id in commands:
                    (record,) = await harness.scheduler.records(world_id)
                    observed_before_destroy.append(
                        (world_id, record.status, record.last_error_code)
                    )
                await set_world_status(world_id, status)

            monkeypatch.setattr(catalog, "set_world_status", observe_set_world_status)

            result = await _run_rollout(
                harness,
                world.world_id,
                RolloutConfig(
                    num_episodes=1,
                    episode_config=EpisodeConfig(max_steps=0),
                    destroy_forks_on_complete=True,
                ),
            )

            fork_id = str(result.episodes[0].world_id)
            assert observed_before_destroy == [(fork_id, "REJECTED", "world_destroyed")]
            (record,) = await harness.scheduler.records(fork_id)
            assert record.command_id == str(commands[fork_id])
            assert record.status == "REJECTED"
            assert record.last_error_code == "world_destroyed"
            durable_world = await catalog.get_world(fork_id)
            assert durable_world is not None and durable_world.status == "destroyed"
            assert not await harness.registry.contains(fork_id)
        finally:
            await harness.close()

    @pytest.mark.asyncio
    async def test_auto_destroy_runs_after_episode_failure(
        self,
        tmp_path,
        monkeypatch,
    ):
        """An episode exception does not strand its fork or queued command."""
        harness = _build_harness(tmp_path)
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        try:
            world = await _make_world(harness, tmp_path, "failed-episode-cleanup")
            commands = _queue_future_command_on_each_fork(harness, monkeypatch)
            catalog = harness.storage.get_control_catalog(storage)
            set_world_status = catalog.set_world_status
            observed_before_destroy: list[tuple[str, str, str | None]] = []

            async def observe_set_world_status(world_id: str, status: str) -> None:
                if status == "destroyed" and world_id in commands:
                    (record,) = await harness.scheduler.records(world_id)
                    observed_before_destroy.append(
                        (world_id, record.status, record.last_error_code)
                    )
                await set_world_status(world_id, status)

            async def fail_episode(*args, **kwargs):
                del args, kwargs
                raise RuntimeError("episode failed")

            monkeypatch.setattr(catalog, "set_world_status", observe_set_world_status)
            monkeypatch.setattr(simulation, "run_episode", fail_episode)

            with pytest.raises(RuntimeError, match="episode failed"):
                await _run_rollout(
                    harness,
                    world.world_id,
                    RolloutConfig(
                        num_episodes=1,
                        episode_config=EpisodeConfig(max_steps=1),
                        destroy_forks_on_complete=True,
                    ),
                )

            (fork_id,) = commands
            assert observed_before_destroy == [(fork_id, "REJECTED", "world_destroyed")]
            (record,) = await harness.scheduler.records(fork_id)
            assert record.status == "REJECTED"
            durable_world = await catalog.get_world(fork_id)
            assert durable_world is not None and durable_world.status == "destroyed"
            assert not await harness.registry.contains(fork_id)
        finally:
            await harness.close()

    @pytest.mark.asyncio
    async def test_auto_destroy_runs_after_rollout_cancellation(
        self,
        tmp_path,
        monkeypatch,
    ):
        """Caller cancellation still executes the fork's teardown finally."""
        harness = _build_harness(tmp_path)
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        episode_entered = asyncio.Event()
        try:
            world = await _make_world(harness, tmp_path, "cancelled-episode-cleanup")
            commands = _queue_future_command_on_each_fork(harness, monkeypatch)
            catalog = harness.storage.get_control_catalog(storage)
            set_world_status = catalog.set_world_status
            observed_before_destroy: list[tuple[str, str, str | None]] = []

            async def observe_set_world_status(world_id: str, status: str) -> None:
                if status == "destroyed" and world_id in commands:
                    (record,) = await harness.scheduler.records(world_id)
                    observed_before_destroy.append(
                        (world_id, record.status, record.last_error_code)
                    )
                await set_world_status(world_id, status)

            async def block_episode(*args, **kwargs):
                del args, kwargs
                episode_entered.set()
                await asyncio.Event().wait()

            monkeypatch.setattr(catalog, "set_world_status", observe_set_world_status)
            monkeypatch.setattr(simulation, "run_episode", block_episode)
            rollout = asyncio.create_task(
                _run_rollout(
                    harness,
                    world.world_id,
                    RolloutConfig(
                        num_episodes=1,
                        episode_config=EpisodeConfig(max_steps=1),
                        destroy_forks_on_complete=True,
                    ),
                )
            )
            await asyncio.wait_for(episode_entered.wait(), timeout=2)
            rollout.cancel()
            rollout.cancel()
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(asyncio.shield(rollout), timeout=2)

            (fork_id,) = commands
            assert observed_before_destroy == [(fork_id, "REJECTED", "world_destroyed")]
            (record,) = await harness.scheduler.records(fork_id)
            assert record.status == "REJECTED"
            durable_world = await catalog.get_world(fork_id)
            assert durable_world is not None and durable_world.status == "destroyed"
            assert not await harness.registry.contains(fork_id)
        finally:
            await harness.close()

    @pytest.mark.asyncio
    async def test_sequential_cancellation_drains_owned_fork_before_stopping(
        self,
        tmp_path,
        monkeypatch,
    ):
        """Cancellation during fork handoff closes it and starts no successor."""
        harness = _build_harness(tmp_path)
        StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        fork_created = asyncio.Event()
        release_fork = asyncio.Event()
        try:
            world = await _make_world(harness, tmp_path, "sequential-fork-handoff")
            commands = _queue_future_command_on_each_fork(harness, monkeypatch)
            fork_and_queue = harness.lifecycle.fork_world

            async def hold_fork_handoff(*args, **kwargs):
                fork = await fork_and_queue(*args, **kwargs)
                fork_created.set()
                await release_fork.wait()
                return fork

            async def unexpected_episode(*_args, **_kwargs):
                pytest.fail("a cancelled fork handoff must skip its episode")

            monkeypatch.setattr(
                harness.lifecycle,
                "fork_world",
                hold_fork_handoff,
            )
            monkeypatch.setattr(simulation, "run_episode", unexpected_episode)
            rollout = asyncio.create_task(
                _run_rollout(
                    harness,
                    world.world_id,
                    RolloutConfig(
                        num_episodes=2,
                        episode_config=EpisodeConfig(max_steps=0),
                        destroy_forks_on_complete=True,
                    ),
                )
            )

            await asyncio.wait_for(fork_created.wait(), timeout=2)
            rollout.cancel()
            rollout.cancel()
            await asyncio.sleep(0)
            assert not rollout.done()

            release_fork.set()
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(asyncio.shield(rollout), timeout=2)

            assert len(commands) == 1
            ((fork_id, command),) = commands.items()
            (record,) = await harness.scheduler.records(fork_id)
            assert record.command_id == str(command)
            assert record.status == "REJECTED"
            assert record.last_error_code == "world_destroyed"
            assert not await harness.registry.contains(fork_id)
        finally:
            release_fork.set()
            await harness.close()

    @pytest.mark.asyncio
    async def test_parallel_immediate_cancellation_drains_unstarted_children(
        self,
        tmp_path,
        monkeypatch,
    ):
        """Cancellation before child entry cannot strand the aggregate waiter."""
        harness = _build_harness(tmp_path)
        StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        create_task = asyncio.create_task
        child_tasks: list[asyncio.Task[object]] = []
        cancellation_injected = False
        try:
            world = await _make_world(harness, tmp_path, "parallel-immediate-cancel")

            def create_and_cancel_parent(coro, *, name=None, context=None):
                nonlocal cancellation_injected
                task = create_task(coro, name=name, context=context)
                if isinstance(name, str) and name.startswith("archetype-rollout:"):
                    child_tasks.append(task)
                    task.cancel()
                    if not cancellation_injected:
                        cancellation_injected = True
                        parent = asyncio.current_task()
                        assert parent is not None
                        parent.cancel()
                return task

            monkeypatch.setattr(asyncio, "create_task", create_and_cancel_parent)
            rollout = create_task(
                _run_rollout(
                    harness,
                    world.world_id,
                    RolloutConfig(
                        num_episodes=2,
                        parallel=True,
                        episode_config=EpisodeConfig(max_steps=0),
                        destroy_forks_on_complete=True,
                    ),
                )
            )

            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(asyncio.shield(rollout), timeout=2)

            assert cancellation_injected
            assert len(child_tasks) == 2
            assert all(task.done() and task.cancelled() for task in child_tasks)
            assert [item.name for item in await harness.dispatcher.apply(ListWorlds())] == [
                "parallel-immediate-cancel"
            ]
        finally:
            await harness.close()

    @pytest.mark.asyncio
    async def test_parent_cancellation_retains_owned_fork_self_cancellation(
        self,
        tmp_path,
        monkeypatch,
    ):
        """Owned acquisition cancellation is material, not waiter interruption."""
        harness = _build_harness(tmp_path)
        StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        acquisition_entered = asyncio.Event()
        release_acquisition = asyncio.Event()
        owned_cancellation = asyncio.CancelledError("owned fork cancellation")
        owned_cancellation.add_note("pre-created fork cancellation note")
        acquisition_tasks: set[asyncio.Task[object]] = set()
        try:
            world = await _make_world(harness, tmp_path, "owned-fork-self-cancel")

            async def self_cancel_fork(*_args, **_kwargs):
                task = asyncio.current_task()
                assert task is not None
                acquisition_tasks.add(task)
                acquisition_entered.set()
                await release_acquisition.wait()
                raise owned_cancellation

            monkeypatch.setattr(
                harness.lifecycle,
                "fork_world",
                self_cancel_fork,
            )
            rollout = asyncio.create_task(
                _run_rollout(
                    harness,
                    world.world_id,
                    RolloutConfig(
                        num_episodes=1,
                        parallel=True,
                        episode_config=EpisodeConfig(max_steps=0),
                        destroy_forks_on_complete=True,
                    ),
                )
            )

            await asyncio.wait_for(acquisition_entered.wait(), timeout=2)
            rollout.cancel("parent cancellation during acquisition")
            release_acquisition.set()
            with pytest.raises(asyncio.CancelledError) as raised:
                await rollout

            assert raised.value.args == ("parent cancellation during acquisition",)
            assert isinstance(raised.value.__cause__, BaseExceptionGroup)
            assert raised.value.__cause__.exceptions == (owned_cancellation,)
            assert getattr(owned_cancellation, "__notes__", ()) == [
                "pre-created fork cancellation note"
            ]
            assert [item.name for item in await harness.dispatcher.apply(ListWorlds())] == [
                "owned-fork-self-cancel"
            ]
            assert len(acquisition_tasks) == 1
            assert all(task.done() for task in acquisition_tasks)
        finally:
            release_acquisition.set()
            await harness.close()

    @pytest.mark.asyncio
    async def test_parallel_failure_drains_sibling_teardown_before_admission(
        self,
        tmp_path,
        monkeypatch,
    ):
        """A first child failure cannot outlive rollout-owned fork cleanup."""
        harness = _build_harness(tmp_path)
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        sibling_entered = asyncio.Event()
        release_sibling = asyncio.Event()
        failed_fork_destroyed = asyncio.Event()
        failure = RuntimeError("first parallel episode failed")
        episode_tasks: set[asyncio.Task[object]] = set()
        failed_fork_id: str | None = None
        original_run_episode = simulation.run_episode
        try:
            world = await _make_world(harness, tmp_path, "parallel-failure-drain")
            commands = _queue_future_command_on_each_fork(harness, monkeypatch)
            catalog = harness.storage.get_control_catalog(storage)
            set_world_status = catalog.set_world_status
            observed_before_destroy: list[tuple[str, str, str | None]] = []

            async def observe_set_world_status(world_id: str, status: str) -> None:
                if status == "destroyed" and world_id in commands:
                    (record,) = await harness.scheduler.records(world_id)
                    observed_before_destroy.append(
                        (world_id, record.status, record.last_error_code)
                    )
                    if world_id == failed_fork_id:
                        failed_fork_destroyed.set()
                await set_world_status(world_id, status)

            async def fail_first_and_block_sibling(
                registry,
                storage_service,
                fork_world_id,
                config,
                **input_kwargs,
            ):
                nonlocal failed_fork_id
                task = asyncio.current_task()
                assert task is not None
                episode_tasks.add(task)
                fork = await harness.registry.live_world(fork_world_id)
                assert fork is not None
                if fork.name.endswith(":0"):
                    failed_fork_id = str(fork_world_id)
                    await sibling_entered.wait()
                    raise failure
                sibling_entered.set()
                await release_sibling.wait()
                return await original_run_episode(
                    registry,
                    storage_service,
                    fork_world_id,
                    config,
                    **input_kwargs,
                )

            monkeypatch.setattr(catalog, "set_world_status", observe_set_world_status)
            monkeypatch.setattr(
                simulation,
                "run_episode",
                fail_first_and_block_sibling,
            )
            rollout = asyncio.create_task(
                _run_rollout(
                    harness,
                    world.world_id,
                    RolloutConfig(
                        num_episodes=2,
                        parallel=True,
                        episode_config=EpisodeConfig(max_steps=0),
                        destroy_forks_on_complete=True,
                    ),
                )
            )

            await asyncio.wait_for(failed_fork_destroyed.wait(), timeout=2)
            assert not rollout.done()
            stop = asyncio.create_task(_stop_admission(harness))
            await asyncio.sleep(0)
            assert not stop.done()

            release_sibling.set()
            with pytest.raises(RuntimeError) as raised:
                await asyncio.wait_for(rollout, timeout=2)
            assert raised.value is failure
            await asyncio.wait_for(stop, timeout=2)

            assert len(commands) == 2
            assert len(observed_before_destroy) == 2
            assert all(
                (status, error_code) == ("REJECTED", "world_destroyed")
                for _world_id, status, error_code in observed_before_destroy
            )
            for fork_id, command in commands.items():
                (record,) = await harness.scheduler.records(fork_id)
                assert record.command_id == str(command)
                assert record.status == "REJECTED"
                assert not await harness.registry.contains(fork_id)
            assert len(episode_tasks) == 2
            assert all(task.done() for task in episode_tasks)
        finally:
            release_sibling.set()
            await harness.close()

    @pytest.mark.asyncio
    async def test_parallel_multiple_failures_keep_first_exception_primary(
        self,
        tmp_path,
        monkeypatch,
    ):
        """Later failures remain available without replacing the prompt one."""
        harness = _build_harness(tmp_path)
        StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        primary_ready = asyncio.Event()
        primary = RuntimeError("primary episode failure")
        secondary = ValueError("secondary episode failure")
        try:
            world = await _make_world(harness, tmp_path, "parallel-multiple-failures")
            commands = _queue_future_command_on_each_fork(harness, monkeypatch)

            async def fail_in_observed_order(
                _registry,
                _storage_service,
                fork_world_id,
                _config,
                **_input_kwargs,
            ):
                fork = await harness.registry.live_world(fork_world_id)
                assert fork is not None
                if fork.name.endswith(":0"):
                    await primary_ready.wait()
                    await asyncio.sleep(0)
                    raise secondary
                primary_ready.set()
                raise primary

            monkeypatch.setattr(
                simulation,
                "run_episode",
                fail_in_observed_order,
            )
            with pytest.raises(RuntimeError) as raised:
                await _run_rollout(
                    harness,
                    world.world_id,
                    RolloutConfig(
                        num_episodes=2,
                        parallel=True,
                        episode_config=EpisodeConfig(max_steps=0),
                        destroy_forks_on_complete=True,
                    ),
                )

            assert raised.value is primary
            assert raised.value.__cause__ is None
            assert any(
                "additional rollout failure: child_index=0 phase=episode "
                "type=builtins.ValueError fork_world_id=" in note
                for note in getattr(raised.value, "__notes__", ())
            )
            assert len(commands) == 2
            for fork_id in commands:
                assert not await harness.registry.contains(fork_id)
        finally:
            await harness.close()

    @pytest.mark.asyncio
    async def test_parallel_cancellation_waits_for_every_fork_teardown(
        self,
        tmp_path,
        monkeypatch,
    ):
        """Repeated caller cancellation cannot interrupt child cleanup."""
        harness = _build_harness(tmp_path)
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        episodes_entered = asyncio.Event()
        teardown_entered = asyncio.Event()
        release_teardown = asyncio.Event()
        episode_tasks: set[asyncio.Task[object]] = set()
        fork_tasks: set[asyncio.Task[object]] = set()
        teardown_tasks: set[asyncio.Task[object]] = set()
        entered_ids: set[str] = set()
        teardown_ids: set[str] = set()
        try:
            world = await _make_world(harness, tmp_path, "parallel-cancellation-drain")
            commands = _queue_future_command_on_each_fork(harness, monkeypatch)
            fork_world = harness.lifecycle.fork_world
            catalog = harness.storage.get_control_catalog(storage)
            set_world_status = catalog.set_world_status
            observed_before_destroy: list[tuple[str, str, str | None]] = []
            destroy_world = harness.destroy_world

            async def track_fork(*args, **kwargs):
                task = asyncio.current_task()
                assert task is not None
                fork_tasks.add(task)
                return await fork_world(*args, **kwargs)

            async def observe_set_world_status(world_id: str, status: str) -> None:
                if status == "destroyed" and world_id in commands:
                    (record,) = await harness.scheduler.records(world_id)
                    observed_before_destroy.append(
                        (world_id, record.status, record.last_error_code)
                    )
                await set_world_status(world_id, status)

            async def block_episode(
                _registry,
                _storage_service,
                fork_world_id,
                _config,
                **_input_kwargs,
            ):
                task = asyncio.current_task()
                assert task is not None
                episode_tasks.add(task)
                entered_ids.add(str(fork_world_id))
                if len(entered_ids) == 2:
                    episodes_entered.set()
                await asyncio.Event().wait()

            async def block_teardown(fork_world_id):
                task = asyncio.current_task()
                assert task is not None
                teardown_tasks.add(task)
                teardown_ids.add(str(fork_world_id))
                if len(teardown_ids) == 2:
                    teardown_entered.set()
                await release_teardown.wait()
                await destroy_world(fork_world_id)

            monkeypatch.setattr(catalog, "set_world_status", observe_set_world_status)
            monkeypatch.setattr(harness.lifecycle, "fork_world", track_fork)
            monkeypatch.setattr(simulation, "run_episode", block_episode)
            rollout = asyncio.create_task(
                _run_rollout(
                    harness,
                    world.world_id,
                    RolloutConfig(
                        num_episodes=2,
                        parallel=True,
                        episode_config=EpisodeConfig(max_steps=0),
                        destroy_forks_on_complete=True,
                    ),
                    destroy_world=block_teardown,
                )
            )

            await asyncio.wait_for(episodes_entered.wait(), timeout=2)
            rollout.cancel()
            await asyncio.wait_for(teardown_entered.wait(), timeout=2)
            assert not rollout.done()
            stop = asyncio.create_task(_stop_admission(harness))
            await asyncio.sleep(0)
            assert not stop.done()
            rollout.cancel()
            rollout.cancel()
            await asyncio.sleep(0)
            assert not rollout.done()
            release_teardown.set()
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(asyncio.shield(rollout), timeout=2)
            await asyncio.wait_for(stop, timeout=2)

            assert set(commands) == teardown_ids
            assert len(observed_before_destroy) == 2
            assert all(
                (status, error_code) == ("REJECTED", "world_destroyed")
                for _world_id, status, error_code in observed_before_destroy
            )
            for fork_id in commands:
                assert not await harness.registry.contains(fork_id)
            assert len(episode_tasks) == 2
            assert all(task.done() for task in episode_tasks)
            assert len(fork_tasks) == 2
            assert all(task.done() for task in fork_tasks)
            assert len(teardown_tasks) == 2
            assert all(task.done() for task in teardown_tasks)
        finally:
            release_teardown.set()
            await harness.close()

    @pytest.mark.asyncio
    async def test_cancellation_during_successful_teardown_keeps_parent_exact(
        self,
        tmp_path,
        monkeypatch,
    ):
        """Waiter interruption is not a successful teardown failure."""
        harness = _build_harness(tmp_path)
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        teardown_entered = asyncio.Event()
        release_teardown = asyncio.Event()
        teardown_tasks: set[asyncio.Task[object]] = set()
        try:
            world = await _make_world(harness, tmp_path, "cancel-during-successful-teardown")
            commands = _queue_future_command_on_each_fork(harness, monkeypatch)
            catalog = harness.storage.get_control_catalog(storage)
            set_world_status = catalog.set_world_status
            observed_before_destroy: list[tuple[str, str, str | None]] = []
            destroy_world = harness.destroy_world

            async def observe_set_world_status(world_id: str, status: str) -> None:
                if status == "destroyed" and world_id in commands:
                    (record,) = await harness.scheduler.records(world_id)
                    observed_before_destroy.append(
                        (world_id, record.status, record.last_error_code)
                    )
                await set_world_status(world_id, status)

            async def block_successful_teardown(fork_world_id):
                task = asyncio.current_task()
                assert task is not None
                teardown_tasks.add(task)
                teardown_entered.set()
                await release_teardown.wait()
                await destroy_world(fork_world_id)

            monkeypatch.setattr(catalog, "set_world_status", observe_set_world_status)
            rollout = asyncio.create_task(
                _run_rollout(
                    harness,
                    world.world_id,
                    RolloutConfig(
                        num_episodes=1,
                        parallel=True,
                        episode_config=EpisodeConfig(max_steps=0),
                        destroy_forks_on_complete=True,
                    ),
                    destroy_world=block_successful_teardown,
                )
            )

            await asyncio.wait_for(teardown_entered.wait(), timeout=2)
            rollout.cancel("original")
            await asyncio.sleep(0)
            assert not rollout.done()
            release_teardown.set()
            with pytest.raises(asyncio.CancelledError) as raised:
                await rollout

            assert raised.value.args == ("original",)
            assert raised.value.__cause__ is None
            (fork_id,) = commands
            assert observed_before_destroy == [(fork_id, "REJECTED", "world_destroyed")]
            (record,) = await harness.scheduler.records(fork_id)
            assert record.status == "REJECTED"
            assert record.last_error_code == "world_destroyed"
            assert not await harness.registry.contains(fork_id)
            assert len(teardown_tasks) == 1
            assert all(task.done() for task in teardown_tasks)
        finally:
            release_teardown.set()
            await harness.close()

    @pytest.mark.asyncio
    async def test_cancellation_during_teardown_keeps_episode_failure_as_cause(
        self,
        tmp_path,
        monkeypatch,
    ):
        """A child cancellation cannot erase a failure observed before cleanup."""
        harness = _build_harness(tmp_path)
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        teardown_entered = asyncio.Event()
        release_teardown = asyncio.Event()
        episode_failure = RuntimeError("episode failed before teardown")
        episode_tasks: set[asyncio.Task[object]] = set()
        teardown_tasks: set[asyncio.Task[object]] = set()
        try:
            world = await _make_world(harness, tmp_path, "cancel-after-episode-failure")
            commands = _queue_future_command_on_each_fork(harness, monkeypatch)
            catalog = harness.storage.get_control_catalog(storage)
            set_world_status = catalog.set_world_status
            observed_before_destroy: list[tuple[str, str, str | None]] = []
            destroy_world = harness.destroy_world

            async def observe_set_world_status(world_id: str, status: str) -> None:
                if status == "destroyed" and world_id in commands:
                    (record,) = await harness.scheduler.records(world_id)
                    observed_before_destroy.append(
                        (world_id, record.status, record.last_error_code)
                    )
                await set_world_status(world_id, status)

            async def fail_episode(*_args, **_kwargs):
                task = asyncio.current_task()
                assert task is not None
                episode_tasks.add(task)
                raise episode_failure

            async def block_teardown(fork_world_id):
                task = asyncio.current_task()
                assert task is not None
                teardown_tasks.add(task)
                teardown_entered.set()
                await release_teardown.wait()
                await destroy_world(fork_world_id)

            monkeypatch.setattr(catalog, "set_world_status", observe_set_world_status)
            monkeypatch.setattr(simulation, "run_episode", fail_episode)
            rollout = asyncio.create_task(
                _run_rollout(
                    harness,
                    world.world_id,
                    RolloutConfig(
                        num_episodes=1,
                        parallel=True,
                        episode_config=EpisodeConfig(max_steps=0),
                        destroy_forks_on_complete=True,
                    ),
                    destroy_world=block_teardown,
                )
            )

            await asyncio.wait_for(teardown_entered.wait(), timeout=2)
            rollout.cancel("original rollout cancellation")
            await asyncio.sleep(0)
            assert not rollout.done()
            stop = asyncio.create_task(_stop_admission(harness))
            await asyncio.sleep(0)
            assert not stop.done()
            rollout.cancel("repeated rollout cancellation")
            await asyncio.sleep(0)
            assert not rollout.done()

            release_teardown.set()
            with pytest.raises(asyncio.CancelledError) as raised:
                await rollout
            await asyncio.wait_for(stop, timeout=2)

            assert raised.value.args == ("original rollout cancellation",)
            assert isinstance(raised.value.__cause__, BaseExceptionGroup)
            assert raised.value.__cause__.exceptions == (episode_failure,)
            (fork_id,) = commands
            assert observed_before_destroy == [(fork_id, "REJECTED", "world_destroyed")]
            (record,) = await harness.scheduler.records(fork_id)
            assert record.status == "REJECTED"
            assert record.last_error_code == "world_destroyed"
            assert not await harness.registry.contains(fork_id)
            assert len(episode_tasks) == 1
            assert all(task.done() for task in episode_tasks)
            assert len(teardown_tasks) == 1
            assert all(task.done() for task in teardown_tasks)
        finally:
            release_teardown.set()
            await harness.close()

    @pytest.mark.asyncio
    async def test_cancellation_driven_teardown_failure_becomes_primary(
        self,
        tmp_path,
        monkeypatch,
    ):
        """Cleanup failure outranks the cancellation that initiated cleanup."""
        harness = _build_harness(tmp_path)
        StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        episode_entered = asyncio.Event()
        teardown_entered = asyncio.Event()
        teardown_failure = RuntimeError("cancellation-driven teardown failed")
        teardown_failure.add_note("pre-created teardown sentinel")
        episode_tasks: set[asyncio.Task[object]] = set()
        teardown_tasks: set[asyncio.Task[object]] = set()
        try:
            world = await _make_world(harness, tmp_path, "cancel-driven-teardown-failure")
            commands = _queue_future_command_on_each_fork(harness, monkeypatch)

            async def block_episode(*_args, **_kwargs):
                task = asyncio.current_task()
                assert task is not None
                episode_tasks.add(task)
                episode_entered.set()
                await asyncio.Event().wait()

            async def fail_teardown(_fork_world_id):
                task = asyncio.current_task()
                assert task is not None
                teardown_tasks.add(task)
                teardown_entered.set()
                raise teardown_failure

            monkeypatch.setattr(simulation, "run_episode", block_episode)
            rollout = asyncio.create_task(
                _run_rollout(
                    harness,
                    world.world_id,
                    RolloutConfig(
                        num_episodes=1,
                        parallel=True,
                        episode_config=EpisodeConfig(max_steps=0),
                        destroy_forks_on_complete=True,
                    ),
                    destroy_world=fail_teardown,
                )
            )

            await asyncio.wait_for(episode_entered.wait(), timeout=2)
            rollout.cancel("cancellation that initiated teardown")
            await asyncio.wait_for(teardown_entered.wait(), timeout=2)
            with pytest.raises(RuntimeError) as raised:
                await rollout

            assert raised.value is teardown_failure
            assert isinstance(raised.value.__cause__, asyncio.CancelledError)
            assert raised.value.__cause__.args == ()
            (fork_id,) = commands
            notes = getattr(raised.value, "__notes__", ())
            assert "pre-created teardown sentinel" in notes
            assert any(f"world_id={fork_id}" in note for note in notes)
            assert any(
                "rollout caller cancellation preceded teardown failure" in note
                and "cancellation that initiated teardown" in note
                for note in notes
            )
            (record,) = await harness.scheduler.records(fork_id)
            assert record.status == "PENDING"
            assert await harness.registry.contains(fork_id)
            assert len(episode_tasks) == 1
            assert all(task.done() for task in episode_tasks)
            assert len(teardown_tasks) == 1
            assert all(task.done() for task in teardown_tasks)

            await harness.dispatcher.apply(DestroyWorld(world_id=fork_id))
            (record,) = await harness.scheduler.records(fork_id)
            assert record.status == "REJECTED"
            assert not await harness.registry.contains(fork_id)
        finally:
            await harness.close()

    @pytest.mark.asyncio
    async def test_later_cancellation_keeps_prior_teardown_failure_beneath(
        self,
        tmp_path,
        monkeypatch,
    ):
        """Caller cancellation outranks teardown failure already observed."""
        harness = _build_harness(tmp_path)
        StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        sibling_entered = asyncio.Event()
        teardown_observed = asyncio.Event()

        class ObservedTeardownFailure(RuntimeError):
            def add_note(self, note):
                super().add_note(note)
                if note.startswith("rollout fork teardown failed"):
                    teardown_observed.set()

        teardown_failure = ObservedTeardownFailure("teardown failed before cancellation")
        fork_names: dict[str, str] = {}
        try:
            world = await _make_world(harness, tmp_path, "teardown-before-cancel")
            commands = _queue_future_command_on_each_fork(harness, monkeypatch)
            fork_world = harness.lifecycle.fork_world
            destroy_world = harness.destroy_world
            run_episode = simulation.run_episode

            async def record_fork(*args, **kwargs):
                fork = await fork_world(*args, **kwargs)
                assert fork.name is not None
                fork_names[str(fork.world_id)] = fork.name
                return fork

            async def run_or_block(
                registry,
                storage_service,
                fork_world_id,
                config,
                **input_kwargs,
            ):
                if fork_names[str(fork_world_id)].endswith(":1"):
                    sibling_entered.set()
                    await asyncio.Event().wait()
                return await run_episode(
                    registry,
                    storage_service,
                    fork_world_id,
                    config,
                    **input_kwargs,
                )

            async def fail_first_teardown(fork_world_id):
                if fork_names[str(fork_world_id)].endswith(":0"):
                    raise teardown_failure
                await destroy_world(fork_world_id)

            monkeypatch.setattr(harness.lifecycle, "fork_world", record_fork)
            monkeypatch.setattr(simulation, "run_episode", run_or_block)
            rollout = asyncio.create_task(
                _run_rollout(
                    harness,
                    world.world_id,
                    RolloutConfig(
                        num_episodes=2,
                        parallel=True,
                        episode_config=EpisodeConfig(max_steps=0),
                        destroy_forks_on_complete=True,
                    ),
                    destroy_world=fail_first_teardown,
                )
            )

            await asyncio.wait_for(sibling_entered.wait(), timeout=2)
            await asyncio.wait_for(teardown_observed.wait(), timeout=2)
            rollout.cancel("cancellation after teardown failure")
            with pytest.raises(asyncio.CancelledError) as raised:
                await rollout

            assert raised.value.args == ("cancellation after teardown failure",)
            assert isinstance(raised.value.__cause__, BaseExceptionGroup)
            assert raised.value.__cause__.exceptions == (teardown_failure,)
            failed_fork_id = next(
                fork_id for fork_id, name in fork_names.items() if name.endswith(":0")
            )
            assert any(
                f"world_id={failed_fork_id}" in note
                for note in getattr(teardown_failure, "__notes__", ())
            )
            assert await harness.registry.contains(failed_fork_id)

            await harness.dispatcher.apply(DestroyWorld(world_id=failed_fork_id))
            assert not await harness.registry.contains(failed_fork_id)
            assert len(commands) == 2
            for fork_id in commands:
                assert not await harness.registry.contains(fork_id)
        finally:
            await harness.close()

    @pytest.mark.asyncio
    async def test_later_cancellation_keeps_prior_episode_cancellation_beneath(
        self,
        tmp_path,
        monkeypatch,
    ):
        """An independently cancelled child is material before parent cancel."""
        harness = _build_harness(tmp_path)
        StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        sibling_entered = asyncio.Event()
        child_cancellation = asyncio.CancelledError("independent episode cancellation")
        fork_names: dict[str, str] = {}
        child_tasks: dict[int, asyncio.Task[object]] = {}
        create_task = asyncio.create_task
        try:
            world = await _make_world(harness, tmp_path, "episode-cancel-before-parent")
            commands = _queue_future_command_on_each_fork(harness, monkeypatch)
            fork_world = harness.lifecycle.fork_world

            def track_rollout_children(coro, *, name=None, context=None):
                task = create_task(coro, name=name, context=context)
                if isinstance(name, str) and name.startswith("archetype-rollout:"):
                    child_tasks[int(name.rsplit(":", 1)[1])] = task
                return task

            async def record_fork(*args, **kwargs):
                fork = await fork_world(*args, **kwargs)
                assert fork.name is not None
                fork_names[str(fork.world_id)] = fork.name
                return fork

            async def cancel_or_block(
                _registry,
                _storage_service,
                fork_world_id,
                _config,
                **_input_kwargs,
            ):
                if fork_names[str(fork_world_id)].endswith(":0"):
                    raise child_cancellation
                sibling_entered.set()
                await asyncio.Event().wait()

            monkeypatch.setattr(asyncio, "create_task", track_rollout_children)
            monkeypatch.setattr(harness.lifecycle, "fork_world", record_fork)
            monkeypatch.setattr(simulation, "run_episode", cancel_or_block)
            rollout = create_task(
                _run_rollout(
                    harness,
                    world.world_id,
                    RolloutConfig(
                        num_episodes=2,
                        parallel=True,
                        episode_config=EpisodeConfig(max_steps=0),
                        destroy_forks_on_complete=True,
                    ),
                )
            )

            await asyncio.wait_for(sibling_entered.wait(), timeout=2)
            while 0 not in child_tasks or not child_tasks[0].done():
                await asyncio.sleep(0)
            rollout.cancel("later parent cancellation")
            with pytest.raises(asyncio.CancelledError) as raised:
                await rollout

            assert raised.value.args == ("later parent cancellation",)
            assert isinstance(raised.value.__cause__, BaseExceptionGroup)
            assert raised.value.__cause__.exceptions == (child_cancellation,)
            assert len(commands) == 2
            for fork_id in commands:
                assert not await harness.registry.contains(fork_id)
            assert all(task.done() for task in child_tasks.values())
        finally:
            await harness.close()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("caller_cancelled", [False, True])
    async def test_owned_teardown_self_cancel_preserves_exact_exception(
        self,
        tmp_path,
        monkeypatch,
        caller_cancelled,
    ):
        """Owned result retrieval preserves self-cancellation identity."""
        harness = _build_harness(tmp_path)
        StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        episode_entered = asyncio.Event()
        teardown_entered = asyncio.Event()
        owned_cancellation = asyncio.CancelledError("owned teardown cancellation")
        owned_cancellation.add_note("pre-created owned cancellation note")
        teardown_tasks: set[asyncio.Task[object]] = set()
        try:
            world = await _make_world(
                harness, tmp_path, f"owned-teardown-cancel-{caller_cancelled}"
            )
            commands = _queue_future_command_on_each_fork(harness, monkeypatch)
            run_episode = simulation.run_episode

            async def run_or_block(*args, **kwargs):
                if caller_cancelled:
                    episode_entered.set()
                    await asyncio.Event().wait()
                return await run_episode(*args, **kwargs)

            async def self_cancel_teardown(_fork_world_id):
                task = asyncio.current_task()
                assert task is not None
                teardown_tasks.add(task)
                teardown_entered.set()
                raise owned_cancellation

            monkeypatch.setattr(simulation, "run_episode", run_or_block)
            rollout = asyncio.create_task(
                _run_rollout(
                    harness,
                    world.world_id,
                    RolloutConfig(
                        num_episodes=1,
                        parallel=True,
                        episode_config=EpisodeConfig(max_steps=0),
                        destroy_forks_on_complete=True,
                    ),
                    destroy_world=self_cancel_teardown,
                )
            )

            if caller_cancelled:
                await asyncio.wait_for(episode_entered.wait(), timeout=2)
                rollout.cancel("parent cancellation before owned teardown")
            await asyncio.wait_for(teardown_entered.wait(), timeout=2)
            with pytest.raises(asyncio.CancelledError) as raised:
                await rollout

            assert raised.value is owned_cancellation
            assert raised.value.args == ("owned teardown cancellation",)
            (fork_id,) = commands
            notes = getattr(raised.value, "__notes__", ())
            assert "pre-created owned cancellation note" in notes
            assert any(f"world_id={fork_id}" in note for note in notes)
            if caller_cancelled:
                assert any(
                    "rollout caller cancellation preceded teardown failure" in note
                    and "parent cancellation before owned teardown" in note
                    for note in notes
                )
            assert await harness.registry.contains(fork_id)
            assert len(teardown_tasks) == 1
            assert all(task.done() for task in teardown_tasks)

            await harness.dispatcher.apply(DestroyWorld(world_id=fork_id))
            assert not await harness.registry.contains(fork_id)
        finally:
            await harness.close()

    @pytest.mark.asyncio
    async def test_multiple_failures_preserve_primary_episode_teardown_chain(
        self,
        tmp_path,
        monkeypatch,
    ):
        """Secondary notes never replace the primary teardown's episode cause."""
        harness = _build_harness(tmp_path)
        StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        release_secondaries = asyncio.Event()
        primary_observed = asyncio.Event()
        episode_failure = RuntimeError("primary episode failure")
        secondary_one = KeyError("secondary child one")
        secondary_two = ValueError("secondary child two")

        class ObservedPrimaryTeardown(RuntimeError):
            def add_note(self, note):
                super().add_note(note)
                if note.startswith("rollout fork teardown failed"):
                    primary_observed.set()

        teardown_failure = ObservedPrimaryTeardown("primary teardown failure")
        fork_names: dict[str, str] = {}
        try:
            world = await _make_world(harness, tmp_path, "multiple-causal-failures")
            commands = _queue_future_command_on_each_fork(harness, monkeypatch)
            fork_world = harness.lifecycle.fork_world
            destroy_world = harness.destroy_world

            async def record_fork(*args, **kwargs):
                fork = await fork_world(*args, **kwargs)
                assert fork.name is not None
                fork_names[str(fork.world_id)] = fork.name
                return fork

            async def fail_episode_by_index(
                _registry,
                _storage_service,
                fork_world_id,
                _config,
                **_input_kwargs,
            ):
                name = fork_names[str(fork_world_id)]
                if name.endswith(":0"):
                    raise episode_failure
                await release_secondaries.wait()
                if name.endswith(":1"):
                    raise secondary_one
                raise secondary_two

            async def fail_primary_teardown(fork_world_id):
                if fork_names[str(fork_world_id)].endswith(":0"):
                    raise teardown_failure
                await destroy_world(fork_world_id)

            monkeypatch.setattr(harness.lifecycle, "fork_world", record_fork)
            monkeypatch.setattr(simulation, "run_episode", fail_episode_by_index)
            rollout = asyncio.create_task(
                _run_rollout(
                    harness,
                    world.world_id,
                    RolloutConfig(
                        num_episodes=3,
                        parallel=True,
                        episode_config=EpisodeConfig(max_steps=0),
                        destroy_forks_on_complete=True,
                    ),
                    destroy_world=fail_primary_teardown,
                )
            )

            await asyncio.wait_for(primary_observed.wait(), timeout=2)
            assert not rollout.done()
            release_secondaries.set()
            with pytest.raises(ObservedPrimaryTeardown) as raised:
                await rollout

            assert raised.value is teardown_failure
            assert raised.value.__cause__ is episode_failure
            fork_ids = {
                int(name.rsplit(":", 1)[1]): fork_id for fork_id, name in fork_names.items()
            }
            additional_notes = [
                note
                for note in getattr(raised.value, "__notes__", ())
                if note.startswith("additional rollout failure:")
            ]
            assert additional_notes == [
                "additional rollout failure: "
                "child_index=1 phase=episode type=builtins.KeyError "
                f"fork_world_id={fork_ids[1]}",
                "additional rollout failure: "
                "child_index=2 phase=episode type=builtins.ValueError "
                f"fork_world_id={fork_ids[2]}",
            ]
            assert len(commands) == 3
            assert await harness.registry.contains(fork_ids[0])
            assert not await harness.registry.contains(fork_ids[1])
            assert not await harness.registry.contains(fork_ids[2])

            await harness.dispatcher.apply(DestroyWorld(world_id=fork_ids[0]))
            assert not await harness.registry.contains(fork_ids[0])
        finally:
            release_secondaries.set()
            await harness.close()

    @pytest.mark.asyncio
    async def test_earlier_failure_stays_primary_while_cleanup_delays_capture(
        self,
        tmp_path,
        monkeypatch,
    ):
        """Observation order, not child completion order, selects primary."""
        harness = _build_harness(tmp_path)
        StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        cleanup_entered = asyncio.Event()
        release_cleanup = asyncio.Event()
        first_failure = RuntimeError("first observed failure")
        second_failure = ValueError("first completed failure")
        fork_names: dict[str, str] = {}
        child_tasks: dict[int, asyncio.Task[object]] = {}
        create_task = asyncio.create_task
        try:
            world = await _make_world(harness, tmp_path, "failure-observation-order")
            commands = _queue_future_command_on_each_fork(harness, monkeypatch)
            fork_world = harness.lifecycle.fork_world
            destroy_world = harness.destroy_world

            def track_rollout_children(coro, *, name=None, context=None):
                task = create_task(coro, name=name, context=context)
                if isinstance(name, str) and name.startswith("archetype-rollout:"):
                    child_tasks[int(name.rsplit(":", 1)[1])] = task
                return task

            async def record_fork(*args, **kwargs):
                fork = await fork_world(*args, **kwargs)
                assert fork.name is not None
                fork_names[str(fork.world_id)] = fork.name
                return fork

            async def fail_episode_by_index(
                _registry,
                _storage_service,
                fork_world_id,
                _config,
                **_input_kwargs,
            ):
                if fork_names[str(fork_world_id)].endswith(":0"):
                    raise first_failure
                await cleanup_entered.wait()
                raise second_failure

            async def delay_first_cleanup(fork_world_id):
                if fork_names[str(fork_world_id)].endswith(":0"):
                    cleanup_entered.set()
                    await release_cleanup.wait()
                await destroy_world(fork_world_id)

            monkeypatch.setattr(asyncio, "create_task", track_rollout_children)
            monkeypatch.setattr(harness.lifecycle, "fork_world", record_fork)
            monkeypatch.setattr(simulation, "run_episode", fail_episode_by_index)
            rollout = create_task(
                _run_rollout(
                    harness,
                    world.world_id,
                    RolloutConfig(
                        num_episodes=2,
                        parallel=True,
                        episode_config=EpisodeConfig(max_steps=0),
                        destroy_forks_on_complete=True,
                    ),
                    destroy_world=delay_first_cleanup,
                )
            )

            await asyncio.wait_for(cleanup_entered.wait(), timeout=2)
            while 1 not in child_tasks or not child_tasks[1].done():
                await asyncio.sleep(0)
            assert not child_tasks[0].done()
            assert not rollout.done()

            release_cleanup.set()
            with pytest.raises(RuntimeError) as raised:
                await rollout

            assert raised.value is first_failure
            assert raised.value.__cause__ is None
            fork_ids = {
                int(name.rsplit(":", 1)[1]): fork_id for fork_id, name in fork_names.items()
            }
            assert any(
                "additional rollout failure: "
                "child_index=1 phase=episode type=builtins.ValueError "
                f"fork_world_id={fork_ids[1]}" == note
                for note in getattr(raised.value, "__notes__", ())
            )
            assert len(commands) == 2
            for fork_id in commands:
                assert not await harness.registry.contains(fork_id)
            assert all(task.done() for task in child_tasks.values())
        finally:
            release_cleanup.set()
            await harness.close()

    @pytest.mark.asyncio
    async def test_teardown_replacement_inherits_episode_precedence(
        self,
        tmp_path,
        monkeypatch,
    ):
        """Late teardown replacement keeps its child's first-failure rank."""
        harness = _build_harness(tmp_path)
        StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        cleanup_entered = asyncio.Event()
        release_cleanup = asyncio.Event()
        episode_failure = RuntimeError("first observed episode failure")
        sibling_failure = ValueError("second observed sibling failure")
        teardown_failure = RuntimeError("third observed teardown failure")
        fork_names: dict[str, str] = {}
        child_tasks: dict[int, asyncio.Task[object]] = {}
        create_task = asyncio.create_task
        try:
            world = await _make_world(harness, tmp_path, "teardown-replacement-precedence")
            commands = _queue_future_command_on_each_fork(harness, monkeypatch)
            fork_world = harness.lifecycle.fork_world
            destroy_world = harness.destroy_world

            def track_rollout_children(coro, *, name=None, context=None):
                task = create_task(coro, name=name, context=context)
                if isinstance(name, str) and name.startswith("archetype-rollout:"):
                    child_tasks[int(name.rsplit(":", 1)[1])] = task
                return task

            async def record_fork(*args, **kwargs):
                fork = await fork_world(*args, **kwargs)
                assert fork.name is not None
                fork_names[str(fork.world_id)] = fork.name
                return fork

            async def fail_episode_by_index(
                _registry,
                _storage_service,
                fork_world_id,
                _config,
                **_input_kwargs,
            ):
                if fork_names[str(fork_world_id)].endswith(":0"):
                    raise episode_failure
                await cleanup_entered.wait()
                raise sibling_failure

            async def replace_first_failure_during_teardown(fork_world_id):
                if fork_names[str(fork_world_id)].endswith(":0"):
                    cleanup_entered.set()
                    await release_cleanup.wait()
                    raise teardown_failure
                await destroy_world(fork_world_id)

            monkeypatch.setattr(asyncio, "create_task", track_rollout_children)
            monkeypatch.setattr(harness.lifecycle, "fork_world", record_fork)
            monkeypatch.setattr(simulation, "run_episode", fail_episode_by_index)
            rollout = create_task(
                _run_rollout(
                    harness,
                    world.world_id,
                    RolloutConfig(
                        num_episodes=2,
                        parallel=True,
                        episode_config=EpisodeConfig(max_steps=0),
                        destroy_forks_on_complete=True,
                    ),
                    destroy_world=replace_first_failure_during_teardown,
                )
            )

            await asyncio.wait_for(cleanup_entered.wait(), timeout=2)
            while 1 not in child_tasks or not child_tasks[1].done():
                await asyncio.sleep(0)
            assert not child_tasks[0].done()

            release_cleanup.set()
            with pytest.raises(RuntimeError) as raised:
                await rollout

            assert raised.value is teardown_failure
            assert raised.value.__cause__ is episode_failure
            fork_ids = {
                int(name.rsplit(":", 1)[1]): fork_id for fork_id, name in fork_names.items()
            }
            assert any(
                "additional rollout failure: "
                "child_index=1 phase=episode type=builtins.ValueError "
                f"fork_world_id={fork_ids[1]}" == note
                for note in getattr(raised.value, "__notes__", ())
            )
            assert await harness.registry.contains(fork_ids[0])
            assert not await harness.registry.contains(fork_ids[1])

            await harness.dispatcher.apply(DestroyWorld(world_id=fork_ids[0]))
            assert len(commands) == 2
            assert not await harness.registry.contains(fork_ids[0])
            assert all(task.done() for task in child_tasks.values())
        finally:
            release_cleanup.set()
            await harness.close()

    @pytest.mark.asyncio
    async def test_parallel_teardown_failure_retains_fork_identity_for_retry(
        self,
        tmp_path,
        monkeypatch,
    ):
        """A failed scheduler cleanup names the still-owned retry target."""
        harness = _build_harness(tmp_path)
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        failure = RuntimeError("scheduler cancellation failed")
        try:
            world = await _make_world(harness, tmp_path, "parallel-teardown-retry")
            commands = _queue_future_command_on_each_fork(harness, monkeypatch)
            cancel_world = harness.scheduler.cancel_world
            cancel_attempts = 0

            async def fail_once_cancel_world(
                world_id,
                *,
                reason="world destroyed",
            ):
                nonlocal cancel_attempts
                cancel_attempts += 1
                if cancel_attempts == 1:
                    raise failure
                return await cancel_world(world_id, reason=reason)

            monkeypatch.setattr(
                harness.scheduler,
                "cancel_world",
                fail_once_cancel_world,
            )
            with pytest.raises(RuntimeError) as raised:
                await _run_rollout(
                    harness,
                    world.world_id,
                    RolloutConfig(
                        num_episodes=1,
                        parallel=True,
                        episode_config=EpisodeConfig(max_steps=0),
                        destroy_forks_on_complete=True,
                    ),
                )
            assert raised.value is failure

            (fork_id,) = commands
            assert any(
                f"world_id={fork_id}" in note for note in getattr(raised.value, "__notes__", ())
            )
            (record,) = await harness.scheduler.records(fork_id)
            assert record.status == "PENDING"
            assert await harness.registry.contains(fork_id)
            assert cancel_attempts == 1

            await harness.dispatcher.apply(DestroyWorld(world_id=fork_id))
            assert cancel_attempts == 2
            (record,) = await harness.scheduler.records(fork_id)
            assert record.status == "REJECTED"
            assert record.last_error_code == "world_destroyed"
            durable_world = await harness.storage.get_control_catalog(storage).get_world(fork_id)
            assert durable_world is not None and durable_world.status == "destroyed"
            assert not await harness.registry.contains(fork_id)
        finally:
            await harness.close()

    @pytest.mark.asyncio
    async def test_total_duration_steps_populated(self, tmp_path):
        """RolloutResult.total_duration_steps equals sum of episode durations."""
        harness = _build_harness(tmp_path)
        try:
            world = await _make_world(harness, tmp_path)

            config = RolloutConfig(
                num_episodes=3,
                episode_config=EpisodeConfig(max_steps=4),
            )
            result = await _run_rollout(harness, world.world_id, config)

            expected = sum(ep.duration_steps for ep in result.episodes)
            assert result.total_duration_steps == expected
            assert result.total_duration_steps > 0
        finally:
            await harness.close()

    @pytest.mark.asyncio
    async def test_parallel_rollout(self, tmp_path):
        """parallel=True episodes all run and produce results."""
        harness = _build_harness(tmp_path)
        try:
            world = await _make_world(harness, tmp_path)

            config = RolloutConfig(
                num_episodes=3,
                episode_config=EpisodeConfig(max_steps=3),
                parallel=True,
            )
            result = await _run_rollout(harness, world.world_id, config)

            assert len(result.episodes) == 3
            for ep in result.episodes:
                assert ep.duration_steps == 3
                assert ep.final_tick == 3
        finally:
            await harness.close()
