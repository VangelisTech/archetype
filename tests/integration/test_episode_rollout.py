# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Contract tests for the execution hierarchy: episode and rollout semantics.

Tests RuntimeApplication.run_episode and run_rollout through the ServiceContainer.
Verifies termination predicates, fork isolation, registry cleanup, and result shapes.
"""

import asyncio

import pytest
from daft import DataFrame, col

from archetype.app.container import ServiceContainer
from archetype.app.models import Command, CommandType
from archetype.core.aio.async_processor import AsyncProcessor
from archetype.core.component import Component
from archetype.core.config import StorageConfig, WorldConfig
from archetype.world import simulation
from archetype.world.models import EpisodeConfig, RolloutConfig

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


async def _make_world(container: ServiceContainer, tmp_path, name: str = "test"):
    storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
    world = await container.world_lifecycle.create_world(WorldConfig(name=name), storage)
    return world


def _queue_future_command_on_each_fork(
    container: ServiceContainer,
    monkeypatch: pytest.MonkeyPatch,
) -> dict[str, Command]:
    """Admit one command after fork creation and before its episode starts."""

    original_fork = container.world_lifecycle.fork_world
    commands: dict[str, Command] = {}

    async def fork_and_queue(*args, **kwargs):
        fork = await original_fork(*args, **kwargs)
        command = Command(
            type=CommandType.CUSTOM,
            tick=10_000,
            payload={"source": "rollout-teardown-contract"},
        )
        await container.application.submit(fork.world_id, command)
        commands[str(fork.world_id)] = command
        return fork

    monkeypatch.setattr(container.world_lifecycle, "fork_world", fork_and_queue)
    return commands


# ---------------------------------------------------------------------------
# Episode tests
# ---------------------------------------------------------------------------


class TestEpisode:
    @pytest.mark.asyncio
    async def test_episode_terminates_on_terminal_component(self, tmp_path):
        """Episode stops when an entity with the terminal_component is detected."""
        container = ServiceContainer()
        try:
            world = await _make_world(container, tmp_path)

            # Spawn an entity with Terminal before the episode starts.
            # terminal_component check happens before each step, so
            # the episode should terminate immediately with 0 steps.
            await world.create_entity([Pos(x=1.0), Terminal(done=True)])

            config = EpisodeConfig(
                max_steps=100,
                terminal_component=Terminal,
            )
            result = await container.application.run_episode(world.world_id, config)

            assert result.terminated is True
            assert result.duration_steps == 0
        finally:
            await container.shutdown()

    @pytest.mark.asyncio
    async def test_episode_terminates_on_callable(self, tmp_path):
        """Episode stops when the termination callable returns True."""
        container = ServiceContainer()
        try:
            world = await _make_world(container, tmp_path)

            config = EpisodeConfig(
                max_steps=100,
                termination=lambda w: w.tick >= 5,
            )
            result = await container.application.run_episode(world.world_id, config)

            assert result.terminated is True
            assert result.final_tick == 5
            assert result.duration_steps == 5
        finally:
            await container.shutdown()

    @pytest.mark.asyncio
    async def test_episode_caps_at_max_steps(self, tmp_path):
        """No termination predicate: episode runs exactly max_steps ticks."""
        container = ServiceContainer()
        try:
            world = await _make_world(container, tmp_path)

            config = EpisodeConfig(max_steps=10)
            result = await container.application.run_episode(world.world_id, config)

            assert result.duration_steps == 10
            assert result.run_id == world.run_id
            assert result.start_tick == 0
            assert result.final_tick == 10
        finally:
            await container.shutdown()

    @pytest.mark.asyncio
    async def test_episode_does_not_fork(self, tmp_path):
        """world_id before and after episode is the same."""
        container = ServiceContainer()
        try:
            world = await _make_world(container, tmp_path)
            wid_before = world.world_id

            config = EpisodeConfig(max_steps=3)
            result = await container.application.run_episode(world.world_id, config)

            assert str(result.world_id) == str(wid_before)
            assert result.run_id is not None
        finally:
            await container.shutdown()

    @pytest.mark.asyncio
    async def test_terminated_false_when_max_steps_cap(self, tmp_path):
        """terminated is False when the episode stopped due to max_steps, not a predicate."""
        container = ServiceContainer()
        try:
            world = await _make_world(container, tmp_path)

            config = EpisodeConfig(max_steps=5)
            result = await container.application.run_episode(world.world_id, config)

            assert result.terminated is False
            assert result.duration_steps == 5
        finally:
            await container.shutdown()


# ---------------------------------------------------------------------------
# Value-based "all done" termination (bug B2)
# ---------------------------------------------------------------------------


async def _make_countdown_world(container, tmp_path, name="b2"):
    storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
    world = await container.world_lifecycle.create_world(WorldConfig(name=name), storage)
    await world.add_processor(CountToGoal())
    return world


async def _done_by_entity(world) -> dict[int, bool]:
    """``done`` at the latest committed frame, keyed by entity id."""
    rows = (await world.get_components([Countdown])).to_pylist()
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
        container = ServiceContainer()
        try:
            world = await _make_countdown_world(container, tmp_path)
            await world.create_entity([Countdown(goal=3)])

            config = EpisodeConfig(
                max_steps=50,
                terminal_component=Countdown,
                terminal_field="done",
                terminal_all=True,
            )
            result = await container.application.run_episode(world.world_id, config)

            assert result.terminated is True
            assert result.duration_steps < 50  # stopped on the data, not the cap
            # The reason it stopped: the entity is genuinely done.
            assert all((await _done_by_entity(world)).values())
        finally:
            await container.shutdown()

    @pytest.mark.asyncio
    async def test_terminal_field_suppresses_structural_check(self, tmp_path):
        """With terminal_field set, the entity merely *carrying* Countdown must
        NOT end the episode at tick 0 (that is the structural path)."""
        container = ServiceContainer()
        try:
            world = await _make_countdown_world(container, tmp_path)
            await world.create_entity([Countdown(goal=4)])

            config = EpisodeConfig(
                max_steps=50,
                terminal_component=Countdown,
                terminal_field="done",
            )
            result = await container.application.run_episode(world.world_id, config)

            assert result.terminated is True
            assert result.duration_steps > 1  # ran, did not fire structurally at tick 0
        finally:
            await container.shutdown()

    @pytest.mark.asyncio
    async def test_all_mode_waits_for_the_slowest_entity(self, tmp_path):
        container = ServiceContainer()
        try:
            world = await _make_countdown_world(container, tmp_path)
            fast = await world.create_entity([Countdown(goal=2)])
            slow = await world.create_entity([Countdown(goal=6)])

            config = EpisodeConfig(
                max_steps=50,
                terminal_component=Countdown,
                terminal_field="done",
                terminal_all=True,
            )
            result = await container.application.run_episode(world.world_id, config)

            assert result.terminated is True
            assert result.duration_steps < 50
            done = await _done_by_entity(world)
            # Waited for *both* — including the slow one.
            assert done[fast] is True
            assert done[slow] is True
        finally:
            await container.shutdown()

    @pytest.mark.asyncio
    async def test_any_mode_stops_at_the_first_entity(self, tmp_path):
        container = ServiceContainer()
        try:
            world = await _make_countdown_world(container, tmp_path)
            fast = await world.create_entity([Countdown(goal=2)])
            slow = await world.create_entity([Countdown(goal=6)])

            config = EpisodeConfig(
                max_steps=50,
                terminal_component=Countdown,
                terminal_field="done",
                terminal_all=False,
            )
            result = await container.application.run_episode(world.world_id, config)

            assert result.terminated is True
            done = await _done_by_entity(world)
            # Stopped at the first finisher; the slow one is still running.
            assert done[fast] is True
            assert done[slow] is False
        finally:
            await container.shutdown()

    @pytest.mark.asyncio
    async def test_caps_at_max_steps_when_never_done(self, tmp_path):
        container = ServiceContainer()
        try:
            world = await _make_countdown_world(container, tmp_path)
            await world.create_entity([Countdown(goal=999)])

            config = EpisodeConfig(
                max_steps=4,
                terminal_component=Countdown,
                terminal_field="done",
                terminal_all=True,
            )
            result = await container.application.run_episode(world.world_id, config)

            assert result.terminated is False
            assert result.duration_steps == 4
        finally:
            await container.shutdown()


# ---------------------------------------------------------------------------
# Rollout tests
# ---------------------------------------------------------------------------


class TestRollout:
    @pytest.mark.asyncio
    async def test_rollout_creates_n_forks(self, tmp_path):
        """num_episodes=3 produces 3 EpisodeResult entries."""
        container = ServiceContainer()
        try:
            world = await _make_world(container, tmp_path)

            config = RolloutConfig(
                num_episodes=3,
                episode_config=EpisodeConfig(max_steps=2),
            )
            result = await container.application.run_rollout(world.world_id, config)

            assert result.num_episodes == 3
            assert len(result.episodes) == 3
        finally:
            await container.shutdown()

    @pytest.mark.asyncio
    async def test_base_world_tick_unchanged_by_rollout(self, tmp_path):
        """Forks do the work, not the base world."""
        container = ServiceContainer()
        try:
            world = await _make_world(container, tmp_path)
            tick_before = world.tick

            config = RolloutConfig(
                num_episodes=2,
                episode_config=EpisodeConfig(max_steps=5),
            )
            await container.application.run_rollout(world.world_id, config)

            assert world.tick == tick_before
        finally:
            await container.shutdown()

    @pytest.mark.asyncio
    async def test_destroy_forks_on_complete(self, tmp_path):
        """Forks are removed from the registry after rollout when flag is set."""
        container = ServiceContainer()
        try:
            world = await _make_world(container, tmp_path)

            config = RolloutConfig(
                num_episodes=2,
                episode_config=EpisodeConfig(max_steps=2),
                destroy_forks_on_complete=True,
            )
            result = await container.application.run_rollout(world.world_id, config)

            # Fork worlds should no longer be in the registry
            for ep in result.episodes:
                assert not await container.world_registry.contains(str(ep.world_id))

            # Base world should still be in the registry
            assert await container.world_registry.contains(str(world.world_id))
        finally:
            await container.shutdown()

    @pytest.mark.asyncio
    async def test_auto_destroy_rejects_queued_command_before_durable_destroy(
        self,
        tmp_path,
        monkeypatch,
    ):
        """Rollout cleanup follows reconcile -> cancel -> lifecycle destroy."""
        container = ServiceContainer()
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        try:
            world = await container.world_lifecycle.create_world(
                WorldConfig(name="command-cleanup"),
                storage,
            )
            commands = _queue_future_command_on_each_fork(container, monkeypatch)
            catalog = container.storage_service.get_control_catalog(storage)
            set_world_status = catalog.set_world_status
            observed_before_destroy: list[tuple[str, str, str | None]] = []

            async def observe_set_world_status(world_id: str, status: str) -> None:
                if status == "destroyed" and world_id in commands:
                    (record,) = await container.command_scheduler.records(world_id)
                    observed_before_destroy.append(
                        (world_id, record.status, record.last_error_code)
                    )
                await set_world_status(world_id, status)

            monkeypatch.setattr(catalog, "set_world_status", observe_set_world_status)

            result = await container.application.run_rollout(
                world.world_id,
                RolloutConfig(
                    num_episodes=1,
                    episode_config=EpisodeConfig(max_steps=0),
                    destroy_forks_on_complete=True,
                ),
            )

            fork_id = str(result.episodes[0].world_id)
            assert observed_before_destroy == [(fork_id, "REJECTED", "world_destroyed")]
            (record,) = await container.command_scheduler.records(fork_id)
            assert record.command_id == str(commands[fork_id].id)
            assert record.status == "REJECTED"
            assert record.last_error_code == "world_destroyed"
            durable_world = await catalog.get_world(fork_id)
            assert durable_world is not None and durable_world.status == "destroyed"
            assert not await container.world_registry.contains(fork_id)
        finally:
            await container.shutdown()

    @pytest.mark.asyncio
    async def test_auto_destroy_runs_after_episode_failure(
        self,
        tmp_path,
        monkeypatch,
    ):
        """An episode exception does not strand its fork or queued command."""
        container = ServiceContainer()
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        try:
            world = await container.world_lifecycle.create_world(
                WorldConfig(name="failed-episode-cleanup"),
                storage,
            )
            commands = _queue_future_command_on_each_fork(container, monkeypatch)
            catalog = container.storage_service.get_control_catalog(storage)
            set_world_status = catalog.set_world_status
            observed_before_destroy: list[tuple[str, str, str | None]] = []

            async def observe_set_world_status(world_id: str, status: str) -> None:
                if status == "destroyed" and world_id in commands:
                    (record,) = await container.command_scheduler.records(world_id)
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
                await container.application.run_rollout(
                    world.world_id,
                    RolloutConfig(
                        num_episodes=1,
                        episode_config=EpisodeConfig(max_steps=1),
                        destroy_forks_on_complete=True,
                    ),
                )

            (fork_id,) = commands
            assert observed_before_destroy == [(fork_id, "REJECTED", "world_destroyed")]
            (record,) = await container.command_scheduler.records(fork_id)
            assert record.status == "REJECTED"
            durable_world = await catalog.get_world(fork_id)
            assert durable_world is not None and durable_world.status == "destroyed"
            assert not await container.world_registry.contains(fork_id)
        finally:
            await container.shutdown()

    @pytest.mark.asyncio
    async def test_auto_destroy_runs_after_rollout_cancellation(
        self,
        tmp_path,
        monkeypatch,
    ):
        """Caller cancellation still executes the fork's teardown finally."""
        container = ServiceContainer()
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        episode_entered = asyncio.Event()
        try:
            world = await container.world_lifecycle.create_world(
                WorldConfig(name="cancelled-episode-cleanup"),
                storage,
            )
            commands = _queue_future_command_on_each_fork(container, monkeypatch)
            catalog = container.storage_service.get_control_catalog(storage)
            set_world_status = catalog.set_world_status
            observed_before_destroy: list[tuple[str, str, str | None]] = []

            async def observe_set_world_status(world_id: str, status: str) -> None:
                if status == "destroyed" and world_id in commands:
                    (record,) = await container.command_scheduler.records(world_id)
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
                container.application.run_rollout(
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
            (record,) = await container.command_scheduler.records(fork_id)
            assert record.status == "REJECTED"
            durable_world = await catalog.get_world(fork_id)
            assert durable_world is not None and durable_world.status == "destroyed"
            assert not await container.world_registry.contains(fork_id)
        finally:
            await container.shutdown()

    @pytest.mark.asyncio
    async def test_sequential_cancellation_drains_owned_fork_before_stopping(
        self,
        tmp_path,
        monkeypatch,
    ):
        """Cancellation during fork handoff closes it and starts no successor."""
        container = ServiceContainer()
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        fork_created = asyncio.Event()
        release_fork = asyncio.Event()
        try:
            world = await container.world_lifecycle.create_world(
                WorldConfig(name="sequential-fork-handoff"),
                storage,
            )
            commands = _queue_future_command_on_each_fork(container, monkeypatch)
            fork_and_queue = container.world_lifecycle.fork_world

            async def hold_fork_handoff(*args, **kwargs):
                fork = await fork_and_queue(*args, **kwargs)
                fork_created.set()
                await release_fork.wait()
                return fork

            async def unexpected_episode(*_args, **_kwargs):
                pytest.fail("a cancelled fork handoff must skip its episode")

            monkeypatch.setattr(
                container.world_lifecycle,
                "fork_world",
                hold_fork_handoff,
            )
            monkeypatch.setattr(simulation, "run_episode", unexpected_episode)
            rollout = asyncio.create_task(
                container.application.run_rollout(
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
            (record,) = await container.command_scheduler.records(fork_id)
            assert record.command_id == str(command.id)
            assert record.status == "REJECTED"
            assert record.last_error_code == "world_destroyed"
            assert not await container.world_registry.contains(fork_id)
        finally:
            release_fork.set()
            await container.shutdown()

    @pytest.mark.asyncio
    async def test_parallel_immediate_cancellation_drains_unstarted_children(
        self,
        tmp_path,
        monkeypatch,
    ):
        """Cancellation before child entry cannot strand the aggregate waiter."""
        container = ServiceContainer()
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        create_task = asyncio.create_task
        child_tasks: list[asyncio.Task[object]] = []
        cancellation_injected = False
        try:
            world = await container.world_lifecycle.create_world(
                WorldConfig(name="parallel-immediate-cancel"),
                storage,
            )

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
                container.application.run_rollout(
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
            assert [item.name for item in await container.application.list_worlds()] == [
                "parallel-immediate-cancel"
            ]
        finally:
            await container.shutdown()

    @pytest.mark.asyncio
    async def test_parallel_failure_drains_sibling_teardown_before_admission(
        self,
        tmp_path,
        monkeypatch,
    ):
        """A first child failure cannot outlive rollout-owned fork cleanup."""
        container = ServiceContainer()
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        sibling_entered = asyncio.Event()
        release_sibling = asyncio.Event()
        failed_fork_destroyed = asyncio.Event()
        failure = RuntimeError("first parallel episode failed")
        episode_tasks: set[asyncio.Task[object]] = set()
        failed_fork_id: str | None = None
        original_run_episode = simulation.run_episode
        try:
            world = await container.world_lifecycle.create_world(
                WorldConfig(name="parallel-failure-drain"),
                storage,
            )
            commands = _queue_future_command_on_each_fork(container, monkeypatch)
            catalog = container.storage_service.get_control_catalog(storage)
            set_world_status = catalog.set_world_status
            observed_before_destroy: list[tuple[str, str, str | None]] = []

            async def observe_set_world_status(world_id: str, status: str) -> None:
                if status == "destroyed" and world_id in commands:
                    (record,) = await container.command_scheduler.records(world_id)
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
                fork = await container.world_registry.live_world(fork_world_id)
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
                container.application.run_rollout(
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
            stop = asyncio.create_task(container.application.stop_admission())
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
                (record,) = await container.command_scheduler.records(fork_id)
                assert record.command_id == str(command.id)
                assert record.status == "REJECTED"
                assert not await container.world_registry.contains(fork_id)
            assert len(episode_tasks) == 2
            assert all(task.done() for task in episode_tasks)
        finally:
            release_sibling.set()
            await container.shutdown()

    @pytest.mark.asyncio
    async def test_parallel_multiple_failures_keep_first_exception_primary(
        self,
        tmp_path,
        monkeypatch,
    ):
        """Later failures remain available without replacing the prompt one."""
        container = ServiceContainer()
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        primary_ready = asyncio.Event()
        primary = RuntimeError("primary episode failure")
        secondary = ValueError("secondary episode failure")
        try:
            world = await container.world_lifecycle.create_world(
                WorldConfig(name="parallel-multiple-failures"),
                storage,
            )
            commands = _queue_future_command_on_each_fork(container, monkeypatch)

            async def fail_in_observed_order(
                _registry,
                _storage_service,
                fork_world_id,
                _config,
                **_input_kwargs,
            ):
                fork = await container.world_registry.live_world(fork_world_id)
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
                await container.application.run_rollout(
                    world.world_id,
                    RolloutConfig(
                        num_episodes=2,
                        parallel=True,
                        episode_config=EpisodeConfig(max_steps=0),
                        destroy_forks_on_complete=True,
                    ),
                )

            assert raised.value is primary
            assert isinstance(raised.value.__cause__, BaseExceptionGroup)
            assert raised.value.__cause__.exceptions == (secondary,)
            assert len(commands) == 2
            for fork_id in commands:
                assert not await container.world_registry.contains(fork_id)
        finally:
            await container.shutdown()

    @pytest.mark.asyncio
    async def test_parallel_cancellation_waits_for_every_fork_teardown(
        self,
        tmp_path,
        monkeypatch,
    ):
        """Repeated caller cancellation cannot interrupt child cleanup."""
        container = ServiceContainer()
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
            world = await container.world_lifecycle.create_world(
                WorldConfig(name="parallel-cancellation-drain"),
                storage,
            )
            commands = _queue_future_command_on_each_fork(container, monkeypatch)
            fork_world = container.world_lifecycle.fork_world
            catalog = container.storage_service.get_control_catalog(storage)
            set_world_status = catalog.set_world_status
            observed_before_destroy: list[tuple[str, str, str | None]] = []
            destroy_world = container.application.destroy_world

            async def track_fork(*args, **kwargs):
                task = asyncio.current_task()
                assert task is not None
                fork_tasks.add(task)
                return await fork_world(*args, **kwargs)

            async def observe_set_world_status(world_id: str, status: str) -> None:
                if status == "destroyed" and world_id in commands:
                    (record,) = await container.command_scheduler.records(world_id)
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
            monkeypatch.setattr(container.world_lifecycle, "fork_world", track_fork)
            monkeypatch.setattr(simulation, "run_episode", block_episode)
            monkeypatch.setattr(container.application, "destroy_world", block_teardown)
            rollout = asyncio.create_task(
                container.application.run_rollout(
                    world.world_id,
                    RolloutConfig(
                        num_episodes=2,
                        parallel=True,
                        episode_config=EpisodeConfig(max_steps=0),
                        destroy_forks_on_complete=True,
                    ),
                )
            )

            await asyncio.wait_for(episodes_entered.wait(), timeout=2)
            rollout.cancel()
            await asyncio.wait_for(teardown_entered.wait(), timeout=2)
            assert not rollout.done()
            stop = asyncio.create_task(container.application.stop_admission())
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
                assert not await container.world_registry.contains(fork_id)
            assert len(episode_tasks) == 2
            assert all(task.done() for task in episode_tasks)
            assert len(fork_tasks) == 2
            assert all(task.done() for task in fork_tasks)
            assert len(teardown_tasks) == 2
            assert all(task.done() for task in teardown_tasks)
        finally:
            release_teardown.set()
            await container.shutdown()

    @pytest.mark.asyncio
    async def test_parallel_teardown_failure_retains_fork_identity_for_retry(
        self,
        tmp_path,
        monkeypatch,
    ):
        """A failed scheduler cleanup names the still-owned retry target."""
        container = ServiceContainer()
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        failure = RuntimeError("scheduler cancellation failed")
        try:
            world = await container.world_lifecycle.create_world(
                WorldConfig(name="parallel-teardown-retry"),
                storage,
            )
            commands = _queue_future_command_on_each_fork(container, monkeypatch)
            cancel_world = container.command_scheduler.cancel_world

            async def fail_cancel_world(_world_id):
                raise failure

            monkeypatch.setattr(
                container.command_scheduler,
                "cancel_world",
                fail_cancel_world,
            )
            with pytest.raises(RuntimeError) as raised:
                await container.application.run_rollout(
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
            (record,) = await container.command_scheduler.records(fork_id)
            assert record.status == "PENDING"
            assert await container.world_registry.contains(fork_id)

            monkeypatch.setattr(
                container.command_scheduler,
                "cancel_world",
                cancel_world,
            )
            await container.application.destroy_world(fork_id)
            (record,) = await container.command_scheduler.records(fork_id)
            assert record.status == "REJECTED"
            assert record.last_error_code == "world_destroyed"
            durable_world = await container.storage_service.get_control_catalog(storage).get_world(
                fork_id
            )
            assert durable_world is not None and durable_world.status == "destroyed"
            assert not await container.world_registry.contains(fork_id)
        finally:
            await container.shutdown()

    @pytest.mark.asyncio
    async def test_total_duration_steps_populated(self, tmp_path):
        """RolloutResult.total_duration_steps equals sum of episode durations."""
        container = ServiceContainer()
        try:
            world = await _make_world(container, tmp_path)

            config = RolloutConfig(
                num_episodes=3,
                episode_config=EpisodeConfig(max_steps=4),
            )
            result = await container.application.run_rollout(world.world_id, config)

            expected = sum(ep.duration_steps for ep in result.episodes)
            assert result.total_duration_steps == expected
            assert result.total_duration_steps > 0
        finally:
            await container.shutdown()

    @pytest.mark.asyncio
    async def test_parallel_rollout(self, tmp_path):
        """parallel=True episodes all run and produce results."""
        container = ServiceContainer()
        try:
            world = await _make_world(container, tmp_path)

            config = RolloutConfig(
                num_episodes=3,
                episode_config=EpisodeConfig(max_steps=3),
                parallel=True,
            )
            result = await container.application.run_rollout(world.world_id, config)

            assert len(result.episodes) == 3
            for ep in result.episodes:
                assert ep.duration_steps == 3
                assert ep.final_tick == 3
        finally:
            await container.shutdown()
