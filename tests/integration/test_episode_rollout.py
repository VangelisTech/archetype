# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Contract tests for the execution hierarchy: episode and rollout semantics.

Tests SimulationService.run_episode and run_rollout through the ServiceContainer.
Verifies termination predicates, fork isolation, registry cleanup, and result shapes.
"""

import pytest
from daft import DataFrame, col

from archetype.app.container import ServiceContainer
from archetype.app.models import EpisodeConfig, RolloutConfig
from archetype.core.aio.async_processor import AsyncProcessor
from archetype.core.component import Component
from archetype.core.config import StorageConfig, WorldConfig

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
    world = await container.world_service.create_world(WorldConfig(name=name), storage)
    return world


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
            result = await container.simulation_service.run_episode(world.world_id, config)

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
            result = await container.simulation_service.run_episode(world.world_id, config)

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
            result = await container.simulation_service.run_episode(world.world_id, config)

            assert result.duration_steps == 10
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
            result = await container.simulation_service.run_episode(world.world_id, config)

            assert str(result.world_id) == str(wid_before)
        finally:
            await container.shutdown()

    @pytest.mark.asyncio
    async def test_terminated_false_when_max_steps_cap(self, tmp_path):
        """terminated is False when the episode stopped due to max_steps, not a predicate."""
        container = ServiceContainer()
        try:
            world = await _make_world(container, tmp_path)

            config = EpisodeConfig(max_steps=5)
            result = await container.simulation_service.run_episode(world.world_id, config)

            assert result.terminated is False
            assert result.duration_steps == 5
        finally:
            await container.shutdown()


# ---------------------------------------------------------------------------
# Value-based "all done" termination (bug B2)
# ---------------------------------------------------------------------------


async def _make_countdown_world(container, tmp_path, name="b2"):
    storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
    world = await container.world_service.create_world(WorldConfig(name=name), storage)
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
            result = await container.simulation_service.run_episode(world.world_id, config)

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
            result = await container.simulation_service.run_episode(world.world_id, config)

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
            result = await container.simulation_service.run_episode(world.world_id, config)

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
            result = await container.simulation_service.run_episode(world.world_id, config)

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
            result = await container.simulation_service.run_episode(world.world_id, config)

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
            result = await container.simulation_service.run_rollout(world.world_id, config)

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
            await container.simulation_service.run_rollout(world.world_id, config)

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
            result = await container.simulation_service.run_rollout(world.world_id, config)

            # Fork worlds should no longer be in the registry
            for ep in result.episodes:
                assert not container.world_service._orchestrator._registry.has(ep.world_id)

            # Base world should still be in the registry
            assert container.world_service._orchestrator._registry.has(world.world_id)
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
            result = await container.simulation_service.run_rollout(world.world_id, config)

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
            result = await container.simulation_service.run_rollout(world.world_id, config)

            assert len(result.episodes) == 3
            for ep in result.episodes:
                assert ep.duration_steps == 3
                assert ep.final_tick == 3
        finally:
            await container.shutdown()
