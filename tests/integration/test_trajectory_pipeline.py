# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Integration tests for the trajectory pipeline (archetype#76).

These tests validate the full flow through the service layer:
ingest via CommandService -> step via SimulationService -> query via
get_components -> fork via WorldService.fork_world.

No LLM API key required — tests use SamplingProcessor + ScoringProcessor
only (LabelingProcessor is excluded since it needs an LLM).
"""

from __future__ import annotations

import pytest
from uuid_utils import uuid7

from archetype.app.auth.guard import reset_daily_tokens, reset_tick_counters
from archetype.app.auth.models import ActorCtx
from archetype.app.container import ServiceContainer
from archetype.app.models import Command, CommandType
from archetype.core.config import RunConfig, StorageConfig, WorldConfig
from archetype.trajectories.components import Label, Trajectory, Turn
from archetype.trajectories.processors import (
    SamplingConfig,
    SamplingProcessor,
    ScoringProcessor,
)

# ── Fixtures ──


@pytest.fixture(autouse=True)
def _reset_quotas():
    reset_tick_counters()
    reset_daily_tokens()
    yield
    reset_tick_counters()
    reset_daily_tokens()


def _operator_ctx() -> ActorCtx:
    return ActorCtx(id=uuid7(), roles={"operator"})


def _viewer_ctx() -> ActorCtx:
    return ActorCtx(id=uuid7(), roles={"viewer"})


def _make_trajectory(
    tid: str, num_turns: int, *, outcome: str = "success", tags: list[str] | None = None
) -> Trajectory:
    """Helper to build a Trajectory with N turns."""
    turns = [
        Turn(role="user" if j % 2 == 0 else "assistant", content=f"turn {j}", tokens=10)
        for j in range(num_turns)
    ]
    return Trajectory.from_turns(tid, turns, source="test", outcome=outcome, tags=tags)


async def _setup_pipeline_world(container: ServiceContainer, tmp_path, name: str = "traj-test"):
    """Create a world with SamplingProcessor + ScoringProcessor (no LabelingProcessor)."""
    storage = StorageConfig(uri=str(tmp_path / "store"), namespace="trajectories")
    world = await container.world_service.create_world(WorldConfig(name=name), storage)
    await world.system.add_processor(SamplingProcessor())
    await world.system.add_processor(ScoringProcessor())
    return world, storage


async def _ingest_trajectories(
    container: ServiceContainer,
    world,
    trajectories: list[Trajectory],
    labels: list[tuple[str, str]],
    ctx: ActorCtx,
) -> list:
    """Submit SPAWN commands for (trajectory, technique) pairs via CommandService."""
    cmd_ids = []
    for traj in trajectories:
        for technique, description in labels:
            label = Label(technique=technique, description=description)
            cmd = Command(
                type=CommandType.SPAWN,
                payload={
                    "components": [
                        {"type": "Trajectory", **traj.model_dump()},
                        {"type": "Label", **label.model_dump()},
                    ],
                },
            )
            cmd_id = await container.command_service.submit(str(world.world_id), cmd, ctx)
            cmd_ids.append(cmd_id)
    return cmd_ids


# ── Tests ──


class TestIngestStepQuery:
    """Flow: SPAWN commands hydrate Trajectory + Label correctly,
    step runs processors, results() returns real data."""

    @pytest.mark.asyncio
    async def test_ingest_step_query_basic(self, tmp_path):
        container = ServiceContainer()
        try:
            world, storage = await _setup_pipeline_world(container, tmp_path)
            ctx = _operator_ctx()

            # Ingest 3 trajectories with 1 labeling technique
            trajs = [_make_trajectory(f"t-{i}", num_turns=i + 3) for i in range(3)]
            labels = [("efficiency", "Rate how directly the agent solved the problem")]

            cmd_ids = await _ingest_trajectories(container, world, trajs, labels, ctx)
            assert len(cmd_ids) == 3

            # Verify commands are pending
            pending = await container.broker.get_pending_count(str(world.world_id))
            assert pending == 3

            # Insert SamplingConfig resource
            world.resources.insert(SamplingConfig())

            # Step: drain commands + run processors
            rc = RunConfig(num_steps=1, prefer_live_reads=True)
            cmds_applied = await container.simulation_service.step(world.world_id, rc)
            assert cmds_applied == 3

            # Commands should be drained
            pending = await container.broker.get_pending_count(str(world.world_id))
            assert pending == 0

            # Query results via get_components
            df = await world.get_components([Trajectory, Label])
            assert df is not None
            rows = df.collect().to_pylist()
            active_rows = [r for r in rows if r.get("is_active", True)]
            assert len(active_rows) == 3

            # Verify trajectory data was hydrated correctly
            tids = sorted(r["trajectory__trajectory_id"] for r in active_rows)
            assert tids == ["t-0", "t-1", "t-2"]

            # Verify label metadata was hydrated
            for row in active_rows:
                assert row["label__technique"] == "efficiency"
                assert "Rate how directly" in row["label__description"]
                assert (
                    row["label__sampled"] is True
                )  # SamplingProcessor with no filters = all sampled

            # Verify turn counts match
            turn_counts = {
                r["trajectory__trajectory_id"]: r["trajectory__total_turns"] for r in active_rows
            }
            assert turn_counts["t-0"] == 3
            assert turn_counts["t-1"] == 4
            assert turn_counts["t-2"] == 5
        finally:
            await container.shutdown()

    @pytest.mark.asyncio
    async def test_multiple_techniques_create_multiple_entities(self, tmp_path):
        """Each (trajectory, technique) pair should produce a separate entity."""
        container = ServiceContainer()
        try:
            world, storage = await _setup_pipeline_world(container, tmp_path)
            ctx = _operator_ctx()

            trajs = [_make_trajectory("t-0", num_turns=5)]
            labels = [
                ("efficiency", "Rate efficiency"),
                ("correctness", "Rate correctness"),
            ]

            cmd_ids = await _ingest_trajectories(container, world, trajs, labels, ctx)
            assert len(cmd_ids) == 2  # 1 trajectory * 2 techniques

            world.resources.insert(SamplingConfig())
            await container.simulation_service.step(
                world.world_id, RunConfig(num_steps=1, prefer_live_reads=True)
            )

            df = await world.get_components([Trajectory, Label])
            rows = df.collect().to_pylist()
            active = [r for r in rows if r.get("is_active", True)]
            assert len(active) == 2

            techniques = sorted(r["label__technique"] for r in active)
            assert techniques == ["correctness", "efficiency"]

            # Both entities reference the same trajectory
            assert all(r["trajectory__trajectory_id"] == "t-0" for r in active)
        finally:
            await container.shutdown()


class TestSamplingFiltering:
    """Entities with insufficient turns are marked unsampled (not dropped),
    max_trajectories caps correctly."""

    @pytest.mark.asyncio
    async def test_min_turns_marks_unsampled(self, tmp_path):
        container = ServiceContainer()
        try:
            world, storage = await _setup_pipeline_world(container, tmp_path)
            ctx = _operator_ctx()

            # t-0: 2 turns, t-1: 5 turns, t-2: 8 turns
            trajs = [
                _make_trajectory("t-short", num_turns=2),
                _make_trajectory("t-mid", num_turns=5),
                _make_trajectory("t-long", num_turns=8),
            ]
            labels = [("test", "test description")]

            await _ingest_trajectories(container, world, trajs, labels, ctx)

            # Only sample trajectories with >= 4 turns
            world.resources.insert(SamplingConfig(min_turns=4))

            await container.simulation_service.step(
                world.world_id, RunConfig(num_steps=1, prefer_live_reads=True)
            )

            df = await world.get_components([Trajectory, Label])
            rows = df.collect().to_pylist()
            active = [r for r in rows if r.get("is_active", True)]

            # All 3 rows preserved (not dropped)
            assert len(active) == 3

            sampled = [r for r in active if r["label__sampled"]]
            unsampled = [r for r in active if not r["label__sampled"]]

            # t-mid (5 turns) and t-long (8 turns) should be sampled
            assert len(sampled) == 2
            sampled_ids = sorted(r["trajectory__trajectory_id"] for r in sampled)
            assert sampled_ids == ["t-long", "t-mid"]

            # t-short (2 turns) should be unsampled
            assert len(unsampled) == 1
            assert unsampled[0]["trajectory__trajectory_id"] == "t-short"
        finally:
            await container.shutdown()

    @pytest.mark.asyncio
    async def test_max_trajectories_caps(self, tmp_path):
        container = ServiceContainer()
        try:
            world, storage = await _setup_pipeline_world(container, tmp_path)
            ctx = _operator_ctx()

            trajs = [_make_trajectory(f"t-{i}", num_turns=5) for i in range(5)]
            labels = [("test", "test description")]

            await _ingest_trajectories(container, world, trajs, labels, ctx)

            # Cap at 2 trajectories
            world.resources.insert(SamplingConfig(max_trajectories=2))

            await container.simulation_service.step(
                world.world_id, RunConfig(num_steps=1, prefer_live_reads=True)
            )

            df = await world.get_components([Trajectory, Label])
            rows = df.collect().to_pylist()
            active = [r for r in rows if r.get("is_active", True)]

            # All 5 preserved
            assert len(active) == 5

            sampled = [r for r in active if r["label__sampled"]]
            assert len(sampled) == 2  # capped at max_trajectories
        finally:
            await container.shutdown()

    @pytest.mark.asyncio
    async def test_outcome_filter(self, tmp_path):
        container = ServiceContainer()
        try:
            world, storage = await _setup_pipeline_world(container, tmp_path)
            ctx = _operator_ctx()

            trajs = [
                _make_trajectory("t-pass", num_turns=3, outcome="success"),
                _make_trajectory("t-fail", num_turns=3, outcome="failure"),
                _make_trajectory("t-partial", num_turns=3, outcome="partial success"),
            ]
            labels = [("test", "test")]

            await _ingest_trajectories(container, world, trajs, labels, ctx)
            world.resources.insert(SamplingConfig(outcome_filter="success"))

            await container.simulation_service.step(
                world.world_id, RunConfig(num_steps=1, prefer_live_reads=True)
            )

            df = await world.get_components([Trajectory, Label])
            rows = df.collect().to_pylist()
            active = [r for r in rows if r.get("is_active", True)]
            sampled = [r for r in active if r["label__sampled"]]

            # "success" matches "success" and "partial success" (substring match)
            sampled_ids = sorted(r["trajectory__trajectory_id"] for r in sampled)
            assert "t-pass" in sampled_ids
            assert "t-partial" in sampled_ids
            assert "t-fail" not in sampled_ids
        finally:
            await container.shutdown()


class TestForkDivergeCompare:
    """Fork clones entities, change sampling config on fork,
    step both, verify different sampling results on same trajectories."""

    @pytest.mark.asyncio
    async def test_fork_diverge_compare(self, tmp_path):
        container = ServiceContainer()
        try:
            world, storage = await _setup_pipeline_world(container, tmp_path)
            ctx = _operator_ctx()

            # Ingest 5 trajectories with varying turn counts
            trajs = [_make_trajectory(f"t-{i}", num_turns=i + 2) for i in range(5)]
            # t-0: 2, t-1: 3, t-2: 4, t-3: 5, t-4: 6
            labels = [("efficiency", "Rate efficiency")]

            await _ingest_trajectories(container, world, trajs, labels, ctx)

            # Source: sample all (no filters)
            world.resources.insert(SamplingConfig())

            # Step source to materialize entities (required before fork)
            await container.simulation_service.step(
                world.world_id, RunConfig(num_steps=1, prefer_live_reads=True)
            )

            # Fork the world
            fork = await container.world_service.fork_world(world.world_id, "fork-strict", storage)

            # Change sampling config on fork: only trajectories with >= 4 turns
            fork.resources.insert(SamplingConfig(min_turns=4))

            # Step fork with prefer_live_reads (critical for forked worlds)
            await container.simulation_service.step(
                fork.world_id, RunConfig(num_steps=1, prefer_live_reads=True)
            )

            # Also step source again to verify independence
            await container.simulation_service.step(
                world.world_id, RunConfig(num_steps=1, prefer_live_reads=True)
            )

            # Source: all 5 sampled (no filters)
            src_df = await world.get_components([Trajectory, Label])
            src_rows = src_df.collect().to_pylist()
            src_active = [r for r in src_rows if r.get("is_active", True)]
            src_sampled = [r for r in src_active if r["label__sampled"]]
            assert len(src_sampled) == 5

            # Fork: only t-2 (4 turns), t-3 (5 turns), t-4 (6 turns) sampled
            fork_df = await fork.get_components([Trajectory, Label])
            fork_rows = fork_df.collect().to_pylist()
            fork_active = [r for r in fork_rows if r.get("is_active", True)]
            fork_sampled = [r for r in fork_active if r["label__sampled"]]
            assert len(fork_sampled) == 3

            fork_sampled_ids = sorted(r["trajectory__trajectory_id"] for r in fork_sampled)
            assert fork_sampled_ids == ["t-2", "t-3", "t-4"]
        finally:
            await container.shutdown()

    @pytest.mark.asyncio
    async def test_fork_preserves_entity_state(self, tmp_path):
        """Fork should clone trajectory entities identically."""
        container = ServiceContainer()
        try:
            world, storage = await _setup_pipeline_world(container, tmp_path)
            ctx = _operator_ctx()

            trajs = [_make_trajectory("t-0", num_turns=5, outcome="success", tags=["python"])]
            labels = [("efficiency", "Rate efficiency")]

            await _ingest_trajectories(container, world, trajs, labels, ctx)
            world.resources.insert(SamplingConfig())

            await container.simulation_service.step(
                world.world_id, RunConfig(num_steps=1, prefer_live_reads=True)
            )

            fork = await container.world_service.fork_world(world.world_id, "fork-check", storage)

            # Fork should have same tick
            assert fork.tick == world.tick
            # Fork should have same entity mapping
            assert fork._entity2sig == world._entity2sig
            # Fork should have different world_id
            assert fork.world_id != world.world_id
        finally:
            await container.shutdown()


class TestSampleAfterInit:
    """Calling sample() after first run updates the live resource
    and affects the next step."""

    @pytest.mark.asyncio
    async def test_sample_update_affects_next_step(self, tmp_path):
        container = ServiceContainer()
        try:
            world, storage = await _setup_pipeline_world(container, tmp_path)
            ctx = _operator_ctx()

            # Ingest: t-0 (2 turns), t-1 (5 turns), t-2 (8 turns)
            trajs = [
                _make_trajectory("t-short", num_turns=2),
                _make_trajectory("t-mid", num_turns=5),
                _make_trajectory("t-long", num_turns=8),
            ]
            labels = [("test", "test")]

            await _ingest_trajectories(container, world, trajs, labels, ctx)

            # First run: no filters, all sampled
            world.resources.insert(SamplingConfig())
            await container.simulation_service.step(
                world.world_id, RunConfig(num_steps=1, prefer_live_reads=True)
            )

            df1 = await world.get_components([Trajectory, Label])
            rows1 = [r for r in df1.collect().to_pylist() if r.get("is_active", True)]
            assert all(r["label__sampled"] for r in rows1)

            # Update sampling config live — now require >= 4 turns
            world.resources.insert(SamplingConfig(min_turns=4))

            # Step again — sampling should recompute
            await container.simulation_service.step(
                world.world_id, RunConfig(num_steps=1, prefer_live_reads=True)
            )

            df2 = await world.get_components([Trajectory, Label])
            rows2 = [r for r in df2.collect().to_pylist() if r.get("is_active", True)]

            sampled = [r for r in rows2 if r["label__sampled"]]
            unsampled = [r for r in rows2 if not r["label__sampled"]]

            # Only t-mid (5) and t-long (8) sampled now
            assert len(sampled) == 2
            assert len(unsampled) == 1
            assert unsampled[0]["trajectory__trajectory_id"] == "t-short"
        finally:
            await container.shutdown()


class TestRBAC:
    """Operator role can spawn and step, viewer cannot."""

    @pytest.mark.asyncio
    async def test_operator_can_spawn(self, tmp_path):
        container = ServiceContainer()
        try:
            world, storage = await _setup_pipeline_world(container, tmp_path)
            ctx = _operator_ctx()

            traj = _make_trajectory("t-0", num_turns=3)
            label = Label(technique="test", description="test")
            cmd = Command(
                type=CommandType.SPAWN,
                payload={
                    "components": [
                        {"type": "Trajectory", **traj.model_dump()},
                        {"type": "Label", **label.model_dump()},
                    ],
                },
            )

            # Operator should succeed
            cmd_id = await container.command_service.submit(str(world.world_id), cmd, ctx)
            assert cmd_id is not None
        finally:
            await container.shutdown()

    @pytest.mark.asyncio
    async def test_viewer_cannot_spawn(self, tmp_path):
        container = ServiceContainer()
        try:
            world, storage = await _setup_pipeline_world(container, tmp_path)
            ctx = _viewer_ctx()

            traj = _make_trajectory("t-0", num_turns=3)
            label = Label(technique="test", description="test")
            cmd = Command(
                type=CommandType.SPAWN,
                payload={
                    "components": [
                        {"type": "Trajectory", **traj.model_dump()},
                        {"type": "Label", **label.model_dump()},
                    ],
                },
            )

            with pytest.raises(PermissionError):
                await container.command_service.submit(str(world.world_id), cmd, ctx)
        finally:
            await container.shutdown()


class TestPipelineLifecycle:
    """shutdown() doesn't error, double-shutdown on shared container doesn't crash."""

    @pytest.mark.asyncio
    async def test_shutdown_clean(self, tmp_path):
        container = ServiceContainer()
        try:
            world, storage = await _setup_pipeline_world(container, tmp_path)
            ctx = _operator_ctx()

            trajs = [_make_trajectory("t-0", num_turns=3)]
            labels = [("test", "test")]
            await _ingest_trajectories(container, world, trajs, labels, ctx)
            world.resources.insert(SamplingConfig())

            await container.simulation_service.step(
                world.world_id, RunConfig(num_steps=1, prefer_live_reads=True)
            )
        finally:
            await container.shutdown()

    @pytest.mark.asyncio
    async def test_double_shutdown_no_crash(self, tmp_path):
        container = ServiceContainer()
        try:
            world, storage = await _setup_pipeline_world(container, tmp_path)

            await container.shutdown()
        finally:
            # Second shutdown should not raise
            await container.shutdown()

    @pytest.mark.asyncio
    async def test_fork_isolation_shutdown(self, tmp_path):
        """Shutting down a fork's container should not affect the source."""
        container = ServiceContainer()
        try:
            world, storage = await _setup_pipeline_world(container, tmp_path)
            ctx = _operator_ctx()

            trajs = [_make_trajectory("t-0", num_turns=3)]
            labels = [("test", "test")]
            await _ingest_trajectories(container, world, trajs, labels, ctx)
            world.resources.insert(SamplingConfig())

            await container.simulation_service.step(
                world.world_id, RunConfig(num_steps=1, prefer_live_reads=True)
            )

            # Fork uses the same container (WorldService manages both worlds)
            fork = await container.world_service.fork_world(
                world.world_id, "fork-shutdown", storage
            )

            # Remove fork — source should still be queryable
            container.world_service.remove_world(fork.world_id)

            # Source still works
            df = await world.get_components([Trajectory, Label])
            assert df is not None
            rows = df.collect().to_pylist()
            active = [r for r in rows if r.get("is_active", True)]
            assert len(active) == 1
        finally:
            await container.shutdown()


class TestPreferLiveReads:
    """prefer_live_reads=True is required for fork steps to work."""

    @pytest.mark.asyncio
    async def test_fork_step_default_runconfig_preserves_state(self, tmp_path):
        """Fork + step with *default* RunConfig must not lose the cloned snapshot.

        Regression for archetype#72: previously, ``fork_world`` persisted the
        cloned snapshot under a placeholder ``run_id`` while the next step
        queried under a fresh run_id, producing an empty previous-state read
        and wiping the fork's entities after one tick. Callers had to know to
        pass ``prefer_live_reads=True`` — silent data loss otherwise. The fix
        makes ``_live`` authoritative for post-tick-0 reads, so forks survive
        default run configs.
        """
        container = ServiceContainer()
        try:
            world, storage = await _setup_pipeline_world(container, tmp_path)
            ctx = _operator_ctx()

            trajs = [_make_trajectory(f"t-{i}", num_turns=3) for i in range(4)]
            labels = [("efficiency", "test")]
            await _ingest_trajectories(container, world, trajs, labels, ctx)
            world.resources.insert(SamplingConfig())

            # Step source with default RunConfig — no prefer_live_reads flag.
            await container.simulation_service.step(world.world_id)

            fork = await container.world_service.fork_world(world.world_id, "fork-default", storage)
            fork.resources.insert(SamplingConfig(min_turns=0))

            # Step fork with default RunConfig — the #72 failure mode.
            await container.simulation_service.step(fork.world_id)

            fork_df = await fork.get_components([Trajectory, Label])
            fork_rows = fork_df.collect().to_pylist()
            fork_active = [r for r in fork_rows if r.get("is_active", True)]
            assert len(fork_active) == 4, (
                f"Fork lost entities after default-config step (#72). Got {len(fork_active)} rows."
            )
        finally:
            await container.shutdown()

    @pytest.mark.asyncio
    async def test_fork_step_with_prefer_live_reads(self, tmp_path):
        """Fork + step with prefer_live_reads=True should use in-memory snapshots."""
        container = ServiceContainer()
        try:
            world, storage = await _setup_pipeline_world(container, tmp_path)
            ctx = _operator_ctx()

            trajs = [_make_trajectory(f"t-{i}", num_turns=3) for i in range(3)]
            labels = [("test", "test")]
            await _ingest_trajectories(container, world, trajs, labels, ctx)
            world.resources.insert(SamplingConfig())

            # Step source with prefer_live_reads
            await container.simulation_service.step(
                world.world_id, RunConfig(num_steps=1, prefer_live_reads=True)
            )

            # Fork
            fork = await container.world_service.fork_world(world.world_id, "fork-live", storage)
            fork.resources.insert(SamplingConfig(max_trajectories=1))

            # Step fork with prefer_live_reads — this should use _live snapshots
            await container.simulation_service.step(
                fork.world_id, RunConfig(num_steps=1, prefer_live_reads=True)
            )

            fork_df = await fork.get_components([Trajectory, Label])
            fork_rows = fork_df.collect().to_pylist()
            fork_active = [r for r in fork_rows if r.get("is_active", True)]

            # All 3 entities preserved, only 1 sampled
            assert len(fork_active) == 3
            sampled = [r for r in fork_active if r["label__sampled"]]
            assert len(sampled) == 1
        finally:
            await container.shutdown()


class TestScoringIntegration:
    """ScoringProcessor clamps scores through the full pipeline."""

    @pytest.mark.asyncio
    async def test_scoring_clamp_through_pipeline(self, tmp_path):
        """Scores set on Label components should be clamped by ScoringProcessor after step."""
        container = ServiceContainer()
        try:
            world, storage = await _setup_pipeline_world(container, tmp_path)
            ctx = _operator_ctx()

            # Create a trajectory with a pre-set out-of-range score
            traj = _make_trajectory("t-0", num_turns=3)
            label = Label(technique="test", description="test", score=1.5)

            cmd = Command(
                type=CommandType.SPAWN,
                payload={
                    "components": [
                        {"type": "Trajectory", **traj.model_dump()},
                        {"type": "Label", **label.model_dump()},
                    ],
                },
            )
            await container.command_service.submit(str(world.world_id), cmd, ctx)
            world.resources.insert(SamplingConfig())

            await container.simulation_service.step(
                world.world_id, RunConfig(num_steps=1, prefer_live_reads=True)
            )

            df = await world.get_components([Trajectory, Label])
            rows = df.collect().to_pylist()
            active = [r for r in rows if r.get("is_active", True)]

            # Score should be clamped to 1.0
            assert len(active) == 1
            assert active[0]["label__score"] == 1.0
        finally:
            await container.shutdown()
