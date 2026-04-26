# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Smoke tests for examples/06_trajectory_analysis.py components and processors."""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest
from uuid_utils import uuid7

from archetype.app.auth.guard import reset_daily_tokens, reset_tick_counters
from archetype.app.auth.models import ActorCtx
from archetype.app.container import ServiceContainer
from archetype.app.models import Command, CommandType
from archetype.core.config import RunConfig, StorageConfig, WorldConfig
from archetype.core.resources import Resources

# Import the example script directly — it isn't a package on sys.path.
_EXAMPLE = Path(__file__).resolve().parents[2] / "examples" / "06_trajectory_analysis.py"
_spec = importlib.util.spec_from_file_location("traj_mod", _EXAMPLE)
traj_mod = importlib.util.module_from_spec(_spec)
sys.modules[_spec.name] = traj_mod
_spec.loader.exec_module(traj_mod)

Turn = traj_mod.Turn
Trajectory = traj_mod.Trajectory
Label = traj_mod.Label
SamplingProcessor = traj_mod.SamplingProcessor
ScoringProcessor = traj_mod.ScoringProcessor
SamplingConfig = traj_mod.SamplingConfig


@pytest.fixture(autouse=True)
def _reset_quotas():
    reset_tick_counters()
    reset_daily_tokens()
    yield
    reset_tick_counters()
    reset_daily_tokens()


# ── Turn round-trip ──────────────────────────────────────────────────


class TestTurn:
    def test_round_trip(self):
        turn = Turn(
            role="tool_call",
            content="reading file",
            tool_name="Read",
            tool_input='{"path": "a.py"}',
            tokens=5,
            duration_ms=100.0,
        )
        d = turn.to_dict()
        assert d["role"] == "tool_call"
        assert d["tool_name"] == "Read"
        restored = Turn.from_dict(d)
        assert restored.role == turn.role
        assert restored.tool_name == turn.tool_name
        assert restored.tokens == turn.tokens

    def test_optional_fields_omitted(self):
        turn = Turn(role="user", content="hello", tokens=3)
        d = turn.to_dict()
        assert "tool_name" not in d
        assert "error" not in d


# ── Trajectory component ────────────────────────────────────────────


class TestTrajectoryComponent:
    def test_from_turns_aggregates(self):
        turns = [
            Turn(role="user", content="hi", tokens=5, duration_ms=100),
            Turn(role="assistant", content="hey", tokens=8, duration_ms=200),
        ]
        t = Trajectory.from_turns("t1", turns, source="test", outcome="ok", tags=["a"])
        assert t.total_turns == 2
        assert t.total_tokens == 13
        assert t.duration_seconds == pytest.approx(0.3)
        assert json.loads(t.tags_json) == ["a"]

    def test_get_turns_round_trip(self):
        turns = [Turn(role="user", content="x", tokens=1)]
        t = Trajectory.from_turns("t2", turns)
        restored = t.get_turns()
        assert len(restored) == 1
        assert restored[0].role == "user"


# ── SamplingProcessor ───────────────────────────────────────────────


@pytest.mark.asyncio
async def test_sampling_processor_filters_by_min_turns(tmp_path):
    """SamplingProcessor marks entities with fewer turns than min_turns as not sampled."""
    container = ServiceContainer()
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="samp")
        world = await container.world_service.create_world(WorldConfig(name="samp-test"), storage)

        await world.system.add_processor(SamplingProcessor())

        resources = Resources()
        resources.insert(SamplingConfig(min_turns=3))
        world.resources = resources

        ctx = ActorCtx(id=uuid7(), roles={"operator"})

        short_traj = Trajectory.from_turns(
            "short",
            [Turn(role="user", content="hi", tokens=1)],
            source="test",
            outcome="ok",
        )
        long_traj = Trajectory.from_turns(
            "long",
            [
                Turn(role="user", content="a", tokens=1),
                Turn(role="assistant", content="b", tokens=1),
                Turn(role="tool_call", content="c", tokens=1),
                Turn(role="tool_result", content="d", tokens=1),
            ],
            source="test",
            outcome="ok",
        )

        for traj in [short_traj, long_traj]:
            label = Label(technique="test", description="test eval")
            cmd = Command(
                type=CommandType.SPAWN,
                payload={
                    "components": [
                        {"type": "Trajectory", **traj.model_dump()},
                        {"type": "Label", **label.model_dump()},
                    ],
                },
            )
            await container.command_service.submit(ctx, str(world.world_id), cmd)

        await container.simulation_service.step(world.world_id, RunConfig(num_steps=1))

        df = await world.get_components([Trajectory, Label])
        assert df is not None
        rows = df.collect().to_pylist()
        sampled_by_id = {r["trajectory__trajectory_id"]: r["label__sampled"] for r in rows}
        assert sampled_by_id["short"] is False
        assert sampled_by_id["long"] is True
    finally:
        await container.shutdown()


# ── ScoringProcessor ────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_scoring_processor_clamps_score(tmp_path):
    """ScoringProcessor clamps label__score into [0, 1]."""
    container = ServiceContainer()
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="score")
        world = await container.world_service.create_world(WorldConfig(name="score-test"), storage)

        await world.system.add_processor(ScoringProcessor())

        ctx = ActorCtx(id=uuid7(), roles={"operator"})

        traj = Trajectory.from_turns(
            "t1",
            [Turn(role="user", content="hi", tokens=1)],
            source="test",
            outcome="ok",
        )
        label = Label(technique="test", description="test", score=5.0, sampled=True)
        cmd = Command(
            type=CommandType.SPAWN,
            payload={
                "components": [
                    {"type": "Trajectory", **traj.model_dump()},
                    {"type": "Label", **label.model_dump()},
                ],
            },
        )
        await container.command_service.submit(ctx, str(world.world_id), cmd)

        await container.simulation_service.step(world.world_id, RunConfig(num_steps=1))

        df = await world.get_components([Trajectory, Label])
        assert df is not None
        rows = df.collect().to_pylist()
        assert len(rows) == 1
        assert rows[0]["label__score"] == pytest.approx(1.0)
    finally:
        await container.shutdown()


# ── Full pipeline (no LLM) ──────────────────────────────────────────


@pytest.mark.asyncio
async def test_full_pipeline_without_llm(tmp_path):
    """End-to-end: ingest, sample, fork — skipping LabelingProcessor (needs LLM)."""
    container = ServiceContainer()
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="full")
        world = await container.world_service.create_world(WorldConfig(name="full-test"), storage)

        await world.system.add_processor(SamplingProcessor())
        await world.system.add_processor(ScoringProcessor())

        resources = Resources()
        resources.insert(SamplingConfig(min_turns=2))
        world.resources = resources

        ctx = ActorCtx(id=uuid7(), roles={"operator"})

        trajectories = traj_mod.make_trajectories()
        for trajectory in trajectories:
            label = Label(technique="efficiency", description="eval", score=0.7)
            cmd = Command(
                type=CommandType.SPAWN,
                payload={
                    "components": [
                        {"type": "Trajectory", **trajectory.model_dump()},
                        {"type": "Label", **label.model_dump()},
                    ],
                },
            )
            await container.command_service.submit(ctx, str(world.world_id), cmd)

        await container.simulation_service.step(world.world_id, RunConfig(num_steps=1))

        df = await world.get_components([Trajectory, Label])
        assert df is not None
        rows = df.collect().to_pylist()
        assert len(rows) == 3

        # Fork should succeed
        fork = await container.world_service.fork_world(
            source_world_id=world.world_id,
            name="fork-strict",
            storage_config=storage,
        )
        assert fork.world_id != world.world_id
        assert fork.tick == world.tick
    finally:
        await container.shutdown()
