# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Tests for trajectory processors — sampling, scoring."""

import daft
import pytest

from archetype.core.resources import Resources
from archetype.trajectories.components import Label, Trajectory, Turn
from archetype.trajectories.processors import (
    SamplingConfig,
    SamplingProcessor,
    ScoringProcessor,
)


def _make_df(trajectories: list[Trajectory], labels: list[Label]) -> daft.DataFrame:
    """Build a DataFrame matching what the ECS would produce for (Trajectory, Label) entities."""
    rows = []
    for traj, lab in zip(trajectories, labels, strict=True):
        row = {}
        for field, val in traj.model_dump().items():
            row[f"trajectory__{field}"] = val
        for field, val in lab.model_dump().items():
            row[f"label__{field}"] = val
        row["entity_id"] = len(rows)
        row["is_active"] = True
        rows.append(row)
    return daft.from_pylist(rows)


def _make_trajectories(n: int) -> list[Trajectory]:
    trajs = []
    for i in range(n):
        trajs.append(
            Trajectory.from_turns(
                trajectory_id=f"t-{i}",
                turns=[Turn(role="user", content=f"turn {j}") for j in range(i + 2)],
                source="test",
                outcome="success" if i % 2 == 0 else "failure",
                tags=["python"] if i < 2 else ["rust"],
            )
        )
    return trajs


@pytest.mark.asyncio
async def test_sampling_preserves_all_rows():
    """max_trajectories should mark excess rows as unsampled, not drop them."""
    trajs = _make_trajectories(5)
    labels = [Label(technique="test", description="test") for _ in trajs]
    df = _make_df(trajs, labels)

    resources = Resources()
    resources.insert(SamplingConfig(max_trajectories=2))

    proc = SamplingProcessor()
    result = await proc.process(df, resources=resources)
    collected = result.collect().to_pylist()

    # All 5 rows preserved
    assert len(collected) == 5
    sampled_count = sum(1 for r in collected if r["label__sampled"])
    assert sampled_count == 2


@pytest.mark.asyncio
async def test_sampling_starts_fresh():
    """Sampling should start from True each tick, not be monotonic."""
    trajs = _make_trajectories(3)
    # Pre-set sampled to False — sampling should override
    labels = [Label(technique="test", description="test", sampled=False) for _ in trajs]
    df = _make_df(trajs, labels)

    resources = Resources()
    resources.insert(SamplingConfig())  # no filters = all sampled

    proc = SamplingProcessor()
    result = await proc.process(df, resources=resources)
    collected = result.collect().to_pylist()

    # All should be sampled (start from True, no filters)
    assert all(r["label__sampled"] for r in collected)


@pytest.mark.asyncio
async def test_sampling_min_turns():
    trajs = _make_trajectories(4)  # turns: 2, 3, 4, 5
    labels = [Label(technique="t", description="d") for _ in trajs]
    df = _make_df(trajs, labels)

    resources = Resources()
    resources.insert(SamplingConfig(min_turns=4))

    proc = SamplingProcessor()
    result = await proc.process(df, resources=resources)
    collected = result.collect().to_pylist()

    assert len(collected) == 4  # all rows kept
    sampled = [r for r in collected if r["label__sampled"]]
    # Only trajectories with >= 4 turns: t-2 (4 turns) and t-3 (5 turns)
    assert len(sampled) == 2


@pytest.mark.asyncio
async def test_sampling_tag_exact_match():
    """Tags should use exact match, not substring."""
    trajs = [
        Trajectory.from_turns("t-0", [Turn(role="user", content="x")], tags=["python"]),
        Trajectory.from_turns("t-1", [Turn(role="user", content="x")], tags=["pythonic"]),
        Trajectory.from_turns("t-2", [Turn(role="user", content="x")], tags=["py"]),
    ]
    labels = [Label(technique="t", description="d") for _ in trajs]
    df = _make_df(trajs, labels)

    resources = Resources()
    resources.insert(SamplingConfig(require_tags=["python"]))

    proc = SamplingProcessor()
    result = await proc.process(df, resources=resources)
    collected = result.collect().to_pylist()

    sampled = [r for r in collected if r["label__sampled"]]
    # Only exact match "python", not "pythonic" or "py"
    assert len(sampled) == 1
    assert sampled[0]["trajectory__trajectory_id"] == "t-0"


@pytest.mark.asyncio
async def test_sampling_exclude_tags():
    trajs = [
        Trajectory.from_turns("t-0", [Turn(role="user", content="x")], tags=["python", "clean"]),
        Trajectory.from_turns("t-1", [Turn(role="user", content="x")], tags=["python", "failed"]),
    ]
    labels = [Label(technique="t", description="d") for _ in trajs]
    df = _make_df(trajs, labels)

    resources = Resources()
    resources.insert(SamplingConfig(exclude_tags=["failed"]))

    proc = SamplingProcessor()
    result = await proc.process(df, resources=resources)
    collected = result.collect().to_pylist()

    sampled = [r for r in collected if r["label__sampled"]]
    assert len(sampled) == 1
    assert sampled[0]["trajectory__trajectory_id"] == "t-0"


@pytest.mark.asyncio
async def test_scoring_clamps():
    """ScoringProcessor should clamp scores to [0, 1]."""
    rows = [
        {"label__score": 1.5, "label__sampled": True, "entity_id": 0},
        {"label__score": -0.3, "label__sampled": True, "entity_id": 1},
        {"label__score": 0.7, "label__sampled": True, "entity_id": 2},
    ]
    df = daft.from_pylist(rows)

    proc = ScoringProcessor()
    result = await proc.process(df)
    collected = result.collect().to_pylist()

    scores = {r["entity_id"]: r["label__score"] for r in collected}
    assert scores[0] == 1.0
    assert scores[1] == 0.0
    assert scores[2] == 0.7
