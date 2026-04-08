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
    extract_rationale,
    extract_score,
    extract_value,
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


# ── Response parsing UDF tests (#78) ──


def _apply_udf(udf_fn, values: list[str]) -> list:
    """Helper: apply a @daft.func UDF to a list of strings via a DataFrame."""
    df = daft.from_pydict({"response": values})
    df = df.with_column("result", udf_fn(daft.col("response")))
    return [r["result"] for r in df.collect().to_pylist()]


class TestExtractValue:
    def test_clean_response(self):
        response = "VALUE: efficient\nSCORE: 0.85\nRATIONALE: Solved on first attempt."
        [result] = _apply_udf(extract_value, [response])
        assert result == "efficient"

    def test_missing_value_line(self):
        response = "SCORE: 0.85\nRATIONALE: Good work."
        [result] = _apply_udf(extract_value, [response])
        # Falls back to first 100 chars
        assert result == response[:100]

    def test_empty_response(self):
        [result] = _apply_udf(extract_value, [""])
        assert result == ""

    def test_extra_whitespace(self):
        response = "VALUE:  efficient \nSCORE: 0.5"
        [result] = _apply_udf(extract_value, [response])
        assert result == "efficient"

    def test_fallback_truncates_long_response(self):
        long_text = "x" * 200
        [result] = _apply_udf(extract_value, [long_text])
        assert len(result) == 100


class TestExtractScore:
    def test_clean_response(self):
        response = "VALUE: efficient\nSCORE: 0.85\nRATIONALE: Great."
        [result] = _apply_udf(extract_score, [response])
        assert result == 0.85

    def test_malformed_score(self):
        response = "VALUE: ok\nSCORE: not_a_number\nRATIONALE: Hmm."
        [result] = _apply_udf(extract_score, [response])
        assert result == 0.0

    def test_empty_response(self):
        [result] = _apply_udf(extract_score, [""])
        assert result == 0.0

    def test_missing_score_line(self):
        response = "VALUE: ok\nRATIONALE: Missing score."
        [result] = _apply_udf(extract_score, [response])
        assert result == 0.0

    def test_score_with_whitespace(self):
        response = "SCORE:  0.42 "
        [result] = _apply_udf(extract_score, [response])
        assert result == pytest.approx(0.42)


class TestExtractRationale:
    def test_clean_response(self):
        response = "VALUE: ok\nSCORE: 0.5\nRATIONALE: Agent solved the problem."
        [result] = _apply_udf(extract_rationale, [response])
        assert result == "Agent solved the problem."

    def test_empty_response(self):
        [result] = _apply_udf(extract_rationale, [""])
        assert result == ""

    def test_missing_rationale_line(self):
        response = "VALUE: ok\nSCORE: 0.5"
        [result] = _apply_udf(extract_rationale, [response])
        assert result == ""

    def test_rationale_with_whitespace(self):
        response = "RATIONALE:   Good job  "
        [result] = _apply_udf(extract_rationale, [response])
        assert result == "Good job"


# ── Sampled/unsampled split tests (#78) ──


@pytest.mark.asyncio
async def test_labeling_split_preserves_row_count():
    """Sampled + unsampled concat should preserve all rows."""
    trajs = _make_trajectories(4)
    labels = [
        Label(technique="t", description="d", sampled=True),
        Label(technique="t", description="d", sampled=False),
        Label(technique="t", description="d", sampled=True),
        Label(technique="t", description="d", sampled=False),
    ]
    df = _make_df(trajs, labels)

    # Verify the split/concat logic preserves rows
    sampled_df = df.where(daft.col("label__sampled"))
    unsampled_df = df.where(~daft.col("label__sampled"))
    rejoined = sampled_df.concat(unsampled_df)
    collected = rejoined.collect().to_pylist()

    assert len(collected) == 4


@pytest.mark.asyncio
async def test_labeling_unsampled_rows_keep_existing_labels():
    """Unsampled rows should retain their pre-existing label values."""
    trajs = _make_trajectories(2)
    labels = [
        Label(
            technique="t",
            description="d",
            sampled=False,
            value="pre-set",
            score=0.99,
            rationale="kept",
        ),
        Label(
            technique="t",
            description="d",
            sampled=False,
            value="also-pre",
            score=0.77,
            rationale="also-kept",
        ),
    ]
    df = _make_df(trajs, labels)

    # Simulate the unsampled path: they're filtered by ~sampled, never touch LLM
    unsampled_df = df.where(~daft.col("label__sampled"))
    collected = unsampled_df.collect().to_pylist()

    assert len(collected) == 2
    for row in collected:
        # Values should be unchanged
        assert row["label__value"] in ("pre-set", "also-pre")
        assert row["label__score"] in (0.99, 0.77)
        assert row["label__rationale"] in ("kept", "also-kept")


@pytest.mark.asyncio
async def test_labeling_sampled_rows_isolated_from_unsampled():
    """Only sampled rows should be in the sampled partition."""
    trajs = _make_trajectories(3)
    labels = [
        Label(technique="t", description="d", sampled=True),
        Label(technique="t", description="d", sampled=False),
        Label(technique="t", description="d", sampled=True),
    ]
    df = _make_df(trajs, labels)

    sampled_df = df.where(daft.col("label__sampled"))
    unsampled_df = df.where(~daft.col("label__sampled"))

    sampled_collected = sampled_df.collect().to_pylist()
    unsampled_collected = unsampled_df.collect().to_pylist()

    assert len(sampled_collected) == 2
    assert len(unsampled_collected) == 1
    assert all(r["label__sampled"] for r in sampled_collected)
    assert not any(r["label__sampled"] for r in unsampled_collected)
