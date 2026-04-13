# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Tests for LabelingProcessor — response parsing + sampled/unsampled gating.

The LLM-powered parsing logic is exercised through the module-level pure
functions (`_parse_value`, `_parse_score`, `_parse_rationale`). The gating
behavior is exercised via the full processor with `daft.functions.prompt`
replaced by a small fake that does not touch any network.
"""

from __future__ import annotations

import os
from unittest.mock import patch

import daft
import pytest

from archetype.core.resources import Resources
from archetype.trajectories.components import Label, Trajectory, Turn
from archetype.trajectories.processors import (
    LabelingConfig,
    LabelingProcessor,
    _parse_rationale,
    _parse_score,
    _parse_value,
)

# ── Response parsing: module-level pure functions ──


class TestParseValue:
    def test_clean_response(self):
        response = "VALUE: efficient\nSCORE: 0.85\nRATIONALE: Agent solved it quickly."
        assert _parse_value(response) == "efficient"

    def test_missing_value_line_falls_back_to_first_100_chars(self):
        response = "SCORE: 0.5\nRATIONALE: no value line here"
        out = _parse_value(response)
        assert out == response[:100]
        assert len(out) <= 100

    def test_missing_value_truncates_long_response(self):
        response = "x" * 500
        out = _parse_value(response)
        assert len(out) == 100
        assert out == "x" * 100

    def test_empty_response(self):
        assert _parse_value("") == ""

    def test_extra_whitespace_stripped(self):
        response = "VALUE:    efficient    \nSCORE: 0.9"
        assert _parse_value(response) == "efficient"

    def test_multiline_only_first_value_line_used(self):
        response = "VALUE: first\nOther: x\nVALUE: second"
        # The implementation returns the first matching line
        assert _parse_value(response) == "first"


class TestParseScore:
    def test_clean_response(self):
        response = "VALUE: x\nSCORE: 0.85\nRATIONALE: y"
        assert _parse_score(response) == 0.85

    def test_malformed_score_returns_zero(self):
        response = "SCORE: not_a_number"
        assert _parse_score(response) == 0.0

    def test_missing_score_returns_zero(self):
        assert _parse_score("VALUE: x\nRATIONALE: y") == 0.0

    def test_empty_response(self):
        assert _parse_score("") == 0.0

    def test_extra_whitespace_stripped(self):
        response = "SCORE:    0.42   "
        assert _parse_score(response) == 0.42

    def test_integer_score_parsed_as_float(self):
        assert _parse_score("SCORE: 1") == 1.0


class TestParseRationale:
    def test_clean_response(self):
        response = "VALUE: x\nSCORE: 0.5\nRATIONALE: Agent solved the task quickly."
        assert _parse_rationale(response) == "Agent solved the task quickly."

    def test_missing_rationale_returns_empty(self):
        assert _parse_rationale("VALUE: x\nSCORE: 0.5") == ""

    def test_empty_response(self):
        assert _parse_rationale("") == ""

    def test_extra_whitespace_stripped(self):
        response = "RATIONALE:    Because reasons.    "
        assert _parse_rationale(response) == "Because reasons."


def test_empty_response_all_defaults():
    """An empty LLM response should yield empty value, 0.0 score, empty rationale."""
    assert _parse_value("") == ""
    assert _parse_score("") == 0.0
    assert _parse_rationale("") == ""


def test_clean_response_all_three_populated():
    """The canonical happy-path response populates all three fields."""
    response = "VALUE: efficient\nSCORE: 0.85\nRATIONALE: Agent solved it in 3 turns."
    assert _parse_value(response) == "efficient"
    assert _parse_score(response) == 0.85
    assert _parse_rationale(response) == "Agent solved it in 3 turns."


# ── Sampled/unsampled gating ──


def _make_df(trajectories, labels):
    """Mirror of tests/trajectories/test_processors.py::_make_df."""
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


def _make_fake_prompt(
    response_by_technique: dict[str, str] | None = None,
    default: str = "VALUE: fake\nSCORE: 0.5\nRATIONALE: fake reason",
):
    """Return a drop-in replacement for daft.functions.prompt that doesn't hit the network.

    Ignores the model/max_output_tokens kwargs and returns a string expression
    — a constant by default, so no API key is needed.
    """

    def fake_prompt(_expr, model=None, max_output_tokens=None, **_kwargs):
        # Return a literal string column — Daft treats this like any other expr.
        return daft.lit(default)

    return fake_prompt


@pytest.mark.asyncio
async def test_labeling_only_sampled_rows_updated():
    """Unsampled rows must retain their prior label__value/score/rationale."""
    trajs = [
        Trajectory.from_turns(
            f"t-{i}",
            turns=[Turn(role="user", content="hi")],
            source="test",
            outcome="ok",
        )
        for i in range(4)
    ]
    # Pre-seed label values so we can detect whether they got overwritten.
    labels = []
    for i in range(4):
        labels.append(
            Label(
                technique="eff",
                description="efficiency",
                value=f"prior-{i}",
                score=0.11 * (i + 1),
                rationale=f"prior-rationale-{i}",
                sampled=(i % 2 == 0),  # entities 0, 2 sampled; 1, 3 unsampled
            )
        )
    df = _make_df(trajs, labels)

    fake = _make_fake_prompt(default="VALUE: fresh\nSCORE: 0.77\nRATIONALE: updated")

    with patch("daft.functions.prompt", fake):
        resources = Resources()
        resources.insert(LabelingConfig())
        proc = LabelingProcessor()
        result = await proc.process(df, resources=resources)
        collected = result.collect().to_pylist()

    # No row count change
    assert len(collected) == 4

    by_id = {r["entity_id"]: r for r in collected}

    # Sampled rows: overwritten with fake LLM output
    for eid in (0, 2):
        assert by_id[eid]["label__value"] == "fresh"
        assert by_id[eid]["label__score"] == 0.77
        assert by_id[eid]["label__rationale"] == "updated"

    # Unsampled rows: prior values preserved
    for eid in (1, 3):
        assert by_id[eid]["label__value"] == f"prior-{eid}"
        assert by_id[eid]["label__score"] == pytest.approx(0.11 * (eid + 1))
        assert by_id[eid]["label__rationale"] == f"prior-rationale-{eid}"


@pytest.mark.asyncio
async def test_labeling_all_unsampled_no_llm_calls():
    """When no rows are sampled, all labels stay untouched and the row count is preserved."""
    trajs = [
        Trajectory.from_turns(f"t-{i}", turns=[Turn(role="user", content="x")], source="test")
        for i in range(3)
    ]
    labels = [
        Label(technique="t", description="d", value=f"keep-{i}", score=0.3, sampled=False)
        for i in range(3)
    ]
    df = _make_df(trajs, labels)

    # If fake_prompt is called, return sentinel so we can detect it.
    fake = _make_fake_prompt(default="VALUE: SHOULD_NOT_APPEAR\nSCORE: 1.0\nRATIONALE: x")

    with patch("daft.functions.prompt", fake):
        resources = Resources()
        resources.insert(LabelingConfig())
        proc = LabelingProcessor()
        result = await proc.process(df, resources=resources)
        collected = result.collect().to_pylist()

    assert len(collected) == 3
    for row in collected:
        assert row["label__value"].startswith("keep-")
        assert row["label__score"] == 0.3
        assert "SHOULD_NOT_APPEAR" not in row["label__value"]


@pytest.mark.asyncio
async def test_labeling_all_sampled_all_updated():
    """When every row is sampled, every row gets fresh LLM-derived values."""
    trajs = [
        Trajectory.from_turns(f"t-{i}", turns=[Turn(role="user", content="x")], source="test")
        for i in range(3)
    ]
    labels = [Label(technique="t", description="d", sampled=True) for _ in range(3)]
    df = _make_df(trajs, labels)

    fake = _make_fake_prompt(default="VALUE: v\nSCORE: 0.5\nRATIONALE: r")

    with patch("daft.functions.prompt", fake):
        resources = Resources()
        resources.insert(LabelingConfig())
        proc = LabelingProcessor()
        result = await proc.process(df, resources=resources)
        collected = result.collect().to_pylist()

    assert len(collected) == 3
    for row in collected:
        assert row["label__value"] == "v"
        assert row["label__score"] == 0.5
        assert row["label__rationale"] == "r"


# ── End-to-end LLM integration (guarded) ──


@pytest.mark.skipif(
    not os.environ.get("OPENAI_API_KEY"),
    reason="OPENAI_API_KEY not set; skipping live LLM integration test",
)
@pytest.mark.asyncio
async def test_labeling_end_to_end_with_real_llm():
    """Smoke test against the real LLM. Only runs when OPENAI_API_KEY is present."""
    traj = Trajectory.from_turns(
        "t-live",
        turns=[
            Turn(role="user", content="What's 2+2?"),
            Turn(role="assistant", content="4"),
        ],
        source="test",
        outcome="success",
    )
    label = Label(
        technique="correctness",
        description="Did the assistant answer correctly?",
        sampled=True,
    )
    df = _make_df([traj], [label])

    resources = Resources()
    resources.insert(LabelingConfig(model="gpt-4o-mini", max_output_tokens=128))
    proc = LabelingProcessor()
    result = await proc.process(df, resources=resources)
    collected = result.collect().to_pylist()

    assert len(collected) == 1
    row = collected[0]
    # Parsed fields should be populated (though exact content is model-dependent).
    assert isinstance(row["label__value"], str)
    assert isinstance(row["label__score"], float)
    assert isinstance(row["label__rationale"], str)
    assert 0.0 <= row["label__score"] <= 1.0 or row["label__score"] == 0.0
