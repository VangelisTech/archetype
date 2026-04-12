# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Tests for the LabelingProcessor — response parsing UDFs and sampled/unsampled gating.

These tests exercise the extract_value, extract_score, and extract_rationale
parsing logic using synthetic LLM responses (no API key needed), plus the
sampled/unsampled split-and-rejoin behavior.
"""

import daft
import pytest
from daft import col

from archetype.trajectories.components import Label, Trajectory, Turn

# ── Recreate the parsing UDFs exactly as LabelingProcessor defines them ──
# They are local to process(), so we mirror them here for direct testing.


@daft.func
def extract_value(response: str) -> str:
    if not response:
        return ""
    for line in response.split("\n"):
        if line.startswith("VALUE:"):
            return line[6:].strip()
    return response[:100]


@daft.func
def extract_score(response: str) -> float:
    if not response:
        return 0.0
    for line in response.split("\n"):
        if line.startswith("SCORE:"):
            try:
                return float(line[6:].strip())
            except ValueError:
                return 0.0
    return 0.0


@daft.func
def extract_rationale(response: str) -> str:
    if not response:
        return ""
    for line in response.split("\n"):
        if line.startswith("RATIONALE:"):
            return line[10:].strip()
    return ""


# ── Helper to build a (Trajectory, Label) DataFrame ──


def _make_df(trajectories: list[Trajectory], labels: list[Label]) -> daft.DataFrame:
    """Build a DataFrame matching what the ECS produces for (Trajectory, Label) entities."""
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


def _make_simple_trajectory() -> Trajectory:
    return Trajectory.from_turns(
        trajectory_id="t-0",
        turns=[Turn(role="user", content="hello"), Turn(role="assistant", content="hi")],
        source="test",
        outcome="success",
    )


# ── Response Parsing UDF Tests ──


class TestExtractValue:
    def test_clean_response(self):
        """VALUE line is extracted and stripped."""
        df = daft.from_pydict(
            {"response": ["VALUE: efficient\nSCORE: 0.85\nRATIONALE: Agent solved..."]}
        )
        result = df.with_column("val", extract_value(col("response"))).collect().to_pylist()
        assert result[0]["val"] == "efficient"

    def test_missing_value_line(self):
        """Without VALUE line, fall back to first 100 chars."""
        response = "SCORE: 0.85\nRATIONALE: Agent solved the problem efficiently"
        df = daft.from_pydict({"response": [response]})
        result = df.with_column("val", extract_value(col("response"))).collect().to_pylist()
        assert result[0]["val"] == response[:100]

    def test_empty_response(self):
        """Empty string yields empty value."""
        df = daft.from_pydict({"response": [""]})
        result = df.with_column("val", extract_value(col("response"))).collect().to_pylist()
        assert result[0]["val"] == ""

    def test_extra_whitespace(self):
        """VALUE with extra whitespace is stripped."""
        df = daft.from_pydict({"response": ["VALUE:  efficient "]})
        result = df.with_column("val", extract_value(col("response"))).collect().to_pylist()
        assert result[0]["val"] == "efficient"

    def test_long_fallback_truncated(self):
        """When VALUE line missing and response > 100 chars, truncate to 100."""
        long_text = "A" * 200
        df = daft.from_pydict({"response": [long_text]})
        result = df.with_column("val", extract_value(col("response"))).collect().to_pylist()
        assert len(result[0]["val"]) == 100


class TestExtractScore:
    def test_clean_response(self):
        """SCORE line is extracted as float."""
        df = daft.from_pydict(
            {"response": ["VALUE: efficient\nSCORE: 0.85\nRATIONALE: Agent solved..."]}
        )
        result = df.with_column("sc", extract_score(col("response"))).collect().to_pylist()
        assert result[0]["sc"] == pytest.approx(0.85)

    def test_malformed_score(self):
        """Non-numeric SCORE yields 0.0."""
        df = daft.from_pydict({"response": ["SCORE: not_a_number"]})
        result = df.with_column("sc", extract_score(col("response"))).collect().to_pylist()
        assert result[0]["sc"] == 0.0

    def test_empty_response(self):
        """Empty string yields 0.0."""
        df = daft.from_pydict({"response": [""]})
        result = df.with_column("sc", extract_score(col("response"))).collect().to_pylist()
        assert result[0]["sc"] == 0.0

    def test_missing_score_line(self):
        """No SCORE line yields 0.0."""
        df = daft.from_pydict({"response": ["VALUE: efficient\nRATIONALE: good"]})
        result = df.with_column("sc", extract_score(col("response"))).collect().to_pylist()
        assert result[0]["sc"] == 0.0

    def test_score_with_whitespace(self):
        """SCORE with extra whitespace parses correctly."""
        df = daft.from_pydict({"response": ["SCORE:   0.92  "]})
        result = df.with_column("sc", extract_score(col("response"))).collect().to_pylist()
        assert result[0]["sc"] == pytest.approx(0.92)


class TestExtractRationale:
    def test_clean_response(self):
        """RATIONALE line is extracted and stripped."""
        df = daft.from_pydict(
            {"response": ["VALUE: efficient\nSCORE: 0.85\nRATIONALE: Agent solved the problem."]}
        )
        result = df.with_column("rat", extract_rationale(col("response"))).collect().to_pylist()
        assert result[0]["rat"] == "Agent solved the problem."

    def test_empty_response(self):
        """Empty string yields empty rationale."""
        df = daft.from_pydict({"response": [""]})
        result = df.with_column("rat", extract_rationale(col("response"))).collect().to_pylist()
        assert result[0]["rat"] == ""

    def test_missing_rationale_line(self):
        """No RATIONALE line yields empty string."""
        df = daft.from_pydict({"response": ["VALUE: efficient\nSCORE: 0.85"]})
        result = df.with_column("rat", extract_rationale(col("response"))).collect().to_pylist()
        assert result[0]["rat"] == ""

    def test_rationale_with_whitespace(self):
        """RATIONALE with extra whitespace is stripped."""
        df = daft.from_pydict({"response": ["RATIONALE:   Good reasoning shown.  "]})
        result = df.with_column("rat", extract_rationale(col("response"))).collect().to_pylist()
        assert result[0]["rat"] == "Good reasoning shown."


class TestAllFieldsParseTogether:
    """End-to-end parsing: all three extractors on the same response."""

    def test_clean_full_response(self):
        response = "VALUE: efficient\nSCORE: 0.85\nRATIONALE: Agent solved the problem quickly."
        df = daft.from_pydict({"response": [response]})
        df = df.with_column("value", extract_value(col("response")))
        df = df.with_column("score", extract_score(col("response")))
        df = df.with_column("rationale", extract_rationale(col("response")))
        result = df.collect().to_pylist()[0]

        assert result["value"] == "efficient"
        assert result["score"] == pytest.approx(0.85)
        assert result["rationale"] == "Agent solved the problem quickly."

    def test_all_missing(self):
        """Response with no recognized lines."""
        response = "I think this is a good trajectory."
        df = daft.from_pydict({"response": [response]})
        df = df.with_column("value", extract_value(col("response")))
        df = df.with_column("score", extract_score(col("response")))
        df = df.with_column("rationale", extract_rationale(col("response")))
        result = df.collect().to_pylist()[0]

        # value falls back to first 100 chars
        assert result["value"] == response[:100]
        assert result["score"] == 0.0
        assert result["rationale"] == ""


# ── Sampled/Unsampled Gating Tests ──


class TestSampledUnsampled:
    """Test that LabelingProcessor correctly splits on label__sampled
    and only processes sampled rows, preserving unsampled row values."""

    @pytest.mark.asyncio
    async def test_only_sampled_rows_get_new_labels(self):
        """Sampled rows should be modified; unsampled rows should keep original values."""
        from unittest.mock import patch

        from archetype.core.resources import Resources
        from archetype.trajectories.processors import LabelingConfig, LabelingProcessor

        traj = _make_simple_trajectory()

        # Two entities: one sampled, one not
        sampled_label = Label(technique="efficiency", description="Rate efficiency", sampled=True)
        unsampled_label = Label(
            technique="efficiency",
            description="Rate efficiency",
            sampled=False,
            value="original",
            score=0.5,
            rationale="pre-existing",
        )

        df = _make_df([traj, traj], [sampled_label, unsampled_label])

        resources = Resources()
        resources.insert(LabelingConfig(model="test-model"))

        # Mock daft.functions.prompt to return a canned LLM response
        fake_response = "VALUE: fast\nSCORE: 0.9\nRATIONALE: Very efficient execution."

        def mock_prompt(*args, **kwargs):
            return daft.lit(fake_response)

        proc = LabelingProcessor()

        with patch("daft.functions.prompt", side_effect=mock_prompt):
            result = await proc.process(df, resources=resources)

        collected = result.collect().to_pylist()
        assert len(collected) == 2

        # Find rows by sampled status
        sampled_rows = [r for r in collected if r["label__sampled"]]
        unsampled_rows = [r for r in collected if not r["label__sampled"]]

        assert len(sampled_rows) == 1
        assert len(unsampled_rows) == 1

        # Sampled row should have the new parsed values
        sr = sampled_rows[0]
        assert sr["label__value"] == "fast"
        assert sr["label__score"] == pytest.approx(0.9)
        assert sr["label__rationale"] == "Very efficient execution."

        # Unsampled row should retain its original values
        ur = unsampled_rows[0]
        assert ur["label__value"] == "original"
        assert ur["label__score"] == pytest.approx(0.5)
        assert ur["label__rationale"] == "pre-existing"

    @pytest.mark.asyncio
    async def test_concat_rejoins_all_rows(self):
        """After split + process, concat should return the full row count."""
        from unittest.mock import patch

        from archetype.core.resources import Resources
        from archetype.trajectories.processors import LabelingConfig, LabelingProcessor

        trajs = [_make_simple_trajectory() for _ in range(5)]
        labels = []
        for i in range(5):
            labels.append(
                Label(
                    technique="eff",
                    description="rate",
                    sampled=(i < 3),  # first 3 sampled, last 2 not
                )
            )
        df = _make_df(trajs, labels)

        resources = Resources()
        resources.insert(LabelingConfig(model="test-model"))

        def mock_prompt(*args, **kwargs):
            return daft.lit("VALUE: ok\nSCORE: 0.5\nRATIONALE: fine")

        proc = LabelingProcessor()

        with patch("daft.functions.prompt", side_effect=mock_prompt):
            result = await proc.process(df, resources=resources)

        collected = result.collect().to_pylist()
        assert len(collected) == 5

        sampled_count = sum(1 for r in collected if r["label__sampled"])
        assert sampled_count == 3

    @pytest.mark.asyncio
    async def test_all_unsampled_returns_unchanged(self):
        """If no rows are sampled, all rows should pass through unchanged."""
        from unittest.mock import patch

        from archetype.core.resources import Resources
        from archetype.trajectories.processors import LabelingConfig, LabelingProcessor

        trajs = [_make_simple_trajectory(), _make_simple_trajectory()]
        labels = [
            Label(technique="t", description="d", sampled=False, value="v1", score=0.1),
            Label(technique="t", description="d", sampled=False, value="v2", score=0.2),
        ]
        df = _make_df(trajs, labels)

        resources = Resources()
        resources.insert(LabelingConfig(model="test-model"))

        # prompt should NOT be called since nothing is sampled
        def mock_prompt(*args, **kwargs):
            return daft.lit("VALUE: should-not-appear\nSCORE: 0.99\nRATIONALE: nope")

        proc = LabelingProcessor()

        with patch("daft.functions.prompt", side_effect=mock_prompt):
            result = await proc.process(df, resources=resources)

        collected = result.collect().to_pylist()
        assert len(collected) == 2

        values = sorted(r["label__value"] for r in collected)
        assert values == ["v1", "v2"]
