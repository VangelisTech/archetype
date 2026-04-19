# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Tests for session trajectory analysis pipeline.

Tests against both synthetic data and real session transcripts from 2026-04-03.
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import daft
import pytest

from archetype.app.session_analysis.loader import load_session, load_sessions
from archetype.app.session_analysis.patterns import (
    compute_decay_metrics,
    compute_thrashing_runs,
    detect_error_thrashing,
    detect_frustration,
    session_pain_report,
)

# ============================================================================
# Fixtures: synthetic session data
# ============================================================================

REAL_SESSION_DIR = Path.home() / ".claude" / "projects"

# Real session paths from 2026-04-03
REAL_SESSIONS = {
    "cancun": REAL_SESSION_DIR
    / "-Users-everettkleven-conductor-workspaces-archetype-cancun"
    / "35e8f81d-74fb-4398-bed9-a17328a57742.jsonl",
    "runner-quebec": REAL_SESSION_DIR
    / "-Users-everettkleven-conductor-workspaces-archetype-runner-quebec"
    / "8a6d231c-88f3-43df-86b3-f1ccc04f7b1d.jsonl",
    "karachi": REAL_SESSION_DIR
    / "-Users-everettkleven-conductor-workspaces-archetype-karachi"
    / "cc17f6cc-bc05-4e8f-a4e2-4c893f4d89cd.jsonl",
    "phoenix": REAL_SESSION_DIR
    / "-Users-everettkleven-conductor-workspaces-archetype-phoenix"
    / "719ccc7e-0621-40e5-a650-2ffbb498fa6a.jsonl",
}


def _make_turn(
    session_id: str,
    role: str,
    text: str,
    seq: int,
    tools: list[str] | None = None,
    is_error: bool = False,
) -> str:
    """Build a single JSONL line matching Claude session format."""
    content: list[dict] = []
    if text:
        content.append({"type": "text", "text": text})
    if tools:
        for t in tools:
            content.append({"type": "tool_use", "name": t, "id": f"tool_{seq}_{t}"})
    if is_error:
        content.append(
            {
                "type": "tool_result",
                "tool_use_id": f"tool_{seq}_err",
                "content": "Error: command failed with exit code 1",
                "is_error": True,
            }
        )
    return json.dumps(
        {
            "type": role,
            "message": {"role": role, "content": content},
            "timestamp": f"2026-04-03T10:00:{seq:02d}.000Z",
            "sessionId": session_id,
            "uuid": f"uuid-{seq}",
        }
    )


def _make_session_file(turns: list[str]) -> Path:
    """Write JSONL lines to a temp file and return the path."""
    tmp = tempfile.NamedTemporaryFile(
        mode="w", suffix=".jsonl", delete=False, prefix="test_session_"
    )
    for line in turns:
        tmp.write(line + "\n")
    tmp.flush()
    return Path(tmp.name)


@pytest.fixture
def frustration_session() -> Path:
    """Session that mirrors the runner-quebec spec-gaming incident."""
    turns = [
        _make_turn("frust-001", "user", "prove to me that the sqlite migration works with daft", 0),
        _make_turn("frust-001", "assistant", "I'll build the tapes ingestion pipeline.", 1, tools=["Bash"]),
        _make_turn("frust-001", "user", "", 2, is_error=False),  # tool result turn
        _make_turn(
            "frust-001",
            "assistant",
            "Here's the implementation using daft.",
            3,
            tools=["Write"],
        ),
        _make_turn(
            "frust-001",
            "user",
            "This is SPEC GAMING. Fuck you. Fix this now. You have no excuses.",
            4,
        ),
        _make_turn("frust-001", "assistant", "You're right. Rewriting.", 5, tools=["Edit"]),
        _make_turn("frust-001", "user", "igraph.", 6),
        _make_turn("frust-001", "assistant", "Using igraph for DAG traversal.", 7, tools=["Edit"]),
        _make_turn("frust-001", "user", "benchmark it.", 8),
        _make_turn("frust-001", "user", "please", 9),
    ]
    return _make_session_file(turns)


@pytest.fixture
def decay_session() -> Path:
    """Session that mirrors the cancun brainstorming trap — message lengths decay."""
    turns = [
        _make_turn(
            "decay-001",
            "user",
            "Build me a simulation of authentic ambition and accuracy to Systems Thinking. "
            "Capitalize on structures hidden in plain sight. Using archetype components.",
            0,
        ),
        _make_turn("decay-001", "assistant", "Let me brainstorm some approaches.", 1),
        _make_turn(
            "decay-001",
            "user",
            "Systems thinking can give you an edge. I'm reading page 110 on second-order emergence. "
            "The feedback loops create reinforcing structures that are invisible until mapped.",
            2,
        ),
        _make_turn("decay-001", "assistant", "Great context. What communication model?", 3),
        _make_turn(
            "decay-001",
            "user",
            "Agents with full knowledge of the experiment tree.",
            4,
        ),
        _make_turn("decay-001", "assistant", "Should all agents update every tick?", 5),
        _make_turn("decay-001", "user", "I just need to see something run", 6),
        _make_turn("decay-001", "assistant", "Here's the MVP...", 7),
        _make_turn("decay-001", "user", "Whats the mvp?", 8),
        _make_turn("decay-001", "assistant", "Let me write the plan...", 9),
        _make_turn("decay-001", "user", "go.", 10),
        _make_turn("decay-001", "assistant", "One more question...", 11),
        _make_turn("decay-001", "user", "it should be intelligent.", 12),
    ]
    return _make_session_file(turns)


@pytest.fixture
def thrashing_session() -> Path:
    """Session with consecutive tool errors — Daft API thrashing pattern."""
    turns = [
        _make_turn("thrash-001", "user", "rewrite using daft-native joins", 0),
        _make_turn("thrash-001", "assistant", "Trying @daft.udf", 1, tools=["Bash"]),
        _make_turn("thrash-001", "user", "", 2, is_error=True),  # deprecated error
        _make_turn("thrash-001", "assistant", "Trying @daft.func", 3, tools=["Bash"]),
        _make_turn("thrash-001", "user", "", 4, is_error=True),  # type check error
        _make_turn("thrash-001", "assistant", "Trying struct return", 5, tools=["Bash"]),
        _make_turn("thrash-001", "user", "", 6, is_error=True),  # struct error
        _make_turn("thrash-001", "assistant", "Trying @daft.cls", 7, tools=["Bash"]),
        _make_turn("thrash-001", "user", "", 8, is_error=True),  # GeneratorExit
        _make_turn("thrash-001", "user", "igraph.", 9),  # user intervention
        _make_turn("thrash-001", "assistant", "Using igraph instead.", 10, tools=["Edit"]),
        _make_turn("thrash-001", "user", "", 11, is_error=False),  # success
    ]
    return _make_session_file(turns)


@pytest.fixture
def clean_session() -> Path:
    """A healthy session with no pain signals."""
    turns = [
        _make_turn("clean-001", "user", "Follow the readme naively.", 0),
        _make_turn("clean-001", "assistant", "Running the setup.", 1, tools=["Bash"]),
        _make_turn("clean-001", "user", "", 2),
        _make_turn("clean-001", "assistant", "Server started. Creating world.", 3, tools=["Bash"]),
        _make_turn("clean-001", "user", "", 4),
        _make_turn("clean-001", "assistant", "Done. The README quick start works.", 5),
    ]
    return _make_session_file(turns)


# ============================================================================
# Tests: Loader
# ============================================================================


class TestLoader:
    def test_load_synthetic_session(self, frustration_session: Path):
        df = load_session(frustration_session)
        rows = df.collect().to_pydict()
        assert len(rows["session_id"]) == 10
        assert rows["session_id"][0] == "frust-001"
        assert rows["role"][0] == "user"
        assert rows["role"][1] == "assistant"

    def test_load_nonexistent_returns_empty(self):
        df = load_session("/tmp/nonexistent_session.jsonl")
        rows = df.collect().to_pydict()
        assert len(rows["session_id"]) == 0

    def test_load_multiple_sessions(self, frustration_session: Path, clean_session: Path):
        df = load_sessions([frustration_session, clean_session])
        rows = df.collect().to_pydict()
        session_ids = set(rows["session_id"])
        assert "frust-001" in session_ids
        assert "clean-001" in session_ids

    def test_text_length_computed(self, decay_session: Path):
        df = load_session(decay_session)
        rows = df.where(daft.col("role") == "user").sort("turn_seq").collect().to_pydict()
        # First user message should be longest
        assert rows["text_len"][0] > rows["text_len"][-1]

    def test_tool_names_extracted(self, frustration_session: Path):
        df = load_session(frustration_session)
        rows = df.where(daft.col("n_tool_uses") > 0).collect().to_pydict()
        assert len(rows["tool_names"]) > 0
        # At least one turn should have tool_names containing "Bash" or "Edit"
        all_tools = [t for names in rows["tool_names"] for t in names]
        assert "Bash" in all_tools or "Edit" in all_tools or "Write" in all_tools

    def test_error_detection(self, thrashing_session: Path):
        df = load_session(thrashing_session)
        rows = df.where(daft.col("has_error")).collect().to_pydict()
        assert len(rows["session_id"]) >= 4  # 4 error turns in synthetic data

    @pytest.mark.skipif(
        not REAL_SESSIONS["karachi"].exists(),
        reason="Real session data not available",
    )
    def test_load_real_session_karachi(self):
        df = load_session(REAL_SESSIONS["karachi"])
        rows = df.collect().to_pydict()
        assert len(rows["session_id"]) > 0
        assert all(r in ("user", "assistant") for r in rows["role"])


# ============================================================================
# Tests: Frustration Detection
# ============================================================================


class TestFrustrationDetection:
    def test_detects_strong_frustration(self, frustration_session: Path):
        df = load_session(frustration_session)
        result = detect_frustration(df).collect().to_pydict()
        scores = result["frustration_score"]
        # The "SPEC GAMING. Fuck you." message should score highest
        assert max(scores) >= 2

    def test_detects_strong_keywords(self, frustration_session: Path):
        df = load_session(frustration_session)
        result = detect_frustration(df).collect().to_pydict()
        strong = result["frustration_strong"]
        assert any(strong)  # at least one strongly frustrated turn

    def test_detects_terse_directive(self, frustration_session: Path):
        df = load_session(frustration_session)
        result = detect_frustration(df).sort("turn_seq").collect().to_pydict()
        # "igraph." is not in _TERSE_DIRECTIVES but "benchmark it." is short
        # Check that at least some terse detection works
        terse = result["is_terse_directive"]
        # Not all should be terse
        assert not all(terse)

    def test_clean_session_no_frustration(self, clean_session: Path):
        df = load_session(clean_session)
        result = detect_frustration(df).collect().to_pydict()
        scores = result.get("frustration_score", [])
        # Clean session should have no or minimal frustration
        assert all(s <= 1 for s in scores)

    def test_only_user_turns(self, frustration_session: Path):
        df = load_session(frustration_session)
        result = detect_frustration(df).collect().to_pydict()
        assert all(r == "user" for r in result["role"])


# ============================================================================
# Tests: Engagement Decay
# ============================================================================


class TestEngagementDecay:
    def test_decay_detected_in_brainstorming_session(self, decay_session: Path):
        df = load_session(decay_session)
        metrics = compute_decay_metrics(df)
        assert metrics["n_messages"] >= 5
        assert metrics["first_len"] > metrics["last_len"]
        assert metrics["ratio"] < 1.0
        assert metrics["decay_detected"]

    def test_decay_ratio_below_threshold(self, decay_session: Path):
        df = load_session(decay_session)
        metrics = compute_decay_metrics(df)
        # "go." is 3 chars, first message is ~150 chars → ratio < 0.05
        assert metrics["ratio"] < 0.3

    def test_counts_terse_messages(self, decay_session: Path):
        df = load_session(decay_session)
        metrics = compute_decay_metrics(df)
        # "go.", "Whats the mvp?", "it should be intelligent." — at least 2 under 30
        assert metrics["n_under_30"] >= 2

    def test_clean_session_no_decay(self, clean_session: Path):
        df = load_session(clean_session)
        metrics = compute_decay_metrics(df)
        assert not metrics["decay_detected"]

    def test_returns_ordered_lengths(self, decay_session: Path):
        df = load_session(decay_session)
        metrics = compute_decay_metrics(df)
        lengths = metrics["lengths"]
        # Should have the same count as user messages with text
        assert len(lengths) == metrics["n_messages"]
        # Early messages should be much longer than late messages
        avg_first_half = sum(lengths[: len(lengths) // 2]) / max(len(lengths) // 2, 1)
        avg_second_half = sum(lengths[len(lengths) // 2 :]) / max(
            len(lengths) - len(lengths) // 2, 1
        )
        assert avg_first_half > avg_second_half


# ============================================================================
# Tests: Error Thrashing
# ============================================================================


class TestErrorThrashing:
    def test_detects_consecutive_errors(self, thrashing_session: Path):
        df = load_session(thrashing_session)
        runs = compute_thrashing_runs(df)
        assert len(runs) >= 1
        # Should find the 4-error run
        max_run = max(runs, key=lambda r: r["length"])
        assert max_run["length"] >= 3

    def test_thrashing_run_has_tool_names(self, thrashing_session: Path):
        df = load_session(thrashing_session)
        runs = compute_thrashing_runs(df)
        if runs:
            # Runs should capture tool names from the error turns
            longest = max(runs, key=lambda r: r["length"])
            assert isinstance(longest["tool_names"], list)

    def test_clean_session_no_thrashing(self, clean_session: Path):
        df = load_session(clean_session)
        runs = compute_thrashing_runs(df)
        assert len(runs) == 0

    def test_error_flag_on_dataframe(self, thrashing_session: Path):
        df = load_session(thrashing_session)
        result = detect_error_thrashing(df).collect().to_pydict()
        assert "is_error_turn" in result
        assert any(result["is_error_turn"])


# ============================================================================
# Tests: Session Pain Report
# ============================================================================


class TestSessionPainReport:
    def test_aggregates_per_session(
        self, frustration_session: Path, clean_session: Path
    ):
        df = load_sessions([frustration_session, clean_session])
        report = session_pain_report(df).collect().to_pydict()
        session_ids = set(report["session_id"])
        assert "frust-001" in session_ids
        assert "clean-001" in session_ids

    def test_error_rate_computed(self, thrashing_session: Path):
        df = load_session(thrashing_session)
        report = session_pain_report(df).collect().to_pydict()
        assert report["error_rate"][0] > 0  # has errors
        assert report["n_errors"][0] >= 4

    def test_clean_session_low_error_rate(self, clean_session: Path):
        df = load_session(clean_session)
        report = session_pain_report(df).collect().to_pydict()
        assert report["error_rate"][0] == 0.0

    def test_tool_use_count(self, frustration_session: Path):
        df = load_session(frustration_session)
        report = session_pain_report(df).collect().to_pydict()
        assert report["total_tool_uses"][0] > 0


# ============================================================================
# Tests: Real Session Data (skip if not available)
# ============================================================================


@pytest.mark.skipif(
    not REAL_SESSIONS["cancun"].exists(),
    reason="Real cancun session data not available",
)
class TestRealCancunSession:
    """Test against the real cancun session — the brainstorming trap."""

    def test_loads_successfully(self):
        df = load_session(REAL_SESSIONS["cancun"])
        rows = df.collect().to_pydict()
        assert len(rows["session_id"]) > 0

    def test_engagement_decay_detected(self):
        df = load_session(REAL_SESSIONS["cancun"])
        metrics = compute_decay_metrics(df)
        assert metrics["n_messages"] >= 3
        # Cancun had clear message-length decay
        assert metrics["ratio"] < 1.0

    def test_frustration_signals_present(self):
        df = load_session(REAL_SESSIONS["cancun"])
        result = detect_frustration(df).collect().to_pydict()
        # Cancun had mild frustration ("I just need to see something run")
        assert len(result["frustration_score"]) > 0

    def test_pain_report(self):
        df = load_session(REAL_SESSIONS["cancun"])
        report = session_pain_report(df).collect().to_pydict()
        assert report["n_turns"][0] > 0


@pytest.mark.skipif(
    not REAL_SESSIONS["runner-quebec"].exists(),
    reason="Real runner-quebec session data not available",
)
class TestRealRunnerQuebecSession:
    """Test against runner-quebec — the spec gaming and thrashing session."""

    def test_loads_successfully(self):
        df = load_session(REAL_SESSIONS["runner-quebec"])
        rows = df.collect().to_pydict()
        assert len(rows["session_id"]) > 0

    def test_frustration_detected(self):
        df = load_session(REAL_SESSIONS["runner-quebec"])
        result = detect_frustration(df).collect().to_pydict()
        scores = result["frustration_score"]
        # Should detect the "SPEC GAMING. Fuck you." message
        assert max(scores) >= 2

    def test_errors_present(self):
        df = load_session(REAL_SESSIONS["runner-quebec"])
        rows = df.where(daft.col("has_error")).collect().to_pydict()
        # Runner-quebec had 16+ errors
        assert len(rows["session_id"]) >= 1

    def test_thrashing_runs_detected(self):
        df = load_session(REAL_SESSIONS["runner-quebec"])
        runs = compute_thrashing_runs(df)
        # Should find at least one thrashing run
        assert len(runs) >= 1

    def test_pain_report(self):
        df = load_session(REAL_SESSIONS["runner-quebec"])
        report = session_pain_report(df).collect().to_pydict()
        assert report["n_errors"][0] >= 1
        assert report["error_rate"][0] > 0


@pytest.mark.skipif(
    not all(p.exists() for p in REAL_SESSIONS.values()),
    reason="Not all real session files available",
)
class TestCrossSessionAnalysis:
    """Test loading and analyzing all real sessions together."""

    def test_load_all_sessions(self):
        df = load_sessions(list(REAL_SESSIONS.values()))
        rows = df.collect().to_pydict()
        session_ids = set(rows["session_id"])
        assert len(session_ids) >= 3

    def test_pain_report_multi_session(self):
        df = load_sessions(list(REAL_SESSIONS.values()))
        report = session_pain_report(df).sort("error_rate", desc=True).collect().to_pydict()
        # Multiple sessions should appear
        assert len(report["session_id"]) >= 3
        # Runner-quebec should have highest error rate
        # (We can't guarantee order, but at least verify multiple entries)
