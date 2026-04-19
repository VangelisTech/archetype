# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Structural analysis patterns for session transcripts.

All patterns are pure Daft operations — no LLM calls. They detect:
  - Frustration signals (profanity, terse directives, "go.", "fix this")
  - Engagement decay (message-length slope across user turns)
  - Error thrashing (consecutive tool failures on the same tool)
  - Session-level pain metrics (time-to-value, abandonment, retries)
"""

from __future__ import annotations

import daft
from daft import DataFrame, col, lit

# ============================================================================
# Pattern 1: Frustration Detection
# ============================================================================

# Keywords that signal frustration — weighted by severity
_FRUSTRATION_STRONG = [
    "fuck",
    "spec gaming",
    "fix this now",
    "no excuses",
    "wrong",
    "broken",
    "terrible",
    "useless",
]
_FRUSTRATION_MILD = [
    "hold on",
    "wait",
    "not that",
    "don't",
    "stop",
    "yikes",
    "ugh",
    "sigh",
    "come on",
    "seriously",
]
_TERSE_DIRECTIVES = [
    "go.",
    "do it.",
    "proceed",
    "just do it",
    "ship it",
    "fix it",
]


def detect_frustration(df: DataFrame) -> DataFrame:
    """Classify each user turn by frustration level.

    Adds columns:
        frustration_strong: bool — contains strong frustration keywords
        frustration_mild: bool — contains mild frustration keywords
        is_terse_directive: bool — message is a short directive (< 30 chars)
        frustration_score: int — 0 (none), 1 (mild), 2 (strong), 3 (strong + terse)
    """
    user_df = df.where(col("role") == "user")
    text_lower = col("text").lower()

    # Strong frustration: profanity or explicit anger
    strong_expr = lit(False)
    for kw in _FRUSTRATION_STRONG:
        strong_expr = strong_expr | text_lower.contains(kw)

    # Mild frustration: corrections, redirections
    mild_expr = lit(False)
    for kw in _FRUSTRATION_MILD:
        mild_expr = mild_expr | text_lower.contains(kw)

    # Terse directives: very short messages that are commands
    terse_expr = col("text_len") < lit(30)
    directive_expr = lit(False)
    for kw in _TERSE_DIRECTIVES:
        directive_expr = directive_expr | text_lower.contains(kw)
    is_terse = terse_expr & directive_expr

    return (
        user_df.with_column("frustration_strong", strong_expr)
        .with_column("frustration_mild", mild_expr)
        .with_column("is_terse_directive", is_terse)
        .with_column(
            "frustration_score",
            col("frustration_strong").cast(daft.DataType.int64()) * lit(2)
            + col("frustration_mild").cast(daft.DataType.int64())
            + col("is_terse_directive").cast(daft.DataType.int64()),
        )
    )


# ============================================================================
# Pattern 2: Engagement Decay
# ============================================================================


def detect_engagement_decay(df: DataFrame) -> DataFrame:
    """Track user message-length over consecutive turns.

    Adds columns:
        msg_rank: int — sequential rank among user messages (0-indexed)
        text_len: int — already present, character count
        len_delta: int — change from previous user message length

    Returns only user turns, sorted by turn_seq.
    """
    user_df = (
        df.where(col("is_user_authored"))
        .where(col("text_len") > 0)
        .sort("turn_seq")
        .with_column("msg_rank", col("turn_seq"))  # preserve ordering
    )
    return user_df


def compute_decay_metrics(df: DataFrame) -> dict:
    """Compute engagement decay summary from a user-turns DataFrame.

    Returns dict with:
        n_messages: int
        lengths: list[int] — message lengths in order
        first_len: int
        last_len: int
        ratio: float — last_len / first_len (< 1.0 means decay)
        n_under_30: int — messages under 30 chars
        decay_detected: bool — ratio < 0.3 and n_messages >= 3
    """
    rows = (
        df.where(col("is_user_authored"))
        .where(col("text_len") > 0)
        .sort("turn_seq")
        .select("text_len")
        .collect()
        .to_pydict()
    )  # justified .collect(): need full ordered sequence for slope calculation
    lengths = rows.get("text_len", [])
    if len(lengths) < 2:
        return {
            "n_messages": len(lengths),
            "lengths": lengths,
            "first_len": lengths[0] if lengths else 0,
            "last_len": lengths[-1] if lengths else 0,
            "ratio": 1.0,
            "n_under_30": sum(1 for n in lengths if n < 30),
            "decay_detected": False,
        }

    first_len = lengths[0]
    last_len = lengths[-1]
    ratio = last_len / first_len if first_len > 0 else 1.0
    n_under_30 = sum(1 for n in lengths if n < 30)

    return {
        "n_messages": len(lengths),
        "lengths": lengths,
        "first_len": first_len,
        "last_len": last_len,
        "ratio": round(ratio, 3),
        "n_under_30": n_under_30,
        "decay_detected": ratio < 0.3 and len(lengths) >= 3,
    }


# ============================================================================
# Pattern 3: Error Thrashing Detection
# ============================================================================


def detect_error_thrashing(df: DataFrame) -> DataFrame:
    """Find consecutive error turns and identify thrashing sequences.

    Adds columns:
        is_error_turn: bool — this turn had an error
        consecutive_errors: int — count of sequential errors ending at this turn

    An error turn is any assistant turn where tool results contain errors,
    OR any user turn whose tool_result blocks have is_error=True.
    """
    # Mark error turns
    error_df = df.with_column("is_error_turn", col("has_error"))
    return error_df


def compute_thrashing_runs(df: DataFrame) -> list[dict]:
    """Compute runs of consecutive errors from a turn-level DataFrame.

    Returns list of thrashing episodes:
        [{"start_seq": int, "end_seq": int, "length": int, "tool_names": list[str]}]
    """
    rows = (
        df.sort("turn_seq")
        .select("turn_seq", "has_error", "tool_names", "role")
        .collect()
        .to_pydict()
    )  # justified .collect(): need sequential scan for run-length encoding

    seqs = rows.get("turn_seq", [])
    errors = rows.get("has_error", [])
    tools = rows.get("tool_names", [])
    roles = rows.get("role", [])

    runs = []

    # Count consecutive error turns (an error turn is any turn where has_error=True,
    # regardless of role — tool results arrive as user turns in the JSONL format)
    error_count = 0
    run_start_seq = None
    run_tools: list[str] = []

    for i, (seq, is_err, tool_list, role) in enumerate(
        zip(seqs, errors, tools, roles, strict=True)
    ):
        if is_err:
            if error_count == 0:
                run_start_seq = seq
                run_tools = []
            error_count += 1
            if isinstance(tool_list, list):
                run_tools.extend(tool_list)
        elif role == "user" and not is_err and error_count > 0:
            # Non-error user turn after errors — could be user intervention
            # Close the run
            if error_count >= 2:
                runs.append(
                    {
                        "start_seq": run_start_seq,
                        "end_seq": seqs[i - 1],
                        "length": error_count,
                        "tool_names": run_tools,
                    }
                )
            error_count = 0
            run_start_seq = None
            run_tools = []
        elif role == "assistant":
            # Assistant turns between errors are part of the retry cycle —
            # don't break the run. Capture tool names from retry attempts.
            if isinstance(tool_list, list):
                run_tools.extend(tool_list)

    # Close any open run
    if error_count >= 2 and run_start_seq is not None:
        runs.append(
            {
                "start_seq": run_start_seq,
                "end_seq": seqs[-1] if seqs else 0,
                "length": error_count,
                "tool_names": run_tools,
            }
        )

    return runs


# ============================================================================
# Pattern 4: Session Pain Report (Aggregation)
# ============================================================================


def session_pain_report(df: DataFrame) -> DataFrame:
    """Aggregate per-session pain metrics.

    Groups by session_id and computes:
        n_turns: int — total turns
        n_user_turns: int — user turns only
        n_errors: int — turns with errors
        n_tool_uses: int — total tool invocations
        max_text_len: int — longest user message
        min_text_len: int — shortest non-empty user message
        error_rate: float — n_errors / n_turns
    """
    return (
        df.groupby("session_id")
        .agg(
            col("turn_seq").count().alias("n_turns"),
            col("has_error")
            .cast(daft.DataType.int64())
            .sum()
            .alias("n_errors"),
            col("n_tool_uses").sum().alias("total_tool_uses"),
            col("text_len").max().alias("max_text_len"),
            col("text_len").min().alias("min_text_len"),
        )
        .with_column(
            "error_rate",
            col("n_errors").cast(daft.DataType.float64())
            / col("n_turns").cast(daft.DataType.float64()),
        )
    )
