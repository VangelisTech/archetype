# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Session trajectory analysis pipeline.

Parses Claude Code session transcripts (JSONL) into Daft DataFrames,
then applies structural analysis patterns to detect frustration signals,
engagement decay, error thrashing, and abandoned tasks.

No LLM calls required — all analysis is structural (Daft expressions only).
"""

from archetype.app.session_analysis.loader import load_session, load_sessions
from archetype.app.session_analysis.patterns import (
    detect_engagement_decay,
    detect_error_thrashing,
    detect_frustration,
    session_pain_report,
)

__all__ = [
    "load_session",
    "load_sessions",
    "detect_frustration",
    "detect_engagement_decay",
    "detect_error_thrashing",
    "session_pain_report",
]
