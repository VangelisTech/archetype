# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Secret-safe diagnostics for paid coding-agent release evidence."""

from __future__ import annotations

import hashlib
from collections.abc import Iterable

_CLASSIFICATION_MARKERS = (
    (
        "provider_configuration",
        (
            "configured namespace",
            "environment identity does not match",
            "namespace lookup failed",
            "workspace identity does not match",
        ),
    ),
    (
        "authentication",
        (
            "auth.json",
            "authentication",
            "credential",
            "not logged in",
            "oauth",
            "refresh token",
            "unauthorized",
        ),
    ),
    ("rate_limit", ("quota", "rate limit", "usage limit")),
    ("timeout", ("timed out", "timeout")),
    (
        "transport",
        (
            "connection refused",
            "connection reset",
            "dns",
            "network",
            "service unavailable",
        ),
    ),
)
_KNOWN_STATUSES = frozenset({"errored", "exited", "interrupted"})


def bounded_text_summary(value: str, *, allowlisted_markers: Iterable[str] = ()) -> str:
    """Return an allowlisted marker or only the length and digest of arbitrary text."""

    stripped = value.strip()
    markers = frozenset(allowlisted_markers)
    if stripped in markers:
        return stripped
    digest = hashlib.sha256(value.encode()).hexdigest()[:16]
    return f"unrecognized(length={len(value)},sha256={digest})"


def classify_agent_failure(*values: str) -> str:
    """Map private provider output to one bounded operational category."""

    normalized = "\n".join(values).lower()
    for classification, markers in _CLASSIFICATION_MARKERS:
        if any(marker in normalized for marker in markers):
            return classification
    return "execution"


def summarize_agent_failure(
    *,
    status: str,
    returncode: int,
    stdout: str = "",
    stderr: str = "",
    error: str = "",
    friction_messages: Iterable[str] = (),
    allowlisted_stdout: Iterable[str] = (),
) -> str:
    """Summarize one failure without returning raw agent or provider output."""

    friction = tuple(friction_messages)
    classification = classify_agent_failure(stderr, error, *friction)
    safe_status = status if status in _KNOWN_STATUSES else "unknown"
    return " ".join(
        (
            f"classification={classification}",
            f"status={safe_status}",
            f"returncode={returncode}",
            f"stdout={bounded_text_summary(stdout, allowlisted_markers=allowlisted_stdout)}",
            f"stderr={bounded_text_summary(stderr)}",
            f"error={bounded_text_summary(error)}",
            f"friction_count={len(friction)}",
        )
    )


__all__ = [
    "bounded_text_summary",
    "classify_agent_failure",
    "summarize_agent_failure",
]
