# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Contracts for secret-safe coding-agent release diagnostics."""

from __future__ import annotations

from scripts.release_agent_diagnostics import (
    bounded_text_summary,
    classify_agent_failure,
    summarize_agent_failure,
)


def test_bounded_text_summary_emits_only_exact_allowlisted_markers() -> None:
    marker = "ARCHETYPE_MODAL_AUTH_OK"

    assert bounded_text_summary(marker, allowlisted_markers=(marker,)) == marker
    summary = bounded_text_summary(f"token=secret {marker}", allowlisted_markers=(marker,))

    assert summary.startswith("unrecognized(length=")
    assert "secret" not in summary
    assert marker not in summary


def test_failure_classification_is_bounded() -> None:
    assert classify_agent_failure("OAuth refresh token expired") == "authentication"
    assert classify_agent_failure("workspace identity does not match") == ("provider_configuration")
    assert classify_agent_failure("request timed out") == "timeout"
    assert classify_agent_failure("unexpected exit") == "execution"


def test_failure_summary_does_not_disclose_provider_output() -> None:
    secret = "ghp_do-not-print"
    summary = summarize_agent_failure(
        status="exited",
        returncode=1,
        stdout=f"stdout {secret}",
        stderr=f"Unauthorized credential {secret}",
        error=f"OAuth failed for {secret}",
        friction_messages=(f"refresh token rejected: {secret}",),
    )

    assert "classification=authentication" in summary
    assert "status=exited" in summary
    assert "returncode=1" in summary
    assert "friction_count=1" in summary
    assert secret not in summary
    assert "Unauthorized" not in summary
