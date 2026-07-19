# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Contracts for required quality contexts and review-gated auto-merge."""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
QUALITY_WORKFLOW = ROOT / ".github" / "workflows" / "python-tests.yml"
AUTOMERGE_WORKFLOW = ROOT / ".github" / "workflows" / "automerge.yml"
MAKEFILE = ROOT / "Makefile"


def _job(workflow: str, job_id: str) -> str:
    match = re.search(
        rf"^  {re.escape(job_id)}:\n(?P<body>.*?)(?=^  [a-z][a-z0-9-]*:\n|\Z)",
        workflow,
        re.MULTILINE | re.DOTALL,
    )
    assert match is not None, f"workflow lost the {job_id!r} job"
    return match.group("body")


def test_quality_workflow_preserves_active_required_context_names() -> None:
    workflow = QUALITY_WORKFLOW.read_text(encoding="utf-8")

    assert "  merge_group:\n" in workflow
    ci = _job(workflow, "ci")
    assert ci.startswith("    runs-on:")
    assert 'python-version: ["3.12", "3.13"]' in ci
    for job_id in ("evals", "format", "typecheck", "examples"):
        assert _job(workflow, job_id).startswith(("    if:", "    runs-on:"))


def test_quality_workflow_keeps_fail_loud_coverage_and_infrastructure() -> None:
    workflow = QUALITY_WORKFLOW.read_text(encoding="utf-8")
    ci = _job(workflow, "ci")
    evals = _job(workflow, "evals")

    assert "uses: codecov/codecov-action@v5" in ci
    assert "fail_ci_if_error: true" in ci
    assert "github.event_name != 'merge_group'" in ci
    assert "if: always()" in evals
    assert "needs: infrastructure-idempotency" in evals
    assert 'case "$INFRASTRUCTURE" in success|skipped) ;; *) exit 1 ;; esac' in evals
    assert "make eval-conformance" in evals
    assert "make eval-reliability" in evals
    assert "make eval-capability" in evals
    assert "make package-smoke" in evals


def test_observability_audit_uses_the_existing_required_format_context() -> None:
    workflow = QUALITY_WORKFLOW.read_text(encoding="utf-8")
    makefile = MAKEFILE.read_text(encoding="utf-8")
    format_job = _job(workflow, "format")

    assert "- run: make lint" in format_job
    lint_target = re.search(r"^lint:(?P<dependencies>[^\n]*)$", makefile, re.MULTILINE)
    assert lint_target is not None
    assert "observability-audit" in lint_target.group("dependencies").split()
    assert re.search(
        r"^observability-audit:\n\t@uv run python scripts/check_observability\.py$",
        makefile,
        re.MULTILINE,
    )


def test_quality_gate_aggregates_every_applicable_job() -> None:
    gate = _job(QUALITY_WORKFLOW.read_text(encoding="utf-8"), "quality-gate")

    assert "if: always()" in gate
    for job_id in (
        "ci",
        "format",
        "typecheck",
        "infrastructure-idempotency",
        "evals",
        "examples",
        "process-reliability",
    ):
        assert f"      - {job_id}\n" in gate


def test_automerge_remains_bound_to_latest_reviewed_head() -> None:
    workflow = AUTOMERGE_WORKFLOW.read_text(encoding="utf-8")

    assert (
        "types: [opened, synchronize, reopened, ready_for_review, auto_merge_enabled]" in workflow
    )
    assert 'workflows: ["Deterministic Review Gate"]' in workflow
    assert "github.event.workflow_run.conclusion == 'success'" in workflow
    assert "github.event.workflow_run.head_repository.full_name == github.repository" in workflow
    assert "sort_by(.started_at) | last | .conclusion // empty" in workflow
    assert r".headRefOid == \"${HEAD_SHA}\"" in workflow
    assert ".isDraft | not" in workflow
    assert 'gh pr merge --auto --squash --repo "${GITHUB_REPOSITORY}" "$pr"' in workflow
