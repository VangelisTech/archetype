# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Contracts for the deliberately small pull-request verification profile."""

from __future__ import annotations

import re
from pathlib import Path

from scripts.run_operational_scenarios import _select_scenarios
from scripts.validate_operational_scenarios import load_scenarios

ROOT = Path(__file__).resolve().parents[2]
QUALITY_WORKFLOW = ROOT / ".github" / "workflows" / "python-tests.yml"
RELEASE_WORKFLOW = ROOT / ".github" / "workflows" / "release.yml"
MAKEFILE = ROOT / "Makefile"
QUARANTINE = ROOT / "quality" / "quarantine" / "review-gate"


def _job(workflow: str, job_id: str) -> str:
    match = re.search(
        rf"^  {re.escape(job_id)}:\n(?P<body>.*?)(?=^  [a-z][a-z0-9-]*:\n|\Z)",
        workflow,
        re.MULTILINE | re.DOTALL,
    )
    assert match is not None, f"workflow lost the {job_id!r} job"
    return match.group("body")


def test_pull_request_workflow_has_only_two_jobs() -> None:
    workflow = QUALITY_WORKFLOW.read_text(encoding="utf-8")
    _, _, jobs = workflow.partition("\njobs:\n")

    assert re.findall(r"^  ([a-z][a-z0-9-]*):$", jobs, re.MULTILINE) == [
        "static",
        "tests",
    ]
    assert "merge_group:" not in workflow
    assert "make static" in _job(workflow, "static")
    assert "make test" in _job(workflow, "tests")
    assert "make test-cov" not in workflow
    assert "R2_" not in workflow
    assert "codecov" not in workflow.lower()


def test_local_pr_profile_matches_the_two_ci_jobs() -> None:
    makefile = MAKEFILE.read_text(encoding="utf-8")
    verify_pr = re.search(r"^verify-pr:(?P<dependencies>[^\n]*)$", makefile, re.MULTILINE)
    verify_full = re.search(r"^verify-full:(?P<dependencies>[^\n]*)$", makefile, re.MULTILINE)

    assert verify_pr is not None
    assert verify_pr.group("dependencies").split() == ["static", "test"]
    assert verify_full is not None
    full = verify_full.group("dependencies").split()
    for target in (
        "test-cov",
        "eval-conformance",
        "eval-reliability",
        "eval-capability",
        "package-smoke",
        "examples-smoke",
        "operational-runtime",
        "operational-commands",
        "operational-wheel",
        "docs",
        "test-process",
    ):
        assert target in full


def test_review_gate_and_merge_queue_are_not_executable_workflows() -> None:
    active = ROOT / ".github" / "workflows"
    for name in (
        "deterministic-review.yml",
        "automerge.yml",
        "queue-reevaluator.yml",
        "merge-group-recheck.yml",
    ):
        assert not (active / name).exists()
        assert (QUARANTINE / "workflows" / name).is_file()


def test_release_publish_requires_credentialed_r2_evidence() -> None:
    workflow = RELEASE_WORKFLOW.read_text(encoding="utf-8")
    r2_release = _job(workflow, "r2-release")
    publish = _job(workflow, "publish")

    for variable in (
        "R2_ACCESS_KEY_ID",
        "R2_SECRET_ACCESS_KEY",
        "R2_API_ENDPOINT",
        "R2_BUCKET",
    ):
        assert variable in r2_release
    assert "tests/infrastructure/test_r2_idempotency.py" in r2_release
    assert "--min-tier 5" in r2_release
    assert "--max-tier 5" in r2_release
    assert "--scenario dogfood.storage.r2" in r2_release
    assert "--cadence release" in r2_release
    assert "--require-run" in r2_release
    assert "r2-release-results.json" in r2_release
    assert "needs: [release-profile, python-compatibility, r2-release]" in publish

    selected = _select_scenarios(
        load_scenarios(),
        mode="source",
        cadence="release",
        scenario_ids={"dogfood.storage.r2"},
        kind=None,
        min_tier=5,
        max_tier=5,
    )
    assert [row["id"] for row in selected] == ["dogfood.storage.r2"]
