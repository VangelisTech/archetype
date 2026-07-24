# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Contracts for required quality contexts and review-gated auto-merge."""

from __future__ import annotations

import ast
import json
import re
import subprocess
import tomllib
from pathlib import Path

import pytest

from scripts.run_operational_scenarios import run_scenarios

ROOT = Path(__file__).resolve().parents[2]
QUALITY_WORKFLOW = ROOT / ".github" / "workflows" / "python-tests.yml"
AUTOMERGE_WORKFLOW = ROOT / ".github" / "workflows" / "automerge.yml"
MAKEFILE = ROOT / "Makefile"
OPERATIONAL_SCENARIOS = ROOT / "quality" / "operational_scenarios.toml"


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
    assert "make operational-wheel" in evals
    assert "operational-results.json" in evals
    assert re.search(r"- run: make operational-wheel\n\s+if: always\(\)", evals)
    assert evals.index("make operational-wheel") < evals.index(
        "Require applicable infrastructure evidence"
    )
    assert re.search(
        r"- name: Require applicable infrastructure evidence\n\s+if: always\(\)",
        evals,
    )


def test_codecov_statuses_restate_the_repository_coverage_floor() -> None:
    """Codecov judges diffs against the 70% floor, not the moving project average.

    The default `target: auto` pinned `codecov/patch` to whole-project coverage
    (88.7%), so ordinary refactors failed a non-required check and cost triage
    (#646, #647, #649). Both statuses now restate `fail_under`, and this test
    keeps the two numbers from drifting apart.
    """

    config = (ROOT / ".github" / "codecov.yml").read_text(encoding="utf-8")
    with (ROOT / "pyproject.toml").open("rb") as stream:
        floor = tomllib.load(stream)["tool"]["coverage"]["report"]["fail_under"]

    assert re.search(r"^    project:\n      default:\n        target: ", config, re.MULTILINE)
    assert re.search(r"^    patch:\n      default:\n        target: ", config, re.MULTILINE)
    assert config.count(f"target: {floor}%") == 2
    assert config.count("threshold: 0%") == 2
    assert config.count("informational: false") == 2


def test_r2_job_runs_each_oracle_once_and_retains_the_redacted_receipt() -> None:
    workflow = QUALITY_WORKFLOW.read_text(encoding="utf-8")
    infrastructure = _job(workflow, "infrastructure-idempotency")
    with OPERATIONAL_SCENARIOS.open("rb") as stream:
        scenarios = tomllib.load(stream)["scenario"]
    scenario = next(row for row in scenarios if row["id"] == "dogfood.storage.r2")

    assert scenario["source_command"] == [
        "pytest",
        "-q",
        "tests/infrastructure/test_r2_artifact_context.py",
    ]
    assert scenario["semantic_oracle"] == {
        "kind": "pytest",
        "ref": "tests/infrastructure/test_r2_artifact_context.py",
    }
    assert scenario["required_cadence"] == ["pr", "main", "release"]
    assert scenario["artifact_policy"] == "redacted_receipt"
    assert scenario["contracts"] == [
        "runtime.trust.actor_free",
        "world.fork.lineage",
        "world.run_identity.cold_resume",
        "ingestion.catalog.cold_roundtrip",
        "artifacts.ingestion.occurrence_identity",
        "artifacts.ingestion.common_visibility",
    ]

    assert "make test-infra" not in infrastructure
    assert infrastructure.count("tests/infrastructure/test_r2_idempotency.py") == 1
    assert infrastructure.count("--scenario dogfood.storage.r2") == 1
    assert "--cadence pr" in infrastructure
    assert "--require-run" in infrastructure
    assert '--expected-revision "$GITHUB_SHA"' in infrastructure
    assert "--require-clean" in infrastructure
    assert re.search(
        r"- name: Run public-runtime R2 scenario\n\s+if: always\(\)",
        infrastructure,
    )
    assert re.search(
        r"- name: Require R2 operational receipt\n"
        r"\s+if: always\(\)\n"
        r"\s+run: test -f r2-operational-results\.json",
        infrastructure,
    )
    assert re.search(
        r"name: r2-operational-evidence\n"
        r"\s+path: \|\n"
        r"\s+r2-operational-results\.json\n"
        r"\s+r2-operational-results\.d/\n"
        r"\s+if-no-files-found: error",
        infrastructure,
    )


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


def test_example_smoke_keeps_the_coding_agent_authoring_check_credential_free() -> None:
    makefile = MAKEFILE.read_text(encoding="utf-8")
    assert re.search(r"^examples-smoke: examples-local$", makefile, re.MULTILINE)
    assert "--mode source --cadence pr --kind example --max-tier 1" in makefile

    with OPERATIONAL_SCENARIOS.open("rb") as stream:
        scenarios = tomllib.load(stream)["scenario"]
    mission = next(
        row for row in scenarios if row["id"] == "example.11_coding_agent_mission.dry_run"
    )

    assert mission["source_command"][-3:] == ["--dry-run", "--backend", "docker"]
    assert mission["prerequisites"] == []
    assert mission["missing_prerequisite"] == "fail"
    assert mission["tier"] == 1
    assert "pr" in mission["required_cadence"]


def test_commands_operational_receipt_is_required_from_source_and_wheel() -> None:
    makefile = MAKEFILE.read_text(encoding="utf-8")
    with OPERATIONAL_SCENARIOS.open("rb") as stream:
        scenarios = tomllib.load(stream)["scenario"]
    commands = next(row for row in scenarios if row["id"] == "dogfood.commands.local")

    assert commands["owner"] == "commands"
    assert commands["source_command"] == [
        "pytest",
        "-q",
        "tests/integration/test_commands_operational.py",
    ]
    assert commands["semantic_oracle"] == {
        "kind": "pytest",
        "ref": "tests/integration/test_commands_operational.py",
    }
    assert commands["tier"] == 1
    assert commands["applicability"] == ["source", "wheel"]
    assert commands["prerequisites"] == []
    assert commands["missing_prerequisite"] == "fail"
    assert commands["contracts"] == [
        "gateway.authorization.rbac",
        "commands.identity.idempotent",
        "commands.settlement.atomic",
        "commands.failure.preserves_progress",
    ]

    source = re.search(
        r"^operational-commands:\n(?P<body>(?:\t.*\n)+)",
        makefile,
        re.MULTILINE,
    )
    wheel = re.search(
        r"^operational-wheel:\n(?P<body>(?:\t.*\n)+)",
        makefile,
        re.MULTILINE,
    )
    verify_pr = re.search(r"^verify-pr:(?P<dependencies>[^\n]*)$", makefile, re.MULTILINE)
    assert source is not None
    assert wheel is not None
    assert verify_pr is not None
    assert source.group("body").count("--scenario dogfood.commands.local") == 1
    assert wheel.group("body").count("--scenario dogfood.commands.local") == 1
    assert "operational-commands" in verify_pr.group("dependencies").split()


def test_runtime_loopback_is_explicitly_required_from_source_and_wheel() -> None:
    makefile = MAKEFILE.read_text(encoding="utf-8")
    source = re.search(
        r"^operational-runtime:\n(?P<body>(?:\t.*\n)+)",
        makefile,
        re.MULTILINE,
    )
    wheel = re.search(
        r"^operational-wheel:\n(?P<body>(?:\t.*\n)+)",
        makefile,
        re.MULTILINE,
    )
    verify_pr = re.search(r"^verify-pr:(?P<dependencies>[^\n]*)$", makefile, re.MULTILINE)

    assert source is not None
    assert wheel is not None
    assert verify_pr is not None
    source_body = source.group("body")
    wheel_body = wheel.group("body")
    assert "--require-run" in source_body
    assert "--require-run" in wheel_body
    assert source_body.count("--scenario dogfood.runtime.loopback") == 1
    assert wheel_body.count("--scenario dogfood.runtime.loopback") == 1
    assert "operational-runtime" in verify_pr.group("dependencies").split()

    packet_scenarios = (
        "example.00_quickstart",
        "example.01_world_mutations",
        "example.02_fork_counterfactual",
        "example.03_time_travel",
        "example.10_autoresearch",
        "example.14_physical_ai",
        "dogfood.agent_mission.scripted",
    )
    for scenario_id in packet_scenarios:
        assert wheel_body.count(f"--scenario {scenario_id}") == 1


def test_required_operational_execution_cannot_accept_not_run(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)

    envelope, passed = run_scenarios(
        root=ROOT,
        registry=OPERATIONAL_SCENARIOS,
        output=tmp_path / "required-not-run.json",
        mode="source",
        wheel=None,
        cadence="release",
        scenario_ids={"example.05_llm_agents"},
        kind="example",
        min_tier=6,
        max_tier=6,
        expected_revision=None,
        require_clean=False,
        require_run=True,
    )

    assert passed is False
    assert envelope["outcome"] == "failed"
    assert envelope["status_counts"] == {"passed": 0, "failed": 0, "not_run": 1}


def test_commands_operational_oracle_does_not_import_pytest_modules() -> None:
    """The standalone oracle must not collect a second copy of another test module."""

    path = ROOT / "tests" / "integration" / "test_commands_operational.py"
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    imported_modules = [
        module
        for node in ast.walk(tree)
        for module in (
            [alias.name for alias in node.names]
            if isinstance(node, ast.Import)
            else [node.module or ""]
            if isinstance(node, ast.ImportFrom)
            else []
        )
    ]

    assert not [
        module
        for module in imported_modules
        if any(part.startswith("test_") for part in module.split("."))
    ]


def test_operational_receipts_are_uploaded_even_when_a_scenario_fails() -> None:
    workflow = QUALITY_WORKFLOW.read_text(encoding="utf-8")
    evals = _job(workflow, "evals")
    examples = _job(workflow, "examples")

    assert re.search(
        r"- name: Require installed-wheel operational receipt\n"
        r"\s+if: always\(\)\n"
        r"\s+run: test -f operational-results\.json",
        evals,
    )
    assert re.search(
        r"name: installed-wheel-operational-evidence\n"
        r"\s+path: \|\n"
        r"\s+operational-results\.json\n"
        r"\s+operational-results\.d/\n"
        r"\s+if-no-files-found: error",
        evals,
    )
    pull_request_evidence = evals.split("name: pull-request-evidence", 1)[1]
    assert "operational-results.json" not in pull_request_evidence

    assert re.search(
        r"- name: Require source operational receipt\n"
        r"\s+if: always\(\)\n"
        r"\s+run: test -f operational-source-results\.json",
        examples,
    )
    assert re.search(
        r"name: semantic-example-evidence\n"
        r"\s+path: \|\n"
        r"\s+operational-source-results\.json\n"
        r"\s+operational-source-results\.d/\n"
        r"\s+if-no-files-found: error",
        examples,
    )


def test_operational_wheel_target_routes_build_and_wheel_setup_failures_through_runner(
    tmp_path: Path,
) -> None:
    makefile = MAKEFILE.read_text(encoding="utf-8")
    target = re.search(
        r"^operational-wheel:(?P<dependencies>[^\n]*)\n"
        r"(?P<body>(?:\t.*\n)+)",
        makefile,
        re.MULTILINE,
    )
    assert target is not None
    assert target.group("dependencies").strip() == ""
    body = target.group("body")
    assert "$(OPERATIONAL_BUILD_COMMAND) || build_status=$$?" in body
    assert 'wheel="$(OPERATIONAL_DIST_DIR)/.missing-operational-wheel.whl"' in body
    assert "scripts/run_operational_scenarios.py" in body

    for label, build_command in (("build-failed", "false"), ("wheel-missing", "true")):
        output = tmp_path / f"{label}.json"
        completed = subprocess.run(
            [
                "make",
                "--no-print-directory",
                "operational-wheel",
                f"OPERATIONAL_BUILD_COMMAND={build_command}",
                f"OPERATIONAL_DIST_DIR={tmp_path / label / 'dist'}",
                f"OPERATIONAL_WHEEL_RESULTS={output}",
            ],
            cwd=ROOT,
            check=False,
            capture_output=True,
            text=True,
        )

        assert completed.returncode != 0
        receipt = json.loads(output.read_text(encoding="utf-8"))
        assert receipt["schema"] == "archetype.operational-results/v1"
        assert receipt["outcome"] == "failed"
        assert "--wheel must name one built wheel" in receipt["error"]


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
