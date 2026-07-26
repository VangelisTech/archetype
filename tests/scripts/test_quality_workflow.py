# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Contracts for required quality contexts and review-gated auto-merge."""

from __future__ import annotations

import ast
import json
import os
import re
import shutil
import subprocess
import tomllib
from pathlib import Path

import pytest

from scripts.run_operational_scenarios import run_scenarios

ROOT = Path(__file__).resolve().parents[2]
QUALITY_WORKFLOW = ROOT / ".github" / "workflows" / "python-tests.yml"
AUTOMERGE_WORKFLOW = ROOT / ".github" / "workflows" / "automerge.yml"
QUEUE_READY_HELPER = ROOT / "scripts" / "gh_pr_queue_ready.sh"
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


def _job_ids(workflow: str) -> list[str]:
    """Every job in the workflow, so new jobs inherit the checks by default."""
    _, _, jobs = workflow.partition("\njobs:\n")
    assert jobs, "workflow lost its jobs: block"
    ids = re.findall(r"^  ([a-z][a-z0-9-]*):$", jobs, re.MULTILINE)
    assert ids, "workflow declares no jobs"
    return ids


def _code_only(text: str) -> str:
    """Drop whole-line comments.

    Assertions must bind to what runs, not to prose about what runs. A comment
    that quotes the very expression it explains — as the ones in this workflow
    do — otherwise satisfies the assertion after the code has been deleted.
    """
    return "\n".join(line for line in text.splitlines() if not line.lstrip().startswith("#"))


_HELPER_PATH = QUEUE_READY_HELPER.relative_to(ROOT).as_posix()


def _helper_call(head_sha: str = r"\w+") -> re.Pattern[str]:
    """Match a real three-argument invocation, not a bare mention of the path."""
    return re.compile(
        rf'{re.escape(_HELPER_PATH)} "\$\{{GITHUB_REPOSITORY\}}" "\$\{{\w+\}}" "\$\{{{head_sha}\}}"'
    )


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
        r"\s+r2-operational-results\.attempt1\.json\n"
        r"\s+r2-operational-results\.attempt1\.d/\n"
        r"\s+if-no-files-found: error",
        infrastructure,
    )

    # One sanctioned in-job retry: the scenario command is defined once
    # (count above), invoked through a function, and a failed first attempt
    # keeps its receipt as the classification record before the single
    # retry. Nothing may retry more than once, and the retry must not
    # discard attempt 1's evidence.
    code = _code_only(infrastructure)
    assert code.count("run_r2_scenario()") == 1, "scenario command must be defined exactly once"
    assert code.count("run_r2_scenario\n") + code.count("run_r2_scenario;") == 2, (
        "the scenario must be invoked exactly twice: first attempt and one retry"
    )
    assert "r2-operational-results.attempt1.json" in code
    # Attempt 1's receipt must reference its OWN logs after the rename: the
    # retry recreates r2-operational-results.d, so without the path rewrite
    # the retained receipt resolves to attempt 2's logs.
    assert re.search(
        r"jq .+gsub\(\"r2-operational-results\\\\\.d\"; "
        r"\"r2-operational-results\.attempt1\.d\"\)",
        code,
    ), "the retained attempt-1 receipt must have its log paths rewritten"
    assert "::warning::" in infrastructure, "a silent retry hides the flake it absorbed"

    # The job budget must cover BOTH attempts at the scenario's full
    # timeout plus setup overhead, or the retry is unreachable exactly when
    # the first attempt is the stall it exists to absorb.
    timeout_match = re.search(r"timeout-minutes: (\d+)", infrastructure)
    assert timeout_match is not None, "the R2 job lost its timeout"
    scenario_seconds = scenario["timeout_seconds"]
    assert int(timeout_match.group(1)) * 60 >= 2 * scenario_seconds + 240, (
        "job timeout must cover two scenario attempts plus ~4 min overhead"
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
        "dogfood.evaluation.durable_receipt",
        "dogfood.artifacts.local",
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


# The four tests below protect ONE invariant, split by the file that owns each
# half: auto-merge may only ever be armed for a head whose latest review verdict
# passed cleanly, and the arm must not survive that verdict changing.
#
#   * `scripts/gh_pr_queue_ready.sh` owns the *decision* — latest-run-wins on the
#     exact head, plus zero open non-outdated threads, failing closed on doubt.
#   * `.github/workflows/automerge.yml` owns the *plumbing* — which events
#     re-evaluate arming, that every arm/guard/dequeue path delegates the
#     decision to that one script, and that the sha it arms is the sha it
#     checked.
#
# If the decision logic moves again, re-anchor these assertions to the file that
# owns it afterwards — do NOT copy the string literals across. Mirroring
# literals is how this test broke in the first place: it pinned workflow text
# that had since moved into the helper, so the test failed while the behaviour
# it guards was intact. Assert the property, in its home, against a value
# derived from the file — never a copy of a line that lives somewhere else.


def test_queue_ready_helper_owns_latest_run_wins_and_fails_closed() -> None:
    helper = QUEUE_READY_HELPER.read_text(encoding="utf-8")

    # Latest run wins. review-complete can be re-run on an unchanged head, so
    # ordering the check runs and taking the newest is what stops a stale
    # success outvoting a fresh failure.
    assert "sort_by(.started_at) | last" in helper

    # The verdict is read for the sha the caller passed, never for whatever
    # GitHub currently calls the PR head.
    sha_arg = re.search(r'^(\w+)="\$3"$', helper, re.MULTILINE)
    assert sha_arg is not None, "helper no longer takes the head sha as its third argument"
    assert re.search(rf"commits/\$\{{{sha_arg.group(1)}\}}/check-runs", helper)
    assert "check_name=review-complete" in helper

    # Fail closed. Only an explicit success is ready; a missing conclusion, an
    # API error, and an unparsable thread count all land on the not-ready path.
    assert re.search(r'!=\s*"success"', helper)
    assert "refusing to treat as queue-ready" in helper
    assert "isResolved == false and .isOutdated == false" in helper

    # Exit-code contract: exactly one success exit, and it is the last word of
    # the script — every guard above it exits non-zero.
    assert re.findall(r"^exit 0$", helper, re.MULTILINE) == ["exit 0"]
    assert helper.rstrip().endswith("exit 0")


# The tests below run the helper for real against a stubbed `gh`, so they
# assert the decision it reaches rather than the text of its queries. The stub
# evaluates the helper's own `--jq` filters with real jq, which is what makes
# "did it even ask for hasNextPage?" observable without pinning the query.
_STUB_HEAD_SHA = "0" * 40
_STUB_REPO = "VangelisTech/archetype"
_STUB_PR = "654"

# `gh api [graphql] ... --jq FILTER` reduced to: pick the canned payload for
# whichever endpoint was called, then apply the filter the helper passed.
#
# GraphQL returns only the fields the query selected, so the stub prunes
# pageInfo when the query did not ask for it. Without that, a helper that
# reads `.pageInfo.hasNextPage` while querying only `nodes` would still be
# handed a value here and would look correct against a server that would
# never have sent one.
_GH_STUB = """#!/bin/sh
filter='.'
query=''
prev=''
for arg in "$@"; do
  if [ "$prev" = "--jq" ]; then filter="$arg"; fi
  case "$arg" in query=*) query="$arg" ;; esac
  prev="$arg"
done

payload="$QUEUE_READY_STUB_DIR/check_runs.json"
case " $* " in
  *" graphql "*)
    payload="$QUEUE_READY_STUB_DIR/graphql.json"
    # Inspect the QUERY, not the whole argv: the --jq filter names the same
    # fields, so matching on argv would report every field as selected.
    case "$query" in
      *pageInfo*) ;;
      *) jq 'del(.data.repository.pullRequest.reviewThreads.pageInfo)' "$payload" \
           > "$QUEUE_READY_STUB_DIR/pruned.json"
         payload="$QUEUE_READY_STUB_DIR/pruned.json" ;;
    esac
    ;;
esac
exec jq -r "$filter" "$payload"
"""


def _thread(*, resolved: bool, outdated: bool = False) -> dict[str, bool]:
    return {"isResolved": resolved, "isOutdated": outdated}


def _run_queue_ready(
    tmp_path: Path,
    *,
    threads: list[dict[str, bool]],
    has_next_page: bool,
    review_conclusion: str = "success",
) -> subprocess.CompletedProcess[str]:
    """Run the real helper with `gh` stubbed to serve the given API responses."""
    if shutil.which("jq") is None:
        pytest.skip("jq is required to evaluate the helper's own --jq filters")

    stub_dir = tmp_path / "stubs"
    bin_dir = tmp_path / "bin"
    stub_dir.mkdir()
    bin_dir.mkdir()

    (stub_dir / "check_runs.json").write_text(
        json.dumps(
            {
                "check_runs": [
                    {"started_at": "2026-07-24T10:00:00Z", "conclusion": review_conclusion}
                ]
            }
        ),
        encoding="utf-8",
    )
    (stub_dir / "graphql.json").write_text(
        json.dumps(
            {
                "data": {
                    "repository": {
                        "pullRequest": {
                            "reviewThreads": {
                                "pageInfo": {"hasNextPage": has_next_page},
                                "nodes": threads,
                            }
                        }
                    }
                }
            }
        ),
        encoding="utf-8",
    )

    gh = bin_dir / "gh"
    gh.write_text(_GH_STUB, encoding="utf-8")
    gh.chmod(0o755)

    env = os.environ.copy()
    env["PATH"] = f"{bin_dir}{os.pathsep}{env['PATH']}"
    env["QUEUE_READY_STUB_DIR"] = str(stub_dir)
    return subprocess.run(
        [str(QUEUE_READY_HELPER), _STUB_REPO, _STUB_PR, _STUB_HEAD_SHA],
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )


def test_queue_ready_refuses_a_truncated_review_thread_page(tmp_path: Path) -> None:
    """Truncation is a clean response, so no other guard can catch it."""
    # 100 returned threads, all resolved, and more beyond the page. Every
    # unresolved thread could be sitting past node 100 — the count is a valid
    # integer over an incomplete set, so nothing "fails" and the helper would
    # otherwise call this queue-ready and arm a blocked PR.
    result = _run_queue_ready(
        tmp_path,
        threads=[_thread(resolved=True) for _ in range(100)],
        has_next_page=True,
    )

    assert result.returncode == 1, result.stdout + result.stderr
    assert "queue-ready" != result.stdout.strip()
    assert "refusing to treat as queue-ready" in result.stdout


def test_queue_ready_accepts_a_complete_page_with_every_thread_resolved(tmp_path: Path) -> None:
    """Control: the truncation refusal must not swallow the ready path."""
    result = _run_queue_ready(
        tmp_path,
        threads=[_thread(resolved=True), _thread(resolved=False, outdated=True)],
        has_next_page=False,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    assert result.stdout.strip() == "queue-ready"


def test_queue_ready_still_counts_unresolved_threads_on_a_complete_page(tmp_path: Path) -> None:
    result = _run_queue_ready(
        tmp_path,
        threads=[_thread(resolved=False), _thread(resolved=False), _thread(resolved=True)],
        has_next_page=False,
    )

    assert result.returncode == 1, result.stdout + result.stderr
    assert "2 unresolved review thread(s) remain" in result.stdout


def test_queue_ready_refuses_a_head_whose_review_did_not_pass(tmp_path: Path) -> None:
    result = _run_queue_ready(
        tmp_path,
        threads=[],
        has_next_page=False,
        review_conclusion="failure",
    )

    assert result.returncode == 1, result.stdout + result.stderr
    assert "no authoritative passing review-complete" in result.stdout


def test_automerge_remains_bound_to_latest_reviewed_head() -> None:
    workflow = AUTOMERGE_WORKFLOW.read_text(encoding="utf-8")

    # Every new head and every review signal re-evaluates arming.
    assert (
        "types: [opened, synchronize, reopened, ready_for_review, auto_merge_enabled]" in workflow
    )
    assert 'workflows: ["Deterministic Review Gate"]' in workflow
    assert "github.event.workflow_run.conclusion == 'success'" in workflow
    assert "github.event.workflow_run.head_repository.full_name == github.repository" in workflow

    # Every job that decides whether an arm may stand runs the one helper and
    # branches on its exit status, instead of re-deriving readiness inline...
    for job_id in ("guard", "arm", "dequeue"):
        body = _code_only(_job(workflow, job_id))
        assert _helper_call().search(body), (
            f"the {job_id!r} job stopped invoking {_HELPER_PATH} for its decision"
        )
        assert "status=$?" in body, f"the {job_id!r} job ignores the queue-ready verdict"
    # ...and the decision has not leaked back into the workflow.
    assert "check_name=review-complete" not in workflow
    assert "reviewThreads" not in workflow

    arm = _code_only(_job(workflow, "arm"))
    head_match = re.search(r'\.headRefOid == \\"\$\{?(\w+)\}?\\"', arm)
    assert head_match is not None, "the arm job no longer resolves the PR by matching its head sha"
    # The sha the PR was matched on is the sha readiness is checked for: an arm
    # is never granted on one head's verdict and applied to another.
    assert _helper_call(head_sha=head_match.group(1)).search(arm), (
        "the arm job checks readiness for a different sha than the one it matched the PR on"
    )

    # Both PR-resolution paths (workflow_run and review) filter, so a new path
    # cannot quietly arm a draft or a non-maintainer PR.
    pr_filters = re.findall(r"select\((.+?)\)\s*\|", arm, re.DOTALL)
    assert pr_filters, "the arm job no longer filters the PRs it resolves"
    for pr_filter in pr_filters:
        assert ".isDraft | not" in pr_filter
        assert "author.login" in pr_filter
        # Conjunction, never disjunction: an `or` would make either predicate
        # optional and arm exactly what the other one exists to exclude.
        assert " or " not in pr_filter

    # ...and both agree on state, by whichever mechanism each path has. gh pr
    # view serves closed and merged PRs, and neither is ever isDraft, so
    # without an explicit state check the review path would try to arm a PR
    # that can no longer be merged.
    assert "--state open" in arm, "the gh pr list path stopped restricting to open PRs"
    assert '.state == "OPEN"' in arm, "the review path stopped restricting to open PRs"

    assert 'gh pr merge --auto --squash --repo "${GITHUB_REPOSITORY}" "$pr"' in arm


def test_automerge_runs_repo_scripts_from_the_trusted_default_branch() -> None:
    """A PR must not be able to edit the script that judges it."""
    workflow = AUTOMERGE_WORKFLOW.read_text(encoding="utf-8")

    checked = 0
    for job_id in _job_ids(workflow):
        body = _code_only(_job(workflow, job_id))
        if "scripts/" not in body:
            continue
        checked += 1
        # `actions/checkout` without `ref:` follows GITHUB_SHA, which is the PR
        # merge ref on pull_request and pull_request_review* events.
        assert "ref: ${{ github.event.repository.default_branch }}" in body, (
            f"the {job_id!r} job executes a repo script from an untrusted checkout"
        )
    assert checked, "no job executes a repo script; re-anchor this test"


def test_automerge_triggers_are_real_github_actions_events() -> None:
    """A webhook name Actions does not support invalidates the whole file."""
    workflow = AUTOMERGE_WORKFLOW.read_text(encoding="utf-8")

    # `pull_request_review_thread` is an Apps-only webhook. Naming it in `on:`
    # makes GitHub reject the file outright: no job runs, every push gets a
    # bare "Invalid workflow file" run, and the arming gate silently does
    # nothing at all. Actions has no trigger for thread resolution — do not
    # reintroduce one. See the workflow header.
    assert "pull_request_review_thread:" not in workflow


def test_automerge_disarms_when_new_findings_land_on_an_armed_pr() -> None:
    workflow = AUTOMERGE_WORKFLOW.read_text(encoding="utf-8")

    # The #646 -> #647 race: findings arrived after review-complete passed,
    # while the PR was already armed and queue-locked. A submitted review is
    # the signal Actions actually delivers for that.
    assert "  pull_request_review:\n    types: [submitted, edited, dismissed]\n" in workflow

    dequeue = _code_only(_job(workflow, "dequeue"))
    assert "github.event_name == 'pull_request_review'" in dequeue
    # Judged on what would actually merge, not on the event's cached head.
    assert "--json headRefOid" in dequeue
    # Disarm is verified by reading autoMergeRequest back, not by exit code:
    # `--disable-auto` exits nonzero on the benign already-unarmed no-op.
    assert dequeue.count("--json autoMergeRequest --jq '.autoMergeRequest // empty'") >= 2
    assert "gh pr merge --disable-auto" in dequeue
    # Both outcomes are distinguishable in the log (the #645 shepherd failure).
    assert "nothing to disarm" in dequeue
    assert "auto-merge disarmed on new findings" in dequeue
