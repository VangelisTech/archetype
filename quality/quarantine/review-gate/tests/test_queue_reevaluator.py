# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Quarantined contracts for the retired queue-readiness re-evaluator.

The automerge workflow is event-driven; the re-evaluator is the event-free
half of the same arming contract (thread resolution emits no Actions event).
These tests hold three promises:

1. The reconcile script delegates the readiness decision to the one oracle
   (``gh_pr_queue_ready.sh``) and acts only on its answer.
2. The eligibility filter mirrors automerge.yml's arm job — the two literals
   are held together here so they change together or not at all.
3. Each reconcile outcome is verified by postcondition (arm state read back),
   never inferred from ``gh pr merge``'s exit code.

Behavior tests run the real script against a stubbed ``gh`` (the idiom
established in test_quality_workflow.py) and assert the decision reached.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
RECONCILE_SCRIPT = ROOT / "scripts" / "gh_pr_arm_reconcile.sh"
REEVALUATOR_WORKFLOW = ROOT / ".github" / "workflows" / "queue-reevaluator.yml"
AUTOMERGE_WORKFLOW = ROOT / ".github" / "workflows" / "automerge.yml"

_STUB_REPO = "VangelisTech/archetype"
_STUB_PR = "654"
_STUB_HEAD = "1" * 40


def _code_only(text: str) -> str:
    """Drop whole-line comments so assertions bind to code, not prose."""
    return "\n".join(line for line in text.splitlines() if not line.lstrip().startswith("#"))


def test_reevaluator_workflow_runs_the_reconcile_script_on_a_schedule() -> None:
    workflow = REEVALUATOR_WORKFLOW.read_text(encoding="utf-8")
    code = _code_only(workflow)

    # Scheduled plus manually dispatchable; no PR-triggered path exists, so
    # the workflow file and both helpers always come from the default branch.
    assert "schedule:" in code
    assert re.search(r'cron:\s*"\*/\d+ \* \* \* \*"', code)
    assert "workflow_dispatch:" in code
    assert "pull_request" not in code

    # The decision belongs to the reconcile script, which owns the oracle
    # delegation; the workflow only enumerates PRs.
    assert "scripts/gh_pr_arm_reconcile.sh" in code
    assert "gh pr merge" not in code, (
        "the workflow must not arm PRs directly; arming lives in the "
        "reconcile script behind its postcondition check"
    )

    # Arming must use the PAT so the merge commit triggers on:push workflows
    # (same reasoning as automerge.yml's arm job).
    assert "AUTOMERGE_PAT" in code


def test_reconcile_script_delegates_to_the_one_queue_ready_oracle() -> None:
    script = _code_only(RECONCILE_SCRIPT.read_text(encoding="utf-8"))

    # One oracle: the readiness decision is the helper's exit code, and no
    # local re-derivation of "ready" exists (no review-complete query here).
    assert "scripts/gh_pr_queue_ready.sh" in script
    assert "check_name=review-complete" not in script, (
        "the reconcile script re-derives readiness instead of delegating"
    )

    # Postcondition over exit code, both directions.
    assert "autoMergeRequest != null" in script
    assert "--disable-auto" in script
    # Arming is pinned to the head the oracle judged: without
    # --match-head-commit, a push racing the readiness read would arm the
    # new, unreviewed head on a stale verdict.
    assert '--auto --squash --match-head-commit "$head_sha"' in _code_only(script)


def test_eligibility_filter_mirrors_the_automerge_arm_job() -> None:
    """automerge.yml and the reconcile script restate one maintainer literal."""
    script = RECONCILE_SCRIPT.read_text(encoding="utf-8")
    automerge = AUTOMERGE_WORKFLOW.read_text(encoding="utf-8")

    script_maintainer = re.search(r'^MAINTAINER="(\w+)"$', script, re.MULTILINE)
    assert script_maintainer is not None, "reconcile script lost its MAINTAINER literal"

    automerge_maintainers = set(re.findall(r"author\.login == .{1,2}?(\w+)", automerge))
    assert automerge_maintainers, "automerge.yml lost its author filter"
    assert automerge_maintainers == {script_maintainer.group(1)}, (
        "the reconcile script and automerge.yml disagree on the maintainer "
        "eligibility literal; change them together or not at all"
    )


# ---------------------------------------------------------------------------
# Behavior: run the real script against a stubbed `gh`.
#
# The stub dispatches on argv: PR metadata GraphQL (mergeQueueEntry in the
# query), review-thread GraphQL (the oracle's query), review-complete
# check-runs, `pr merge` (records the call, flips an arm marker file), and
# `pr view --json autoMergeRequest` (reads the marker) — so the script's
# postcondition read-back observes the effect of its own arm/disarm.
# ---------------------------------------------------------------------------

_GH_STUB = """#!/bin/sh
filter='.'
query=''
prev=''
for arg in "$@"; do
  if [ "$prev" = "--jq" ]; then filter="$arg"; fi
  case "$arg" in query=*) query="$arg" ;; esac
  prev="$arg"
done

case "$1" in
  pr)
    case "$2" in
      merge)
        echo "merge $*" >> "$STUB_DIR/merge_calls.log"
        case " $* " in
          *" --auto "*) touch "$STUB_DIR/armed" ;;
          *" --disable-auto "*) rm -f "$STUB_DIR/armed" ;;
        esac
        exit 0
        ;;
      view)
        if [ -e "$STUB_DIR/armed" ]; then
          echo '{"autoMergeRequest":{"enabledAt":"now"}}' | jq -r "$filter"
        else
          echo '{"autoMergeRequest":null}' | jq -r "$filter"
        fi
        exit 0
        ;;
    esac
    ;;
  api)
    case " $* " in
      *" graphql "*)
        case "$query" in
          *mergeQueueEntry*) exec jq -r "$filter" "$STUB_DIR/pr_meta.json" ;;
          *reviewThreads*) exec jq -r "$filter" "$STUB_DIR/threads.json" ;;
        esac
        ;;
      *check-runs*) exec jq -r "$filter" "$STUB_DIR/check_runs.json" ;;
    esac
    ;;
esac
echo "gh stub: unhandled call: $*" >&2
exit 64
"""


def _run_reconcile(
    tmp_path: Path,
    *,
    armed: bool,
    queued: bool = False,
    draft: bool = False,
    author: str = "everettVT",
    review_conclusion: str = "success",
    unresolved_threads: int = 0,
) -> tuple[subprocess.CompletedProcess[str], list[str]]:
    if shutil.which("jq") is None:
        pytest.skip("jq is required to evaluate the scripts' --jq filters")

    stub_dir = tmp_path / "stubs"
    bin_dir = tmp_path / "bin"
    stub_dir.mkdir()
    bin_dir.mkdir()

    (stub_dir / "pr_meta.json").write_text(
        json.dumps(
            {
                "data": {
                    "repository": {
                        "pullRequest": {
                            "state": "OPEN",
                            "isDraft": draft,
                            "author": {"login": author},
                            "headRefOid": _STUB_HEAD,
                            "autoMergeRequest": {"enabledAt": "now"} if armed else None,
                            "mergeQueueEntry": {"position": 1} if queued else None,
                        }
                    }
                }
            }
        ),
        encoding="utf-8",
    )
    (stub_dir / "check_runs.json").write_text(
        json.dumps(
            {
                "check_runs": [
                    {"started_at": "2026-07-26T10:00:00Z", "conclusion": review_conclusion}
                ]
            }
        ),
        encoding="utf-8",
    )
    threads = [{"isResolved": False, "isOutdated": False}] * unresolved_threads
    (stub_dir / "threads.json").write_text(
        json.dumps(
            {
                "data": {
                    "repository": {
                        "pullRequest": {
                            "reviewThreads": {
                                "pageInfo": {"hasNextPage": False},
                                "nodes": threads,
                            }
                        }
                    }
                }
            }
        ),
        encoding="utf-8",
    )
    if armed:
        (stub_dir / "armed").touch()

    gh = bin_dir / "gh"
    gh.write_text(_GH_STUB, encoding="utf-8")
    gh.chmod(0o755)
    # `sleep` between postcondition retries would slow a failing test; stub it.
    sleep = bin_dir / "sleep"
    sleep.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    sleep.chmod(0o755)

    env = os.environ.copy()
    env["PATH"] = f"{bin_dir}{os.pathsep}{env['PATH']}"
    env["STUB_DIR"] = str(stub_dir)
    env["QUEUE_READY_STUB_DIR"] = str(stub_dir)
    result = subprocess.run(
        [str(RECONCILE_SCRIPT), _STUB_REPO, _STUB_PR],
        capture_output=True,
        text=True,
        env=env,
        check=False,
        cwd=ROOT,
    )
    merge_log = stub_dir / "merge_calls.log"
    calls = merge_log.read_text(encoding="utf-8").splitlines() if merge_log.exists() else []
    return result, calls


def test_ready_and_unarmed_arms_with_postcondition(tmp_path: Path) -> None:
    result, calls = _run_reconcile(tmp_path, armed=False)

    assert result.returncode == 0, result.stdout + result.stderr
    assert "armed auto-merge" in result.stdout
    assert any(
        "--auto" in call and "--squash" in call and f"--match-head-commit {_STUB_HEAD}" in call
        for call in calls
    ), f"arming must be pinned to the judged head; calls: {calls}"


def test_not_ready_and_armed_disarms(tmp_path: Path) -> None:
    result, calls = _run_reconcile(tmp_path, armed=True, unresolved_threads=2)

    assert result.returncode == 0, result.stdout + result.stderr
    assert "disarmed auto-merge" in result.stdout
    assert any("--disable-auto" in call for call in calls)


def test_queued_pr_is_left_alone(tmp_path: Path) -> None:
    """The merge-group recheck owns the queue phase; the cron must not act."""
    result, calls = _run_reconcile(tmp_path, armed=True, queued=True, unresolved_threads=2)

    assert result.returncode == 0, result.stdout + result.stderr
    assert "merge queue" in result.stdout
    assert calls == []


def test_draft_pr_is_left_alone(tmp_path: Path) -> None:
    result, calls = _run_reconcile(tmp_path, armed=False, draft=True)

    assert result.returncode == 0, result.stdout + result.stderr
    assert "not eligible" in result.stdout
    assert calls == []


def test_failed_review_never_arms(tmp_path: Path) -> None:
    result, calls = _run_reconcile(tmp_path, armed=False, review_conclusion="failure")

    assert result.returncode == 0, result.stdout + result.stderr
    assert "not queue-ready" in result.stdout
    assert calls == []


def test_ready_and_armed_is_a_no_op(tmp_path: Path) -> None:
    result, calls = _run_reconcile(tmp_path, armed=True)

    assert result.returncode == 0, result.stdout + result.stderr
    assert "nothing to do" in result.stdout
    assert calls == []
