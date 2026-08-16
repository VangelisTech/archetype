# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Behavioral contracts for checkout-free child publisher reauthorization."""

from __future__ import annotations

import json
import os
import re
import subprocess
from copy import deepcopy
from pathlib import Path
from typing import Any

from scripts.release_artifact import PUBLISHER_WORKFLOWS

ROOT = Path(__file__).resolve().parents[2]
AUTOMATION_ACTOR = "github-actions[bot]"
CHILD_RUN_ID = 201
COMMIT = "a" * 40
DISTRIBUTION = "archetype-missions"
PARENT_RUN_ATTEMPT = 2
PARENT_RUN_ID = 101
REGISTRY = "testpypi"
REPOSITORY = "VangelisTech/archetype"
TAG = "v0.6.0"
TAG_OBJECT = "b" * 40
WORKFLOW = "publish-archetype-missions.yml"


def _job(workflow: str, job_id: str) -> str:
    match = re.search(
        rf"^  {re.escape(job_id)}:\n(?P<body>.*?)(?=^  [a-z][a-z0-9-]*:\n|\Z)",
        workflow,
        re.MULTILINE | re.DOTALL,
    )
    assert match is not None, f"workflow lost the {job_id!r} job"
    return match.group("body")


def _parent_check_bodies() -> tuple[str, ...]:
    bodies: list[str] = []
    for workflow_name in PUBLISHER_WORKFLOWS.values():
        if workflow_name == "release.yml":
            continue
        workflow = (ROOT / ".github" / "workflows" / workflow_name).read_text(encoding="utf-8")
        for job_id in ("publish-testpypi", "publish-pypi"):
            job = _job(workflow, job_id)
            step_marker = "      - name: Reauthorize the live parent release\n"
            assert job.count(step_marker) == 1
            _, step = job.split(step_marker)
            run_marker = "        run: |\n"
            assert step.count(run_marker) >= 1
            _, shell_and_tag_check = step.split(run_marker, maxsplit=1)
            raw_body, tag_marker, _ = shell_and_tag_check.partition(
                "      - name: Reauthorize the remote release tag\n"
            )
            assert tag_marker

            lines = raw_body.splitlines()
            assert lines
            assert all(not line or line.startswith("          ") for line in lines)
            bodies.append("\n".join(line[10:] if line else "" for line in lines) + "\n")

    assert len(bodies) == (len(PUBLISHER_WORKFLOWS) - 1) * 2
    return tuple(bodies)


def _parent_check_body() -> str:
    bodies = _parent_check_bodies()
    assert len(set(bodies)) == 1
    return bodies[0]


def _run_ids() -> dict[str, int]:
    return {
        distribution: CHILD_RUN_ID + index
        for index, (distribution, workflow) in enumerate(PUBLISHER_WORKFLOWS.items())
        if workflow != "release.yml"
    }


def _allowlist() -> dict[str, Any]:
    run_ids = _run_ids()
    return {
        "schema": "archetype.publisher-dispatch/v2",
        "repository": REPOSITORY,
        "parent_run_id": PARENT_RUN_ID,
        "parent_run_attempt": PARENT_RUN_ATTEMPT,
        "tag": TAG,
        "commit": COMMIT,
        "tag_object": TAG_OBJECT,
        "registry": REGISTRY,
        "runs": [
            {
                "distribution": distribution,
                "workflow": workflow,
                "run_id": run_ids[distribution],
                "url": f"https://github.com/{REPOSITORY}/actions/runs/{run_ids[distribution]}",
            }
            for distribution, workflow in PUBLISHER_WORKFLOWS.items()
            if workflow != "release.yml"
        ],
    }


def _parent_run() -> dict[str, Any]:
    return {
        "id": PARENT_RUN_ID,
        "run_attempt": PARENT_RUN_ATTEMPT,
        "event": "workflow_dispatch",
        "path": ".github/workflows/release.yml",
        "head_sha": COMMIT,
        "head_branch": TAG,
        "status": "in_progress",
        "conclusion": None,
        "repository": {"full_name": REPOSITORY},
        "actor": {"login": "everettVT"},
        "triggering_actor": {"login": "everettVT"},
    }


def _run_parent_check(
    tmp_path: Path,
    *,
    parent_run: dict[str, Any] | None = None,
    allowlist: dict[str, Any] | None = None,
    gh_status: int = 0,
    expect_gh: bool = True,
) -> subprocess.CompletedProcess[str]:
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    gh_arguments = tmp_path / "gh-arguments.txt"
    fake_gh = fake_bin / "gh"
    fake_gh.write_text(
        "#!/usr/bin/env bash\n"
        'printf \'%s\\n\' "$@" > "$FAKE_GH_ARGUMENTS"\n'
        "printf '%s' \"$FAKE_GH_OUTPUT\"\n"
        'exit "$FAKE_GH_STATUS"\n',
        encoding="utf-8",
    )
    fake_gh.chmod(0o755)

    evidence = tmp_path / ".context" / "publisher-dispatch-live"
    evidence.mkdir(parents=True)
    (evidence / f"publisher-dispatch-{REGISTRY}.json").write_text(
        json.dumps(allowlist or _allowlist()),
        encoding="utf-8",
    )
    run_ids = _run_ids()
    environment = {
        **os.environ,
        "PATH": f"{fake_bin}{os.pathsep}{os.environ['PATH']}",
        "FAKE_GH_ARGUMENTS": str(gh_arguments),
        "FAKE_GH_OUTPUT": json.dumps(parent_run or _parent_run()),
        "FAKE_GH_STATUS": str(gh_status),
        "CHILD_RUN_ID": str(run_ids[DISTRIBUTION]),
        "EXPECTED_COMMIT": COMMIT,
        "EXPECTED_TAG_OBJECT": TAG_OBJECT,
        "GH_TOKEN": "test-token",
        "GITHUB_ACTOR": AUTOMATION_ACTOR,
        "GITHUB_EVENT_NAME": "workflow_dispatch",
        "GITHUB_REPOSITORY": REPOSITORY,
        "GITHUB_SHA": COMMIT,
        "GITHUB_TRIGGERING_ACTOR": AUTOMATION_ACTOR,
        "GITHUB_WORKFLOW_REF": (f"{REPOSITORY}/.github/workflows/{WORKFLOW}@refs/tags/{TAG}"),
        "PARENT_RUN_ATTEMPT": str(PARENT_RUN_ATTEMPT),
        "PARENT_RUN_ID": str(PARENT_RUN_ID),
        "PUBLISHER_DISTRIBUTION": DISTRIBUTION,
        "PUBLISHER_REGISTRY": REGISTRY,
        "PUBLISHER_TAG": TAG,
        "PUBLISHER_WORKFLOW": WORKFLOW,
    }
    result = subprocess.run(  # noqa: S603 - exact trusted workflow shell under test
        ["bash", "-c", _parent_check_body()],
        cwd=tmp_path,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )
    if expect_gh:
        assert gh_arguments.read_text(encoding="utf-8").splitlines() == [
            "api",
            "-H",
            "X-GitHub-Api-Version: 2026-03-10",
            f"/repos/{REPOSITORY}/actions/runs/{PARENT_RUN_ID}",
        ]
    else:
        assert not gh_arguments.exists()
    return result


def test_child_publishers_execute_identical_parent_check_shells() -> None:
    bodies = _parent_check_bodies()

    assert len(set(bodies)) == 1
    assert all("${{" not in body for body in bodies)


def test_parent_check_accepts_the_live_exact_parent_and_allowlisted_child(
    tmp_path: Path,
) -> None:
    result = _run_parent_check(tmp_path)

    assert result.returncode == 0, result.stderr


def test_parent_check_rejects_a_canceled_parent(tmp_path: Path) -> None:
    parent = _parent_run()
    parent["status"] = "completed"
    parent["conclusion"] = "cancelled"

    result = _run_parent_check(tmp_path, parent_run=parent)

    assert result.returncode != 0
    assert "parent is no longer authorized and live" in result.stderr


def test_parent_check_rejects_a_different_parent_attempt(tmp_path: Path) -> None:
    parent = _parent_run()
    parent["run_attempt"] += 1

    result = _run_parent_check(tmp_path, parent_run=parent)

    assert result.returncode != 0
    assert "parent is no longer authorized and live" in result.stderr


def test_parent_check_rejects_a_different_operator(tmp_path: Path) -> None:
    parent = _parent_run()
    parent["triggering_actor"] = {"login": "someone-else"}

    result = _run_parent_check(tmp_path, parent_run=parent)

    assert result.returncode != 0
    assert "parent is no longer authorized and live" in result.stderr


def test_parent_check_rejects_a_child_missing_from_the_allowlist(tmp_path: Path) -> None:
    allowlist = deepcopy(_allowlist())
    selected = next(run for run in allowlist["runs"] if run["distribution"] == DISTRIBUTION)
    selected["run_id"] += 1000
    selected["url"] = f"https://github.com/{REPOSITORY}/actions/runs/{selected['run_id']}"

    result = _run_parent_check(tmp_path, allowlist=allowlist, expect_gh=False)

    assert result.returncode != 0
    assert "child run is not allowlisted" in result.stderr


def test_parent_check_rejects_a_different_tag_object_receipt(tmp_path: Path) -> None:
    allowlist = _allowlist()
    allowlist["tag_object"] = "c" * 40

    result = _run_parent_check(tmp_path, allowlist=allowlist, expect_gh=False)

    assert result.returncode != 0
    assert "child run is not allowlisted" in result.stderr


def test_parent_check_fails_closed_when_github_lookup_fails(tmp_path: Path) -> None:
    result = _run_parent_check(tmp_path, gh_status=2)

    assert result.returncode != 0
    assert "parent lookup failed" in result.stderr
