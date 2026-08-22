# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Contracts for release-authorized distribution publisher dispatches."""

from __future__ import annotations

from copy import deepcopy
from typing import Any

import pytest

from scripts.release_artifact import PUBLISHER_WORKFLOWS
from scripts.verify_publisher_dispatch import verify_publisher_dispatch

_AUTOMATION_ACTOR = "github-actions[bot]"
_COMMIT = "a" * 40
_TAG_OBJECT = "b" * 40
_DISTRIBUTION = "archetype-missions"
_PARENT_RUN_ATTEMPT = 2
_PARENT_RUN_ID = 101
_REPOSITORY = "VangelisTech/archetype"
_TAG = "v0.6.0"
_WORKFLOW = "publish-archetype-missions.yml"


def _run_ids() -> dict[str, int]:
    return {
        distribution: 201 + index
        for index, (distribution, workflow) in enumerate(PUBLISHER_WORKFLOWS.items())
        if workflow != "release.yml"
    }


def _parent_run() -> dict[str, Any]:
    return {
        "id": _PARENT_RUN_ID,
        "run_attempt": _PARENT_RUN_ATTEMPT,
        "event": "workflow_dispatch",
        "path": ".github/workflows/release.yml",
        "head_sha": _COMMIT,
        "head_branch": _TAG,
        "status": "in_progress",
        "conclusion": None,
        "repository": {"full_name": _REPOSITORY},
        "actor": {"login": "everettVT"},
        "triggering_actor": {"login": "everettVT"},
    }


def _allowlist() -> dict[str, Any]:
    run_ids = _run_ids()
    return {
        "schema": "archetype.publisher-dispatch/v2",
        "repository": _REPOSITORY,
        "parent_run_id": _PARENT_RUN_ID,
        "parent_run_attempt": _PARENT_RUN_ATTEMPT,
        "tag": _TAG,
        "commit": _COMMIT,
        "tag_object": _TAG_OBJECT,
        "registry": "testpypi",
        "runs": [
            {
                "distribution": distribution,
                "workflow": workflow,
                "run_id": run_ids[distribution],
                "url": (f"https://github.com/{_REPOSITORY}/actions/runs/{run_ids[distribution]}"),
            }
            for distribution, workflow in PUBLISHER_WORKFLOWS.items()
            if workflow != "release.yml"
        ],
    }


def _arguments() -> dict[str, Any]:
    return {
        "repository": _REPOSITORY,
        "event_name": "workflow_dispatch",
        "ref": f"refs/tags/{_TAG}",
        "ref_name": _TAG,
        "ref_type": "tag",
        "commit": _COMMIT,
        "workflow_ref": (f"{_REPOSITORY}/.github/workflows/{_WORKFLOW}@refs/tags/{_TAG}"),
        "actor": _AUTOMATION_ACTOR,
        "triggering_actor": _AUTOMATION_ACTOR,
        "expected_workflow": _WORKFLOW,
        "distribution": _DISTRIBUTION,
        "child_run_id": _run_ids()[_DISTRIBUTION],
        "parent_run_id": _PARENT_RUN_ID,
        "parent_run_attempt": _PARENT_RUN_ATTEMPT,
        "parent_run": _parent_run(),
        "allowlist": _allowlist(),
        "tag": _TAG,
        "expected_commit": _COMMIT,
        "expected_tag_object": _TAG_OBJECT,
        "registry": "testpypi",
    }


def test_verify_publisher_dispatch_accepts_allowlisted_bot_child_and_live_parent() -> None:
    result = verify_publisher_dispatch(**_arguments())

    assert result == {
        "repository": _REPOSITORY,
        "workflow": _WORKFLOW,
        "tag": _TAG,
        "commit": _COMMIT,
        "tag_object": _TAG_OBJECT,
        "registry": "testpypi",
        "environment": "release-testpypi",
        "child_run_id": _run_ids()[_DISTRIBUTION],
        "parent_run_id": _PARENT_RUN_ID,
        "parent_run_attempt": _PARENT_RUN_ATTEMPT,
    }


@pytest.mark.parametrize(
    "path",
    [
        f".github/workflows/release.yml@{_TAG}",
        f".github/workflows/release.yml@refs/tags/{_TAG}",
    ],
)
def test_verify_publisher_dispatch_accepts_ref_qualified_parent_path(path: str) -> None:
    arguments = _arguments()
    arguments["parent_run"]["path"] = path

    verify_publisher_dispatch(**arguments)


@pytest.mark.parametrize("field", ["actor", "triggering_actor"])
def test_verify_publisher_dispatch_rejects_manual_child_actor(field: str) -> None:
    arguments = _arguments()
    arguments[field] = "everettVT"

    with pytest.raises(PermissionError, match="release workflow token"):
        verify_publisher_dispatch(**arguments)


def test_verify_publisher_dispatch_rejects_child_run_not_in_allowlist() -> None:
    arguments = _arguments()
    selected = next(
        run for run in arguments["allowlist"]["runs"] if run["distribution"] == _DISTRIBUTION
    )
    selected["run_id"] += 1000
    selected["url"] = f"https://github.com/{_REPOSITORY}/actions/runs/{selected['run_id']}"

    with pytest.raises(PermissionError, match="not authorized"):
        verify_publisher_dispatch(**arguments)


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("expected_workflow", "unknown.yml", "not registered"),
        (
            "expected_workflow",
            "publish-archetype-physical-ai.yml",
            "distribution and workflow differ",
        ),
        ("distribution", "archetype-physical-ai", "distribution and workflow differ"),
    ],
)
def test_verify_publisher_dispatch_rejects_wrong_workflow_or_distribution(
    field: str,
    value: str,
    message: str,
) -> None:
    arguments = _arguments()
    arguments[field] = value

    with pytest.raises(ValueError, match=message):
        verify_publisher_dispatch(**arguments)


@pytest.mark.parametrize(
    ("case", "message"),
    [
        ("id", "identity differs"),
        ("path", "not the release workflow"),
        ("status", "not actively coordinating"),
        ("actor", "actor must be everettVT"),
        ("triggering_actor", "triggering actor must be everettVT"),
        ("tag", "exact tag commit"),
        ("sha", "exact tag commit"),
        ("attempt", "attempt differs"),
        ("repository", "another repository"),
    ],
)
def test_verify_publisher_dispatch_rejects_wrong_parent_run(
    case: str,
    message: str,
) -> None:
    arguments = _arguments()
    parent = arguments["parent_run"]
    if case == "id":
        parent["id"] += 1
    elif case == "path":
        parent["path"] = ".github/workflows/other.yml"
    elif case == "status":
        parent["status"] = "completed"
        parent["conclusion"] = "success"
    elif case == "actor":
        parent["actor"] = {"login": "someone-else"}
    elif case == "triggering_actor":
        parent["triggering_actor"] = {"login": "someone-else"}
    elif case == "tag":
        parent["head_branch"] = "v0.6.3"
    elif case == "sha":
        parent["head_sha"] = "c" * 40
    elif case == "attempt":
        parent["run_attempt"] += 1
    elif case == "repository":
        parent["repository"] = {"full_name": "someone-else/archetype"}

    with pytest.raises(PermissionError, match=message):
        verify_publisher_dispatch(**arguments)


@pytest.mark.parametrize(
    ("case", "error", "message"),
    [
        ("schema", PermissionError, "unsupported schema"),
        ("extra_top_level", PermissionError, "unexpected fields"),
        ("not_a_list", PermissionError, "incomplete run matrix"),
        ("missing", PermissionError, "incomplete run matrix"),
        ("duplicate", PermissionError, "invalid run"),
        ("wrong_workflow", PermissionError, "invalid run"),
        ("boolean_run_id", PermissionError, "invalid run"),
        ("wrong_url", PermissionError, "invalid run"),
        ("non_object", TypeError, "must be an object"),
        ("extra_run_field", PermissionError, "unexpected fields"),
    ],
)
def test_verify_publisher_dispatch_rejects_malformed_allowlist_matrix(
    case: str,
    error: type[Exception],
    message: str,
) -> None:
    arguments = _arguments()
    allowlist = deepcopy(arguments["allowlist"])
    arguments["allowlist"] = allowlist
    runs = allowlist["runs"]
    if case == "schema":
        allowlist["schema"] = "archetype.publisher-dispatch/v1"
    elif case == "extra_top_level":
        allowlist["extra"] = True
    elif case == "not_a_list":
        allowlist["runs"] = {}
    elif case == "missing":
        runs.pop()
    elif case == "duplicate":
        runs[1] = deepcopy(runs[0])
    elif case == "wrong_workflow":
        runs[0]["workflow"] = "other.yml"
    elif case == "boolean_run_id":
        runs[0]["run_id"] = True
    elif case == "wrong_url":
        runs[0]["url"] += "?download=1"
    elif case == "non_object":
        runs[0] = "not-an-object"
    elif case == "extra_run_field":
        runs[0]["extra"] = True

    with pytest.raises(error, match=message):
        verify_publisher_dispatch(**arguments)


def test_verify_publisher_dispatch_rejects_different_tag_object_in_allowlist() -> None:
    arguments = _arguments()
    arguments["allowlist"]["tag_object"] = "c" * 40

    with pytest.raises(PermissionError, match="tag_object differs"):
        verify_publisher_dispatch(**arguments)


@pytest.mark.parametrize("value", ["", "B" * 40, "b" * 39, 3, None])
def test_verify_publisher_dispatch_rejects_malformed_tag_object(value: object) -> None:
    arguments = _arguments()
    arguments["expected_tag_object"] = value

    with pytest.raises(ValueError, match="expected tag object"):
        verify_publisher_dispatch(**arguments)
