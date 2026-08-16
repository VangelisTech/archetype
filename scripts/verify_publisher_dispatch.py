#!/usr/bin/env python3
# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Authorize one package publisher dispatched by the release workflow."""

from __future__ import annotations

import argparse
import json
import os
import re
from collections.abc import Mapping
from http.client import HTTPException
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import HTTPRedirectHandler, Request, build_opener

if __package__:
    from .release_artifact import PUBLISHER_WORKFLOWS
else:  # pragma: no cover - exercised by the command-line entry point
    from release_artifact import PUBLISHER_WORKFLOWS

_API_VERSION = "2026-03-10"
_AUTOMATION_ACTOR = "github-actions[bot]"
_COMMIT = re.compile(r"[0-9a-f]{40}\Z")
_OPERATOR = "everettVT"
_REPOSITORY = "VangelisTech/archetype"
_TAG = re.compile(r"v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\Z")
_REGISTRY_ENVIRONMENTS = {
    "testpypi": "release-testpypi",
    "pypi": "release-pypi",
}
_PUBLISHER_WORKFLOWS = frozenset(PUBLISHER_WORKFLOWS.values()) - {"release.yml"}


class _RejectRedirects(HTTPRedirectHandler):
    def redirect_request(self, *args: object, **kwargs: object) -> None:
        del args, kwargs
        return None


_OPENER = build_opener(_RejectRedirects())


def _required_string(value: object, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"parent release run has no {label}")
    return value


def _login(value: object, label: str) -> str:
    if not isinstance(value, Mapping):
        raise ValueError(f"parent release run has no {label}")
    return _required_string(value.get("login"), f"{label} login")


def verify_publisher_dispatch(
    *,
    repository: str,
    event_name: str,
    ref: str,
    ref_name: str,
    ref_type: str,
    commit: str,
    workflow_ref: str,
    actor: str,
    triggering_actor: str,
    expected_workflow: str,
    distribution: str,
    child_run_id: int,
    parent_run_id: int,
    parent_run_attempt: int,
    parent_run: Mapping[str, Any],
    allowlist: Mapping[str, Any],
    tag: str,
    expected_commit: str,
    expected_tag_object: str,
    registry: str,
) -> dict[str, Any]:
    """Bind a child publisher to one live, operator-authorized release run."""

    if repository != _REPOSITORY:
        raise PermissionError(f"publisher repository must be {_REPOSITORY}")
    if event_name != "workflow_dispatch":
        raise PermissionError("publisher must use workflow_dispatch")
    if actor != _AUTOMATION_ACTOR or triggering_actor != _AUTOMATION_ACTOR:
        raise PermissionError("publisher must be dispatched by the release workflow token")
    if _TAG.fullmatch(tag) is None:
        raise ValueError("publisher tag must be canonical vMAJOR.MINOR.PATCH")
    if _COMMIT.fullmatch(expected_commit) is None:
        raise ValueError("publisher expected commit must be a full Git commit")
    if not isinstance(expected_tag_object, str) or _COMMIT.fullmatch(expected_tag_object) is None:
        raise ValueError("publisher expected tag object must be a full Git object ID")
    if expected_workflow not in _PUBLISHER_WORKFLOWS:
        raise ValueError("publisher workflow is not registered to a release distribution")
    if PUBLISHER_WORKFLOWS.get(distribution) != expected_workflow:
        raise ValueError("publisher distribution and workflow differ")
    if registry not in _REGISTRY_ENVIRONMENTS:
        raise ValueError("publisher registry must be testpypi or pypi")
    if child_run_id < 1 or parent_run_id < 1 or parent_run_attempt < 1:
        raise ValueError("publisher parent run coordinates must be positive")

    tag_ref = f"refs/tags/{tag}"
    exact_workflow_ref = f"{_REPOSITORY}/.github/workflows/{expected_workflow}@{tag_ref}"
    if ref_type != "tag" or ref != tag_ref or ref_name != tag:
        raise PermissionError("publisher workflow is not running at the exact release tag")
    if commit != expected_commit:
        raise PermissionError("publisher workflow commit differs from the authorized release")
    if workflow_ref != exact_workflow_ref:
        raise PermissionError("publisher workflow identity differs from the trusted file and tag")

    if parent_run.get("id") != parent_run_id:
        raise PermissionError("parent release run identity differs")
    if parent_run.get("run_attempt") != parent_run_attempt:
        raise PermissionError("parent release run attempt differs")
    if parent_run.get("event") != "workflow_dispatch":
        raise PermissionError("parent release run was not operator-dispatched")
    parent_paths = {
        ".github/workflows/release.yml",
        f".github/workflows/release.yml@{tag}",
        f".github/workflows/release.yml@refs/tags/{tag}",
    }
    if parent_run.get("path") not in parent_paths:
        raise PermissionError("parent run is not the release workflow")
    if parent_run.get("head_sha") != expected_commit or parent_run.get("head_branch") != tag:
        raise PermissionError("parent release run is not bound to the exact tag commit")
    if parent_run.get("status") != "in_progress" or parent_run.get("conclusion") is not None:
        raise PermissionError("parent release run is not actively coordinating publication")
    parent_repository = parent_run.get("repository")
    if (
        not isinstance(parent_repository, Mapping)
        or parent_repository.get("full_name") != repository
    ):
        raise PermissionError("parent release run belongs to another repository")
    if _login(parent_run.get("actor"), "actor") != _OPERATOR:
        raise PermissionError(f"parent release actor must be {_OPERATOR}")
    if _login(parent_run.get("triggering_actor"), "triggering actor") != _OPERATOR:
        raise PermissionError(f"parent release triggering actor must be {_OPERATOR}")

    expected_runs = {
        distribution: workflow
        for distribution, workflow in PUBLISHER_WORKFLOWS.items()
        if workflow != "release.yml"
    }
    expected_allowlist_keys = {
        "schema",
        "repository",
        "parent_run_id",
        "parent_run_attempt",
        "tag",
        "commit",
        "tag_object",
        "registry",
        "runs",
    }
    if set(allowlist) != expected_allowlist_keys:
        raise PermissionError("publisher dispatch allowlist has unexpected fields")
    if allowlist.get("schema") != "archetype.publisher-dispatch/v2":
        raise PermissionError("publisher dispatch allowlist has an unsupported schema")
    for field, expected in (
        ("repository", repository),
        ("parent_run_id", parent_run_id),
        ("parent_run_attempt", parent_run_attempt),
        ("tag", tag),
        ("commit", expected_commit),
        ("tag_object", expected_tag_object),
        ("registry", registry),
    ):
        if allowlist.get(field) != expected:
            raise PermissionError(f"publisher dispatch allowlist {field} differs")
    raw_runs = allowlist.get("runs")
    if not isinstance(raw_runs, list) or len(raw_runs) != len(expected_runs):
        raise PermissionError("publisher dispatch allowlist has an incomplete run matrix")
    observed_runs: dict[str, Mapping[str, Any]] = {}
    for value in raw_runs:
        if not isinstance(value, Mapping):
            raise TypeError("publisher dispatch allowlist run must be an object")
        if set(value) != {"distribution", "workflow", "run_id", "url"}:
            raise PermissionError("publisher dispatch allowlist run has unexpected fields")
        run_distribution = value.get("distribution")
        run_workflow = value.get("workflow")
        run_id = value.get("run_id")
        url = value.get("url")
        if (
            not isinstance(run_distribution, str)
            or run_distribution in observed_runs
            or expected_runs.get(run_distribution) != run_workflow
            or not isinstance(run_id, int)
            or isinstance(run_id, bool)
            or run_id < 1
            or url != f"https://github.com/{repository}/actions/runs/{run_id}"
        ):
            raise PermissionError("publisher dispatch allowlist contains an invalid run")
        observed_runs[run_distribution] = value
    if observed_runs.keys() != expected_runs.keys():
        raise PermissionError("publisher dispatch allowlist has the wrong distributions")
    selected = observed_runs[distribution]
    if selected.get("workflow") != expected_workflow or selected.get("run_id") != child_run_id:
        raise PermissionError("publisher run is not authorized by the parent dispatch")

    return {
        "repository": repository,
        "workflow": expected_workflow,
        "tag": tag,
        "commit": expected_commit,
        "tag_object": expected_tag_object,
        "registry": registry,
        "environment": _REGISTRY_ENVIRONMENTS[registry],
        "child_run_id": child_run_id,
        "parent_run_id": parent_run_id,
        "parent_run_attempt": parent_run_attempt,
    }


def _fetch_parent_run(repository: str, run_id: int, token: str) -> dict[str, Any]:
    if not token:
        raise ValueError("publisher authorization requires GITHUB_TOKEN")
    url = f"https://api.github.com/repos/{repository}/actions/runs/{run_id}"
    request = Request(
        url,
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
            "User-Agent": "archetype-release-publisher",
            "X-GitHub-Api-Version": _API_VERSION,
        },
    )
    try:
        with _OPENER.open(request, timeout=30) as response:
            payload = json.load(response)
    except HTTPError as error:
        raise RuntimeError(f"GitHub parent run lookup failed with HTTP {error.code}") from error
    except (HTTPException, OSError, URLError, json.JSONDecodeError) as error:
        reason = getattr(error, "reason", error)
        raise RuntimeError(f"GitHub parent run lookup failed: {reason}") from error
    if not isinstance(payload, dict):
        raise TypeError("GitHub parent run response must be an object")
    return payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--expected-workflow", required=True)
    parser.add_argument("--distribution", required=True)
    parser.add_argument("--child-run-id", type=int, required=True)
    parser.add_argument("--parent-run-id", type=int, required=True)
    parser.add_argument("--parent-run-attempt", type=int, required=True)
    parser.add_argument("--tag", required=True)
    parser.add_argument("--expected-commit", required=True)
    parser.add_argument("--expected-tag-object", required=True)
    parser.add_argument("--registry", choices=tuple(_REGISTRY_ENVIRONMENTS), required=True)
    parser.add_argument("--allowlist", type=Path, required=True)
    args = parser.parse_args(argv)
    token = os.environ.get("GITHUB_TOKEN", "")
    parent_run = _fetch_parent_run(_REPOSITORY, args.parent_run_id, token)
    allowlist = json.loads(args.allowlist.read_text(encoding="utf-8"))
    if not isinstance(allowlist, dict):
        raise TypeError("publisher dispatch allowlist must be an object")
    result = verify_publisher_dispatch(
        repository=os.environ.get("GITHUB_REPOSITORY", ""),
        event_name=os.environ.get("GITHUB_EVENT_NAME", ""),
        ref=os.environ.get("GITHUB_REF", ""),
        ref_name=os.environ.get("GITHUB_REF_NAME", ""),
        ref_type=os.environ.get("GITHUB_REF_TYPE", ""),
        commit=os.environ.get("GITHUB_SHA", ""),
        workflow_ref=os.environ.get("GITHUB_WORKFLOW_REF", ""),
        actor=os.environ.get("GITHUB_ACTOR", ""),
        triggering_actor=os.environ.get("GITHUB_TRIGGERING_ACTOR", ""),
        expected_workflow=args.expected_workflow,
        distribution=args.distribution,
        child_run_id=args.child_run_id,
        parent_run_id=args.parent_run_id,
        parent_run_attempt=args.parent_run_attempt,
        parent_run=parent_run,
        allowlist=allowlist,
        tag=args.tag,
        expected_commit=args.expected_commit,
        expected_tag_object=args.expected_tag_object,
        registry=args.registry,
    )
    print(
        "Authorized publisher dispatch: "
        f"{result['workflow']} -> {result['registry']} for {result['tag']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
