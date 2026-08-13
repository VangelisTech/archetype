#!/usr/bin/env python3
# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Pin the release Mac runner group to one immutable release workflow ref."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
from collections.abc import Sequence
from typing import Any
from urllib.parse import quote

OWNER = "VangelisTech"
REPOSITORY = "archetype"
RELEASE_OPERATOR = "everettVT"
RUNNER_GROUP = "archetype-release-macos"
WORKFLOW_PATH = ".github/workflows/release.yml"
TAG_PATTERN = re.compile(r"v[0-9A-Za-z][0-9A-Za-z._+-]*\Z")


def _gh_api(
    endpoint: str,
    *,
    method: str = "GET",
    payload: dict[str, Any] | None = None,
) -> Any:
    command = ["gh", "api", "--method", method, endpoint]
    serialized = None
    if payload is not None:
        command.extend(("--input", "-"))
        serialized = json.dumps(payload)
    completed = subprocess.run(
        command,
        input=serialized,
        text=True,
        capture_output=True,
        check=False,
    )
    if completed.returncode != 0:
        detail = completed.stderr.strip() or completed.stdout.strip()
        raise RuntimeError(f"gh api failed for {endpoint}: {detail}")
    if not completed.stdout.strip():
        return None
    return json.loads(completed.stdout)


def _selected_workflow(tag: str) -> str:
    if TAG_PATTERN.fullmatch(tag) is None:
        raise ValueError("release tag must start with v and contain no slash")
    return f"{OWNER}/{REPOSITORY}/{WORKFLOW_PATH}@refs/tags/{tag}"


def _group_payload(repo_id: int, tag: str) -> dict[str, Any]:
    return {
        "name": RUNNER_GROUP,
        "visibility": "selected",
        "allows_public_repositories": True,
        "restricted_to_workflows": True,
        "selected_repository_ids": [repo_id],
        "selected_workflows": [_selected_workflow(tag)],
    }


def _require_release_ref(tag: str) -> tuple[int, str]:
    user = _gh_api("user")
    if user["login"] != RELEASE_OPERATOR:
        raise RuntimeError(f"gh must be authenticated as {RELEASE_OPERATOR}")

    repo = _gh_api(f"repos/{OWNER}/{REPOSITORY}")
    repo_id = int(repo["id"])
    encoded_tag = quote(tag, safe="")
    _gh_api(f"repos/{OWNER}/{REPOSITORY}/contents/{WORKFLOW_PATH}?ref={encoded_tag}")
    comparison = _gh_api(
        f"repos/{OWNER}/{REPOSITORY}/compare/{encoded_tag}...{repo['default_branch']}"
    )
    if comparison["status"] not in {"ahead", "identical"}:
        raise RuntimeError(
            f"{tag} must point to a commit on {repo['default_branch']}; "
            f"comparison was {comparison['status']}"
        )
    return repo_id, str(repo["default_branch"])


def configure(tag: str) -> dict[str, Any]:
    _selected_workflow(tag)
    repo_id, _ = _require_release_ref(tag)
    groups = _gh_api(f"orgs/{OWNER}/actions/runner-groups")["runner_groups"]
    group = next((row for row in groups if row["name"] == RUNNER_GROUP), None)

    payload = _group_payload(repo_id, tag)
    if group is None:
        group = _gh_api(
            f"orgs/{OWNER}/actions/runner-groups",
            method="POST",
            payload=payload,
        )
    else:
        group_id = int(group["id"])
        runners = _gh_api(f"orgs/{OWNER}/actions/runner-groups/{group_id}/runners")
        if int(runners["total_count"]) != 0:
            raise RuntimeError(
                "refusing to change runner-group authority while a runner is registered"
            )
        update = dict(payload)
        update.pop("selected_repository_ids")
        group = _gh_api(
            f"orgs/{OWNER}/actions/runner-groups/{group_id}",
            method="PATCH",
            payload=update,
        )
        _gh_api(
            f"orgs/{OWNER}/actions/runner-groups/{group_id}/repositories/{repo_id}",
            method="PUT",
        )

    expected = [_selected_workflow(tag)]
    if not group["restricted_to_workflows"] or group["selected_workflows"] != expected:
        raise RuntimeError("GitHub did not retain the exact workflow restriction")
    if group["visibility"] != "selected" or not group["allows_public_repositories"]:
        raise RuntimeError("GitHub did not retain the selected public repository policy")
    return group


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("tag", help="existing release tag, for example v0.5.0")
    args = parser.parse_args(argv)
    group = configure(args.tag)
    print(
        f"runner group {group['name']} ({group['id']}) is pinned to "
        f"{group['selected_workflows'][0]}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
