#!/usr/bin/env python3
# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Revalidate the exact immutable release ref immediately before publication."""

from __future__ import annotations

import argparse
import re
import subprocess
from collections.abc import Callable, Sequence
from pathlib import Path

_COMMIT = re.compile(r"[0-9a-f]{40}\Z")
_TAG = re.compile(r"v[0-9]+\.[0-9]+\.[0-9]+\Z")
_REPOSITORY = "VangelisTech/archetype"
_OPERATOR = "everettVT"
Run = Callable[..., subprocess.CompletedProcess[str]]


def _git(root: Path, arguments: Sequence[str], *, run: Run) -> str:
    process = run(
        ["git", *arguments],
        cwd=root,
        check=False,
        capture_output=True,
        text=True,
    )
    if process.returncode:
        raise RuntimeError(
            f"release ref git command failed: {' '.join(arguments)}\n"
            f"stdout:\n{process.stdout}\nstderr:\n{process.stderr}"
        )
    return process.stdout.strip()


def verify_release_ref(
    *,
    root: Path,
    tag: str,
    expected_commit: str,
    repository: str,
    actor: str,
    triggering_actor: str,
    run: Run = subprocess.run,
) -> dict[str, str]:
    """Require the authorized operator and one unchanged remote tag commit."""

    if repository != _REPOSITORY:
        raise ValueError(f"release repository must be {_REPOSITORY}")
    if actor != _OPERATOR or triggering_actor != _OPERATOR:
        raise PermissionError(f"release publication requires {_OPERATOR}")
    if _TAG.fullmatch(tag) is None:
        raise ValueError("release tag must be canonical vMAJOR.MINOR.PATCH")
    if _COMMIT.fullmatch(expected_commit) is None:
        raise ValueError("release expected commit must be a full Git commit")

    local_commit = _git(root, ("rev-parse", "HEAD"), run=run)
    if local_commit != expected_commit:
        raise ValueError(
            f"release checkout moved: expected {expected_commit}, observed {local_commit}"
        )

    reference = f"refs/tags/{tag}"
    remote = _git(root, ("ls-remote", "origin", reference, f"{reference}^{{}}"), run=run)
    references: dict[str, str] = {}
    for line in remote.splitlines():
        fields = line.split("\t")
        if len(fields) != 2 or _COMMIT.fullmatch(fields[0]) is None:
            raise ValueError("release remote returned malformed tag evidence")
        if fields[1] in references:
            raise ValueError("release remote returned duplicate tag evidence")
        references[fields[1]] = fields[0]
    remote_commit = references.get(f"{reference}^{{}}", references.get(reference))
    if set(references) - {reference, f"{reference}^{{}}"} or remote_commit is None:
        raise ValueError(f"release tag {tag!r} is missing or ambiguous on origin")
    if remote_commit != expected_commit:
        raise ValueError(f"release tag moved: expected {expected_commit}, observed {remote_commit}")
    return {
        "repository": repository,
        "tag": tag,
        "commit": expected_commit,
        "actor": actor,
        "triggering_actor": triggering_actor,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tag", required=True)
    parser.add_argument("--expected-commit", required=True)
    parser.add_argument("--repository", required=True)
    parser.add_argument("--actor", required=True)
    parser.add_argument("--triggering-actor", required=True)
    args = parser.parse_args(argv)
    result = verify_release_ref(
        root=Path.cwd(),
        tag=args.tag,
        expected_commit=args.expected_commit,
        repository=args.repository,
        actor=args.actor,
        triggering_actor=args.triggering_actor,
    )
    print(f"Authorized immutable release ref: {result['tag']} at {result['commit']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
