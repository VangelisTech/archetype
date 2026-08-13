# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Contracts for immediate pre-publication ref authorization."""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from scripts.verify_release_ref import verify_release_ref

COMMIT = "a" * 40


def _run(
    command: list[str],
    **_kwargs: object,
) -> subprocess.CompletedProcess[str]:
    if command[1:3] == ["rev-parse", "HEAD"]:
        return subprocess.CompletedProcess(command, 0, stdout=COMMIT + "\n", stderr="")
    return subprocess.CompletedProcess(
        command,
        0,
        stdout=f"{'b' * 40}\trefs/tags/v0.6.0\n{COMMIT}\trefs/tags/v0.6.0^{{}}\n",
        stderr="",
    )


def test_release_ref_accepts_exact_annotated_tag_and_operator(tmp_path: Path) -> None:
    result = verify_release_ref(
        root=tmp_path,
        tag="v0.6.0",
        expected_commit=COMMIT,
        repository="VangelisTech/archetype",
        actor="everettVT",
        triggering_actor="everettVT",
        run=_run,
    )

    assert result["commit"] == COMMIT
    assert result["tag"] == "v0.6.0"


def test_release_ref_rejects_rerun_by_another_actor(tmp_path: Path) -> None:
    with pytest.raises(PermissionError, match="requires everettVT"):
        verify_release_ref(
            root=tmp_path,
            tag="v0.6.0",
            expected_commit=COMMIT,
            repository="VangelisTech/archetype",
            actor="everettVT",
            triggering_actor="another-user",
            run=_run,
        )


def test_release_ref_rejects_moved_remote_tag(tmp_path: Path) -> None:
    def moved(command: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        if command[1:3] == ["rev-parse", "HEAD"]:
            return _run(command, **kwargs)
        return subprocess.CompletedProcess(
            command,
            0,
            stdout=f"{'b' * 40}\trefs/tags/v0.6.0\n",
            stderr="",
        )

    with pytest.raises(ValueError, match="release tag moved"):
        verify_release_ref(
            root=tmp_path,
            tag="v0.6.0",
            expected_commit=COMMIT,
            repository="VangelisTech/archetype",
            actor="everettVT",
            triggering_actor="everettVT",
            run=moved,
        )


def test_release_ref_rejects_duplicate_remote_evidence(tmp_path: Path) -> None:
    def duplicate(command: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        if command[1:3] == ["rev-parse", "HEAD"]:
            return _run(command, **kwargs)
        return subprocess.CompletedProcess(
            command,
            0,
            stdout=(f"{COMMIT}\trefs/tags/v0.6.0\n{COMMIT}\trefs/tags/v0.6.0\n"),
            stderr="",
        )

    with pytest.raises(ValueError, match="duplicate tag evidence"):
        verify_release_ref(
            root=tmp_path,
            tag="v0.6.0",
            expected_commit=COMMIT,
            repository="VangelisTech/archetype",
            actor="everettVT",
            triggering_actor="everettVT",
            run=duplicate,
        )
