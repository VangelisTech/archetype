# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Behavioral contracts for the checkout-free publisher tag reauthorization."""

from __future__ import annotations

import os
import re
import subprocess
from pathlib import Path

import pytest

from scripts.release_artifact import PUBLISHER_WORKFLOWS

ROOT = Path(__file__).resolve().parents[2]
RELEASE_WORKFLOW = ROOT / ".github" / "workflows" / "release.yml"
EXPECTED_COMMIT = "a" * 40
EXPECTED_TAG_OBJECT = "b" * 40
REPLACEMENT_TAG_OBJECT = "c" * 40
OTHER_COMMIT = "d" * 40
TAG = "v0.6.0"
TAG_REF = f"refs/tags/{TAG}"
REMOTE = "https://github.com/VangelisTech/archetype.git"


def _job(workflow: str, job_id: str) -> str:
    match = re.search(
        rf"^  {re.escape(job_id)}:\n(?P<body>.*?)(?=^  [a-z][a-z0-9-]*:\n|\Z)",
        workflow,
        re.MULTILINE | re.DOTALL,
    )
    assert match is not None, f"workflow lost the {job_id!r} job"
    return match.group("body")


def _publisher_jobs() -> tuple[tuple[Path, str], ...]:
    jobs: list[tuple[Path, str]] = []
    for workflow in PUBLISHER_WORKFLOWS.values():
        path = ROOT / ".github" / "workflows" / workflow
        job_ids = (
            ("publish-testpypi", "publish")
            if workflow == "release.yml"
            else ("publish-testpypi", "publish-pypi")
        )
        jobs.extend((path, job_id) for job_id in job_ids)
    return tuple(jobs)


def _tag_check_bodies() -> tuple[str, ...]:
    bodies: list[str] = []
    for path, job_id in _publisher_jobs():
        workflow = path.read_text(encoding="utf-8")
        job = _job(workflow, job_id)
        step_marker = "      - name: Reauthorize the remote release tag\n"
        assert job.count(step_marker) == 1
        _, step = job.split(step_marker)
        run_marker = "        run: |\n"
        assert step.count(run_marker) == 1
        _, shell_and_publisher = step.split(run_marker)
        raw_body, publisher_marker, _ = shell_and_publisher.partition(
            "      - uses: pypa/gh-action-pypi-publish@"
        )
        assert publisher_marker

        lines = raw_body.splitlines()
        assert lines
        assert all(not line or line.startswith("          ") for line in lines)
        bodies.append("\n".join(line[10:] if line else "" for line in lines) + "\n")

    assert len(bodies) == len(PUBLISHER_WORKFLOWS) * 2
    return tuple(bodies)


def _tag_check_body() -> str:
    bodies = _tag_check_bodies()
    assert len(set(bodies)) == 1
    return bodies[0]


def _run_tag_check(
    tmp_path: Path,
    *,
    output: str,
    tag: str = TAG,
    expected_commit: str = EXPECTED_COMMIT,
    expected_tag_object: str = EXPECTED_TAG_OBJECT,
    git_status: int = 0,
    expect_git: bool = True,
) -> subprocess.CompletedProcess[str]:
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    git_arguments = tmp_path / "git-arguments.txt"
    fake_git = fake_bin / "git"
    fake_git.write_text(
        "#!/usr/bin/env bash\n"
        'printf \'%s\\n\' "$@" > "$FAKE_GIT_ARGUMENTS"\n'
        "printf '%s' \"$FAKE_GIT_OUTPUT\"\n"
        'exit "$FAKE_GIT_STATUS"\n',
        encoding="utf-8",
    )
    fake_git.chmod(0o755)

    environment = {
        **os.environ,
        "PATH": f"{fake_bin}{os.pathsep}{os.environ['PATH']}",
        "FAKE_GIT_ARGUMENTS": str(git_arguments),
        "FAKE_GIT_OUTPUT": output,
        "FAKE_GIT_STATUS": str(git_status),
        "GITHUB_REF": f"refs/tags/{tag}",
        "GITHUB_REF_NAME": tag,
        "GITHUB_REF_TYPE": "tag",
        "GITHUB_REPOSITORY": "VangelisTech/archetype",
        "GITHUB_SHA": expected_commit,
        "EXPECTED_TAG_OBJECT": expected_tag_object,
        "RELEASE_INPUT_TAG": tag,
    }
    result = subprocess.run(  # noqa: S603 - exact trusted workflow shell under test
        ["bash", "-c", _tag_check_body()],
        cwd=tmp_path,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )

    if expect_git:
        assert git_arguments.read_text(encoding="utf-8").splitlines() == [
            "ls-remote",
            "--exit-code",
            REMOTE,
            f"refs/tags/{tag}",
            f"refs/tags/{tag}^{{}}",
        ]
    else:
        assert not git_arguments.exists()
    return result


def test_publisher_jobs_execute_identical_tag_check_shells() -> None:
    bodies = _tag_check_bodies()

    assert len(set(bodies)) == 1
    assert all("${{" not in body for body in bodies)


def test_publisher_tag_check_accepts_a_lightweight_tag(tmp_path: Path) -> None:
    result = _run_tag_check(
        tmp_path,
        output=f"{EXPECTED_COMMIT}\t{TAG_REF}\n",
        expected_tag_object=EXPECTED_COMMIT,
    )

    assert result.returncode == 0, result.stderr


def test_publisher_tag_check_accepts_an_annotated_tag(tmp_path: Path) -> None:
    result = _run_tag_check(
        tmp_path,
        output=(f"{EXPECTED_TAG_OBJECT}\t{TAG_REF}\n{EXPECTED_COMMIT}\t{TAG_REF}^{{}}\n"),
    )

    assert result.returncode == 0, result.stderr


def test_publisher_tag_check_rejects_a_moved_commit(tmp_path: Path) -> None:
    result = _run_tag_check(
        tmp_path,
        output=f"{OTHER_COMMIT}\t{TAG_REF}\n",
        expected_tag_object=OTHER_COMMIT,
    )

    assert result.returncode != 0
    assert "remote tag commit differs" in result.stderr


def test_publisher_tag_check_rejects_replaced_annotated_tag_object(tmp_path: Path) -> None:
    result = _run_tag_check(
        tmp_path,
        output=(f"{REPLACEMENT_TAG_OBJECT}\t{TAG_REF}\n{EXPECTED_COMMIT}\t{TAG_REF}^{{}}\n"),
    )

    assert result.returncode != 0
    assert "remote tag object differs" in result.stderr


def test_publisher_tag_check_rejects_a_duplicate_ref(tmp_path: Path) -> None:
    result = _run_tag_check(
        tmp_path,
        output=(f"{EXPECTED_COMMIT}\t{TAG_REF}\n{EXPECTED_COMMIT}\t{TAG_REF}\n"),
    )

    assert result.returncode != 0
    assert "duplicate direct tag ref" in result.stderr


@pytest.mark.parametrize(
    "output",
    [
        f"{EXPECTED_COMMIT} {TAG_REF}\n",
        f"{EXPECTED_COMMIT}\t{TAG_REF}\textra\n",
    ],
    ids=("missing-tab", "extra-field"),
)
def test_publisher_tag_check_rejects_malformed_output(
    tmp_path: Path,
    output: str,
) -> None:
    result = _run_tag_check(tmp_path, output=output)

    assert result.returncode != 0
    assert "malformed ls-remote output" in result.stderr


def test_publisher_tag_check_rejects_an_unexpected_ref(tmp_path: Path) -> None:
    result = _run_tag_check(
        tmp_path,
        output=f"{EXPECTED_COMMIT}\trefs/tags/v0.6.1\n",
    )

    assert result.returncode != 0
    assert "unexpected remote ref" in result.stderr


def test_publisher_tag_check_requires_the_direct_ref(tmp_path: Path) -> None:
    result = _run_tag_check(
        tmp_path,
        output=f"{EXPECTED_COMMIT}\t{TAG_REF}^{{}}\n",
    )

    assert result.returncode != 0
    assert "direct tag ref is absent" in result.stderr


@pytest.mark.parametrize(
    "tag",
    ["v00.6.0", "v0.6.0; echo injected"],
    ids=("noncanonical", "shell-injection"),
)
def test_publisher_tag_check_rejects_an_invalid_tag(
    tmp_path: Path,
    tag: str,
) -> None:
    result = _run_tag_check(
        tmp_path,
        output=f"{EXPECTED_COMMIT}\t{TAG_REF}\n",
        tag=tag,
        expect_git=False,
    )

    assert result.returncode != 0
    assert "tag must be canonical" in result.stderr
    assert "injected" not in result.stdout


def test_publisher_tag_check_fails_closed_when_git_fails(tmp_path: Path) -> None:
    result = _run_tag_check(
        tmp_path,
        output="",
        git_status=2,
    )

    assert result.returncode != 0
