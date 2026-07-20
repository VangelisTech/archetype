# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Repository contracts for the provider-neutral coding-agent harness."""

from __future__ import annotations

import asyncio
import os
import subprocess
from pathlib import Path

import pytest

from archetype.missions import (
    AgentExecutionStatus,
    CommandValidator,
    RepositoryPublicationPolicy,
)
from archetype.missions.coding_agents import (
    AgentProcessObservation,
    CodingAgentHarness,
    CodingAgentHarnessConfig,
    DispatchedValidator,
    TaskDispatchRequest,
    ValidationObservation,
)
from archetype.missions.sandboxes import (
    CheckpointRef,
    ProcessRequest,
    ProcessResult,
    SandboxCapabilities,
    SandboxIdentity,
    SandboxStatus,
)


def _git(*arguments: str, cwd: Path | None = None) -> str:
    result = subprocess.run(
        ("git", *arguments),
        cwd=cwd,
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def _remote(tmp_path: Path) -> Path:
    seed = tmp_path / "seed"
    seed.mkdir()
    _git("init", "-b", "main", cwd=seed)
    _git("config", "user.name", "Fixture", cwd=seed)
    _git("config", "user.email", "fixture@example.com", cwd=seed)
    (seed / "README.md").write_text("seed\n", encoding="utf-8")
    _git("add", "README.md", cwd=seed)
    _git("commit", "-m", "seed", cwd=seed)
    remote = tmp_path / "remote.git"
    _git("clone", "--bare", str(seed), str(remote))
    return remote


class _LocalSession:
    def __init__(self) -> None:
        self._closed = False

    @property
    def identity(self) -> SandboxIdentity:
        return SandboxIdentity("local", "local-contract", "test-environment")

    @property
    def capabilities(self) -> SandboxCapabilities:
        return SandboxCapabilities(secret_names=("github",))

    async def status(self) -> SandboxStatus:
        return SandboxStatus.CLOSED if self._closed else SandboxStatus.READY

    async def exec(self, request: ProcessRequest) -> ProcessResult:
        environment = os.environ.copy()
        environment.update(request.environment_dict())
        process = await asyncio.create_subprocess_exec(
            *request.argv,
            cwd=request.workdir,
            env=environment,
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await asyncio.wait_for(
            process.communicate(), timeout=request.timeout_seconds
        )
        return ProcessResult(
            request.argv,
            int(process.returncode or 0),
            stdout.decode(),
            stderr.decode(),
        )

    async def checkpoint(self) -> CheckpointRef:
        raise NotImplementedError

    async def close(self) -> None:
        self._closed = True


class _EditingDriver:
    def __init__(self, workspace: Path, *, commit: bool) -> None:
        self.workspace = workspace
        self.commit = commit

    async def run(self, session, request, prompt: str) -> AgentProcessObservation:
        command = "printf 'done\\n' > feature.txt"
        if self.commit:
            command += " && git add feature.txt && git commit -m 'agent-authored change'"
        result = await session.exec(
            ProcessRequest(
                ("sh", "-lc", command),
                workdir=str(self.workspace),
            )
        )
        return AgentProcessObservation(
            result.returncode,
            result.stdout,
            result.stderr,
            session_id="agent-session",
        )


def _request(remote: Path, *, validator_command: tuple[str, ...]) -> TaskDispatchRequest:
    return TaskDispatchRequest(
        mission_id=1,
        task_id=2,
        task_name="implementation",
        dispatch_id="dispatch-1",
        dispatch_sequence=1,
        repository=str(remote),
        branch="agent/harness-contract",
        base_ref="main",
        prompt="Create feature.txt.",
        validators=(
            DispatchedValidator(
                validator_id=3,
                spec=CommandValidator("feature", validator_command),
            ),
        ),
        publication_policy=RepositoryPublicationPolicy.COMMIT_AND_PUSH,
    )


@pytest.mark.parametrize("agent_commits", [False, True])
@pytest.mark.asyncio
async def test_harness_preserves_agent_commits_and_publishes_the_validated_tree(
    tmp_path: Path,
    agent_commits: bool,
) -> None:
    remote = _remote(tmp_path)
    workspace = tmp_path / "sandbox" / "repo"
    harness = CodingAgentHarness(
        _EditingDriver(workspace, commit=agent_commits),
        CodingAgentHarnessConfig(workspace=str(workspace)),
    )
    result = await harness.execute(
        _LocalSession(),
        _request(remote, validator_command=("sh", "-lc", "test -f feature.txt")),
    )

    assert result.status is AgentExecutionStatus.EXITED
    assert len(result.validation) == 1
    assert result.validation[0].passed is True
    assert result.final_revision
    assert result.commits[-1].sha == result.final_revision
    assert result.commits[-1].pushed is True
    assert result.commits[-1].final_revision is True
    assert [commit.message for commit in result.commits] == [
        "agent-authored change" if agent_commits else "implementation: Create feature.txt."
    ]
    assert (
        _git(
            "--git-dir",
            str(remote),
            "rev-parse",
            "refs/heads/agent/harness-contract",
        )
        == result.final_revision
    )


@pytest.mark.asyncio
async def test_failed_validator_is_an_observation_not_a_sandbox_verdict(tmp_path: Path) -> None:
    remote = _remote(tmp_path)
    workspace = tmp_path / "sandbox" / "repo"
    harness = CodingAgentHarness(
        _EditingDriver(workspace, commit=False),
        CodingAgentHarnessConfig(workspace=str(workspace)),
    )
    result = await harness.execute(
        _LocalSession(),
        _request(remote, validator_command=("sh", "-lc", "exit 7")),
    )

    assert result.status is AgentExecutionStatus.EXITED
    assert result.validation[0].actual_returncode == 7
    assert result.validation[0].passed is False
    assert result.commits == ()
    assert result.error == ""
    assert [friction.kind for friction in result.friction] == ["validation"]
    assert "outcome" not in result.__dataclass_fields__
    assert "accepted" not in result.__dataclass_fields__


def test_validator_verdict_is_derived_from_actual_and_expected_codes() -> None:
    result = ValidationObservation(
        validator_id=1,
        name="expected-red",
        command=("pytest", "-q"),
        expected_returncode=1,
        actual_returncode=1,
        revision="abc123",
    )
    assert result.passed is True
    assert "passed" not in result.__dataclass_fields__
