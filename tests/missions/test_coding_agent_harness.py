# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Repository contracts for the provider-neutral coding-agent harness."""

from __future__ import annotations

import asyncio
import hashlib
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
    CodexDriver,
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
    def __init__(
        self,
        workspace: Path,
        *,
        commit: bool,
        returncode: int = 0,
        trace_uri: str = "",
        filename: str = "feature.txt",
    ) -> None:
        self.workspace = workspace
        self.commit = commit
        self.returncode = returncode
        self.trace_uri = trace_uri
        self.filename = filename

    async def run(self, session, request, prompt: str) -> AgentProcessObservation:
        command = f"printf 'done\\n' > {self.filename}"
        if self.commit:
            command += f" && git add {self.filename} && git commit -m 'agent-authored change'"
        result = await session.exec(
            ProcessRequest(
                ("sh", "-lc", command),
                workdir=str(self.workspace),
            )
        )
        return AgentProcessObservation(
            self.returncode or result.returncode,
            result.stdout,
            result.stderr or ("agent failed after editing" if self.returncode else ""),
            session_id="agent-session",
            trace_uri=self.trace_uri,
        )


class _RawByteEditingDriver:
    def __init__(self, workspace: Path) -> None:
        self.workspace = workspace

    async def run(self, session, request, prompt: str) -> AgentProcessObservation:
        del request, prompt
        result = await session.exec(
            ProcessRequest(
                (
                    "python",
                    "-c",
                    "from pathlib import Path; Path('latin.txt').write_bytes(b'value=\\xe9\\n')",
                ),
                workdir=str(self.workspace),
            )
        )
        return AgentProcessObservation(
            result.returncode,
            result.stdout,
            result.stderr,
            session_id="raw-byte-agent",
        )


class _TraceSession:
    def __init__(self) -> None:
        self.requests: list[ProcessRequest] = []

    @property
    def identity(self) -> SandboxIdentity:
        return SandboxIdentity("modal", "sb-trace", "test-environment")

    @property
    def capabilities(self) -> SandboxCapabilities:
        return SandboxCapabilities(
            live_output=True,
            secret_names=("codex_oauth",),
        )

    async def status(self) -> SandboxStatus:
        return SandboxStatus.READY

    async def exec(self, request: ProcessRequest) -> ProcessResult:
        self.requests.append(request)
        return ProcessResult(
            request.argv,
            0,
            stdout='{"type":"thread.started","thread_id":"thread-1"}\n',
            trace_uri=(
                "modal-sandbox://sb-trace/tmp/archetype-agent-missions/live/"
                "executions/process-1/agent.stdout.log"
            ),
        )

    async def checkpoint(self) -> CheckpointRef:
        raise NotImplementedError

    async def close(self) -> None:
        return None


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


@pytest.mark.asyncio
async def test_codex_driver_forwards_provider_trace() -> None:
    session = _TraceSession()
    request = _request(Path("/tmp/remote.git"), validator_command=("true",))

    observed = await CodexDriver().run(session, request, "Fix it.")

    assert len(session.requests) == 1
    assert observed.session_id == "thread-1"
    assert observed.trace_uri == (
        "modal-sandbox://sb-trace/tmp/archetype-agent-missions/live/"
        "executions/process-1/agent.stdout.log"
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
        _EditingDriver(
            workspace,
            commit=agent_commits,
            trace_uri="local-sandbox://local-contract/traces/dispatch-1",
        ),
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
    assert result.trace_uri == "local-sandbox://local-contract/traces/dispatch-1"
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
async def test_author_diff_digest_hashes_exact_git_bytes_before_publication(
    tmp_path: Path,
) -> None:
    remote = _remote(tmp_path)
    setup = tmp_path / "setup"
    _git("clone", str(remote), str(setup))
    _git("config", "user.name", "Fixture", cwd=setup)
    _git("config", "user.email", "fixture@example.com", cwd=setup)
    (setup / ".gitattributes").write_text("latin.txt diff\n", encoding="utf-8")
    (setup / "latin.txt").write_bytes(b"base\n")
    _git("add", ".gitattributes", "latin.txt", cwd=setup)
    _git("commit", "-m", "add byte-exact text fixture", cwd=setup)
    _git("push", "origin", "main", cwd=setup)

    workspace = tmp_path / "sandbox" / "repo"
    harness = CodingAgentHarness(
        _RawByteEditingDriver(workspace),
        CodingAgentHarnessConfig(workspace=str(workspace)),
    )

    result = await harness.execute(
        _LocalSession(),
        _request(remote, validator_command=("true",)),
    )
    raw_diff = subprocess.run(
        (
            "git",
            "diff",
            "--no-ext-diff",
            "--no-textconv",
            "--binary",
            result.starting_revision,
            result.final_revision,
        ),
        cwd=workspace,
        check=True,
        capture_output=True,
    ).stdout

    assert result.status is AgentExecutionStatus.EXITED
    assert b"\xe9" in raw_diff
    assert result.diff_digest == hashlib.sha256(raw_diff).hexdigest()
    assert (
        result.diff_digest != hashlib.sha256(raw_diff.decode(errors="replace").encode()).hexdigest()
    )


@pytest.mark.asyncio
async def test_validator_can_scope_agent_commit_from_task_base_revision(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ARCHETYPE_TASK_BASE_REVISION", "ambient-value-must-not-win")
    remote = _remote(tmp_path)
    workspace = tmp_path / "sandbox" / "repo"
    harness = CodingAgentHarness(
        _EditingDriver(workspace, commit=True),
        CodingAgentHarnessConfig(workspace=str(workspace)),
    )

    result = await harness.execute(
        _LocalSession(),
        _request(
            remote,
            validator_command=(
                "sh",
                "-lc",
                "printf '%s\\n' \"$ARCHETYPE_TASK_BASE_REVISION\" && "
                'test "$ARCHETYPE_TASK_BASE_REVISION" = "$(git rev-parse HEAD^)" && '
                'test "$(git diff --name-only '
                '"$ARCHETYPE_TASK_BASE_REVISION"...HEAD)" = feature.txt',
            ),
        ),
    )

    assert result.validation[0].passed is True
    assert result.validation[0].stdout.strip() == result.starting_revision
    assert result.starting_revision != "ambient-value-must-not-win"


@pytest.mark.asyncio
async def test_validator_task_base_diff_includes_dirty_tracked_changes(tmp_path: Path) -> None:
    remote = _remote(tmp_path)
    workspace = tmp_path / "sandbox" / "repo"
    harness = CodingAgentHarness(
        _EditingDriver(workspace, commit=False, filename="README.md"),
        CodingAgentHarnessConfig(workspace=str(workspace)),
    )

    result = await harness.execute(
        _LocalSession(),
        _request(
            remote,
            validator_command=(
                "sh",
                "-lc",
                'test -n "$ARCHETYPE_TASK_BASE_REVISION" && '
                "git merge-base --is-ancestor "
                '"$ARCHETYPE_TASK_BASE_REVISION" HEAD && '
                'test "$(git diff --name-only '
                '"$ARCHETYPE_TASK_BASE_REVISION" --)" = README.md',
            ),
        ),
    )

    assert result.validation[0].passed is True
    assert result.commits[-1].pushed is True


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


@pytest.mark.asyncio
async def test_validators_still_decide_and_publish_after_agent_process_exits_nonzero(
    tmp_path: Path,
) -> None:
    remote = _remote(tmp_path)
    workspace = tmp_path / "sandbox" / "repo"
    harness = CodingAgentHarness(
        _EditingDriver(workspace, commit=False, returncode=9),
        CodingAgentHarnessConfig(workspace=str(workspace)),
    )

    result = await harness.execute(
        _LocalSession(),
        _request(remote, validator_command=("sh", "-lc", "test -f feature.txt")),
    )

    assert result.agent_returncode == 9
    assert result.validation[0].passed is True
    assert result.commits[-1].pushed is True
    assert result.final_revision == result.commits[-1].sha
    assert [item.kind for item in result.friction] == ["agent_process"]


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
