# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Dogfood contract for the batteries-included Agent Missions V1 surface."""

from __future__ import annotations

import asyncio
import os
import subprocess
from pathlib import Path

import pytest

from archetype import ArchetypeRuntime
from archetype.core.config import StorageConfig
from archetype.missions import (
    AgentExecution,
    AgentMissionConfig,
    AgentTask,
    Checkpoint,
    CommandValidator,
    Commit,
    DependsOn,
    FrictionLog,
    RepositoryPublicationPolicy,
    Sandbox,
    TaskPolicy,
    TaskState,
    TaskValidator,
    ValidationResult,
)
from archetype.missions.coding_agents import AgentProcessObservation
from archetype.missions.relations import Guards
from archetype.missions.sandboxes import (
    CheckpointRef,
    ProcessRequest,
    ProcessResult,
    SandboxCapabilities,
    SandboxEvent,
    SandboxEventType,
    SandboxIdentity,
    SandboxSpec,
    SandboxStatus,
)
from archetype.missions.sandboxes.apple_container import AppleContainerSandboxSession
from archetype.projections import latest


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
    def __init__(self, spec: SandboxSpec) -> None:
        self.spec = spec
        self.closed = 0
        self.close_attempts = 0
        self.close_error = False
        self.requests: list[ProcessRequest] = []

    @property
    def identity(self) -> SandboxIdentity:
        return SandboxIdentity("local", "sandbox-contract", self.spec.environment)

    @property
    def capabilities(self) -> SandboxCapabilities:
        return SandboxCapabilities(secret_names=("github",))

    async def status(self) -> SandboxStatus:
        return SandboxStatus.CLOSED if self.closed else SandboxStatus.READY

    async def exec(self, request: ProcessRequest) -> ProcessResult:
        self.requests.append(request)
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
            int(process.returncode),
            stdout.decode(),
            stderr.decode(),
        )

    async def checkpoint(self) -> CheckpointRef:
        raise NotImplementedError

    async def close(self) -> None:
        self.close_attempts += 1
        if self.close_error:
            raise RuntimeError("provider close unavailable")
        self.closed += 1


class _LocalBackend:
    name = "local"

    def __init__(self) -> None:
        self.creates = 0
        self.session: _LocalSession | None = None

    async def create(self, spec: SandboxSpec) -> _LocalSession:
        self.creates += 1
        self.session = _LocalSession(spec)
        return self.session

    async def restore(self, spec: SandboxSpec, checkpoint: CheckpointRef) -> _LocalSession:
        raise NotImplementedError


class _LiveOutputSession(_LocalSession):
    @property
    def capabilities(self) -> SandboxCapabilities:
        return SandboxCapabilities(live_output=True, secret_names=("github",))


class _LiveOutputBackend(_LocalBackend):
    async def create(self, spec: SandboxSpec) -> _LiveOutputSession:
        self.creates += 1
        session = _LiveOutputSession(spec)
        self.session = session
        return session


class _AutoReplacementSession(_LocalSession):
    def __init__(
        self,
        spec: SandboxSpec,
        sandbox_id: str,
        *,
        fail_checkpoint: bool = False,
        close_failures: int = 0,
    ) -> None:
        super().__init__(spec)
        self.sandbox_id = sandbox_id
        self.fail_checkpoint = fail_checkpoint
        self.checkpoints = 0
        self.close_failures = close_failures
        self._status = SandboxStatus.READY

    @property
    def identity(self) -> SandboxIdentity:
        return SandboxIdentity("local", self.sandbox_id, self.spec.environment)

    @property
    def capabilities(self) -> SandboxCapabilities:
        return SandboxCapabilities(checkpoints=True, secret_names=("github",))

    async def status(self) -> SandboxStatus:
        return self._status

    async def exec(self, request: ProcessRequest) -> ProcessResult:
        if self._status is not SandboxStatus.READY:
            raise RuntimeError(f"local sandbox session is {self._status.value}")
        return await super().exec(request)

    async def checkpoint(self) -> CheckpointRef:
        self.checkpoints += 1
        if self.fail_checkpoint:
            self._status = SandboxStatus.ERRORED
            raise RuntimeError("simulated checkpoint restart failure")
        return CheckpointRef(
            "local",
            f"checkpoint-{self.sandbox_id}",
            f"local-checkpoint://{self.sandbox_id}",
            1,
            environment=self.spec.environment,
            source_sandbox_id=self.sandbox_id,
            owner_id=self.spec.metadata_dict()["mission"],
        )

    async def close(self) -> None:
        self.close_attempts += 1
        if self.close_error or self.close_failures:
            self.close_failures = max(0, self.close_failures - 1)
            self._status = SandboxStatus.ERRORED
            raise RuntimeError("provider close unavailable")
        self.closed += 1
        self._status = SandboxStatus.CLOSED

    def simulate_transport_loss(self) -> None:
        self._status = SandboxStatus.ERRORED


class _AutoReplacementBackend:
    name = "local"

    def __init__(
        self,
        *,
        fail_first_checkpoint: bool = False,
        fail_first_close: bool = False,
        fail_create_sequences: tuple[int, ...] = (),
    ) -> None:
        self.fail_first_checkpoint = fail_first_checkpoint
        self.fail_first_close = fail_first_close
        self.fail_create_sequences = fail_create_sequences
        self.create_attempts = 0
        self.sessions: list[_AutoReplacementSession] = []

    async def create(self, spec: SandboxSpec) -> _AutoReplacementSession:
        self.create_attempts += 1
        sequence = self.create_attempts
        if sequence in self.fail_create_sequences:
            raise RuntimeError(f"simulated create failure {sequence}")
        session = _AutoReplacementSession(
            spec,
            f"sandbox-replacement-{sequence}",
            fail_checkpoint=self.fail_first_checkpoint and sequence == 1,
            close_failures=1 if self.fail_first_close and sequence == 1 else 0,
        )
        self.sessions.append(session)
        return session

    async def restore(
        self,
        spec: SandboxSpec,
        checkpoint: CheckpointRef,
    ) -> _AutoReplacementSession:
        del spec, checkpoint
        raise NotImplementedError


class _ProviderFailureBackend:
    name = "local"

    async def create(self, spec: SandboxSpec) -> _LocalSession:
        del spec
        token = "ghp_" + "A" * 36
        output = "x" * 100 + token + "s" * 3_970
        assert len(output) == 4_110
        AppleContainerSandboxSession._raise(  # noqa: SLF001 - provider/app boundary probe
            ProcessResult(("container", "run"), 7, stderr=output),
            "provider preflight",
        )
        raise AssertionError("provider failure must raise")

    async def restore(self, spec: SandboxSpec, checkpoint: CheckpointRef) -> _LocalSession:
        del spec, checkpoint
        raise NotImplementedError


class _MissionDriver:
    """Fail one validator, repair in place, then complete the dependent task."""

    def __init__(self, workspace: Path) -> None:
        self.workspace = workspace
        self.requests = []

    async def run(self, session, request, prompt: str) -> AgentProcessObservation:
        self.requests.append(request)
        if request.task_name == "regression":
            content = "bad" if request.dispatch_sequence == 1 else "good"
            filename = "regression.txt"
        else:
            content = "fixed"
            filename = "implementation.txt"
        commit = (
            " && git add regression.txt && git commit -m 'rejected agent checkpoint'"
            if request.task_name == "regression" and request.dispatch_sequence == 1
            else ""
        )
        result = await session.exec(
            ProcessRequest(
                ("sh", "-lc", f"printf '%s\\n' {content} > {filename}{commit}"),
                workdir=str(self.workspace),
            )
        )
        return AgentProcessObservation(
            result.returncode,
            result.stdout,
            result.stderr,
            session_id=f"session-{request.task_name}",
        )


class _TransportLossThenRepairDriver:
    def __init__(self, workspace: Path) -> None:
        self.workspace = workspace
        self.calls = 0

    async def run(
        self,
        session,
        request,
        prompt: str,
    ) -> AgentProcessObservation:
        del request, prompt
        self.calls += 1
        assert isinstance(session, _AutoReplacementSession)
        if self.calls == 1:
            session.simulate_transport_loss()
            raise RuntimeError("simulated provider transport loss")

        result = await session.exec(
            ProcessRequest(
                ("sh", "-lc", "printf 'good\\n' > artifact.txt"),
                workdir=str(self.workspace),
            )
        )
        return AgentProcessObservation(
            result.returncode,
            result.stdout,
            result.stderr,
            session_id="repaired-session",
        )


class _SameTickTransportLossDriver:
    """Fail the first ready task while allowing its sibling to replace the session."""

    def __init__(self, workspace: Path) -> None:
        self.workspace = workspace
        self.calls: list[str] = []

    async def run(
        self,
        session,
        request,
        prompt: str,
    ) -> AgentProcessObservation:
        del prompt
        self.calls.append(request.task_name)
        assert isinstance(session, _AutoReplacementSession)
        if len(self.calls) == 1:
            session.simulate_transport_loss()
            raise RuntimeError("simulated batched provider transport loss")

        result = await session.exec(
            ProcessRequest(
                ("sh", "-lc", f"printf 'good\\n' > {request.task_name}.txt"),
                workdir=str(self.workspace),
            )
        )
        return AgentProcessObservation(
            result.returncode,
            result.stdout,
            result.stderr,
            session_id=f"session-{request.task_name}",
        )


class _CheckpointSession(_LocalSession):
    def __init__(self, spec: SandboxSpec, *, fail: bool) -> None:
        super().__init__(spec)
        self.fail = fail
        self.checkpoints = 0

    @property
    def capabilities(self) -> SandboxCapabilities:
        return SandboxCapabilities(checkpoints=True, secret_names=("github",))

    async def checkpoint(self) -> CheckpointRef:
        self.checkpoints += 1
        if self.fail:
            raise RuntimeError("provider snapshot unavailable")
        return CheckpointRef(
            "local",
            f"checkpoint-{self.checkpoints}",
            f"local-checkpoint://{self.checkpoints}",
            self.checkpoints,
            environment=self.spec.environment,
            source_sandbox_id=self.identity.sandbox_id,
            owner_id=self.spec.metadata_dict().get("mission", ""),
        )


class _CheckpointBackend(_LocalBackend):
    def __init__(self, *, fail: bool) -> None:
        super().__init__()
        self.fail = fail
        self.restore_fails = False
        self.restores = 0

    async def create(self, spec: SandboxSpec) -> _CheckpointSession:
        self.creates += 1
        session = _CheckpointSession(spec, fail=self.fail)
        self.session = session
        return session

    async def restore(
        self,
        spec: SandboxSpec,
        checkpoint: CheckpointRef,
    ) -> _CheckpointSession:
        assert checkpoint.provider == self.name
        self.restores += 1
        if self.restore_fails:
            raise RuntimeError("provider restore unavailable")
        session = _CheckpointSession(spec, fail=self.fail)
        self.session = session
        return session


class _SecretOutputDriver:
    def __init__(self, workspace: Path) -> None:
        self.workspace = workspace

    async def run(self, session, request, prompt: str) -> AgentProcessObservation:
        del request, prompt
        result = await session.exec(
            ProcessRequest(
                ("sh", "-lc", "printf 'done\\n' > feature.txt"),
                workdir=str(self.workspace),
            )
        )
        token = "ghp_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
        output_length = 16_000 + 110
        suffix_length = output_length - 100 - len(token)
        safe_line = "safe output line\n"
        safe_suffix = (safe_line * (suffix_length // len(safe_line) + 1))[:suffix_length]
        output = "x" * 100 + token + safe_suffix
        return AgentProcessObservation(
            result.returncode,
            stdout=output,
            session_id="session-redaction",
        )


class _TraceOutputDriver:
    def __init__(self, workspace: Path, trace_uri: str) -> None:
        self.workspace = workspace
        self.trace_uri = trace_uri

    async def run(self, session, request, prompt: str) -> AgentProcessObservation:
        del request, prompt
        result = await session.exec(
            ProcessRequest(
                ("sh", "-lc", "printf 'done\\n' > feature.txt"),
                workdir=str(self.workspace),
            )
        )
        return AgentProcessObservation(
            result.returncode,
            result.stdout,
            result.stderr,
            session_id="session-trace",
            trace_uri=self.trace_uri,
        )


async def _raise_after_sandbox_acquisition(*args, **kwargs):
    del args, kwargs
    raise RuntimeError("injected post-acquisition failure")


@pytest.mark.asyncio
async def test_explicit_graph_drives_revision_bound_retry_and_downstream_readiness(
    tmp_path: Path,
) -> None:
    remote = _remote(tmp_path)
    workspace = tmp_path / "sandbox" / "repo"
    backend = _LocalBackend()
    driver = _MissionDriver(workspace)
    storage = StorageConfig(uri=str(tmp_path / "agent_missions"), namespace="contract")

    async with ArchetypeRuntime() as runtime:
        missions = runtime.missions(
            "agent-mission-contract",
            config=AgentMissionConfig(
                sandbox_backend=backend,
                sandbox_environment="local-test@sha256:contract",
                driver=driver,
                workspace=str(workspace),
                max_ticks=40,
            ),
            storage=storage,
        )
        submitted = await missions.submit(
            repository=str(remote),
            branch="agent/explicit-task-graph",
            tasks=[
                AgentTask(
                    name="regression",
                    prompt="Add the deterministic regression marker.",
                    validators=(
                        CommandValidator(
                            "focused",
                            ("sh", "-lc", 'test "$(cat regression.txt)" = good'),
                        ),
                    ),
                    max_dispatches=2,
                ),
                AgentTask(
                    name="implementation",
                    prompt="Implement the smallest fix.",
                    validators=(
                        CommandValidator(
                            "focused",
                            ("sh", "-lc", "test -f implementation.txt"),
                        ),
                    ),
                    depends_on=("regression",),
                ),
            ],
        )

        result = await missions.run(submitted)

        assert result.status == "succeeded"
        assert [(task.name, task.dispatches) for task in result.tasks] == [
            ("regression", 2),
            ("implementation", 1),
        ]
        assert all(task.commit_shas for task in result.tasks)
        assert all(len(task.commit_shas) == len(set(task.commit_shas)) for task in result.tasks)
        assert backend.creates == 1
        assert backend.session is not None and backend.session.closed == 1
        assert [(request.task_name, request.dispatch_sequence) for request in driver.requests] == [
            ("regression", 1),
            ("regression", 2),
            ("implementation", 1),
        ]
        assert driver.requests[1].task_base_revision
        assert driver.requests[1].previous_validation[0].passed is False
        assert all(
            request.publication_policy is RepositoryPublicationPolicy.COMMIT_AND_PUSH
            for request in driver.requests
        )
        validator_commands = {
            'test "$(cat regression.txt)" = good',
            "test -f implementation.txt",
        }
        validator_requests = [
            request
            for request in backend.session.requests
            if len(request.argv) == 3
            and request.argv[:2] == ("sh", "-lc")
            and request.argv[2] in validator_commands
        ]
        task_bases = [
            request.environment_dict()["ARCHETYPE_TASK_BASE_REVISION"]
            for request in validator_requests
        ]
        assert task_bases == [
            driver.requests[1].task_base_revision,
            driver.requests[1].task_base_revision,
            result.tasks[0].commit_shas[-1],
        ]
        assert result.tasks[0].commit_shas[0] != driver.requests[1].task_base_revision

        policy_rows = latest(await missions.query(TaskPolicy)).to_pylist()
        policy = TaskPolicy.get_prefix()
        assert {
            str(row[f"{policy}publication_policy"]) for row in policy_rows if row["is_active"]
        } == {RepositoryPublicationPolicy.COMMIT_AND_PUSH.value}

        relationships = (await missions.query(DependsOn)).to_pylist()
        dependency = DependsOn.get_prefix()
        assert {
            (row[f"{dependency}source"], row[f"{dependency}target"])
            for row in relationships
            if row["is_active"]
        } == {
            (
                submitted.task_id("implementation"),
                submitted.task_id("regression"),
            )
        }
        assert len(latest(await missions.query(TaskValidator)).to_pylist()) == 2
        assert len(latest(await missions.query(Guards)).to_pylist()) == 2

        validation_rows = latest(await missions.query(ValidationResult)).to_pylist()
        validation = ValidationResult.get_prefix()
        assert [int(row[f"{validation}actual_returncode"]) for row in validation_rows].count(0) == 2
        assert len(validation_rows) == 3
        assert len(latest(await missions.query(AgentExecution)).to_pylist()) == 3
        commit_rows = latest(await missions.query(Commit)).to_pylist()
        commit = Commit.get_prefix()
        assert len(commit_rows) == 4
        assert sum(bool(row[f"{commit}pushed"]) for row in commit_rows) == 3
        assert len(latest(await missions.query(FrictionLog)).to_pylist()) == 1

        sandbox_rows = latest(await missions.query(Sandbox)).to_pylist()
        sandbox = Sandbox.get_prefix()
        assert sandbox_rows[-1][f"{sandbox}status"] == SandboxStatus.CLOSED.value

        history = (await missions.query(TaskState)).to_pylist()
        state_ticks: dict[tuple[int, str], list[int]] = {}
        state = TaskState.get_prefix()
        for row in sorted(history, key=lambda value: value["tick"]):
            if row["is_active"]:
                key = (int(row["entity_id"]), str(row[f"{state}status"]))
                state_ticks.setdefault(key, []).append(int(row["tick"]))
        regression_accepted = min(state_ticks[(submitted.task_id("regression"), "accepted")])
        implementation_dispatched = min(
            state_ticks[(submitted.task_id("implementation"), "dispatched")]
        )
        assert implementation_dispatched > regression_accepted

    assert (
        _git(
            "--git-dir",
            str(remote),
            "rev-parse",
            "refs/heads/agent/explicit-task-graph",
        )
        == result.tasks[-1].commit_shas[-1]
    )


@pytest.mark.asyncio
async def test_automatic_replacement_closes_prior_sandbox_evidence(
    tmp_path: Path,
) -> None:
    remote = _remote(tmp_path)
    workspace = tmp_path / "sandbox" / "repo"
    backend = _AutoReplacementBackend()
    driver = _TransportLossThenRepairDriver(workspace)

    async with ArchetypeRuntime() as runtime:
        async with runtime.missions(
            "automatic-replacement-lifecycle",
            config=AgentMissionConfig(
                sandbox_backend=backend,
                sandbox_environment="local-replacement-test",
                driver=driver,
                workspace=str(workspace),
                checkpoint_after_dispatch=False,
            ),
            storage=StorageConfig(
                uri=str(tmp_path / "automatic_replacement_missions"),
                namespace="automatic_replacement_contract",
            ),
        ) as missions:
            submitted = await missions.submit(
                repository=str(remote),
                branch="agent/automatic-replacement",
                tasks=(
                    AgentTask(
                        "repair",
                        "Create artifact.txt containing good.",
                        (
                            CommandValidator(
                                "focused",
                                ("sh", "-lc", 'test "$(cat artifact.txt)" = good'),
                            ),
                        ),
                        max_dispatches=2,
                    ),
                ),
            )

            result = await missions.run(submitted)

            assert result.status == "succeeded"
            assert driver.calls == 2
            assert len(backend.sessions) == 2
            assert [session.closed for session in backend.sessions] == [1, 1]
            assert [await session.status() for session in backend.sessions] == [
                SandboxStatus.CLOSED,
                SandboxStatus.CLOSED,
            ]

            sandbox = Sandbox.get_prefix()
            rows = latest(await missions.query(Sandbox)).to_pylist()
            by_id = {str(row[f"{sandbox}sandbox_id"]): row for row in rows}

            assert set(by_id) == {
                "sandbox-replacement-1",
                "sandbox-replacement-2",
            }
            assert by_id["sandbox-replacement-1"][f"{sandbox}status"] == SandboxStatus.CLOSED.value
            assert "simulated provider transport loss" in str(
                by_id["sandbox-replacement-1"][f"{sandbox}error"]
            )
            assert by_id["sandbox-replacement-2"][f"{sandbox}status"] == SandboxStatus.CLOSED.value


@pytest.mark.asyncio
async def test_same_tick_replacement_keeps_prior_sandbox_closed(
    tmp_path: Path,
) -> None:
    remote = _remote(tmp_path)
    workspace = tmp_path / "sandbox" / "repo"
    backend = _AutoReplacementBackend()
    driver = _SameTickTransportLossDriver(workspace)

    async with ArchetypeRuntime() as runtime:
        async with runtime.missions(
            "batched-replacement-lifecycle",
            config=AgentMissionConfig(
                sandbox_backend=backend,
                sandbox_environment="local-replacement-test",
                driver=driver,
                workspace=str(workspace),
                checkpoint_after_dispatch=False,
            ),
            storage=StorageConfig(
                uri=str(tmp_path / "batched_replacement_missions"),
                namespace="batched_replacement_contract",
            ),
        ) as missions:
            submitted = await missions.submit(
                repository=str(remote),
                branch="agent/batched-replacement",
                tasks=tuple(
                    AgentTask(
                        task_name,
                        f"Create {task_name}.txt containing good.",
                        (
                            CommandValidator(
                                task_name,
                                (
                                    "sh",
                                    "-lc",
                                    f'test "$(cat {task_name}.txt)" = good',
                                ),
                            ),
                        ),
                        max_dispatches=2,
                    )
                    for task_name in ("first", "second")
                ),
            )

            result = await missions.run(submitted)

            assert result.status == "succeeded"
            assert len(driver.calls) == 3
            assert len(backend.sessions) == 2
            assert [await session.status() for session in backend.sessions] == [
                SandboxStatus.CLOSED,
                SandboxStatus.CLOSED,
            ]

            sandbox = Sandbox.get_prefix()
            rows = latest(await missions.query(Sandbox)).to_pylist()
            by_id = {str(row[f"{sandbox}sandbox_id"]): row for row in rows}
            assert by_id["sandbox-replacement-1"][f"{sandbox}status"] == (
                SandboxStatus.CLOSED.value
            )
            assert "simulated batched provider transport loss" in str(
                by_id["sandbox-replacement-1"][f"{sandbox}error"]
            )
            assert by_id["sandbox-replacement-2"][f"{sandbox}status"] == (
                SandboxStatus.CLOSED.value
            )


@pytest.mark.asyncio
async def test_terminal_close_failure_is_durable_and_retryable(
    tmp_path: Path,
) -> None:
    remote = _remote(tmp_path)
    workspace = tmp_path / "sandbox" / "repo"
    backend = _AutoReplacementBackend(fail_first_close=True)

    async with ArchetypeRuntime() as runtime:
        async with runtime.missions(
            "terminal-close-retry",
            config=AgentMissionConfig(
                sandbox_backend=backend,
                sandbox_environment="local-replacement-test",
                driver=_MissionDriver(workspace),
                workspace=str(workspace),
                checkpoint_after_dispatch=False,
            ),
            storage=StorageConfig(
                uri=str(tmp_path / "terminal_close_retry_missions"),
                namespace="terminal_close_retry_contract",
            ),
        ) as missions:
            submitted = await missions.submit(
                repository=str(remote),
                branch="agent/terminal-close-retry",
                tasks=(
                    AgentTask(
                        "implementation",
                        "Create implementation.txt containing fixed.",
                        (
                            CommandValidator(
                                "focused",
                                (
                                    "sh",
                                    "-lc",
                                    'test "$(cat implementation.txt)" = fixed',
                                ),
                            ),
                        ),
                    ),
                ),
            )

            with pytest.raises(RuntimeError, match="provider close unavailable"):
                await missions.run(submitted)

            assert len(backend.sessions) == 1
            session = backend.sessions[0]
            assert await session.status() is SandboxStatus.ERRORED
            sandbox = Sandbox.get_prefix()
            rows = latest(await missions.query(Sandbox)).to_pylist()
            assert rows[0][f"{sandbox}status"] == SandboxStatus.ERRORED.value
            assert "provider close unavailable" in str(rows[0][f"{sandbox}error"])
            friction = FrictionLog.get_prefix()
            friction_rows = latest(await missions.query(FrictionLog)).to_pylist()
            assert any(
                row[f"{friction}kind"] == "sandbox_teardown"
                and "provider close unavailable" in str(row[f"{friction}message"])
                for row in friction_rows
            )

            result = await missions.run(submitted)

            assert result.status == "succeeded"
            assert session.close_attempts == 2
            assert await session.status() is SandboxStatus.CLOSED
            rows = latest(await missions.query(Sandbox)).to_pylist()
            assert rows[0][f"{sandbox}status"] == SandboxStatus.CLOSED.value
            assert "provider close unavailable" in str(rows[0][f"{sandbox}error"])


@pytest.mark.asyncio
async def test_checkpoint_failure_replacement_closes_prior_sandbox_evidence(
    tmp_path: Path,
) -> None:
    remote = _remote(tmp_path)
    workspace = tmp_path / "sandbox" / "repo"
    backend = _AutoReplacementBackend(fail_first_checkpoint=True)
    driver = _MissionDriver(workspace)

    async with ArchetypeRuntime() as runtime:
        async with runtime.missions(
            "checkpoint-replacement-lifecycle",
            config=AgentMissionConfig(
                sandbox_backend=backend,
                sandbox_environment="local-replacement-test",
                driver=driver,
                workspace=str(workspace),
                checkpoint_after_dispatch=True,
            ),
            storage=StorageConfig(
                uri=str(tmp_path / "checkpoint_replacement_missions"),
                namespace="checkpoint_replacement_contract",
            ),
        ) as missions:
            submitted = await missions.submit(
                repository=str(remote),
                branch="agent/checkpoint-replacement",
                tasks=(
                    AgentTask(
                        "regression",
                        "Create regression.txt containing good.",
                        (
                            CommandValidator(
                                "focused",
                                ("sh", "-lc", 'test "$(cat regression.txt)" = good'),
                            ),
                        ),
                        max_dispatches=2,
                    ),
                ),
            )

            result = await missions.run(submitted)

            assert result.status == "succeeded"
            assert len(backend.sessions) == 2
            assert [session.closed for session in backend.sessions] == [1, 1]
            assert [session.checkpoints for session in backend.sessions] == [1, 1]

            sandbox = Sandbox.get_prefix()
            rows = latest(await missions.query(Sandbox)).to_pylist()
            by_id = {str(row[f"{sandbox}sandbox_id"]): row for row in rows}

            assert set(by_id) == {
                "sandbox-replacement-1",
                "sandbox-replacement-2",
            }
            assert by_id["sandbox-replacement-1"][f"{sandbox}status"] == SandboxStatus.CLOSED.value
            assert "simulated checkpoint restart failure" in str(
                by_id["sandbox-replacement-1"][f"{sandbox}error"]
            )
            assert by_id["sandbox-replacement-2"][f"{sandbox}status"] == SandboxStatus.CLOSED.value

            friction = FrictionLog.get_prefix()
            friction_rows = latest(await missions.query(FrictionLog)).to_pylist()
            assert any(
                row[f"{friction}kind"] == "checkpoint"
                and "simulated checkpoint restart failure" in str(row[f"{friction}message"])
                for row in friction_rows
            )


@pytest.mark.asyncio
async def test_failed_automatic_replacement_uses_retained_sandbox_identity(
    tmp_path: Path,
) -> None:
    remote = _remote(tmp_path)
    workspace = tmp_path / "sandbox" / "repo"
    backend = _AutoReplacementBackend(fail_first_close=True)
    driver = _TransportLossThenRepairDriver(workspace)

    async with ArchetypeRuntime() as runtime:
        async with runtime.missions(
            "automatic-replacement-close-retry",
            config=AgentMissionConfig(
                sandbox_backend=backend,
                sandbox_environment="local-replacement-test",
                driver=driver,
                workspace=str(workspace),
                checkpoint_after_dispatch=False,
            ),
            storage=StorageConfig(
                uri=str(tmp_path / "automatic_replacement_close_retry"),
                namespace="automatic_replacement_close_retry_contract",
            ),
        ) as missions:
            submitted = await missions.submit(
                repository=str(remote),
                branch="agent/automatic-replacement-close-retry",
                tasks=(
                    AgentTask(
                        "repair",
                        "Create artifact.txt containing good.",
                        (
                            CommandValidator(
                                "focused",
                                ("sh", "-lc", 'test "$(cat artifact.txt)" = good'),
                            ),
                        ),
                        max_dispatches=3,
                    ),
                ),
            )

            result = await missions.run(submitted)

            assert result.status == "succeeded"
            assert driver.calls == 2
            assert len(backend.sessions) == 2
            assert backend.sessions[0].close_attempts == 2
            assert [session.closed for session in backend.sessions] == [1, 1]

            sandbox = Sandbox.get_prefix()
            rows = latest(await missions.query(Sandbox)).to_pylist()
            by_id = {str(row[f"{sandbox}sandbox_id"]): row for row in rows}
            assert set(by_id) == {
                "sandbox-replacement-1",
                "sandbox-replacement-2",
            }
            assert all(
                row[f"{sandbox}status"] == SandboxStatus.CLOSED.value for row in by_id.values()
            )
            assert "provider close unavailable" in str(
                by_id["sandbox-replacement-1"][f"{sandbox}error"]
            )

            friction = FrictionLog.get_prefix()
            friction_rows = latest(await missions.query(FrictionLog)).to_pylist()
            assert any(
                row[f"{friction}kind"] == "sandbox_teardown"
                and "provider close unavailable" in str(row[f"{friction}message"])
                for row in friction_rows
            )


@pytest.mark.asyncio
async def test_failed_replacement_create_does_not_bind_unavailable_evidence(
    tmp_path: Path,
) -> None:
    remote = _remote(tmp_path)
    workspace = tmp_path / "sandbox" / "repo"
    backend = _AutoReplacementBackend(fail_create_sequences=(2,))
    driver = _TransportLossThenRepairDriver(workspace)

    async with ArchetypeRuntime() as runtime:
        async with runtime.missions(
            "automatic-replacement-create-retry",
            config=AgentMissionConfig(
                sandbox_backend=backend,
                sandbox_environment="local-replacement-test",
                driver=driver,
                workspace=str(workspace),
                checkpoint_after_dispatch=False,
            ),
            storage=StorageConfig(
                uri=str(tmp_path / "automatic_replacement_create_retry"),
                namespace="automatic_replacement_create_retry_contract",
            ),
        ) as missions:
            submitted = await missions.submit(
                repository=str(remote),
                branch="agent/automatic-replacement-create-retry",
                tasks=(
                    AgentTask(
                        "repair",
                        "Create artifact.txt containing good.",
                        (
                            CommandValidator(
                                "focused",
                                ("sh", "-lc", 'test "$(cat artifact.txt)" = good'),
                            ),
                        ),
                        max_dispatches=3,
                    ),
                ),
            )

            result = await missions.run(submitted)

            assert result.status == "succeeded"
            assert driver.calls == 2
            assert backend.create_attempts == 3
            assert [session.identity.sandbox_id for session in backend.sessions] == [
                "sandbox-replacement-1",
                "sandbox-replacement-3",
            ]
            assert [session.closed for session in backend.sessions] == [1, 1]

            sandbox = Sandbox.get_prefix()
            rows = latest(await missions.query(Sandbox)).to_pylist()
            by_id = {str(row[f"{sandbox}sandbox_id"]): row for row in rows}
            unavailable = [
                sandbox_id for sandbox_id in by_id if sandbox_id.startswith("unavailable-")
            ]
            assert len(unavailable) == 1
            assert by_id["sandbox-replacement-1"][f"{sandbox}status"] == SandboxStatus.CLOSED.value
            assert by_id["sandbox-replacement-3"][f"{sandbox}status"] == SandboxStatus.CLOSED.value
            assert by_id[unavailable[0]][f"{sandbox}status"] == SandboxStatus.ERRORED.value
            assert "simulated create failure 2" in str(by_id[unavailable[0]][f"{sandbox}error"])


@pytest.mark.parametrize(
    "trace_uri",
    (
        "",
        "local-sandbox://sandbox-contract/live/executions/process-1/agent.stdout.log",
    ),
    ids=("untraced", "provider-scoped-trace"),
)
@pytest.mark.asyncio
async def test_execution_trace_requires_exact_per_call_provider_evidence(
    tmp_path: Path,
    trace_uri: str,
) -> None:
    remote = _remote(tmp_path)
    workspace = tmp_path / "sandbox" / "repo"
    backend = _LiveOutputBackend()

    async with ArchetypeRuntime() as runtime:
        async with runtime.missions(
            "per-call-trace-evidence",
            config=AgentMissionConfig(
                sandbox_backend=backend,
                sandbox_environment="local-live-output-test",
                driver=_TraceOutputDriver(workspace, trace_uri),
                workspace=str(workspace),
                checkpoint_after_dispatch=False,
            ),
            storage=StorageConfig(
                uri=str(tmp_path / "per_call_trace_evidence"),
                namespace=("per_call_trace_present" if trace_uri else "per_call_trace_absent"),
            ),
        ) as missions:
            submitted = await missions.submit(
                repository=str(remote),
                branch=(
                    "agent/per-call-trace-present" if trace_uri else "agent/per-call-trace-absent"
                ),
                tasks=(
                    AgentTask(
                        "implementation",
                        "Create feature.txt.",
                        (CommandValidator("focused", ("test", "-f", "feature.txt")),),
                    ),
                ),
            )

            result = await missions.run(submitted)

            assert result.status == "succeeded"
            execution = AgentExecution.get_prefix()
            rows = latest(await missions.query(AgentExecution)).to_pylist()
            assert len(rows) == 1
            assert rows[0][f"{execution}trace_uri"] == trace_uri


@pytest.mark.asyncio
async def test_provider_error_is_redacted_before_persistence_bound(tmp_path: Path) -> None:
    remote = _remote(tmp_path)
    workspace = tmp_path / "sandbox" / "repo"

    async with ArchetypeRuntime() as runtime:
        async with runtime.missions(
            "provider-error-redaction",
            config=AgentMissionConfig(
                sandbox_backend=_ProviderFailureBackend(),
                sandbox_environment="local-test@sha256:contract",
                driver=_MissionDriver(workspace),
                workspace=str(workspace),
                max_ticks=20,
            ),
            storage=StorageConfig(
                uri=str(tmp_path / "provider_error_redaction"),
                namespace="contract",
            ),
        ) as missions:
            submitted = await missions.submit(
                repository=str(remote),
                branch="agent/provider-error-redaction",
                tasks=(
                    AgentTask(
                        "implementation",
                        "This provider fails before the agent starts.",
                        (CommandValidator("focused", ("true",)),),
                        max_dispatches=1,
                    ),
                ),
            )

            result = await missions.run(submitted)

            assert result.status == "failed"
            rows = latest(await missions.query(AgentExecution)).to_pylist()
            execution = AgentExecution.get_prefix()
            error = str(rows[0][f"{execution}error"])
            assert 0 < len(error) <= 4_000
            assert "ghp_" not in error
            assert "A" * 20 not in error
            assert "<redacted:github-token>" in error


@pytest.mark.asyncio
async def test_post_acquisition_failure_reuses_the_registered_sandbox_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    remote = _remote(tmp_path)
    workspace = tmp_path / "sandbox" / "repo"
    backend = _LocalBackend()

    async with ArchetypeRuntime() as runtime:
        async with runtime.missions(
            "post-acquisition-failure",
            config=AgentMissionConfig(
                sandbox_backend=backend,
                sandbox_environment="local-test@sha256:contract",
                driver=_MissionDriver(workspace),
                workspace=str(workspace),
                max_ticks=20,
            ),
            storage=StorageConfig(
                uri=str(tmp_path / "post_acquisition_failure"),
                namespace="contract",
            ),
        ) as missions:
            monkeypatch.setattr(
                missions._service._harness,
                "execute",
                _raise_after_sandbox_acquisition,
            )
            submitted = await missions.submit(
                repository=str(remote),
                branch="agent/post-acquisition-failure",
                tasks=(
                    AgentTask(
                        "implementation",
                        "This process is deliberately interrupted.",
                        (CommandValidator("focused", ("true",)),),
                        max_dispatches=1,
                    ),
                ),
            )

            result = await missions.run(submitted)

            assert result.status == "failed"
            sandbox_rows = latest(await missions.query(Sandbox)).to_pylist()
            sandbox = Sandbox.get_prefix()
            assert len(sandbox_rows) == 1
            assert sandbox_rows[0][f"{sandbox}sandbox_id"] == "sandbox-contract"
            assert sandbox_rows[0][f"{sandbox}status"] == SandboxStatus.CLOSED.value


@pytest.mark.parametrize("checkpoint_fails", [False, True])
@pytest.mark.asyncio
async def test_checkpoint_is_queryable_but_never_gates_a_valid_task(
    tmp_path: Path,
    checkpoint_fails: bool,
) -> None:
    remote = _remote(tmp_path)
    workspace = tmp_path / "sandbox" / "repo"
    backend = _CheckpointBackend(fail=checkpoint_fails)
    observed: list[SandboxEvent] = []

    async with ArchetypeRuntime() as runtime:
        async with runtime.missions(
            "checkpoint-contract",
            config=AgentMissionConfig(
                sandbox_backend=backend,
                sandbox_environment="local-checkpoint-test",
                driver=_SecretOutputDriver(workspace),
                workspace=str(workspace),
                on_sandbox_event=observed.append,
            ),
            storage=StorageConfig(
                uri=str(tmp_path / "checkpoint_missions"),
                namespace=f"checkpoint_{checkpoint_fails}",
            ),
        ) as missions:
            submitted = await missions.submit(
                repository=str(remote),
                branch=f"agent/checkpoint-{checkpoint_fails}",
                tasks=(
                    AgentTask(
                        "implementation",
                        "Create feature.txt.",
                        (CommandValidator("focused", ("test", "-f", "feature.txt")),),
                    ),
                ),
            )

            result = await missions.run(submitted)

            assert result.status == "succeeded"
            assert [
                event.sandbox.sandbox_id
                for event in observed
                if event.kind is SandboxEventType.READY
            ] == ["sandbox-contract"]
            assert SandboxEventType.PROCESS_STARTED in {event.kind for event in observed}
            assert SandboxEventType.PROCESS_FINISHED in {event.kind for event in observed}
            checkpoint_rows = latest(await missions.query(Checkpoint)).to_pylist()
            checkpoint = Checkpoint.get_prefix()
            assert len(checkpoint_rows) == 1
            assert checkpoint_rows[0][f"{checkpoint}restorable"] is (not checkpoint_fails)
            if checkpoint_fails:
                assert "provider snapshot unavailable" in checkpoint_rows[0][f"{checkpoint}error"]
            else:
                assert checkpoint_rows[0][f"{checkpoint}environment"] == "local-checkpoint-test"
                assert checkpoint_rows[0][f"{checkpoint}source_sandbox_id"]

            task_state = TaskState.get_prefix()
            accepted_tick = min(
                int(row["tick"])
                for row in (await missions.query(TaskState)).to_pylist()
                if row[f"{task_state}status"] == "accepted"
            )
            checkpoint_tick = int(checkpoint_rows[0]["tick"])
            assert checkpoint_tick > accepted_tick

            execution_rows = latest(await missions.query(AgentExecution)).to_pylist()
            execution = AgentExecution.get_prefix()
            agent_stdout = execution_rows[0][f"{execution}agent_stdout"]
            assert len(agent_stdout) == 16_000
            assert "ghp_" not in agent_stdout
            assert "A" * 20 not in agent_stdout
            leading_x = len(agent_stdout) - len(agent_stdout.lstrip("x"))
            assert 0 < leading_x < 100
            assert agent_stdout.lstrip("x").startswith("<redacted:github-token>")
            assert execution_rows[0][f"{execution}redaction_policy_id"].startswith(
                "archetype-secret-redaction-v1:"
            )
            sandbox_tick = min(
                int(row["tick"]) for row in (await missions.query(Sandbox)).to_pylist()
            )
            execution_tick = min(
                int(row["tick"]) for row in (await missions.query(AgentExecution)).to_pylist()
            )
            assert sandbox_tick <= execution_tick

            if checkpoint_fails:
                friction_rows = latest(await missions.query(FrictionLog)).to_pylist()
                friction = FrictionLog.get_prefix()
                checkpoint_friction = [
                    row for row in friction_rows if row[f"{friction}kind"] == "checkpoint"
                ]
                assert len(checkpoint_friction) == 1
            else:
                with pytest.raises(KeyError, match="FrictionLog has never been spawned"):
                    await missions.query(FrictionLog)


@pytest.mark.asyncio
async def test_terminal_mission_flushes_checkpoint_outside_the_tick_budget(
    tmp_path: Path,
) -> None:
    remote = _remote(tmp_path)
    workspace = tmp_path / "sandbox" / "repo"
    backend = _CheckpointBackend(fail=False)

    async with ArchetypeRuntime() as runtime:
        async with runtime.missions(
            "terminal-checkpoint-budget",
            config=AgentMissionConfig(
                sandbox_backend=backend,
                sandbox_environment="local-checkpoint-test",
                driver=_SecretOutputDriver(workspace),
                workspace=str(workspace),
            ),
            storage=StorageConfig(
                uri=str(tmp_path / "terminal_checkpoint_budget"),
                namespace="contract",
            ),
        ) as missions:
            submitted = await missions.submit(
                repository=str(remote),
                branch="agent/terminal-checkpoint-budget",
                tasks=(
                    AgentTask(
                        "implementation",
                        "Create feature.txt.",
                        (CommandValidator("focused", ("test", "-f", "feature.txt")),),
                    ),
                ),
            )

            result = await missions.run(submitted, max_ticks=5)

            assert result.status == "succeeded"
            assert backend.session is not None
            assert backend.session.checkpoints == 1
            checkpoint_rows = latest(await missions.query(Checkpoint)).to_pylist()
            assert len(checkpoint_rows) == 1


@pytest.mark.asyncio
async def test_tick_budget_exhaustion_flushes_pending_checkpoint(
    tmp_path: Path,
) -> None:
    remote = _remote(tmp_path)
    workspace = tmp_path / "sandbox" / "repo"
    backend = _CheckpointBackend(fail=False)

    async with ArchetypeRuntime() as runtime:
        async with runtime.missions(
            "exhausted-checkpoint-budget",
            config=AgentMissionConfig(
                sandbox_backend=backend,
                sandbox_environment="local-checkpoint-test",
                driver=_SecretOutputDriver(workspace),
                workspace=str(workspace),
            ),
            storage=StorageConfig(
                uri=str(tmp_path / "exhausted_checkpoint_budget"),
                namespace="contract",
            ),
        ) as missions:
            submitted = await missions.submit(
                repository=str(remote),
                branch="agent/exhausted-checkpoint-budget",
                tasks=(
                    AgentTask(
                        "implementation",
                        "Create feature.txt, but remain nonterminal.",
                        (CommandValidator("never-valid", ("false",)),),
                        max_dispatches=2,
                    ),
                ),
            )

            with pytest.raises(RuntimeError, match="did not terminate after 2 ticks"):
                await missions.run(submitted, max_ticks=2)

            assert backend.session is not None
            assert backend.session.checkpoints == 1
            checkpoint_rows = latest(await missions.query(Checkpoint)).to_pylist()
            assert len(checkpoint_rows) == 1


@pytest.mark.asyncio
async def test_explicit_restore_rehydrates_before_work_without_automatic_supervision(
    tmp_path: Path,
) -> None:
    remote = _remote(tmp_path)
    workspace = tmp_path / "sandbox" / "repo"
    backend = _CheckpointBackend(fail=False)
    observed: list[SandboxEvent] = []

    async with ArchetypeRuntime() as runtime:
        async with runtime.missions(
            "explicit-restore-contract",
            config=AgentMissionConfig(
                sandbox_backend=backend,
                sandbox_environment="local-checkpoint-test",
                driver=_SecretOutputDriver(workspace),
                workspace=str(workspace),
                on_sandbox_event=observed.append,
            ),
            storage=StorageConfig(
                uri=str(tmp_path / "restore_missions"),
                namespace="restore_contract",
            ),
        ) as missions:
            submitted = await missions.submit(
                repository=str(remote),
                branch="agent/explicit-restore",
                tasks=(
                    AgentTask(
                        "implementation",
                        "Create feature.txt.",
                        (CommandValidator("focused", ("test", "-f", "feature.txt")),),
                    ),
                ),
            )
            checkpoint = CheckpointRef(
                "local",
                "checkpoint-before-run",
                "local-checkpoint://before-run",
                1,
                environment="local-checkpoint-test",
                source_sandbox_id="source-before-run",
                owner_id=str(submitted.mission_id),
            )

            identity = await missions.restore_sandbox(submitted, checkpoint)
            result = await missions.run(submitted)

            assert identity.sandbox_id == "sandbox-contract"
            assert result.status == "succeeded"
            assert backend.creates == 0
            assert backend.restores == 1
            assert [
                event.sandbox.sandbox_id
                for event in observed
                if event.kind is SandboxEventType.READY
            ] == ["sandbox-contract"]


@pytest.mark.asyncio
async def test_failed_explicit_restore_closes_the_replaced_sandbox_evidence(
    tmp_path: Path,
) -> None:
    remote = _remote(tmp_path)
    workspace = tmp_path / "sandbox" / "repo"
    backend = _CheckpointBackend(fail=False)

    async with ArchetypeRuntime() as runtime:
        async with runtime.missions(
            "failed-restore-contract",
            config=AgentMissionConfig(
                sandbox_backend=backend,
                sandbox_environment="local-checkpoint-test",
                driver=_SecretOutputDriver(workspace),
                workspace=str(workspace),
            ),
            storage=StorageConfig(
                uri=str(tmp_path / "failed_restore_missions"),
                namespace="failed_restore_contract",
            ),
        ) as missions:
            submitted = await missions.submit(
                repository=str(remote),
                branch="agent/failed-restore",
                tasks=(
                    AgentTask(
                        "implementation",
                        "Create feature.txt.",
                        (CommandValidator("focused", ("test", "-f", "feature.txt")),),
                    ),
                ),
            )

            def checkpoint(identifier: str) -> CheckpointRef:
                return CheckpointRef(
                    "local",
                    identifier,
                    f"local-checkpoint://{identifier}",
                    1,
                    environment="local-checkpoint-test",
                    source_sandbox_id=f"source-{identifier}",
                    owner_id=str(submitted.mission_id),
                )

            await missions.restore_sandbox(submitted, checkpoint("initial"))
            assert backend.session is not None
            replaced = backend.session

            with pytest.raises(ValueError, match="owner"):
                await missions.restore_sandbox(
                    submitted,
                    CheckpointRef(
                        "local",
                        "wrong-owner",
                        "local-checkpoint://wrong-owner",
                        1,
                        environment="local-checkpoint-test",
                        source_sandbox_id="source-wrong-owner",
                        owner_id="another-mission",
                    ),
                )
            sandbox_rows = latest(await missions.query(Sandbox)).to_pylist()
            sandbox = Sandbox.get_prefix()
            assert sandbox_rows[0][f"{sandbox}status"] == SandboxStatus.READY.value
            assert replaced.closed == 0

            backend.restore_fails = True

            with pytest.raises(RuntimeError, match="provider restore unavailable"):
                await missions.restore_sandbox(submitted, checkpoint("replacement"))

            sandbox_rows = latest(await missions.query(Sandbox)).to_pylist()
            assert len(sandbox_rows) == 1
            assert sandbox_rows[0][f"{sandbox}sandbox_id"] == "sandbox-contract"
            assert sandbox_rows[0][f"{sandbox}status"] == SandboxStatus.CLOSED.value
            assert replaced.closed == 1


@pytest.mark.asyncio
async def test_failed_restore_close_retains_live_evidence_and_allows_retry(
    tmp_path: Path,
) -> None:
    remote = _remote(tmp_path)
    workspace = tmp_path / "sandbox" / "repo"
    backend = _CheckpointBackend(fail=False)

    async with ArchetypeRuntime() as runtime:
        async with runtime.missions(
            "restore-close-retry-contract",
            config=AgentMissionConfig(
                sandbox_backend=backend,
                sandbox_environment="local-checkpoint-test",
                driver=_SecretOutputDriver(workspace),
                workspace=str(workspace),
            ),
            storage=StorageConfig(
                uri=str(tmp_path / "restore_close_retry_missions"),
                namespace="restore_close_retry_contract",
            ),
        ) as missions:
            submitted = await missions.submit(
                repository=str(remote),
                branch="agent/restore-close-retry",
                tasks=(
                    AgentTask(
                        "implementation",
                        "Create feature.txt.",
                        (CommandValidator("focused", ("test", "-f", "feature.txt")),),
                    ),
                ),
            )

            def checkpoint(identifier: str) -> CheckpointRef:
                return CheckpointRef(
                    "local",
                    identifier,
                    f"local-checkpoint://{identifier}",
                    1,
                    environment="local-checkpoint-test",
                    source_sandbox_id=f"source-{identifier}",
                    owner_id=str(submitted.mission_id),
                )

            await missions.restore_sandbox(submitted, checkpoint("initial"))
            assert backend.session is not None
            replaced = backend.session
            replaced.close_error = True

            with pytest.raises(RuntimeError, match="provider close unavailable"):
                await missions.restore_sandbox(submitted, checkpoint("replacement"))

            sandbox_rows = latest(await missions.query(Sandbox)).to_pylist()
            sandbox = Sandbox.get_prefix()
            assert sandbox_rows[0][f"{sandbox}status"] == SandboxStatus.ERRORED.value
            assert "provider close unavailable" in sandbox_rows[0][f"{sandbox}error"]
            friction_rows = latest(await missions.query(FrictionLog)).to_pylist()
            friction = FrictionLog.get_prefix()
            assert friction_rows[0][f"{friction}kind"] == "sandbox_restore"
            assert "provider close unavailable" in friction_rows[0][f"{friction}message"]

            replaced.close_error = False
            identity = await missions.restore_sandbox(submitted, checkpoint("replacement"))

            assert identity.sandbox_id == "sandbox-contract"
            assert replaced.close_attempts == 2
            assert replaced.closed == 1
            sandbox_rows = latest(await missions.query(Sandbox)).to_pylist()
            assert sandbox_rows[0][f"{sandbox}status"] == SandboxStatus.READY.value
            assert sandbox_rows[0][f"{sandbox}error"] == ""
