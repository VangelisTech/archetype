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

    @property
    def identity(self) -> SandboxIdentity:
        return SandboxIdentity("local", "sandbox-contract", self.spec.environment)

    @property
    def capabilities(self) -> SandboxCapabilities:
        return SandboxCapabilities(secret_names=("github",))

    async def status(self) -> SandboxStatus:
        return SandboxStatus.CLOSED if self.closed else SandboxStatus.READY

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
            int(process.returncode),
            stdout.decode(),
            stderr.decode(),
        )

    async def checkpoint(self) -> CheckpointRef:
        raise NotImplementedError

    async def close(self) -> None:
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
        result = await session.exec(
            ProcessRequest(
                ("sh", "-lc", f"printf '%s\\n' {content} > {filename}"),
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
        return AgentProcessObservation(
            result.returncode,
            stdout="successful output ghp_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
            session_id="session-redaction",
        )


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
        assert len(latest(await missions.query(Commit)).to_pylist()) == 2
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
            assert "ghp_" not in execution_rows[0][f"{execution}agent_stdout"]
            assert "<redacted:github-token>" in execution_rows[0][f"{execution}agent_stdout"]
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
