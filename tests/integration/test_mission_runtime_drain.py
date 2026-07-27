# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Runtime-lifetime drain contracts for admitted Agent Missions operations.

These tests bind the whole-operation admission contract from issue #627: a
mission operation admitted before runtime shutdown drains as one supported
workflow. The race schedule is the issue's dogfood counterfactual — shutdown
begins while an admitted ``run()`` is blocked in external execution, between
agent completion and validation, when no individual world call is in flight.
An implementation that guards only individual world calls drains at that
barrier and fails these tests.
"""

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
    CommandValidator,
    Commit,
    Sandbox,
    TaskState,
    ValidationResult,
)
from archetype.missions.coding_agents import AgentProcessObservation
from archetype.missions.contracts import MissionResult, SubmittedMission
from archetype.missions.critics import CriticProcessObservation
from archetype.missions.sandboxes import (
    CheckpointRef,
    ProcessRequest,
    ProcessResult,
    SandboxCapabilities,
    SandboxEvent,
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
        self.close_attempts = 0
        self.requests: list[ProcessRequest] = []

    @property
    def identity(self) -> SandboxIdentity:
        dispatch = self.spec.metadata_dict().get("dispatch")
        sandbox_id = f"sandbox-critic-{dispatch[:12]}" if dispatch else "sandbox-drain"
        return SandboxIdentity("local", sandbox_id, self.spec.environment)

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
        self.closed += 1


class _LocalBackend:
    name = "local"

    def __init__(self) -> None:
        self.creates = 0
        self.sessions: list[_LocalSession] = []
        self.critic_sessions: list[_LocalSession] = []

    async def create(self, spec: SandboxSpec) -> _LocalSession:
        session = _LocalSession(spec)
        if spec.metadata_dict().get("role") == "critic":
            self.critic_sessions.append(session)
            return session
        self.creates += 1
        self.sessions.append(session)
        return session

    async def restore(self, spec: SandboxSpec, checkpoint: CheckpointRef) -> _LocalSession:
        del spec, checkpoint
        raise NotImplementedError("local drain backend cannot restore")


class _BlockedAgentDriver:
    """Complete the agent's work, then block inside external execution.

    While blocked, no world call is in flight: the admitted ``run()`` sits
    between its committed dispatch and its later validation/staging calls.
    """

    def __init__(self, workspace: Path, *, content: str = "fixed") -> None:
        self.workspace = workspace
        self.content = content
        self.blocked = asyncio.Event()
        self.release = asyncio.Event()
        self.probe: object | None = None
        self.probe_outcomes: list[str] = []

    async def run(self, session, request, prompt: str) -> AgentProcessObservation:
        del prompt
        result = await session.exec(
            ProcessRequest(
                ("sh", "-lc", f"printf '%s\\n' {self.content} > implementation.txt"),
                workdir=str(self.workspace),
            )
        )
        if self.probe is not None:
            probe = self.probe
            self.probe = None
            try:
                await probe()  # type: ignore[operator]
            except BaseException as error:  # noqa: BLE001 - recorded for assertion
                self.probe_outcomes.append(f"{type(error).__name__}: {error}")
            else:
                self.probe_outcomes.append("returned")
        self.blocked.set()
        await self.release.wait()
        return AgentProcessObservation(
            result.returncode,
            result.stdout,
            result.stderr,
            session_id="session-drain",
        )


class _ApprovingCriticDriver:
    driver_id = "codex"

    def __init__(self) -> None:
        self.requests = []

    async def run(self, session, request, prompt: str) -> CriticProcessObservation:
        del prompt
        self.requests.append(request)
        return CriticProcessObservation(
            returncode=0,
            stdout=(
                '{"schema_version":1,"conclusion":"approved",'
                '"reviewed_scope":"exact task diff","findings":[]}'
            ),
        )


def _config(
    backend: _LocalBackend,
    driver: _BlockedAgentDriver,
    tmp_path: Path,
    *,
    on_sandbox_event=None,
) -> AgentMissionConfig:
    return AgentMissionConfig(
        sandbox_backend=backend,
        sandbox_environment="local-drain-test",
        driver=driver,
        critic_driver=_ApprovingCriticDriver(),
        workspace=str(driver.workspace),
        critic_workspace=str(tmp_path / "critic"),
        checkpoint_after_dispatch=False,
        on_sandbox_event=on_sandbox_event,
    )


def _task() -> AgentTask:
    return AgentTask(
        "implementation",
        "Create implementation.txt containing fixed.",
        (
            CommandValidator(
                "focused",
                ("sh", "-lc", 'test "$(cat implementation.txt)" = fixed'),
            ),
        ),
        max_dispatches=1,
    )


async def _spin(iterations: int = 200) -> None:
    for _ in range(iterations):
        await asyncio.sleep(0)


@pytest.mark.asyncio
async def test_runtime_shutdown_drains_admitted_run_blocked_in_external_execution(
    tmp_path: Path,
) -> None:
    """Issue #627 schedule: graceful shutdown drains the whole admitted run."""

    remote = _remote(tmp_path)
    backend = _LocalBackend()
    driver = _BlockedAgentDriver(tmp_path / "sandbox" / "repo")
    storage = StorageConfig(
        uri=str(tmp_path / "mission_runtime_drain"),
        namespace="mission_runtime_drain_contract",
    )
    world_id_reentries: list[object] = []
    scheduled_reentries: list[asyncio.Task[object]] = []
    handle_box: list = []

    def observer(event: SandboxEvent) -> None:
        # Synchronous re-entry from the admitted task and scheduled async
        # re-entry from a fresh task; neither may deadlock shutdown.
        handle = handle_box[0]
        try:
            world_id_reentries.append(handle.world_id)
        except RuntimeError as error:
            world_id_reentries.append(error)
        scheduled_reentries.append(asyncio.ensure_future(handle.query()))

    runtime = ArchetypeRuntime()
    missions = runtime.missions(
        "runtime-drain",
        config=_config(backend, driver, tmp_path, on_sandbox_event=observer),
        storage=storage,
    )
    handle_box.append(missions)
    submitted = await missions.submit(
        repository=str(remote),
        branch="agent/runtime-drain",
        tasks=(_task(),),
    )
    world_id = missions.world_id

    # Ordinary admitted work is not teardown: from inside the admitted run,
    # runtime shutdown and public mission close both reject deterministically.
    async def teardown_authority_probe() -> None:
        with pytest.raises(RuntimeError, match="cannot close from an admitted operation"):
            await runtime.shutdown()
        with pytest.raises(RuntimeError, match="cannot close from an admitted operation"):
            await missions.close()

    driver.probe = teardown_authority_probe

    run_task = asyncio.create_task(missions.run(submitted))
    await asyncio.wait_for(driver.blocked.wait(), timeout=30)
    assert driver.probe_outcomes == ["returned"]

    shutting_down = asyncio.create_task(runtime.shutdown())
    await _spin()

    # The admitted run is blocked between world calls; a per-call guard would
    # observe zero in-flight calls and let shutdown finish here.
    assert not shutting_down.done()
    author = backend.sessions[0]
    assert author.close_attempts == 0, "sandbox teardown must not begin before drain"

    # New supported mission operations fail before side effects.
    creates_before = backend.creates
    with pytest.raises(RuntimeError, match="closed"):
        await missions.submit(
            repository=str(remote),
            branch="agent/late-submit",
            tasks=(_task(),),
        )
    with pytest.raises(RuntimeError, match="closed"):
        await missions.run(submitted)
    with pytest.raises(RuntimeError, match="closed"):
        await missions.restore_sandbox(
            submitted,
            CheckpointRef("local", "cp", "local://cp", 1),
        )
    with pytest.raises(RuntimeError, match="closed"):
        await missions.query()
    with pytest.raises(RuntimeError, match="closed"):
        runtime.world("rejected-during-shutdown")
    assert backend.creates == creates_before

    driver.release.set()
    result = await asyncio.wait_for(run_task, timeout=60)
    assert isinstance(result, MissionResult)
    assert result.status == "succeeded"

    await asyncio.wait_for(shutting_down, timeout=60)
    assert author.closed == 1

    # Observer re-entry resolved without wedging shutdown.
    resolved = await asyncio.wait_for(
        asyncio.gather(*scheduled_reentries, return_exceptions=True),
        timeout=30,
    )
    assert len(resolved) == len(scheduled_reentries)
    assert any(not isinstance(item, BaseException) for item in world_id_reentries)
    assert all(isinstance(item, str | RuntimeError) for item in world_id_reentries)

    # Execution, validation, publication evidence, task decision, and final
    # sandbox state are durably queryable by a cold reader after the race.
    async with ArchetypeRuntime() as reader:
        attached = reader.attach(world_id, storage=storage)
        executions = latest(await attached.query(AgentExecution)).to_pylist()
        validations = latest(await attached.query(ValidationResult)).to_pylist()
        commits = latest(await attached.query(Commit)).to_pylist()
        states = latest(await attached.query(TaskState)).to_pylist()
        sandboxes = latest(await attached.query(Sandbox)).to_pylist()
    validation = ValidationResult.get_prefix()
    state = TaskState.get_prefix()
    sandbox = Sandbox.get_prefix()
    assert len(executions) == 1
    assert [int(row[f"{validation}actual_returncode"]) for row in validations] == [0]
    assert len(commits) >= 1
    assert {row[f"{state}status"] for row in states} == {"accepted"}
    assert sandboxes
    assert all(row[f"{sandbox}status"] == SandboxStatus.CLOSED.value for row in sandboxes)


@pytest.mark.asyncio
async def test_runtime_shutdown_preserves_factual_failure_of_admitted_run(
    tmp_path: Path,
) -> None:
    """A run that fails validation returns that factual failure, not a
    runtime-closed error, when it raced graceful shutdown."""

    remote = _remote(tmp_path)
    backend = _LocalBackend()
    driver = _BlockedAgentDriver(tmp_path / "sandbox" / "repo", content="broken")
    storage = StorageConfig(
        uri=str(tmp_path / "mission_factual_failure"),
        namespace="mission_factual_failure_contract",
    )
    runtime = ArchetypeRuntime()
    missions = runtime.missions(
        "factual-failure",
        config=_config(backend, driver, tmp_path),
        storage=storage,
    )
    submitted = await missions.submit(
        repository=str(remote),
        branch="agent/factual-failure",
        tasks=(_task(),),
    )
    world_id = missions.world_id

    run_task = asyncio.create_task(missions.run(submitted))
    await asyncio.wait_for(driver.blocked.wait(), timeout=30)
    shutting_down = asyncio.create_task(runtime.shutdown())
    await _spin()
    assert not shutting_down.done()

    driver.release.set()
    result = await asyncio.wait_for(run_task, timeout=60)
    assert isinstance(result, MissionResult)
    assert result.status == "failed"
    await asyncio.wait_for(shutting_down, timeout=60)

    async with ArchetypeRuntime() as reader:
        attached = reader.attach(world_id, storage=storage)
        validations = latest(await attached.query(ValidationResult)).to_pylist()
        states = latest(await attached.query(TaskState)).to_pylist()
        sandboxes = latest(await attached.query(Sandbox)).to_pylist()
    validation = ValidationResult.get_prefix()
    state = TaskState.get_prefix()
    sandbox = Sandbox.get_prefix()
    assert any(int(row[f"{validation}actual_returncode"]) != 0 for row in validations)
    assert any(row[f"{state}status"] == "failed" for row in states)
    assert sandboxes
    assert all(row[f"{sandbox}status"] == SandboxStatus.CLOSED.value for row in sandboxes)


@pytest.mark.asyncio
async def test_public_close_and_runtime_shutdown_race_admitted_run(
    tmp_path: Path,
) -> None:
    """Public mission close racing runtime shutdown stays single-flight while
    both drain the same admitted run."""

    remote = _remote(tmp_path)
    backend = _LocalBackend()
    driver = _BlockedAgentDriver(tmp_path / "sandbox" / "repo")
    runtime = ArchetypeRuntime()
    missions = runtime.missions(
        "close-shutdown-race",
        config=_config(backend, driver, tmp_path),
        storage=StorageConfig(
            uri=str(tmp_path / "mission_close_shutdown_race"),
            namespace="mission_close_shutdown_race_contract",
        ),
    )
    submitted = await missions.submit(
        repository=str(remote),
        branch="agent/close-shutdown-race",
        tasks=(_task(),),
    )
    run_task = asyncio.create_task(missions.run(submitted))
    await asyncio.wait_for(driver.blocked.wait(), timeout=30)

    shutting_down = asyncio.create_task(runtime.shutdown())
    closing = asyncio.create_task(missions.close())
    await _spin()
    assert not shutting_down.done()
    assert not closing.done()
    author = backend.sessions[0]
    assert author.close_attempts == 0

    driver.release.set()
    result = await asyncio.wait_for(run_task, timeout=60)
    assert result.status == "succeeded"
    await asyncio.wait_for(asyncio.gather(shutting_down, closing), timeout=60)
    assert author.close_attempts == 1
    assert author.closed == 1
    assert missions._reservation.released  # noqa: SLF001 - exact owner oracle


@pytest.mark.asyncio
async def test_cancelling_admitted_run_does_not_wedge_runtime_shutdown(
    tmp_path: Path,
) -> None:
    """Caller cancellation of the blocked run releases admission and lets the
    pending graceful shutdown finish."""

    remote = _remote(tmp_path)
    backend = _LocalBackend()
    driver = _BlockedAgentDriver(tmp_path / "sandbox" / "repo")
    runtime = ArchetypeRuntime()
    missions = runtime.missions(
        "cancelled-run",
        config=_config(backend, driver, tmp_path),
        storage=StorageConfig(
            uri=str(tmp_path / "mission_cancelled_run"),
            namespace="mission_cancelled_run_contract",
        ),
    )
    submitted = await missions.submit(
        repository=str(remote),
        branch="agent/cancelled-run",
        tasks=(_task(),),
    )
    run_task = asyncio.create_task(missions.run(submitted))
    await asyncio.wait_for(driver.blocked.wait(), timeout=30)
    shutting_down = asyncio.create_task(runtime.shutdown())
    await _spin()
    assert not shutting_down.done()

    run_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(run_task, timeout=30)
    await asyncio.wait_for(shutting_down, timeout=60)
    assert backend.sessions[0].closed == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["submit", "restore_sandbox", "query"])
async def test_runtime_shutdown_drains_each_admitted_mission_operation(
    tmp_path: Path,
    operation: str,
) -> None:
    """Each admitted public operation keeps graceful shutdown pending and then
    finishes with its own factual outcome through the real coordinator."""

    remote = _remote(tmp_path)
    backend = _LocalBackend()
    driver = _BlockedAgentDriver(tmp_path / "sandbox" / "repo")
    runtime = ArchetypeRuntime()
    missions = runtime.missions(
        f"drain-{operation}",
        config=_config(backend, driver, tmp_path),
        storage=StorageConfig(
            uri=str(tmp_path / f"mission_drain_{operation}"),
            namespace=f"mission_drain_{operation}_contract",
        ),
    )
    submitted = await missions.submit(
        repository=str(remote),
        branch=f"agent/drain-{operation}",
        tasks=(_task(),),
    )
    service = missions._reservation.require_bound()  # noqa: SLF001 - exact owner oracle
    started = asyncio.Event()
    release = asyncio.Event()
    original = getattr(service, operation)

    async def barriered(*args: object, **kwargs: object) -> object:
        started.set()
        await release.wait()
        return await original(*args, **kwargs)

    setattr(service, operation, barriered)

    if operation == "submit":
        admitted = asyncio.create_task(
            missions.submit(
                repository=str(remote),
                branch="agent/drain-late-submit",
                tasks=(
                    AgentTask(
                        "second",
                        "Create a second explicit task graph.",
                        (CommandValidator("noop", ("true",)),),
                    ),
                ),
            )
        )
    elif operation == "restore_sandbox":
        admitted = asyncio.create_task(
            missions.restore_sandbox(
                submitted,
                CheckpointRef(
                    "local",
                    "cp",
                    "local://cp",
                    1,
                    environment="local-drain-test",
                    source_sandbox_id="sandbox-drain",
                    owner_id=str(submitted.mission_id),
                ),
            )
        )
    else:
        admitted = asyncio.create_task(missions.query())

    await asyncio.wait_for(started.wait(), timeout=30)
    shutting_down = asyncio.create_task(runtime.shutdown())
    await _spin()
    assert not shutting_down.done(), f"shutdown overtook admitted {operation}"

    release.set()
    if operation == "restore_sandbox":
        # The provider cannot restore; the admitted operation completes with
        # its own factual provider error, never a runtime-closed error.
        with pytest.raises(NotImplementedError, match="cannot restore"):
            await asyncio.wait_for(admitted, timeout=60)
    else:
        outcome = await asyncio.wait_for(admitted, timeout=60)
        if operation == "submit":
            assert isinstance(outcome, SubmittedMission)
        else:
            assert outcome is not None
    await asyncio.wait_for(shutting_down, timeout=60)


@pytest.mark.asyncio
async def test_closed_mission_handle_rejects_new_operations_by_contract(
    tmp_path: Path,
) -> None:
    """After public close releases the owner, every public operation rejects
    with the handle's own closed contract, not an internal registry error."""

    backend = _LocalBackend()
    driver = _BlockedAgentDriver(tmp_path / "sandbox" / "repo")
    runtime = ArchetypeRuntime()
    missions = runtime.missions(
        "released-handle",
        config=_config(backend, driver, tmp_path),
        storage=StorageConfig(
            uri=str(tmp_path / "mission_released_handle"),
            namespace="mission_released_handle_contract",
        ),
    )
    await missions.close()
    assert missions._reservation.released  # noqa: SLF001 - exact owner oracle

    submitted = SubmittedMission(
        mission_id=1,
        task_ids=(),
        repository="owner/repository",
        branch="agent/released-handle",
    )
    with pytest.raises(RuntimeError, match="Agent Missions handle is closed"):
        await missions.submit(
            repository="owner/repository",
            branch="agent/released-handle",
            tasks=(_task(),),
        )
    with pytest.raises(RuntimeError, match="Agent Missions handle is closed"):
        await missions.run(submitted)
    with pytest.raises(RuntimeError, match="Agent Missions handle is closed"):
        await missions.restore_sandbox(
            submitted,
            CheckpointRef("local", "cp", "local://cp", 1),
        )
    with pytest.raises(RuntimeError, match="Agent Missions handle is closed"):
        await missions.query()
    assert backend.creates == 0
    await runtime.shutdown()
