# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Runtime-lifetime drain contracts for admitted Agent Missions operations.

These tests bind the whole-operation admission contract from issue #627: a
mission operation admitted before runtime shutdown drains as one supported
workflow. The race schedule is the issue's dogfood counterfactual — shutdown
begins while an admitted ``run()`` is blocked in external execution, between
its committed dispatch and its later observation staging, when no individual
world call is in flight. An implementation that guards only individual world
calls drains at that barrier and fails these tests.

v0.5 admits only the Modal sandbox backend end to end, and external execution
crosses the durable author/critic Activity binding. These tests therefore
submit through the real Modal-only wiring composition and then replace the two
provider executors on the composed binding with local Git fakes, so the
admitted run still exercises the real dispatcher admission, required
projector, Activity workers, observation stagers, transition processors, and
world machinery without Modal credentials. The executor seam is exactly the
external-execution boundary the #627 schedule blocks on.
"""

from __future__ import annotations

import asyncio
import hashlib
import subprocess
import tempfile
from pathlib import Path
from typing import Any

import pytest

from archetype import ArchetypeRuntime
from archetype import wiring as wiring_module
from archetype.core.config import StorageConfig
from archetype.errors import RuntimeShutdownError
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
from archetype.missions.activities import AuthorExecutionObservation, AuthorRecovered
from archetype.missions.coding_agents.contracts import (
    AgentExecutionResult,
    CommitObservation,
    ValidationObservation,
)
from archetype.missions.contracts import MissionResult, SubmittedMission
from archetype.missions.critics import (
    CriticExecutionResult,
    CriticReceiptValue,
    CriticSubjectPolicy,
    CriticSubjectTransport,
    bind_critic_subject,
)
from archetype.missions.critics.contracts import canonical_digest, validator_bundle_digest
from archetype.missions.sandboxes import CheckpointRef, SandboxIdentity, SandboxStatus
from archetype.missions.sandboxes.modal import (
    MODAL_ACTIVITY_PROTOCOL_EPOCH,
    ModalSandboxBackend,
    ModalSandboxConfig,
)
from archetype.missions.transitions import (
    AgentExecutionStatus,
    CriticConclusion,
    CriticExecutionStatus,
)
from archetype.projections import latest
from archetype.storage.activity_catalog import SqliteActivityCatalog
from archetype.world.errors import WorldHasUnsettledWorkError


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


def _modal_backend() -> ModalSandboxBackend:
    """Pass v0.5's Modal-only admission without any Modal credential."""

    return ModalSandboxBackend(
        ModalSandboxConfig(
            workspace_name="drain-test-workspace",
            environment_name="drain-test-environment",
            operation_protocol_epoch=MODAL_ACTIVITY_PROTOCOL_EPOCH,
        )
    )


class _DrainAuthorExecutor:
    """Complete real local Git author work, then block inside external execution.

    While blocked, no world call is in flight: the admitted ``run()`` sits
    between its committed dispatch and its later observation staging.
    """

    provider = "local-git-author"

    def __init__(
        self,
        remote: Path,
        workspace_root: Path,
        *,
        content: str = "fixed",
    ) -> None:
        self.remote = remote
        self.workspace_root = workspace_root
        self.content = content
        self.blocked = asyncio.Event()
        self.release = asyncio.Event()
        self.probe: Any = None
        self.probe_outcomes: list[str] = []
        self.on_execute: Any = None
        self.execute_calls = 0
        self.observation: AuthorExecutionObservation | None = None

    async def execute(
        self,
        *,
        operation_id: str,
        request: Any,
        attempt: int,
        fence: int,
        retry_guard: Any,
    ) -> AuthorExecutionObservation:
        del attempt, fence, retry_guard
        self.execute_calls += 1
        digest = hashlib.sha256(operation_id.encode()).hexdigest()
        workspace = self.workspace_root / digest[:12]
        _git("clone", str(self.remote), str(workspace))
        _git("switch", "-c", request.branch, cwd=workspace)
        _git("config", "user.name", "Drain Author", cwd=workspace)
        _git("config", "user.email", "author@archetype.local", cwd=workspace)
        starting_revision = _git("rev-parse", "HEAD", cwd=workspace)
        (workspace / "implementation.txt").write_text(f"{self.content}\n")
        _git("add", "implementation.txt", cwd=workspace)
        message = f"{request.task_name}: drain oracle"
        _git("commit", "-m", message, cwd=workspace)
        final_revision = _git("rev-parse", "HEAD", cwd=workspace)
        validation: list[ValidationObservation] = []
        for validator in request.validators:
            actual = subprocess.run(
                validator.spec.command,
                cwd=workspace,
                check=False,
                capture_output=True,
                text=True,
            )
            validation.append(
                ValidationObservation(
                    validator_id=validator.validator_id,
                    name=validator.spec.name,
                    command=validator.spec.command,
                    expected_returncode=validator.spec.expected_returncode,
                    actual_returncode=actual.returncode,
                    revision=final_revision,
                    stdout=actual.stdout,
                    stderr=actual.stderr,
                )
            )
        _git("push", "-u", "origin", f"HEAD:refs/heads/{request.branch}", cwd=workspace)
        diff = subprocess.run(
            (
                "git",
                "diff",
                "--no-ext-diff",
                "--no-textconv",
                "--binary",
                starting_revision,
                final_revision,
            ),
            cwd=workspace,
            check=True,
            capture_output=True,
        ).stdout
        observation = AuthorExecutionObservation(
            result=AgentExecutionResult(
                mission_id=request.mission_id,
                task_id=request.task_id,
                dispatch_id=request.dispatch_id,
                dispatch_sequence=request.dispatch_sequence,
                status=AgentExecutionStatus.EXITED,
                sandbox=SandboxIdentity(
                    "local",
                    f"author-{digest[:12]}",
                    "local-drain-git",
                ),
                worktree=str(workspace),
                agent_session_id=f"session-{digest[:12]}",
                agent_returncode=0,
                starting_revision=starting_revision,
                final_revision=final_revision,
                diff_digest=hashlib.sha256(diff).hexdigest(),
                validator_bundle_digest=validator_bundle_digest(
                    tuple(
                        (
                            validator.validator_id,
                            validator.spec.name,
                            validator.spec.command,
                            validator.spec.expected_returncode,
                            validator.spec.timeout_seconds,
                        )
                        for validator in request.validators
                    )
                ),
                agent_stdout="drain author stdout",
                agent_stderr="",
                trace_uri=f"local-git://{digest}",
                validation=tuple(validation),
                commits=(
                    CommitObservation(
                        sha=final_revision,
                        message=message,
                        branch=request.branch,
                        pushed=True,
                        final_revision=True,
                    ),
                ),
            ),
            sandbox_status=SandboxStatus.CLOSED,
        )
        self.observation = observation
        if self.probe is not None:
            probe = self.probe
            self.probe = None
            try:
                await probe()
            except BaseException as error:  # noqa: BLE001 - recorded for assertion
                self.probe_outcomes.append(f"{type(error).__name__}: {error}")
            else:
                self.probe_outcomes.append("returned")
        if self.on_execute is not None:
            self.on_execute()
        self.blocked.set()
        await self.release.wait()
        return observation

    async def reconcile(self, *, operation_id: str, request: Any) -> Any:
        del operation_id, request
        if self.observation is None:
            raise AssertionError("author recovery requires a completed provider observation")
        return AuthorRecovered(self.observation)


class _DrainCriticExecutor:
    """Recompute the exact candidate diff from the local remote and approve it."""

    provider = "local-git-critic"

    def __init__(self, remote: Path, root: Path) -> None:
        self.remote = remote
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)
        self.execute_calls = 0

    async def execute(
        self,
        *,
        operation_id: str,
        request: Any,
        attempt: int,
        fence: int,
        retry_guard: Any,
    ) -> CriticExecutionResult:
        del attempt, fence, retry_guard
        self.execute_calls += 1
        diff = subprocess.run(
            (
                "git",
                "--git-dir",
                str(self.remote),
                "diff",
                "--no-ext-diff",
                "--no-textconv",
                "--binary",
                request.base_revision,
                request.head_revision,
            ),
            check=True,
            capture_output=True,
        ).stdout
        subject_directory = Path(tempfile.mkdtemp(prefix="archetype-drain-critic.", dir=self.root))
        subject_path = subject_directory / "subject.diff"
        try:
            subject_path.write_bytes(diff)
            subject = bind_critic_subject(
                CriticSubjectPolicy(
                    digest=request.diff_digest,
                    max_bytes=request.subject.max_bytes,
                ),
                metadata=f"review:{request.review_id}".encode(),
                content=subject_path.read_bytes(),
                transport=CriticSubjectTransport.SANDBOX_FILE,
                ref=str(subject_path),
            )
            completed_at_ms = 200
            receipt = CriticReceiptValue(
                review_id=request.review_id,
                conclusion=CriticConclusion.APPROVED,
                candidate_digest=request.candidate_digest,
                policy_digest=request.policy.digest,
                evidence_digest=canonical_digest({"conclusion": "approved"}),
                reviewed_base_revision=request.base_revision,
                reviewed_head_revision=request.head_revision,
                reviewed_diff_digest=request.diff_digest,
                validator_bundle_digest=request.validator_bundle_digest,
                subject_metadata_digest=subject.metadata_digest,
                subject_digest=subject.subject_digest,
                subject_content_size_bytes=subject.content_size_bytes,
                subject_metadata_size_bytes=subject.metadata_size_bytes,
                subject_size_bytes=subject.total_size_bytes,
                subject_media_type=subject.media_type,
                subject_transport=subject.transport.value,
                subject_ref=subject.ref,
                reviewed_scope="exact task diff",
                finding_count=0,
                blocking_count=0,
                output_schema_version=request.policy.output_schema_version,
                completed_at_ms=completed_at_ms,
            )
            operation_digest = hashlib.sha256(operation_id.encode()).hexdigest()
            return CriticExecutionResult(
                request=request.as_review_request(),
                status=CriticExecutionStatus.EXITED,
                sandbox=SandboxIdentity(
                    "local",
                    f"critic-{operation_digest[:12]}",
                    "local-drain-git",
                ),
                sandbox_status=SandboxStatus.CLOSED,
                sandbox_acquired=True,
                started_at_ms=150,
                ended_at_ms=completed_at_ms,
                raw_output='{"conclusion":"approved","schema_version":1}',
                trace_uri=f"local-git-critic://{operation_digest}",
                findings=(),
                receipt=receipt,
            )
        finally:
            if subject_path.exists():
                subject_path.unlink()
            subject_directory.rmdir()

    async def reconcile(self, *, operation_id: str, request: Any) -> Any:
        raise AssertionError("the drain critic fake never reconciles")


def _config(backend: ModalSandboxBackend) -> AgentMissionConfig:
    return AgentMissionConfig(
        sandbox_backend=backend,
        sandbox_environment=backend.environment,
        checkpoint_after_dispatch=False,
    )


def _task() -> AgentTask:
    return AgentTask(
        "implementation",
        "Create implementation.txt containing fixed.",
        (
            CommandValidator(
                "focused",
                # Plain non-login shell: -l would source the runner's profile
                # (bash-specific in the CI container) and make the validator's
                # return code environment-dependent.
                ("sh", "-c", 'test "$(cat implementation.txt)" = fixed'),
            ),
        ),
        max_dispatches=1,
    )


def _swap_executors(
    missions: Any,
    author: _DrainAuthorExecutor,
    critic: _DrainCriticExecutor,
) -> Any:
    """Replace the composed Modal executors at the external-execution seam."""

    service = missions._reservation.require_bound()  # noqa: SLF001 - exact owner oracle
    binding = service._activity  # noqa: SLF001 - concrete composition oracle
    binding.author.worker._executor = author  # noqa: SLF001 - executor seam
    binding.critic.worker._executor = critic  # noqa: SLF001 - executor seam
    return service


async def _spin(iterations: int = 200) -> None:
    for _ in range(iterations):
        await asyncio.sleep(0)


@pytest.mark.asyncio
async def test_runtime_shutdown_drains_admitted_run_blocked_in_external_execution(
    tmp_path: Path,
) -> None:
    """Issue #627 schedule: graceful shutdown drains the whole admitted run."""

    remote = _remote(tmp_path)
    author = _DrainAuthorExecutor(remote, tmp_path / "author")
    critic = _DrainCriticExecutor(remote, tmp_path / "critic")
    storage = StorageConfig(
        uri=str(tmp_path / "mission_runtime_drain"),
        namespace="mission_runtime_drain_contract",
    )
    world_id_reentries: list[object] = []
    scheduled_reentries: list[asyncio.Task[object]] = []

    runtime = ArchetypeRuntime()
    missions = runtime.missions(
        "runtime-drain",
        config=_config(_modal_backend()),
        storage=storage,
    )
    submitted = await missions.submit(
        repository=str(remote),
        branch="agent/runtime-drain",
        tasks=(_task(),),
    )
    _swap_executors(missions, author, critic)
    world_id = str(missions.world_id)

    # Ordinary admitted work is not teardown: from inside the admitted run,
    # runtime shutdown and public mission close both reject deterministically.
    async def teardown_authority_probe() -> None:
        with pytest.raises(RuntimeError, match="cannot close from an admitted operation"):
            await runtime.shutdown()
        with pytest.raises(RuntimeError, match="cannot close from an admitted operation"):
            await missions.close()

    author.probe = teardown_authority_probe

    def reentry_from_admitted_run() -> None:
        # Synchronous re-entry from the admitted task and scheduled async
        # re-entry from a fresh task; neither may deadlock shutdown.
        try:
            world_id_reentries.append(str(missions.world_id))
        except RuntimeError as error:
            world_id_reentries.append(error)
        scheduled_reentries.append(asyncio.ensure_future(missions.query()))

    author.on_execute = reentry_from_admitted_run

    run_task = asyncio.create_task(missions.run(submitted))
    await asyncio.wait_for(author.blocked.wait(), timeout=30)
    assert author.probe_outcomes == ["returned"]

    shutting_down = asyncio.create_task(runtime.shutdown())
    await _spin()

    # The admitted run is blocked between world calls; a per-call guard would
    # observe zero in-flight calls and let shutdown finish here.
    assert not shutting_down.done()

    # New supported mission operations fail before side effects.
    executes_before = author.execute_calls
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
    assert author.execute_calls == executes_before

    author.release.set()
    result = await asyncio.wait_for(run_task, timeout=120)
    assert isinstance(result, MissionResult)
    assert result.status == "succeeded"
    assert author.execute_calls == 1
    assert critic.execute_calls == 1

    await asyncio.wait_for(shutting_down, timeout=60)

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
    author = _DrainAuthorExecutor(remote, tmp_path / "author", content="broken")
    critic = _DrainCriticExecutor(remote, tmp_path / "critic")
    storage = StorageConfig(
        uri=str(tmp_path / "mission_factual_failure"),
        namespace="mission_factual_failure_contract",
    )
    runtime = ArchetypeRuntime()
    missions = runtime.missions(
        "factual-failure",
        config=_config(_modal_backend()),
        storage=storage,
    )
    submitted = await missions.submit(
        repository=str(remote),
        branch="agent/factual-failure",
        tasks=(_task(),),
    )
    _swap_executors(missions, author, critic)
    world_id = str(missions.world_id)

    run_task = asyncio.create_task(missions.run(submitted))
    await asyncio.wait_for(author.blocked.wait(), timeout=30)
    shutting_down = asyncio.create_task(runtime.shutdown())
    await _spin()
    assert not shutting_down.done()

    author.release.set()
    result = await asyncio.wait_for(run_task, timeout=120)
    assert isinstance(result, MissionResult)
    assert result.status == "failed"
    assert critic.execute_calls == 0
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
    author = _DrainAuthorExecutor(remote, tmp_path / "author")
    critic = _DrainCriticExecutor(remote, tmp_path / "critic")
    runtime = ArchetypeRuntime()
    missions = runtime.missions(
        "close-shutdown-race",
        config=_config(_modal_backend()),
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
    _swap_executors(missions, author, critic)
    run_task = asyncio.create_task(missions.run(submitted))
    await asyncio.wait_for(author.blocked.wait(), timeout=30)

    shutting_down = asyncio.create_task(runtime.shutdown())
    closing = asyncio.create_task(missions.close())
    await _spin()
    assert not shutting_down.done()
    assert not closing.done()

    author.release.set()
    result = await asyncio.wait_for(run_task, timeout=120)
    assert result.status == "succeeded"
    await asyncio.wait_for(asyncio.gather(shutting_down, closing), timeout=60)
    assert missions._reservation.released  # noqa: SLF001 - exact owner oracle


@pytest.mark.asyncio
async def test_cancelling_admitted_run_does_not_wedge_runtime_shutdown(
    tmp_path: Path,
) -> None:
    """Caller cancellation of the blocked run releases admission and lets the
    pending graceful shutdown finish.

    Under the v0.5 Activity contract the cancelled run abandons a
    provider-bound author Activity, so the finished shutdown fails closed with
    the retained mission owner instead of silently discarding that durable
    work; settlement belongs to a later process resume's reconciliation."""

    remote = _remote(tmp_path)
    author = _DrainAuthorExecutor(remote, tmp_path / "author")
    critic = _DrainCriticExecutor(remote, tmp_path / "critic")
    runtime = ArchetypeRuntime()
    missions = runtime.missions(
        "cancelled-run",
        config=_config(_modal_backend()),
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
    _swap_executors(missions, author, critic)
    run_task = asyncio.create_task(missions.run(submitted))
    await asyncio.wait_for(author.blocked.wait(), timeout=30)
    shutting_down = asyncio.create_task(runtime.shutdown())
    await _spin()
    assert not shutting_down.done()

    run_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(run_task, timeout=30)
    # Cancellation released admission, so the pending shutdown finishes
    # instead of waiting forever on the cancelled run. It reports the exact
    # retained owner because the abandoned author Activity is still durably
    # unsettled and only reconciliation may settle it.
    with pytest.raises(RuntimeShutdownError) as shutdown_failure:
        await asyncio.wait_for(shutting_down, timeout=60)
    assert shutdown_failure.value.phase == "workflow-handles"
    assert [failure.owner for failure in shutdown_failure.value.failures] == [
        missions._owner_id  # noqa: SLF001 - exact owner oracle
    ]
    assert isinstance(
        shutdown_failure.value.failures[0].cause,
        WorldHasUnsettledWorkError,
    )
    assert not missions._reservation.released  # noqa: SLF001 - retained for retry
    # A later serialized shutdown retries the same phase and keeps failing
    # closed while the Activity remains unsettled; it never discards the owner.
    with pytest.raises(RuntimeShutdownError):
        await runtime.shutdown()
    assert not missions._reservation.released  # noqa: SLF001 - retained for retry


@pytest.mark.asyncio
async def test_replacement_runtime_recovers_cancelled_mission_without_provider_replay(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The public run surface cold-binds the exact durable Mission world."""

    clock = [0.0]
    monkeypatch.setattr(
        wiring_module,
        "SqliteActivityCatalog",
        lambda path: SqliteActivityCatalog(path, now_seconds=lambda: clock[0]),
    )
    remote = _remote(tmp_path)
    author = _DrainAuthorExecutor(remote, tmp_path / "author")
    critic = _DrainCriticExecutor(remote, tmp_path / "critic")
    storage = StorageConfig(
        uri=str(tmp_path / "mission_cold_resume"),
        namespace="mission_cold_resume_contract",
    )
    first_runtime = ArchetypeRuntime()
    first = first_runtime.missions(
        "cold-resume",
        config=_config(_modal_backend()),
        storage=storage,
    )
    submitted = await first.submit(
        repository=str(remote),
        branch="agent/cold-resume",
        tasks=(_task(),),
    )
    _swap_executors(first, author, critic)
    interrupted = asyncio.create_task(first.run(submitted))
    await asyncio.wait_for(author.blocked.wait(), timeout=30)
    interrupted.cancel()
    with pytest.raises(asyncio.CancelledError):
        await interrupted

    # Simulate the durable lease elapsing after process loss. The replacement
    # adapters recover the provider-bound result and review; execute() must not
    # run a second time for the author Activity.
    clock[0] = 301.0
    monkeypatch.setattr(
        wiring_module,
        "ModalMissionAuthorExecutor",
        lambda **kwargs: author,
    )
    monkeypatch.setattr(
        wiring_module,
        "ModalMissionCriticExecutor",
        lambda **kwargs: critic,
    )
    replacement = ArchetypeRuntime()
    resumed_handle = replacement.missions(
        "cold-resume",
        config=_config(_modal_backend()),
        storage=storage,
    )
    result = await resumed_handle.run(submitted)

    assert result.status == "succeeded"
    assert result.mission_id == submitted.mission_id
    assert submitted.world_id == str(first.world_id)
    assert author.execute_calls == 1
    assert critic.execute_calls == 1

    await replacement.shutdown()
    await first_runtime.shutdown()


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["submit", "restore_sandbox", "query"])
async def test_runtime_shutdown_drains_each_admitted_mission_operation(
    tmp_path: Path,
    operation: str,
) -> None:
    """Each admitted public operation keeps graceful shutdown pending and then
    finishes with its own factual outcome through the real coordinator."""

    remote = _remote(tmp_path)
    backend = _modal_backend()
    runtime = ArchetypeRuntime()
    missions = runtime.missions(
        f"drain-{operation}",
        config=_config(backend),
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
                    environment=backend.environment,
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
        # The v0.5 Modal Activity path rejects restore by contract; the
        # admitted operation completes with that factual error, never a
        # runtime-closed error.
        with pytest.raises(NotImplementedError, match="checkpoint restore is unavailable"):
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

    runtime = ArchetypeRuntime()
    missions = runtime.missions(
        "released-handle",
        config=_config(_modal_backend()),
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
        episode_id="episode-released-handle",
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
    # No mission service was ever constructed, so closing produced no provider
    # side effects and left nothing bound to the released owner.
    with pytest.raises(RuntimeError, match="not bound"):
        missions._reservation.require_bound()  # noqa: SLF001 - exact owner oracle
    await runtime.shutdown()
