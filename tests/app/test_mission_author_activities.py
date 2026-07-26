# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Crash contracts for the Mission author-activity coordination substrate.

The tests exercise real local Git effects and restart the durable catalog and
value store.  Their committed-snapshot reader and observation stager remain
test adapters; concrete receipt-pinned world reads and idempotent ECS staging
are a separate integration gate, not something this proof claims to provide.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import subprocess
from dataclasses import replace
from pathlib import Path
from typing import Any

import daft
import pytest

from archetype.activities import ActivityCoordinator
from archetype.app.missions.activities import (
    AuthorActivityReconciliationRequired,
    CommittedMissionSnapshot,
    MissionAuthorActivityProjector,
    MissionAuthorActivityWorker,
)
from archetype.app.missions.activity_coordinator import (
    MissionAuthorActivityCoordinator,
)
from archetype.app.missions.local_activity_values import LocalMissionAuthorValueStore
from archetype.core.component import Component
from archetype.core.interfaces import CommittedTickReceipt
from archetype.missions.activities import (
    AUTHOR_ACTIVITY_KIND,
    AuthorActivityRequestRef,
    AuthorActivityResultRef,
    AuthorActivityRetryGuard,
    AuthorConfirmedAbsent,
    AuthorExecutionObservation,
    AuthorRecovered,
    AuthorRecoveryUnknown,
    DurableAuthorExecutionObservation,
    author_activity_fact_bundle,
    author_provider_operation_id,
)
from archetype.missions.coding_agents.contracts import (
    AgentExecutionResult,
    CommitObservation,
    DispatchedValidator,
    FrictionObservation,
    TaskDispatchRequest,
    ValidationObservation,
)
from archetype.missions.components import (
    AgentExecution,
    AuthorActivityObservation,
    Commit,
    FrictionLog,
    Task,
    TaskCriticPolicy,
    TaskDispatch,
    TaskPolicy,
    TaskState,
    TaskValidator,
    TaskWorkspace,
    ValidationResult,
)
from archetype.missions.contracts import (
    CommandValidator,
    CriticPolicy,
    RepositoryPublicationPolicy,
)
from archetype.missions.relations import Guards, PartOfMission
from archetype.missions.sandboxes import SandboxIdentity, SandboxStatus
from archetype.missions.transitions import AgentExecutionStatus, TaskStatus
from archetype.redaction import RedactionService, SecretQuarantineError
from archetype.storage.activity_catalog import SqliteActivityCatalog

_SECRET = "github_pat_ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890"


class _Reader:
    def __init__(self, snapshot: CommittedMissionSnapshot) -> None:
        self.snapshot = snapshot
        self.receipts: list[CommittedTickReceipt] = []

    async def read(self, receipt: CommittedTickReceipt) -> CommittedMissionSnapshot:
        self.receipts.append(receipt)
        return self.snapshot


def _open_catalog(
    path: Path,
    *,
    lease_seconds: float = 0.01,
) -> tuple[
    SqliteActivityCatalog,
    ActivityCoordinator,
    MissionAuthorActivityCoordinator,
]:
    physical = SqliteActivityCatalog(path)
    generic = ActivityCoordinator(physical)
    return (
        physical,
        generic,
        MissionAuthorActivityCoordinator(
            generic,
            lease_seconds=lease_seconds,
        ),
    )


def _git(
    *arguments: str,
    cwd: Path | None = None,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ("git", *arguments),
        cwd=cwd,
        check=check,
        capture_output=True,
        text=True,
    )


def _git_input(
    *arguments: str,
    input_text: str,
    cwd: Path | None = None,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ("git", *arguments),
        cwd=cwd,
        check=check,
        capture_output=True,
        text=True,
        input=input_text,
    )


def _init_bare_remote(root: Path) -> Path:
    remote = root / "remote.git"
    seed = root / "seed"
    root.mkdir()
    _git("init", "--bare", str(remote))
    _git("init", "--initial-branch=main", str(seed))
    _git("config", "user.name", "Archetype Test", cwd=seed)
    _git("config", "user.email", "test@archetype.local", cwd=seed)
    (seed / "README.md").write_text("base\n")
    _git("add", "README.md", cwd=seed)
    _git("commit", "-m", "base", cwd=seed)
    _git("remote", "add", "origin", str(remote), cwd=seed)
    _git("push", "-u", "origin", "main", cwd=seed)
    _git("--git-dir", str(remote), "symbolic-ref", "HEAD", "refs/heads/main")
    return remote


class _ProviderReplayBlocked(RuntimeError):
    """The durable provider start barrier belongs to another execution attempt."""


class _LocalGitProvider:
    """Real local Git publication with provider reconstruction from the remote."""

    provider = "local-git"

    def __init__(
        self,
        remote: Path,
        counter_path: Path,
        workspace: Path,
        *,
        crash_after_publish: bool = False,
        unknown: bool = False,
    ) -> None:
        self.remote = remote
        self.counter_path = counter_path
        self.workspace = workspace
        self.crash_after_publish = crash_after_publish
        self.unknown = unknown
        if not counter_path.exists():
            counter_path.write_text(json.dumps({"execute_calls": 0, "reconcile_calls": 0}))

    def _read(self) -> dict[str, Any]:
        return json.loads(self.counter_path.read_text())

    def _write(self, state: dict[str, Any]) -> None:
        self.counter_path.write_text(json.dumps(state, sort_keys=True))

    @property
    def execute_calls(self) -> int:
        return int(self._read()["execute_calls"])

    @property
    def reconcile_calls(self) -> int:
        return int(self._read()["reconcile_calls"])

    async def execute(
        self,
        *,
        operation_id: str,
        request,
        attempt: int,
        fence: int,
        retry_guard: AuthorActivityRetryGuard | None,
    ) -> AuthorExecutionObservation:
        self._begin_execution(
            operation_id=operation_id,
            attempt=attempt,
            fence=fence,
            retry_guard=retry_guard,
        )
        state = self._read()
        state["execute_calls"] += 1
        self._write(state)

        starting_revision = _git(
            "--git-dir",
            str(self.remote),
            "rev-parse",
            "refs/heads/main",
        ).stdout.strip()
        _git("clone", str(self.remote), str(self.workspace))
        _git("switch", "-c", request.branch, cwd=self.workspace)
        _git("config", "user.name", "Archetype Author", cwd=self.workspace)
        _git("config", "user.email", "author@archetype.local", cwd=self.workspace)
        (self.workspace / "proof.txt").write_text(f"{operation_id}\n")
        _git("add", "proof.txt", cwd=self.workspace)
        _git("commit", "-m", f"{request.task_name}: prove activity", cwd=self.workspace)
        final_revision = _git("rev-parse", "HEAD", cwd=self.workspace).stdout.strip()
        result_ref = self._result_ref(operation_id)
        _git("update-ref", result_ref, final_revision, cwd=self.workspace)
        _git(
            "push",
            "--atomic",
            f"--force-with-lease={result_ref}:",
            "-u",
            "origin",
            f"HEAD:refs/heads/{request.branch}",
            f"{result_ref}:{result_ref}",
            cwd=self.workspace,
        )
        observation = self._remote_observation(
            operation_id,
            request,
            starting_revision=starting_revision,
            final_revision=final_revision,
        )
        if self.crash_after_publish:
            raise RuntimeError("worker died after external Git publication")
        return observation

    async def reconcile(self, *, operation_id: str, request):
        state = self._read()
        state["reconcile_calls"] += 1
        self._write(state)
        if self.unknown:
            return AuthorRecoveryUnknown("provider lookup unavailable")
        final_revision = self._marker_oid(self._result_ref(operation_id))
        if final_revision:
            parents = (
                _git(
                    "--git-dir",
                    str(self.remote),
                    "rev-list",
                    "--parents",
                    "-n",
                    "1",
                    final_revision,
                )
                .stdout.strip()
                .split()
            )
            if (
                len(parents) != 2
                or parents[0] != final_revision
                or not self._published_operation_matches(final_revision, operation_id)
            ):
                return AuthorRecoveryUnknown(
                    "provider result receipt does not prove one exact publication"
                )
            return AuthorRecovered(
                self._remote_observation(
                    operation_id,
                    request,
                    starting_revision=parents[1],
                    final_revision=final_revision,
                )
            )
        head = self._remote_head(request.branch)
        if head:
            return AuthorRecoveryUnknown(
                "published branch has no exact atomic provider result receipt"
            )
        guard = self._install_replay_barrier(operation_id)
        if guard is None:
            return AuthorRecoveryUnknown(
                "provider operation-start marker exists without a published result"
            )
        return AuthorConfirmedAbsent(guard)

    def _begin_execution(
        self,
        *,
        operation_id: str,
        attempt: int,
        fence: int,
        retry_guard: AuthorActivityRetryGuard | None,
    ) -> None:
        marker_ref = self._marker_ref(operation_id)
        marker_text = self._marker_text(
            operation_id=operation_id,
            mode="attempt",
            attempt=attempt,
            fence=fence,
        )
        marker_oid = self._write_marker(marker_text)
        if retry_guard is None:
            updated = _git(
                "--git-dir",
                str(self.remote),
                "update-ref",
                marker_ref,
                marker_oid,
                "0" * 40,
                check=False,
            )
            if updated.returncode != 0:
                raise _ProviderReplayBlocked(
                    "provider operation already has a durable start barrier"
                )
            return

        if retry_guard.ref != marker_ref:
            raise _ProviderReplayBlocked("retry guard refers to another provider operation")
        current_oid = self._marker_oid(marker_ref)
        if not current_oid:
            raise _ProviderReplayBlocked("retry guard's provider barrier disappeared")
        current_text = self._read_marker(current_oid)
        if hashlib.sha256(current_text.encode()).hexdigest() != retry_guard.digest:
            raise _ProviderReplayBlocked("retry guard digest no longer matches the provider")
        barrier = json.loads(current_text)
        if (
            barrier.get("mode") != "replay-barrier"
            or barrier.get("operation_id") != operation_id
            or barrier.get("attempt") is not None
            or barrier.get("fence") is not None
        ):
            raise _ProviderReplayBlocked("retry guard has already been consumed")
        updated = _git(
            "--git-dir",
            str(self.remote),
            "update-ref",
            marker_ref,
            marker_oid,
            current_oid,
            check=False,
        )
        if updated.returncode != 0:
            raise _ProviderReplayBlocked("another worker consumed the retry guard")

    def _install_replay_barrier(
        self,
        operation_id: str,
    ) -> AuthorActivityRetryGuard | None:
        marker_ref = self._marker_ref(operation_id)
        marker_text = self._marker_text(
            operation_id=operation_id,
            mode="replay-barrier",
        )
        marker_oid = self._write_marker(marker_text)
        current_oid = self._marker_oid(marker_ref)
        if not current_oid:
            _git(
                "--git-dir",
                str(self.remote),
                "update-ref",
                marker_ref,
                marker_oid,
                "0" * 40,
                check=False,
            )
            current_oid = self._marker_oid(marker_ref)
        if current_oid != marker_oid:
            return None
        current_text = self._read_marker(current_oid)
        if current_text != marker_text:
            return None
        return AuthorActivityRetryGuard(
            ref=marker_ref,
            digest=hashlib.sha256(current_text.encode()).hexdigest(),
        )

    @staticmethod
    def _marker_ref(operation_id: str) -> str:
        identity = hashlib.sha256(operation_id.encode()).hexdigest()
        return f"refs/archetype/activity-starts/{identity}"

    @staticmethod
    def _result_ref(operation_id: str) -> str:
        identity = hashlib.sha256(operation_id.encode()).hexdigest()
        return f"refs/archetype/activity-results/{identity}"

    @staticmethod
    def _marker_text(
        *,
        operation_id: str,
        mode: str,
        attempt: int | None = None,
        fence: int | None = None,
    ) -> str:
        return json.dumps(
            {
                "attempt": attempt,
                "fence": fence,
                "mode": mode,
                "operation_id": operation_id,
            },
            separators=(",", ":"),
            sort_keys=True,
        )

    def _write_marker(self, marker_text: str) -> str:
        return _git_input(
            "--git-dir",
            str(self.remote),
            "hash-object",
            "-w",
            "--stdin",
            input_text=marker_text,
        ).stdout.strip()

    def _marker_oid(self, marker_ref: str) -> str:
        result = _git(
            "--git-dir",
            str(self.remote),
            "rev-parse",
            "--verify",
            "-q",
            marker_ref,
            check=False,
        )
        return result.stdout.strip() if result.returncode == 0 else ""

    def _read_marker(self, marker_oid: str) -> str:
        return _git(
            "--git-dir",
            str(self.remote),
            "cat-file",
            "blob",
            marker_oid,
        ).stdout

    def _remote_head(self, branch: str) -> str:
        result = _git(
            "ls-remote",
            str(self.remote),
            f"refs/heads/{branch}",
        )
        return result.stdout.partition("\t")[0].strip()

    def _published_operation_matches(self, revision: str, operation_id: str) -> bool:
        proof = _git(
            "--git-dir",
            str(self.remote),
            "show",
            f"{revision}:proof.txt",
            check=False,
        )
        return proof.returncode == 0 and proof.stdout.strip() == operation_id

    def _remote_observation(
        self,
        operation_id: str,
        request,
        *,
        starting_revision: str,
        final_revision: str,
    ) -> AuthorExecutionObservation:
        if not self._published_operation_matches(final_revision, operation_id):
            raise RuntimeError("provider receipt points at another Git publication")
        if not self.workspace.exists():
            _git(
                "clone",
                "--no-checkout",
                str(self.remote),
                str(self.workspace),
            )
        _git(
            "fetch",
            "origin",
            self._result_ref(operation_id),
            cwd=self.workspace,
        )
        _git("checkout", "--detach", final_revision, cwd=self.workspace)
        validation: list[ValidationObservation] = []
        for validator in request.validators:
            command = (*validator.spec.command, f"--token={_SECRET}")
            actual = subprocess.run(
                command,
                cwd=self.workspace,
                check=False,
                capture_output=True,
                text=True,
            )
            validation.append(
                ValidationObservation(
                    validator_id=validator.validator_id,
                    name=validator.spec.name,
                    command=command,
                    expected_returncode=validator.spec.expected_returncode,
                    actual_returncode=actual.returncode,
                    revision=final_revision,
                    stdout=actual.stdout,
                    stderr=actual.stderr,
                )
            )
        diff = _git(
            "--git-dir",
            str(self.remote),
            "diff",
            "--binary",
            starting_revision,
            final_revision,
        ).stdout
        message = _git(
            "--git-dir",
            str(self.remote),
            "show",
            "-s",
            "--format=%s",
            final_revision,
        ).stdout.strip()
        operation_digest = hashlib.sha256(operation_id.encode()).hexdigest()
        noisy = ("x" * 20_000) + f" token={_SECRET}"
        return AuthorExecutionObservation(
            result=AgentExecutionResult(
                mission_id=request.mission_id,
                task_id=request.task_id,
                dispatch_id=request.dispatch_id,
                dispatch_sequence=request.dispatch_sequence,
                status=AgentExecutionStatus.EXITED,
                sandbox=SandboxIdentity(
                    "local",
                    f"git-{operation_digest[:16]}",
                    "local-bare-git",
                ),
                worktree=str(self.workspace),
                agent_session_id=f"session-{operation_digest[:16]}",
                agent_returncode=0,
                starting_revision=starting_revision,
                final_revision=final_revision,
                diff_digest=hashlib.sha256(diff.encode()).hexdigest(),
                validator_bundle_digest=hashlib.sha256(
                    repr(request.validators).encode()
                ).hexdigest(),
                agent_stdout=noisy,
                agent_stderr=f"password={_SECRET}",
                trace_uri=f"local-git://{operation_digest}",
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
                friction=(FrictionObservation("provider", f"token={_SECRET}"),),
            ),
            sandbox_status=SandboxStatus.READY,
        )


class _OtherGitProvider(_LocalGitProvider):
    provider = "other-local-git"


class _CrashOnceStager:
    def __init__(self, *, crash: bool) -> None:
        self.crash = crash
        self.staged: dict[tuple[str, str], DurableAuthorExecutionObservation] = {}
        self.results: dict[tuple[str, str], AuthorActivityResultRef] = {}

    async def stage_author_observation(
        self,
        *,
        world_id: str,
        activity_id: str,
        request,
        result: AuthorActivityResultRef,
        observation: DurableAuthorExecutionObservation,
    ) -> None:
        assert request.dispatch_id == activity_id
        if self.crash:
            self.crash = False
            raise RuntimeError("worker died before the staged observation could commit")
        self.staged.setdefault((world_id, activity_id), observation)
        self.results.setdefault((world_id, activity_id), result)


def _frame(entity_id: int, tick: int, *components: Component):
    values: dict[str, list[Any]] = {"entity_id": [entity_id], "tick": [tick]}
    for component in components:
        prefix = type(component).get_prefix()
        for field, value in component.model_dump().items():
            values[f"{prefix}{field}"] = [value]
    return daft.from_pydict(values)


def _dispatch_snapshot(
    *,
    world_id: str,
    run_id: str,
    tick: int,
    dispatch_id: str,
    repository: str = "owner/repo",
    visibility_token: str | None = None,
) -> CommittedMissionSnapshot:
    policy = CriticPolicy()
    task_components = (
        Task(name="prove-activity", prompt="commit proof.txt"),
        TaskWorkspace(repository=repository, branch="proof/activity"),
        TaskPolicy(),
        TaskCriticPolicy(
            policy_id=policy.policy_id,
            version=policy.version,
            digest=policy.digest,
            perspective=policy.perspective,
            information_view=policy.information_view,
            driver=policy.driver,
            model=policy.model,
            sampling=policy.sampling,
            max_reviews=policy.max_reviews,
            timeout_seconds=policy.timeout_seconds,
            output_schema_version=policy.output_schema_version,
            max_output_chars=policy.max_output_chars,
        ),
        TaskState(status=TaskStatus.DISPATCHED.value),
        TaskDispatch(dispatch_id=dispatch_id, sequence=1),
    )
    membership = PartOfMission(source=7, target=3)
    validator = TaskValidator(name="proof-exists", command=["sh", "-c", "test -f proof.txt"])
    guard = Guards(source=11, target=7)
    return CommittedMissionSnapshot(
        world_id=world_id,
        run_id=run_id,
        committed_tick=tick,
        visibility_token=visibility_token or f"token-{tick}",
        results={
            tuple(type(item) for item in task_components): _frame(7, tick, *task_components),
            (PartOfMission,): _frame(10, tick, membership),
            (TaskValidator,): _frame(11, tick, validator),
            (Guards,): _frame(12, tick, guard),
        },
    )


def _partial_observation_snapshot(
    *,
    world_id: str,
    run_id: str,
    tick: int,
    observation: DurableAuthorExecutionObservation,
    repository: str = "owner/repo",
    visibility_token: str | None = None,
) -> CommittedMissionSnapshot:
    execution = observation.result
    dispatch = _dispatch_snapshot(
        world_id=world_id,
        run_id=run_id,
        tick=tick,
        dispatch_id=execution.dispatch_id,
        repository=repository,
        visibility_token=visibility_token,
    )
    component = _execution_component(
        execution,
        redaction_policy_id=observation.redaction_policy_id,
    )
    return CommittedMissionSnapshot(
        world_id=world_id,
        run_id=run_id,
        committed_tick=tick,
        visibility_token=visibility_token or f"token-{tick}",
        results={
            **dispatch.results,
            (AgentExecution,): _frame(20, tick, component),
        },
    )


def _execution_component(
    execution: AgentExecutionResult,
    *,
    redaction_policy_id: str,
) -> AgentExecution:
    return AgentExecution(
        task_id=execution.task_id,
        dispatch_id=execution.dispatch_id,
        dispatch_sequence=execution.dispatch_sequence,
        status=execution.status.value,
        sandbox_id=execution.sandbox.sandbox_id,
        agent_session_id=execution.agent_session_id,
        agent_returncode=execution.agent_returncode,
        agent_stdout=execution.agent_stdout,
        agent_stderr=execution.agent_stderr,
        trace_uri=execution.trace_uri,
        redaction_policy_id=redaction_policy_id,
        starting_revision=execution.starting_revision,
        final_revision=execution.final_revision,
        error=execution.error,
    )


def _observation_snapshot(
    *,
    world_id: str,
    run_id: str,
    tick: int,
    observation: DurableAuthorExecutionObservation,
    result: AuthorActivityResultRef,
    repository: str = "owner/repo",
    unrelated_fact_task_id: int | None = None,
    omit_result_children: bool = False,
    visibility_token: str | None = None,
) -> CommittedMissionSnapshot:
    partial = _partial_observation_snapshot(
        world_id=world_id,
        run_id=run_id,
        tick=tick,
        observation=observation,
        repository=repository,
        visibility_token=visibility_token,
    )
    execution_id = 20
    bundle = author_activity_fact_bundle(observation, execution_id=execution_id)
    validation_component = bundle.validations[0]
    commit_component = bundle.commits[0]
    friction_component = bundle.friction[0]
    if unrelated_fact_task_id is not None:
        validation_component = validation_component.model_copy(
            update={"task_id": unrelated_fact_task_id}
        )
        commit_component = commit_component.model_copy(update={"task_id": unrelated_fact_task_id})
        friction_component = friction_component.model_copy(
            update={"task_id": unrelated_fact_task_id}
        )
    marker_bundle = (
        replace(bundle, validations=(), commits=(), friction=()) if omit_result_children else bundle
    )
    marker = marker_bundle.marker(
        result=result,
        redaction_policy_id=observation.redaction_policy_id,
    )
    child_results = (
        {}
        if omit_result_children
        else {
            (ValidationResult,): _frame(21, tick, validation_component),
            (Commit,): _frame(22, tick, commit_component),
            (FrictionLog,): _frame(23, tick, friction_component),
        }
    )
    return CommittedMissionSnapshot(
        world_id=world_id,
        run_id=run_id,
        committed_tick=tick,
        visibility_token=visibility_token or f"token-{tick}",
        results={
            **partial.results,
            **child_results,
            (AuthorActivityObservation,): _frame(24, tick, marker),
        },
    )


def _raw_observation(dispatch_id: str) -> AuthorExecutionObservation:
    noisy = ("x" * 20_000) + f" token={_SECRET}"
    return AuthorExecutionObservation(
        result=AgentExecutionResult(
            mission_id=3,
            task_id=7,
            dispatch_id=dispatch_id,
            dispatch_sequence=1,
            status=AgentExecutionStatus.EXITED,
            sandbox=SandboxIdentity("local", "sandbox-1", "test-image"),
            worktree="/workspace/repo",
            agent_session_id="session-1",
            agent_returncode=0,
            starting_revision="a" * 40,
            final_revision="b" * 40,
            diff_digest="c" * 64,
            validator_bundle_digest="d" * 64,
            agent_stdout=noisy,
            agent_stderr=f"password={_SECRET}",
            trace_uri="local-trace://operation-1",
            validation=(
                ValidationObservation(
                    validator_id=11,
                    name="test",
                    command=("pytest", "-q", f"--token={_SECRET}"),
                    expected_returncode=0,
                    actual_returncode=0,
                    revision="b" * 40,
                    stdout=noisy,
                ),
            ),
            commits=(
                CommitObservation(
                    sha="b" * 40,
                    message=f"prove activity {_SECRET}",
                    branch="proof/activity",
                    pushed=True,
                    final_revision=True,
                ),
            ),
            friction=(FrictionObservation("provider", f"token={_SECRET}"),),
            error=f"authorization: bearer {_SECRET}",
        ),
        sandbox_status=SandboxStatus.READY,
    )


@pytest.mark.asyncio
async def test_provider_metadata_is_quarantined_before_result_persistence(
    tmp_path: Path,
) -> None:
    observation = _raw_observation("dispatch-quarantine")
    poisoned = replace(
        observation,
        result=replace(
            observation.result,
            final_revision=f"revision-{_SECRET}",
        ),
    )
    root = tmp_path / "values"
    values = LocalMissionAuthorValueStore(root, redactor=RedactionService())

    with pytest.raises(SecretQuarantineError, match="github-token"):
        await values.put_result(poisoned)

    assert not list(root.rglob("*.json"))


@pytest.mark.asyncio
async def test_request_is_redacted_and_unsafe_repository_is_quarantined(
    tmp_path: Path,
) -> None:
    request = TaskDispatchRequest(
        mission_id=3,
        task_id=7,
        task_name="prove-activity",
        dispatch_id="dispatch-request-redaction",
        dispatch_sequence=1,
        repository="owner/repo",
        branch="proof/activity",
        base_ref="main",
        prompt=f"write proof.txt with token={_SECRET}",
        validators=(
            DispatchedValidator(
                validator_id=11,
                spec=CommandValidator(
                    name="proof-exists",
                    command=("sh", "-c", f"test -f proof.txt # token={_SECRET}"),
                ),
            ),
        ),
        publication_policy=RepositoryPublicationPolicy.COMMIT_AND_PUSH,
    )
    root = tmp_path / "values"
    values = LocalMissionAuthorValueStore(root, redactor=RedactionService())

    value = await values.put_request(request)
    recovered = await values.get_request(value)

    assert isinstance(value, AuthorActivityRequestRef)
    assert not hasattr(value, "size_bytes")
    assert not hasattr(value, "media_type")
    persisted = b"".join(path.read_bytes() for path in root.rglob("*.json"))
    assert _SECRET.encode() not in persisted
    assert _SECRET not in recovered.prompt
    assert _SECRET not in recovered.validators[0].spec.command[-1]

    unsafe = replace(
        request,
        dispatch_id="dispatch-unsafe-repository",
        repository=f"https://x-access-token:{_SECRET}@github.com/owner/repo.git",
    )
    before = tuple(root.rglob("*.json"))
    with pytest.raises(SecretQuarantineError, match="github-token|uri-userinfo"):
        await values.put_request(unsafe)
    assert tuple(root.rglob("*.json")) == before


@pytest.mark.asyncio
async def test_git_result_ref_expected_absence_rejects_atomic_publication(
    tmp_path: Path,
) -> None:
    world_id = "world-a"
    dispatch_id = "dispatch-preexisting-result-ref"
    operation_id = author_provider_operation_id(world_id, dispatch_id)
    remote = _init_bare_remote(tmp_path / "git")
    provider = _LocalGitProvider(
        remote,
        tmp_path / "provider-counters.json",
        tmp_path / "author-work",
    )
    result_ref = provider._result_ref(operation_id)
    starting_revision = _git(
        "--git-dir",
        str(remote),
        "rev-parse",
        "refs/heads/main",
    ).stdout.strip()
    _git(
        "--git-dir",
        str(remote),
        "update-ref",
        result_ref,
        starting_revision,
        "0" * 40,
    )
    request = TaskDispatchRequest(
        mission_id=3,
        task_id=7,
        task_name="prove-activity",
        dispatch_id=dispatch_id,
        dispatch_sequence=1,
        repository=str(remote),
        branch="proof/activity",
        base_ref="main",
        prompt="write proof.txt",
        validators=(
            DispatchedValidator(
                validator_id=11,
                spec=CommandValidator(
                    name="proof-exists",
                    command=("sh", "-c", "test -f proof.txt"),
                ),
            ),
        ),
        publication_policy=RepositoryPublicationPolicy.COMMIT_AND_PUSH,
    )

    with pytest.raises(subprocess.CalledProcessError):
        await provider.execute(
            operation_id=operation_id,
            request=request,
            attempt=1,
            fence=1,
            retry_guard=None,
        )

    assert provider._marker_oid(result_ref) == starting_revision
    assert not provider._remote_head(request.branch)


@pytest.mark.asyncio
async def test_projector_rejects_snapshot_from_another_visibility_token(
    tmp_path: Path,
) -> None:
    world_id = "world-a"
    physical, generic, catalog = _open_catalog(tmp_path / "activities.db")
    values = LocalMissionAuthorValueStore(
        tmp_path / "values",
        redactor=RedactionService(),
    )
    projector = MissionAuthorActivityProjector(
        reader=_Reader(
            _dispatch_snapshot(
                world_id=world_id,
                run_id="run-a",
                tick=1,
                dispatch_id="dispatch-a",
                visibility_token="token-from-another-commit",
            )
        ),
        catalog=catalog,
        values=values,
    )

    with pytest.raises(ValueError, match="exact committed receipt"):
        await projector.project(
            CommittedTickReceipt(world_id, "run-a", 1, "token-authoritative", 0)
        )

    assert await generic.pending(kind=AUTHOR_ACTIVITY_KIND, world_id=world_id) == ()
    await physical.close()


@pytest.mark.asyncio
async def test_cold_restart_reconciles_published_author_and_redelivers_result(
    tmp_path: Path,
) -> None:
    world_id = "world-a"
    run_id = "run-a"
    dispatch_id = "same-world-local-dispatch"
    catalog_path = tmp_path / "activities.db"
    values_path = tmp_path / "values"
    provider_path = tmp_path / "provider-counters.json"
    remote = _init_bare_remote(tmp_path / "git")
    receipt = CommittedTickReceipt(world_id, run_id, 1, "token-1", 0)

    reader = _Reader(
        _dispatch_snapshot(
            world_id=world_id,
            run_id=run_id,
            tick=1,
            dispatch_id=dispatch_id,
            repository=str(remote),
        )
    )
    physical, generic, catalog = _open_catalog(catalog_path)
    values = LocalMissionAuthorValueStore(values_path, redactor=RedactionService())
    projector = MissionAuthorActivityProjector(
        reader=reader,
        catalog=catalog,
        values=values,
    )
    await projector.project(receipt)
    await projector.project(receipt)
    assert len(await generic.pending(kind=AUTHOR_ACTIVITY_KIND, world_id=world_id)) == 1

    first_provider = _LocalGitProvider(
        remote,
        provider_path,
        tmp_path / "author-work",
        crash_after_publish=True,
    )
    first_worker = MissionAuthorActivityWorker(
        world_id=world_id,
        owner="worker-before-crash",
        catalog=catalog,
        values=values,
        executor=first_provider,
        stager=_CrashOnceStager(crash=False),
    )
    with pytest.raises(RuntimeError, match="after external Git publication"):
        await first_worker.run_once()
    assert first_provider.execute_calls == 1
    published = _git("ls-remote", str(remote), "refs/heads/proof/activity")
    published_revision = published.stdout.partition("\t")[0].strip()
    assert published_revision
    operation_id = author_provider_operation_id(world_id, dispatch_id)
    result_receipt = _git(
        "ls-remote",
        str(remote),
        first_provider._result_ref(operation_id),
    )
    assert result_receipt.stdout.partition("\t")[0].strip() == published_revision
    await physical.close()
    await asyncio.sleep(0.02)

    # A later branch head may retain proof.txt unchanged. Recovery must use the
    # provider's exact completion receipt, never misattribute the later head.
    later_work = tmp_path / "later-work"
    _git("clone", "--branch", "proof/activity", str(remote), str(later_work))
    _git("config", "user.name", "Unrelated Writer", cwd=later_work)
    _git("config", "user.email", "unrelated@archetype.local", cwd=later_work)
    (later_work / "unrelated.txt").write_text("later\n")
    _git("add", "unrelated.txt", cwd=later_work)
    _git("commit", "-m", "unrelated later branch update", cwd=later_work)
    _git("push", "origin", "proof/activity", cwd=later_work)
    later_revision = (
        _git(
            "ls-remote",
            str(remote),
            "refs/heads/proof/activity",
        )
        .stdout.partition("\t")[0]
        .strip()
    )
    assert later_revision != published_revision

    # Reconstruct every Archetype-side object from disk. Reconciliation recovers
    # the already-published provider result and never invokes the author again.
    recovered_physical, _recovered_generic, recovered_catalog = _open_catalog(
        catalog_path,
        lease_seconds=30,
    )
    recovered_values = LocalMissionAuthorValueStore(
        values_path,
        redactor=RedactionService(),
    )
    recovered_provider = _LocalGitProvider(
        remote,
        provider_path,
        tmp_path / "recovery-work",
    )
    crash_before_tick = _CrashOnceStager(crash=True)
    recovered_worker = MissionAuthorActivityWorker(
        world_id=world_id,
        owner="worker-after-provider-crash",
        catalog=recovered_catalog,
        values=recovered_values,
        executor=recovered_provider,
        stager=crash_before_tick,
    )
    with pytest.raises(RuntimeError, match="before the staged observation"):
        await recovered_worker.run_once()
    assert recovered_provider.execute_calls == 1
    assert recovered_provider.reconcile_calls == 1
    await recovered_physical.close()

    # The bounded result was durable before staging failed. A new process can
    # redeliver it without claiming or executing provider work.
    final_physical, _final_generic, final_catalog = _open_catalog(catalog_path)
    final_values = LocalMissionAuthorValueStore(values_path, redactor=RedactionService())
    final_provider = _LocalGitProvider(
        remote,
        provider_path,
        tmp_path / "final-work",
    )
    final_stager = _CrashOnceStager(crash=False)
    final_worker = MissionAuthorActivityWorker(
        world_id=world_id,
        owner="worker-before-observation-tick",
        catalog=final_catalog,
        values=final_values,
        executor=final_provider,
        stager=final_stager,
    )
    assert await final_worker.run_once()
    assert final_provider.execute_calls == 1
    durable = final_stager.staged[(world_id, dispatch_id)]
    result_ref = final_stager.results[(world_id, dispatch_id)]
    assert durable.result.final_revision == published_revision
    assert len(durable.result.agent_stdout) <= 16_000
    persisted = b"".join(path.read_bytes() for path in values_path.rglob("*.json"))
    assert _SECRET.encode() not in persisted
    assert ("x" * 20_000).encode() not in persisted

    # A partial staging crash can commit AgentExecution without the complete
    # bundle marker. It must remain pending and be redelivered after restart.
    reader.snapshot = _partial_observation_snapshot(
        world_id=world_id,
        run_id=run_id,
        tick=2,
        observation=durable,
        repository=str(remote),
    )
    partial_projector = MissionAuthorActivityProjector(
        reader=reader,
        catalog=final_catalog,
        values=final_values,
    )
    await partial_projector.project(CommittedTickReceipt(world_id, run_id, 2, "token-2", 0))
    assert len(await final_catalog.pending_author_results(world_id=world_id)) == 1
    await final_physical.close()

    restage_physical, _restage_generic, restage_catalog = _open_catalog(catalog_path)
    restage = _CrashOnceStager(crash=False)
    restage_worker = MissionAuthorActivityWorker(
        world_id=world_id,
        owner="worker-after-partial-observation",
        catalog=restage_catalog,
        values=LocalMissionAuthorValueStore(values_path, redactor=RedactionService()),
        executor=_LocalGitProvider(
            remote,
            provider_path,
            tmp_path / "restage-work",
        ),
        stager=restage,
    )
    assert await restage_worker.run_once()
    assert (world_id, dispatch_id) in restage.staged

    # Even a complete factual bundle cannot settle another result digest.
    wrong_result = AuthorActivityResultRef(
        ref=result_ref.ref,
        digest="0" * 64,
        media_type=result_ref.media_type,
        size_bytes=result_ref.size_bytes,
    )
    reader.snapshot = _observation_snapshot(
        world_id=world_id,
        run_id=run_id,
        tick=3,
        observation=durable,
        result=wrong_result,
        repository=str(remote),
    )
    await restage_physical.close()
    settling_physical, settling_generic, settling_catalog = _open_catalog(catalog_path)
    settling_projector = MissionAuthorActivityProjector(
        reader=reader,
        catalog=settling_catalog,
        values=LocalMissionAuthorValueStore(
            values_path,
            redactor=RedactionService(),
        ),
    )
    await settling_projector.project(CommittedTickReceipt(world_id, run_id, 3, "token-3", 0))
    assert len(await settling_catalog.pending_author_results(world_id=world_id)) == 1

    # Counts alone are insufficient: unrelated task facts with the same
    # execution/dispatch identities cannot satisfy the result's bundle marker.
    reader.snapshot = _observation_snapshot(
        world_id=world_id,
        run_id=run_id,
        tick=4,
        observation=durable,
        result=result_ref,
        repository=str(remote),
        unrelated_fact_task_id=999,
    )
    await settling_projector.project(CommittedTickReceipt(world_id, run_id, 4, "token-4", 0))
    assert len(await settling_catalog.pending_author_results(world_id=world_id)) == 1

    # A marker can be internally self-consistent while omitting facts present
    # in the exact durable result. Completeness derives from that result, not
    # from marker-authored counts.
    reader.snapshot = _observation_snapshot(
        world_id=world_id,
        run_id=run_id,
        tick=5,
        observation=durable,
        result=result_ref,
        repository=str(remote),
        omit_result_children=True,
    )
    await settling_projector.project(CommittedTickReceipt(world_id, run_id, 5, "token-5", 0))
    assert len(await settling_catalog.pending_author_results(world_id=world_id)) == 1

    # The exact fact bundle and digest marker commit, then the process dies
    # before catalog settlement. A reconstructed projector settles first.
    reader.snapshot = _observation_snapshot(
        world_id=world_id,
        run_id=run_id,
        tick=6,
        observation=durable,
        result=result_ref,
        repository=str(remote),
    )
    observed = CommittedTickReceipt(world_id, run_id, 6, "token-6", 0)
    await settling_projector.project(observed)
    await settling_projector.project(observed)
    assert await settling_catalog.pending_author_results(world_id=world_id) == ()

    # A later full snapshot still contains historical AgentExecution rows but
    # cannot replace the exact observation receipt. Restart also projects the
    # retained receipt before a worker can redeliver the completed result.
    reader.snapshot = _observation_snapshot(
        world_id=world_id,
        run_id=run_id,
        tick=7,
        observation=durable,
        result=result_ref,
        repository=str(remote),
    )
    await settling_projector.project(CommittedTickReceipt(world_id, run_id, 7, "token-7", 0))
    settled = await settling_generic.get(
        kind=AUTHOR_ACTIVITY_KIND,
        world_id=world_id,
        activity_id=dispatch_id,
    )
    assert settled is not None
    assert settled.settlement is not None
    assert settled.settlement.receipt == observed
    assert settled.settlement.result_digest == result_ref.digest
    await settling_physical.close()
    after_observation_stager = _CrashOnceStager(crash=False)
    after_physical, _after_generic, after_catalog = _open_catalog(catalog_path)
    after_observation_worker = MissionAuthorActivityWorker(
        world_id=world_id,
        owner="worker-after-observation-crash",
        catalog=after_catalog,
        values=LocalMissionAuthorValueStore(values_path, redactor=RedactionService()),
        executor=_LocalGitProvider(
            remote,
            provider_path,
            tmp_path / "after-observation-work",
        ),
        stager=after_observation_stager,
    )
    assert not await after_observation_worker.run_once()
    assert after_observation_stager.staged == {}
    await after_physical.close()


@pytest.mark.asyncio
async def test_unknown_provider_state_fails_closed_without_author_replay(
    tmp_path: Path,
) -> None:
    world_id = "world-a"
    dispatch_id = "dispatch-unknown"
    remote = _init_bare_remote(tmp_path / "git")
    receipt = CommittedTickReceipt(world_id, "run-a", 1, "token-1", 0)
    reader = _Reader(
        _dispatch_snapshot(
            world_id=world_id,
            run_id="run-a",
            tick=1,
            dispatch_id=dispatch_id,
            repository=str(remote),
        )
    )
    catalog_path = tmp_path / "activities.db"
    physical, _generic, catalog = _open_catalog(catalog_path)
    values = LocalMissionAuthorValueStore(tmp_path / "values", redactor=RedactionService())
    await MissionAuthorActivityProjector(
        reader=reader,
        catalog=catalog,
        values=values,
    ).project(receipt)
    claim = await catalog.claim_author(world_id=world_id, owner="dead-worker")
    assert claim is not None
    await catalog.bind_provider_operation(
        claim,
        provider="local-git",
        operation_id=author_provider_operation_id(world_id, dispatch_id),
    )
    await physical.close()
    await asyncio.sleep(0.02)

    provider = _LocalGitProvider(
        remote,
        tmp_path / "provider-counters.json",
        tmp_path / "unknown-work",
        unknown=True,
    )
    recovery_physical, _recovery_generic, recovery_catalog = _open_catalog(
        catalog_path,
        lease_seconds=30,
    )
    worker = MissionAuthorActivityWorker(
        world_id=world_id,
        owner="recovery-worker",
        catalog=recovery_catalog,
        values=LocalMissionAuthorValueStore(
            tmp_path / "values",
            redactor=RedactionService(),
        ),
        executor=provider,
        stager=_CrashOnceStager(crash=False),
    )
    with pytest.raises(AuthorActivityReconciliationRequired):
        await worker.run_once()
    assert provider.execute_calls == 0
    assert provider.reconcile_calls == 1
    await recovery_physical.close()


@pytest.mark.asyncio
async def test_reconciliation_refuses_another_provider_adapter(
    tmp_path: Path,
) -> None:
    world_id = "world-a"
    dispatch_id = "dispatch-provider-bound"
    remote = _init_bare_remote(tmp_path / "git")
    catalog_path = tmp_path / "activities.db"
    physical, _generic, catalog = _open_catalog(catalog_path)
    values = LocalMissionAuthorValueStore(
        tmp_path / "values",
        redactor=RedactionService(),
    )
    await MissionAuthorActivityProjector(
        reader=_Reader(
            _dispatch_snapshot(
                world_id=world_id,
                run_id="run-a",
                tick=1,
                dispatch_id=dispatch_id,
                repository=str(remote),
            )
        ),
        catalog=catalog,
        values=values,
    ).project(CommittedTickReceipt(world_id, "run-a", 1, "token-1", 0))
    claim = await catalog.claim_author(world_id=world_id, owner="dead-worker")
    assert claim is not None
    bound = await catalog.bind_provider_operation(
        claim,
        provider="local-git",
        operation_id=author_provider_operation_id(world_id, dispatch_id),
    )
    assert bound.provider == "local-git"
    await physical.close()
    await asyncio.sleep(0.02)

    provider = _OtherGitProvider(
        remote,
        tmp_path / "provider-counters.json",
        tmp_path / "wrong-provider-work",
    )
    recovery_physical, _recovery_generic, recovery_catalog = _open_catalog(
        catalog_path,
        lease_seconds=30,
    )
    worker = MissionAuthorActivityWorker(
        world_id=world_id,
        owner="wrong-provider-worker",
        catalog=recovery_catalog,
        values=LocalMissionAuthorValueStore(
            tmp_path / "values",
            redactor=RedactionService(),
        ),
        executor=provider,
        stager=_CrashOnceStager(crash=False),
    )

    with pytest.raises(ValueError, match="another provider adapter"):
        await worker.run_once()

    assert provider.execute_calls == 0
    assert provider.reconcile_calls == 0
    await recovery_physical.close()


@pytest.mark.asyncio
async def test_confirmed_absence_mints_fresh_fence_before_execution(
    tmp_path: Path,
) -> None:
    world_id = "world-a"
    dispatch_id = "dispatch-pre-call-crash"
    remote = _init_bare_remote(tmp_path / "git")
    reader = _Reader(
        _dispatch_snapshot(
            world_id=world_id,
            run_id="run-a",
            tick=1,
            dispatch_id=dispatch_id,
            repository=str(remote),
        )
    )
    catalog_path = tmp_path / "activities.db"
    values_path = tmp_path / "values"
    physical, _generic, catalog = _open_catalog(catalog_path)
    values = LocalMissionAuthorValueStore(values_path, redactor=RedactionService())
    await MissionAuthorActivityProjector(
        reader=reader,
        catalog=catalog,
        values=values,
    ).project(CommittedTickReceipt(world_id, "run-a", 1, "token-1", 0))
    first = await catalog.claim_author(world_id=world_id, owner="dead-worker")
    assert first is not None
    operation_id = author_provider_operation_id(world_id, dispatch_id)
    await catalog.bind_provider_operation(
        first,
        provider="local-git",
        operation_id=operation_id,
    )
    request = await values.get_request(first.request)
    await physical.close()
    await asyncio.sleep(0.02)

    provider = _LocalGitProvider(
        remote,
        tmp_path / "provider-counters.json",
        tmp_path / "confirmed-work",
    )
    absence = await provider.reconcile(operation_id=operation_id, request=request)
    assert isinstance(absence, AuthorConfirmedAbsent)
    assert provider._marker_oid(absence.guard.ref)
    with pytest.raises(_ProviderReplayBlocked):
        await provider.execute(
            operation_id=operation_id,
            request=request,
            attempt=first.attempt,
            fence=first.fence,
            retry_guard=None,
        )
    assert not provider._remote_head(request.branch)

    # A recovery worker records the exact provider barrier, receives a fresh
    # fenced claim, and then dies before binding or invoking the provider.
    # The unbound replacement claim must retain that same guard after restart.
    barrier_physical, _barrier_generic, barrier_catalog = _open_catalog(
        catalog_path,
        lease_seconds=0.5,
    )
    reconciliation_claim = await barrier_catalog.claim_author(
        world_id=world_id,
        owner="barrier-worker",
    )
    assert reconciliation_claim is not None
    assert reconciliation_claim.reconciliation_required
    confirmed_again = await provider.reconcile(
        operation_id=operation_id,
        request=request,
    )
    assert isinstance(confirmed_again, AuthorConfirmedAbsent)
    fresh = await barrier_catalog.confirm_provider_operation_absent(
        reconciliation_claim,
        confirmed_again.guard,
    )
    assert fresh.retry_guard == absence.guard
    await barrier_physical.close()
    await asyncio.sleep(0.55)

    stager = _CrashOnceStager(crash=False)
    recovery_physical, recovery_generic, recovery_catalog = _open_catalog(
        catalog_path,
        lease_seconds=30,
    )
    worker = MissionAuthorActivityWorker(
        world_id=world_id,
        owner="recovery-worker",
        catalog=recovery_catalog,
        values=LocalMissionAuthorValueStore(values_path, redactor=RedactionService()),
        executor=provider,
        stager=stager,
    )
    assert await worker.run_once()
    assert provider.reconcile_calls == 2
    assert provider.execute_calls == 1
    snapshot = await recovery_generic.get(
        kind=AUTHOR_ACTIVITY_KIND,
        world_id=world_id,
        activity_id=dispatch_id,
    )
    assert snapshot is not None
    assert snapshot.result_attempt == 4
    assert snapshot.result_fence == 4
    assert provider._remote_head("proof/activity")
    assert (world_id, dispatch_id) in stager.staged
    await recovery_physical.close()


@pytest.mark.asyncio
async def test_world_identity_scopes_equal_dispatch_ids(tmp_path: Path) -> None:
    dispatch_id = "world-local-id"
    physical, generic, catalog = _open_catalog(tmp_path / "activities.db")
    values = LocalMissionAuthorValueStore(tmp_path / "values", redactor=RedactionService())
    for world_id in ("world-a", "world-b"):
        receipt = CommittedTickReceipt(world_id, f"run-{world_id}", 1, "token", 0)
        await MissionAuthorActivityProjector(
            reader=_Reader(
                _dispatch_snapshot(
                    world_id=world_id,
                    run_id=f"run-{world_id}",
                    tick=1,
                    dispatch_id=dispatch_id,
                    visibility_token="token",
                )
            ),
            catalog=catalog,
            values=values,
        ).project(receipt)

    assert len(await generic.pending(kind=AUTHOR_ACTIVITY_KIND)) == 2
    first = await catalog.claim_author(world_id="world-a", owner="worker")
    second = await catalog.claim_author(world_id="world-b", owner="worker")
    assert first is not None and second is not None
    assert first.world_id != second.world_id
    assert isinstance(first.request, AuthorActivityRequestRef)
    assert isinstance(second.request, AuthorActivityRequestRef)
    first_operation = author_provider_operation_id(first.world_id, first.activity_id)
    second_operation = author_provider_operation_id(second.world_id, second.activity_id)
    assert first_operation != second_operation
    assert _LocalGitProvider._marker_ref(first_operation) != _LocalGitProvider._marker_ref(
        second_operation
    )
    assert _LocalGitProvider._result_ref(first_operation) != _LocalGitProvider._result_ref(
        second_operation
    )
    first_bound = await catalog.bind_provider_operation(
        first,
        provider="local-git",
        operation_id=first_operation,
    )
    second_bound = await catalog.bind_provider_operation(
        second,
        provider="local-git",
        operation_id=second_operation,
    )
    assert first_bound.provider_operation_id == first_operation
    assert second_bound.provider_operation_id == second_operation
    await physical.close()
