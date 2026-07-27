# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Crash contracts for the guarded Modal Mission author adapter."""

from __future__ import annotations

import hashlib
import json
import sys
from dataclasses import dataclass, replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from archetype.missions.activities import (
    AuthorActivityRetryGuard,
    AuthorConfirmedAbsent,
    AuthorExecutionObservation,
    AuthorRecovered,
    AuthorRecoveryUnknown,
)
from archetype.missions.activity_values import MissionAuthorValueCodec
from archetype.missions.coding_agents.contracts import (
    AgentExecutionResult,
    DispatchedValidator,
    FrictionObservation,
    TaskDispatchRequest,
)
from archetype.missions.contracts import (
    CommandValidator,
    CriticPolicy,
    RepositoryPublicationPolicy,
)
from archetype.missions.critics import (
    CandidateReviewRequest,
    CriticActivityCodec,
    CriticActivityRequest,
    CriticExecutionResult,
    CriticHarnessConfig,
    CriticPrewarmRequest,
    CriticReceiptValue,
    CriticRecovered,
    CriticSubjectPolicy,
    CriticSubjectTransport,
    bind_critic_subject,
)
from archetype.missions.critics.contracts import canonical_digest
from archetype.missions.local_activity_values import LocalMissionAuthorValueStore
from archetype.missions.local_critic_activity_values import LocalMissionCriticValueStore
from archetype.missions.modal_author import (
    ModalAuthorExecutionUnknown,
    ModalMissionAuthorExecutor,
    ModalMissionAuthorExecutorConfig,
)
from archetype.missions.modal_critic import (
    ModalMissionCriticExecutor,
    ModalMissionCriticExecutorConfig,
)
from archetype.missions.sandboxes import (
    MODAL_ACTIVITY_PROTOCOL_EPOCH,
    CheckpointRef,
    ModalProviderStartBarrier,
    ModalSandboxOperationCleanup,
    ModalSandboxOperationIdentity,
    SandboxEvent,
    SandboxEventType,
    SandboxIdentity,
    SandboxStatus,
)
from archetype.missions.transitions import (
    AgentExecutionStatus,
    CriticConclusion,
    CriticExecutionStatus,
)
from archetype.redaction import RedactionService

_SECRET = "github_pat_ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890"


class _AlreadyExistsError(Exception):
    pass


class _NotFoundError(Exception):
    pass


class _AmbiguousPutError(Exception):
    pass


class _AsyncMethod:
    def __init__(self, function: Any) -> None:
        self.aio = function


@dataclass
class _DictData:
    object_id: str
    values: dict[str, Any]


class _DictHandle:
    def __init__(
        self,
        registry: _ModalRegistry,
        *,
        environment_name: str,
        name: str,
    ) -> None:
        self._registry = registry
        self._key = (environment_name, name)
        self.hydrate = _AsyncMethod(self._hydrate)
        self.get = _AsyncMethod(self._get)
        self.put = _AsyncMethod(self._put)

    @property
    def object_id(self) -> str:
        return self._registry.dicts[self._key].object_id

    async def _hydrate(self) -> _DictHandle:
        if self._registry.marker_hydrate_error and self._key[1].startswith("op-v1-"):
            raise ConnectionError("credential-canary")
        if self._key not in self._registry.dicts:
            raise _NotFoundError(self._key[1])
        return self

    async def _get(self, key: str, default: Any = None) -> Any:
        self._registry.get_calls += 1
        if self._registry.get_calls in self._registry.get_error_calls:
            raise ConnectionError("credential-canary")
        return self._registry.dicts[self._key].values.get(key, default)

    async def _put(
        self,
        key: str,
        value: Any,
        *,
        skip_if_exists: bool = False,
    ) -> bool:
        data = self._registry.dicts[self._key]
        if key in data.values and skip_if_exists:
            return False
        if self._registry.raise_before_put_once:
            self._registry.raise_before_put_once = False
            raise _AmbiguousPutError("credential-canary")
        data.values[key] = value
        self._registry.put_calls.append((self._key, key, value, skip_if_exists))
        if self._registry.return_false_after_same_write_once:
            self._registry.return_false_after_same_write_once = False
            return False
        if self._registry.return_false_after_conflicting_write_once:
            self._registry.return_false_after_conflicting_write_once = False
            envelope = json.loads(str(value))
            envelope["result"]["value"]["result"]["agent_stdout"] = "conflicting"
            data.values[key] = json.dumps(
                envelope,
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            )
            return False
        if self._registry.raise_after_put_once:
            self._registry.raise_after_put_once = False
            raise _AmbiguousPutError("credential-canary")
        return True


class _Workspace:
    def __init__(self, registry: _ModalRegistry) -> None:
        self.name = registry.reported_workspace_name
        self.client = registry.client
        self.hydrate = _AsyncMethod(self._hydrate)

    async def _hydrate(self) -> _Workspace:
        return self


class _ModalRegistry:
    def __init__(self) -> None:
        self.workspace_name = "vangelis"
        self.reported_workspace_name = self.workspace_name
        self.environment_name = "main"
        self.app_name = "archetype-agent-missions-test"
        self.client = object()
        self.dicts: dict[tuple[str, str], _DictData] = {}
        self.put_calls: list[tuple[tuple[str, str], str, Any, bool]] = []
        self.get_calls = 0
        self.get_error_calls: set[int] = set()
        self.raise_before_put_once = False
        self.raise_after_put_once = False
        self.return_false_after_same_write_once = False
        self.return_false_after_conflicting_write_once = False
        self.marker_hydrate_error = False
        self._next_object = 1

        registry = self

        class Dict:
            @staticmethod
            def from_name(
                name: str,
                *,
                environment_name: str | None = None,
                create_if_missing: bool = False,
                client: Any = None,
            ) -> _DictHandle:
                assert environment_name == registry.environment_name
                assert client is registry.client
                key = (str(environment_name), name)
                if create_if_missing and key not in registry.dicts:
                    registry._create(key)
                return _DictHandle(
                    registry,
                    environment_name=str(environment_name),
                    name=name,
                )

        async def create(
            name: str,
            *,
            allow_existing: bool,
            environment_name: str,
            client: Any,
        ) -> None:
            assert allow_existing is False
            assert environment_name == registry.environment_name
            assert client is registry.client
            key = (environment_name, name)
            if key in registry.dicts:
                raise _AlreadyExistsError(name)
            registry._create(key)

        Dict.objects = SimpleNamespace(create=_AsyncMethod(create))
        self.modal = SimpleNamespace(
            Dict=Dict,
            Workspace=SimpleNamespace(
                from_context=lambda: _Workspace(registry),
            ),
            exception=SimpleNamespace(
                AlreadyExistsError=_AlreadyExistsError,
                NotFoundError=_NotFoundError,
            ),
        )

    def _create(self, key: tuple[str, str]) -> None:
        self.dicts[key] = _DictData(
            object_id=f"di-{self._next_object}",
            values={},
        )
        self._next_object += 1

    def result_values(
        self,
        executor: ModalMissionAuthorExecutor | ModalMissionCriticExecutor,
    ) -> tuple[str, ...]:
        data = self.dicts[(self.environment_name, executor.result_dict_name)]
        return tuple(str(value) for value in data.values.values())


def _downgrade_provider_result_to_schema_v1(
    registry: _ModalRegistry,
    *,
    result_dict_name: str,
) -> str:
    data = registry.dicts[(registry.environment_name, result_dict_name)]
    ((key, value),) = data.values.items()
    envelope = json.loads(str(value))
    assert envelope["schema_version"] == 2
    envelope.pop("cleanup")
    envelope["schema_version"] = 1
    legacy = json.dumps(
        envelope,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    data.values[key] = legacy
    return legacy


class _Session:
    def __init__(
        self,
        identity: ModalSandboxOperationIdentity,
        sequence: int,
        *,
        checkpoints: bool = False,
        close_failures: int = 0,
    ) -> None:
        self.operation_identity = identity
        self.identity = SandboxIdentity(
            "modal",
            f"sb-author-{sequence}",
            "modal-agent://sha256:test",
        )
        self.closed = 0
        self.is_closed = False
        self.close_failures = close_failures
        self.cohort_id = f"cohort-v1:{sequence:032x}"
        self.auth_sandbox_id = f"sb-auth-{sequence}"
        self.capabilities = SimpleNamespace(checkpoints=checkpoints)
        self.checkpoint_calls = 0

    async def status(self) -> SandboxStatus:
        return SandboxStatus.READY

    async def close(self) -> None:
        self.closed += 1
        if self.close_failures:
            self.close_failures -= 1
            raise RuntimeError("simulated Modal close failure")
        self.is_closed = True

    @property
    def operation_cleanup(self) -> ModalSandboxOperationCleanup:
        return ModalSandboxOperationCleanup(
            identity=self.operation_identity,
            mission_sandbox_id=self.identity.sandbox_id,
            auth_sandbox_id=self.auth_sandbox_id,
            cohort_id=self.cohort_id,
        )

    async def checkpoint(self) -> CheckpointRef:
        self.checkpoint_calls += 1
        return CheckpointRef(
            provider="modal",
            checkpoint_id="im-checkpoint",
            uri="modal-image://im-checkpoint",
            created_at_ms=1,
            environment=self.identity.environment,
            source_sandbox_id=self.identity.sandbox_id,
        )


class _Capability:
    def __init__(
        self,
        registry: _ModalRegistry,
        *,
        checkpoints: bool = False,
        close_failures: int = 0,
    ) -> None:
        self.registry = registry
        self.starts = 0
        self.sessions: list[_Session] = []
        self.checkpoints = checkpoints
        self.close_failures = close_failures
        self.cleanup_calls = 0

    def identity(self, operation_id: str) -> ModalSandboxOperationIdentity:
        return ModalSandboxOperationIdentity(
            workspace_name=self.registry.workspace_name,
            environment_name=self.registry.environment_name,
            app_name=self.registry.app_name,
            operation_id=operation_id,
            protocol_epoch=MODAL_ACTIVITY_PROTOCOL_EPOCH,
        )

    def _validate_spec(self, spec: Any) -> None:
        if spec.provider != "modal" or spec.environment != "modal-agent://sha256:test":
            raise ValueError("fake Modal capability received another provider spec")

    async def _start_after_provider_barrier(
        self,
        *,
        identity: ModalSandboxOperationIdentity,
        spec: Any,
    ) -> _Session:
        self._validate_spec(spec)
        self.starts += 1
        session = _Session(
            identity,
            self.starts,
            checkpoints=self.checkpoints,
            close_failures=self.close_failures,
        )
        self.sessions.append(session)
        return session

    async def cleanup_completed(
        self,
        *,
        cleanup: ModalSandboxOperationCleanup,
        spec: Any,
    ) -> None:
        self._validate_spec(spec)
        self.cleanup_calls += 1
        for session in self.sessions:
            if session.operation_cleanup == cleanup and not session.is_closed:
                await session.close()
                return


class _Harness:
    def __init__(
        self,
        *,
        crash: bool = False,
        workspace: str = "/workspace/repo",
        wrong_sandbox: bool = False,
        wrong_dispatch: bool = False,
    ) -> None:
        self.config = SimpleNamespace(workspace=workspace)
        self.crash = crash
        self.wrong_sandbox = wrong_sandbox
        self.wrong_dispatch = wrong_dispatch
        self.calls = 0
        self.requests: list[TaskDispatchRequest] = []

    async def execute(
        self,
        session: _Session,
        request: TaskDispatchRequest,
    ) -> AgentExecutionResult:
        self.calls += 1
        self.requests.append(request)
        if self.crash:
            raise RuntimeError("simulated process death")
        return AgentExecutionResult(
            mission_id=request.mission_id,
            task_id=request.task_id,
            dispatch_id=("another-dispatch" if self.wrong_dispatch else request.dispatch_id),
            dispatch_sequence=request.dispatch_sequence,
            status=AgentExecutionStatus.EXITED,
            sandbox=(
                SandboxIdentity(
                    "modal",
                    "sb-another",
                    "modal-agent://sha256:test",
                )
                if self.wrong_sandbox
                else session.identity
            ),
            worktree="/workspace/repo",
            agent_session_id="thread-1",
            agent_returncode=0,
            starting_revision="a" * 40,
            final_revision="b" * 40,
            diff_digest="c" * 64,
            validator_bundle_digest="d" * 64,
            agent_stdout=f"authorization: bearer {_SECRET}",
            friction=(
                FrictionObservation(
                    kind="provider",
                    message=f"token={_SECRET}",
                ),
            ),
        )


class _CriticHarness:
    def __init__(self) -> None:
        self.config = CriticHarnessConfig()
        self.prewarm_calls = 0
        self.calls = 0

    async def prewarm(
        self,
        session: _Session,
        request: CriticPrewarmRequest,
    ) -> str:
        del session, request
        self.prewarm_calls += 1
        return "a" * 40

    async def execute(
        self,
        session: _Session,
        request: CandidateReviewRequest,
        **timing: int,
    ) -> CriticExecutionResult:
        self.calls += 1
        content = b"exact diff"
        subject = bind_critic_subject(
            CriticSubjectPolicy(
                digest=hashlib.sha256(content).hexdigest(),
                max_bytes=request.policy.max_subject_bytes,
            ),
            metadata=b"{}",
            content=content,
            transport=CriticSubjectTransport.SANDBOX_FILE,
            ref="/tmp/subject.diff",
        )
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
            completed_at_ms=200,
        )
        return CriticExecutionResult(
            request=request,
            status=CriticExecutionStatus.EXITED,
            sandbox=session.identity,
            sandbox_status=SandboxStatus.READY,
            sandbox_acquired=True,
            started_at_ms=150,
            ended_at_ms=200,
            provision_started_at_ms=timing["provision_started_at_ms"],
            sandbox_ready_at_ms=timing["sandbox_ready_at_ms"],
            base_hydrated_at_ms=timing["base_hydrated_at_ms"],
            critic_started_at_ms=175,
            raw_output=f'{{"conclusion":"approved","token":"{_SECRET}"}}',
            trace_uri="modal://critic-trace",
            receipt=receipt,
        )


def _request(dispatch_id: str = "dispatch-modal-author") -> TaskDispatchRequest:
    return TaskDispatchRequest(
        mission_id=3,
        task_id=7,
        task_name="prove-activity",
        dispatch_id=dispatch_id,
        dispatch_sequence=1,
        repository="owner/repo",
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


def _raw_observation(
    dispatch_id: str = "dispatch-codec",
) -> AuthorExecutionObservation:
    return AuthorExecutionObservation(
        result=AgentExecutionResult(
            mission_id=3,
            task_id=7,
            dispatch_id=dispatch_id,
            dispatch_sequence=1,
            status=AgentExecutionStatus.EXITED,
            sandbox=SandboxIdentity(
                "modal",
                "sb-codec",
                "modal-agent://sha256:test",
            ),
            worktree="/workspace/repo",
            agent_session_id="thread-codec",
            agent_returncode=0,
            starting_revision="a" * 40,
            final_revision="b" * 40,
            agent_stdout=f"token={_SECRET}",
        ),
        sandbox_status=SandboxStatus.READY,
    )


def _executor(
    registry: _ModalRegistry,
    *,
    capability: _Capability | None = None,
    harness: _Harness | None = None,
    observer: Any = None,
) -> tuple[ModalMissionAuthorExecutor, _Capability, _Harness]:
    selected_capability = capability or _Capability(registry)
    selected_harness = harness or _Harness()
    barrier = ModalProviderStartBarrier(
        workspace_name=registry.workspace_name,
        environment_name=registry.environment_name,
        app_name=registry.app_name,
        protocol_epoch=MODAL_ACTIVITY_PROTOCOL_EPOCH,
    )
    executor = ModalMissionAuthorExecutor(
        capability=selected_capability,  # type: ignore[arg-type]
        barrier=barrier,
        harness=selected_harness,  # type: ignore[arg-type]
        redactor=RedactionService(),
        config=ModalMissionAuthorExecutorConfig(
            sandbox_environment="modal-agent://sha256:test",
        ),
        observer=observer,
    )
    return executor, selected_capability, selected_harness


def _critic_request() -> CriticActivityRequest:
    content = b"exact diff"
    raw = CandidateReviewRequest(
        candidate_entity_id=11,
        candidate_id=hashlib.sha256(b"candidate").hexdigest(),
        mission_id=3,
        task_id=7,
        task_name="Review candidate",
        task_prompt="Prove the exact candidate is correct.",
        dispatch_id=hashlib.sha256(b"dispatch").hexdigest(),
        dispatch_sequence=1,
        author_execution_id=9,
        author_sandbox_id="sb-author-original",
        repository="https://github.com/example/repository.git",
        branch="agent/review",
        base_ref="main",
        base_revision="a" * 40,
        head_revision="b" * 40,
        diff_digest=hashlib.sha256(content).hexdigest(),
        validator_bundle_digest=hashlib.sha256(b"validators").hexdigest(),
        policy=CriticPolicy(max_subject_bytes=1 << 20),
        validation=(),
        candidate_published_at_ms=100,
        attempt=1,
    )
    return CriticActivityCodec(RedactionService()).prepare_request(raw)


def _critic_executor(
    registry: _ModalRegistry,
    *,
    capability: _Capability | None = None,
) -> tuple[ModalMissionCriticExecutor, _Capability, _CriticHarness]:
    selected_capability = capability or _Capability(registry)
    harness = _CriticHarness()
    barrier = ModalProviderStartBarrier(
        workspace_name=registry.workspace_name,
        environment_name=registry.environment_name,
        app_name=registry.app_name,
        protocol_epoch=MODAL_ACTIVITY_PROTOCOL_EPOCH,
    )
    executor = ModalMissionCriticExecutor(
        capability=selected_capability,  # type: ignore[arg-type]
        barrier=barrier,
        harness=harness,  # type: ignore[arg-type]
        redactor=RedactionService(),
        config=ModalMissionCriticExecutorConfig(
            sandbox_environment="modal-agent://sha256:test",
        ),
    )
    return executor, selected_capability, harness


@pytest.fixture
def fake_modal(monkeypatch: pytest.MonkeyPatch) -> _ModalRegistry:
    registry = _ModalRegistry()
    monkeypatch.setitem(sys.modules, "modal", registry.modal)
    return registry


@pytest.mark.asyncio
async def test_initial_start_publishes_only_redacted_bounded_first_result(
    fake_modal: _ModalRegistry,
    tmp_path: Path,
) -> None:
    executor, capability, harness = _executor(fake_modal)
    request = replace(
        _request(),
        prompt=f"write proof.txt with token={_SECRET}",
    )

    observation = await executor.execute(
        operation_id="missions.author:world-a:dispatch-modal-author",
        request=request,
        attempt=1,
        fence=1,
        retry_guard=None,
    )

    assert capability.starts == 1
    assert harness.calls == 1
    assert capability.sessions[0].closed == 1
    assert _SECRET not in observation.result.agent_stdout
    assert _SECRET not in observation.result.friction[0].message
    assert _SECRET not in harness.requests[0].prompt
    (provider_result,) = fake_modal.result_values(executor)
    assert _SECRET not in provider_result
    assert len(provider_result.encode()) <= 512 * 1024
    assert fake_modal.put_calls[0][3] is True

    # The local Activity store uses the same family codec; a second scan is
    # idempotent and produces the exact durable observation.
    values = LocalMissionAuthorValueStore(
        tmp_path / "values",
        redactor=RedactionService(),
    )
    result_ref = await values.put_result(observation)
    durable = await values.get_result(result_ref)
    assert durable.result == observation.result


@pytest.mark.asyncio
async def test_modal_author_preserves_checkpoint_and_forwards_live_events(
    fake_modal: _ModalRegistry,
) -> None:
    events: list[SandboxEvent] = []
    capability = _Capability(fake_modal, checkpoints=True)
    executor, _, _ = _executor(
        fake_modal,
        capability=capability,
        observer=events.append,
    )

    observation = await executor.execute(
        operation_id="missions.author:world-a:dispatch-checkpoint",
        request=_request("dispatch-checkpoint"),
        attempt=1,
        fence=1,
        retry_guard=None,
    )

    assert observation.checkpoint is not None
    assert observation.checkpoint.checkpoint_id == "im-checkpoint"
    assert capability.sessions[0].checkpoint_calls == 1
    assert [event.kind for event in events] == [
        SandboxEventType.READY,
        SandboxEventType.PROCESS_STARTED,
        SandboxEventType.PROCESS_FINISHED,
        SandboxEventType.CHECKPOINT_STARTED,
        SandboxEventType.CHECKPOINT_FINISHED,
    ]
    assert events[-1].checkpoint_uri == observation.checkpoint.uri


@pytest.mark.asyncio
async def test_cold_restart_recovers_exact_provider_result_without_start(
    fake_modal: _ModalRegistry,
) -> None:
    first, first_capability, first_harness = _executor(fake_modal)
    request = _request("dispatch-restart")
    operation_id = "missions.author:world-a:dispatch-restart"
    original = await first.execute(
        operation_id=operation_id,
        request=request,
        attempt=1,
        fence=1,
        retry_guard=None,
    )
    restarted, capability, harness = _executor(fake_modal)

    recovered = await restarted.reconcile(
        operation_id=operation_id,
        request=request,
    )
    direct = await restarted.execute(
        operation_id=operation_id,
        request=request,
        attempt=2,
        fence=2,
        retry_guard=None,
    )

    assert isinstance(recovered, AuthorRecovered)
    assert recovered.observation == original
    assert direct == original
    assert first_capability.starts == first_harness.calls == 1
    assert capability.starts == harness.calls == 0


@pytest.mark.asyncio
async def test_schema_v1_author_result_recovers_and_delivers_without_replay(
    fake_modal: _ModalRegistry,
) -> None:
    first, _first_capability, _first_harness = _executor(fake_modal)
    request = _request("dispatch-schema-v1")
    operation_id = "missions.author:world-a:dispatch-schema-v1"
    original = await first.execute(
        operation_id=operation_id,
        request=request,
        attempt=1,
        fence=1,
        retry_guard=None,
    )
    legacy = _downgrade_provider_result_to_schema_v1(
        fake_modal,
        result_dict_name=first.result_dict_name,
    )
    restarted, capability, harness = _executor(fake_modal)

    recovered = await restarted.reconcile(
        operation_id=operation_id,
        request=request,
    )
    delivered = await restarted.execute(
        operation_id=operation_id,
        request=request,
        attempt=2,
        fence=2,
        retry_guard=None,
    )

    assert isinstance(recovered, AuthorRecovered)
    assert recovered.observation == original
    assert delivered == original
    assert capability.starts == harness.calls == capability.cleanup_calls == 0
    assert fake_modal.result_values(restarted) == (legacy,)


@pytest.mark.asyncio
async def test_modal_critic_cold_restart_recovers_exact_receipt_with_one_start(
    fake_modal: _ModalRegistry,
    tmp_path: Path,
) -> None:
    first, first_capability, first_harness = _critic_executor(fake_modal)
    request = _critic_request()
    operation_id = "missions.critic:world-a:review-restart"
    original = await first.execute(
        operation_id=operation_id,
        request=request,
        attempt=1,
        fence=1,
        retry_guard=None,
    )
    restarted, capability, harness = _critic_executor(fake_modal)

    recovered = await restarted.reconcile(
        operation_id=operation_id,
        request=request,
    )
    direct = await restarted.execute(
        operation_id=operation_id,
        request=request,
        attempt=2,
        fence=2,
        retry_guard=None,
    )

    assert isinstance(recovered, CriticRecovered)
    assert recovered.result == original
    assert direct == original
    assert original.receipt is not None
    assert original.receipt.reviewed_diff_digest == request.diff_digest
    assert original.sandbox.sandbox_id != request.author_sandbox_id
    assert first_capability.starts == 1
    assert first_harness.prewarm_calls == first_harness.calls == 1
    assert first_capability.sessions[0].closed == 1
    assert capability.starts == harness.prewarm_calls == harness.calls == 0
    assert _SECRET not in fake_modal.result_values(first)[0]
    values = LocalMissionCriticValueStore(
        tmp_path / "critic-values",
        codec=CriticActivityCodec(RedactionService()),
    )
    result_ref = await values.put_result(original, request)
    durable = await values.get_result(result_ref)
    assert durable.receipt is not None
    assert durable.receipt.reviewed_diff_digest == request.diff_digest


@pytest.mark.asyncio
async def test_schema_v1_critic_result_recovers_and_delivers_without_replay(
    fake_modal: _ModalRegistry,
) -> None:
    first, _first_capability, _first_harness = _critic_executor(fake_modal)
    request = _critic_request()
    operation_id = "missions.critic:world-a:review-schema-v1"
    original = await first.execute(
        operation_id=operation_id,
        request=request,
        attempt=1,
        fence=1,
        retry_guard=None,
    )
    legacy = _downgrade_provider_result_to_schema_v1(
        fake_modal,
        result_dict_name=first.result_dict_name,
    )
    restarted, capability, harness = _critic_executor(fake_modal)

    recovered = await restarted.reconcile(
        operation_id=operation_id,
        request=request,
    )
    delivered = await restarted.execute(
        operation_id=operation_id,
        request=request,
        attempt=2,
        fence=2,
        retry_guard=None,
    )

    assert isinstance(recovered, CriticRecovered)
    assert recovered.result == original
    assert delivered == original
    assert capability.starts == harness.prewarm_calls == harness.calls == 0
    assert capability.cleanup_calls == 0
    assert fake_modal.result_values(restarted) == (legacy,)


@pytest.mark.asyncio
async def test_author_result_cannot_reconcile_until_exact_failed_cleanup_retries(
    fake_modal: _ModalRegistry,
) -> None:
    capability = _Capability(fake_modal, close_failures=1)
    executor, _, harness = _executor(fake_modal, capability=capability)
    request = _request("dispatch-cleanup-retry")
    operation_id = "missions.author:world-a:dispatch-cleanup-retry"

    with pytest.raises(RuntimeError, match="close failure"):
        await executor.execute(
            operation_id=operation_id,
            request=request,
            attempt=1,
            fence=1,
            retry_guard=None,
        )

    recovered = await executor.reconcile(
        operation_id=operation_id,
        request=request,
    )

    assert isinstance(recovered, AuthorRecovered)
    assert recovered.observation.result.dispatch_id == request.dispatch_id
    assert capability.starts == harness.calls == 1
    assert capability.cleanup_calls == 1
    assert capability.sessions[0].closed == 2
    assert capability.sessions[0].is_closed


@pytest.mark.asyncio
async def test_critic_result_cannot_reconcile_until_exact_failed_cleanup_retries(
    fake_modal: _ModalRegistry,
) -> None:
    capability = _Capability(fake_modal, close_failures=1)
    executor, _, harness = _critic_executor(fake_modal, capability=capability)
    request = _critic_request()
    operation_id = "missions.critic:world-a:review-cleanup-retry"

    with pytest.raises(RuntimeError, match="close failure"):
        await executor.execute(
            operation_id=operation_id,
            request=request,
            attempt=1,
            fence=1,
            retry_guard=None,
        )

    recovered = await executor.reconcile(
        operation_id=operation_id,
        request=request,
    )

    assert isinstance(recovered, CriticRecovered)
    assert recovered.result.request.review_id == request.review_id
    assert capability.starts == 1
    assert harness.prewarm_calls == harness.calls == 1
    assert capability.cleanup_calls == 1
    assert capability.sessions[0].closed == 2
    assert capability.sessions[0].is_closed


@pytest.mark.asyncio
async def test_ambiguous_first_result_put_reconciles_exact_value(
    fake_modal: _ModalRegistry,
) -> None:
    executor, capability, harness = _executor(fake_modal)
    fake_modal.raise_after_put_once = True

    observation = await executor.execute(
        operation_id="missions.author:world-a:dispatch-ambiguous-put",
        request=_request("dispatch-ambiguous-put"),
        attempt=1,
        fence=1,
        retry_guard=None,
    )

    assert observation.result.dispatch_id == "dispatch-ambiguous-put"
    assert capability.starts == harness.calls == 1
    assert len(fake_modal.result_values(executor)) == 1


@pytest.mark.asyncio
async def test_start_marker_without_result_is_unknown_and_never_replayed(
    fake_modal: _ModalRegistry,
) -> None:
    crashing_harness = _Harness(crash=True)
    first, capability, _harness = _executor(
        fake_modal,
        harness=crashing_harness,
    )
    request = _request("dispatch-started-no-result")
    operation_id = "missions.author:world-a:dispatch-started-no-result"

    with pytest.raises(RuntimeError, match="simulated process death"):
        await first.execute(
            operation_id=operation_id,
            request=request,
            attempt=1,
            fence=1,
            retry_guard=None,
        )
    restarted, _same_capability, restarted_harness = _executor(
        fake_modal,
        capability=capability,
    )

    reconciliation = await restarted.reconcile(
        operation_id=operation_id,
        request=request,
    )
    with pytest.raises(ModalAuthorExecutionUnknown, match="marker exists"):
        await restarted.execute(
            operation_id=operation_id,
            request=request,
            attempt=2,
            fence=2,
            retry_guard=None,
        )

    assert isinstance(reconciliation, AuthorRecoveryUnknown)
    assert "marker exists" in reconciliation.reason
    assert capability.starts == 1
    assert crashing_harness.calls == 1
    assert restarted_harness.calls == 0


@pytest.mark.asyncio
async def test_missing_marker_routes_retry_back_through_atomic_start_retry(
    fake_modal: _ModalRegistry,
) -> None:
    executor, capability, harness = _executor(fake_modal)
    request = _request("dispatch-pre-call-crash")
    operation_id = "missions.author:world-a:dispatch-pre-call-crash"

    absent = await executor.reconcile(
        operation_id=operation_id,
        request=request,
    )
    assert isinstance(absent, AuthorConfirmedAbsent)
    assert not hasattr(absent.guard, "permit")

    observation = await executor.execute(
        operation_id=operation_id,
        request=request,
        attempt=2,
        fence=2,
        retry_guard=absent.guard,
    )

    assert observation.result.dispatch_id == request.dispatch_id
    assert capability.starts == harness.calls == 1


@pytest.mark.asyncio
async def test_retry_route_is_not_transferable_provider_authority(
    fake_modal: _ModalRegistry,
) -> None:
    executor, capability, harness = _executor(fake_modal)
    request = _request("dispatch-stale-retry")
    operation_id = "missions.author:world-a:dispatch-stale-retry"
    absent = await executor.reconcile(
        operation_id=operation_id,
        request=request,
    )
    assert isinstance(absent, AuthorConfirmedAbsent)

    # A different claimant wins the coupled initial start but dies before
    # result publication. The stale retry route cannot start a second pair.
    crashing, _same_capability, crashing_harness = _executor(
        fake_modal,
        capability=capability,
        harness=_Harness(crash=True),
    )
    with pytest.raises(RuntimeError, match="simulated process death"):
        await crashing.execute(
            operation_id=operation_id,
            request=request,
            attempt=1,
            fence=1,
            retry_guard=None,
        )
    with pytest.raises(ModalAuthorExecutionUnknown, match="marker exists"):
        await executor.execute(
            operation_id=operation_id,
            request=request,
            attempt=2,
            fence=2,
            retry_guard=absent.guard,
        )

    assert capability.starts == 1
    assert crashing_harness.calls == 1
    assert harness.calls == 0


@pytest.mark.parametrize(
    ("overrides", "message"),
    (
        ({"sandbox_environment": ""}, "environment"),
        ({"workspace": "relative"}, "workspace"),
        ({"timeout_seconds": 0}, "timeouts"),
        ({"result_dict_name": "invalid/name"}, "Dict name"),
        ({"max_result_bytes": 4_096}, "provider envelope"),
    ),
)
def test_executor_config_rejects_unsafe_or_unbounded_values(
    overrides: dict[str, Any],
    message: str,
) -> None:
    values: dict[str, Any] = {
        "sandbox_environment": "modal-agent://sha256:test",
        **overrides,
    }
    with pytest.raises(ValueError, match=message):
        ModalMissionAuthorExecutorConfig(**values)


def test_executor_rejects_a_harness_for_another_workspace(
    fake_modal: _ModalRegistry,
) -> None:
    with pytest.raises(ValueError, match="workspaces must match"):
        _executor(fake_modal, harness=_Harness(workspace="/workspace/other"))


@pytest.mark.asyncio
async def test_invalid_attempt_and_retry_route_fail_before_provider_io(
    fake_modal: _ModalRegistry,
) -> None:
    executor, capability, harness = _executor(fake_modal)
    request = _request("dispatch-invalid-route")
    operation_id = "missions.author:world-a:dispatch-invalid-route"

    with pytest.raises(ValueError, match="positive attempt"):
        await executor.execute(
            operation_id=operation_id,
            request=request,
            attempt=0,
            fence=1,
            retry_guard=None,
        )
    with pytest.raises(ValueError, match="exact request"):
        await executor.execute(
            operation_id=operation_id,
            request=request,
            attempt=1,
            fence=1,
            retry_guard=AuthorActivityRetryGuard("not-authority", "not-authority"),
        )

    assert capability.starts == harness.calls == 0
    assert fake_modal.dicts == {}


@pytest.mark.asyncio
async def test_result_lookup_failure_is_unknown_without_leaking_provider_detail(
    fake_modal: _ModalRegistry,
) -> None:
    executor, capability, harness = _executor(fake_modal)
    fake_modal.get_error_calls.add(1)

    recovery = await executor.reconcile(
        operation_id="missions.author:world-a:dispatch-read-failure",
        request=_request("dispatch-read-failure"),
    )

    assert isinstance(recovery, AuthorRecoveryUnknown)
    assert "ConnectionError" in recovery.reason
    assert "credential-canary" not in recovery.reason
    assert capability.starts == harness.calls == 0


@pytest.mark.asyncio
async def test_workspace_mismatch_fails_before_start(
    fake_modal: _ModalRegistry,
) -> None:
    executor, capability, harness = _executor(fake_modal)
    fake_modal.reported_workspace_name = "another-workspace"

    with pytest.raises(ModalAuthorExecutionUnknown, match="workspace does not match"):
        await executor.execute(
            operation_id="missions.author:world-a:dispatch-workspace",
            request=_request("dispatch-workspace"),
            attempt=1,
            fence=1,
            retry_guard=None,
        )

    assert capability.starts == harness.calls == 0


@pytest.mark.asyncio
async def test_marker_lookup_failure_remains_unknown_and_cannot_mint_retry(
    fake_modal: _ModalRegistry,
) -> None:
    executor, capability, harness = _executor(fake_modal)
    fake_modal.marker_hydrate_error = True

    recovery = await executor.reconcile(
        operation_id="missions.author:world-a:dispatch-marker-error",
        request=_request("dispatch-marker-error"),
    )

    assert isinstance(recovery, AuthorRecoveryUnknown)
    assert "ConnectionError" in recovery.reason
    assert "credential-canary" not in recovery.reason
    assert capability.starts == harness.calls == 0


@pytest.mark.asyncio
async def test_ambiguous_put_without_visible_result_stays_unknown(
    fake_modal: _ModalRegistry,
) -> None:
    executor, capability, harness = _executor(fake_modal)
    fake_modal.raise_before_put_once = True

    with pytest.raises(ModalAuthorExecutionUnknown) as failure:
        await executor.execute(
            operation_id="missions.author:world-a:dispatch-put-absent",
            request=_request("dispatch-put-absent"),
            attempt=1,
            fence=1,
            retry_guard=None,
        )

    assert "AmbiguousPutError" in str(failure.value)
    assert "credential-canary" not in str(failure.value)
    assert capability.starts == harness.calls == 1
    assert capability.sessions[0].closed == 1
    assert fake_modal.result_values(executor) == ()


@pytest.mark.asyncio
async def test_atomic_first_result_accepts_an_exact_concurrent_value(
    fake_modal: _ModalRegistry,
) -> None:
    executor, capability, harness = _executor(fake_modal)
    fake_modal.return_false_after_same_write_once = True

    observation = await executor.execute(
        operation_id="missions.author:world-a:dispatch-exact-first",
        request=_request("dispatch-exact-first"),
        attempt=1,
        fence=1,
        retry_guard=None,
    )

    assert observation.result.dispatch_id == "dispatch-exact-first"
    assert capability.starts == harness.calls == 1


@pytest.mark.asyncio
async def test_atomic_first_result_rejects_a_conflicting_concurrent_value(
    fake_modal: _ModalRegistry,
) -> None:
    executor, capability, harness = _executor(fake_modal)
    fake_modal.return_false_after_conflicting_write_once = True

    with pytest.raises(RuntimeError, match="conflicting first result"):
        await executor.execute(
            operation_id="missions.author:world-a:dispatch-conflict",
            request=_request("dispatch-conflict"),
            attempt=1,
            fence=1,
            retry_guard=None,
        )

    assert capability.starts == harness.calls == 1
    assert capability.sessions[0].closed == 1


@pytest.mark.parametrize(
    ("harness", "message"),
    (
        (_Harness(wrong_dispatch=True), "exact request"),
        (_Harness(wrong_sandbox=True), "another sandbox"),
    ),
)
@pytest.mark.asyncio
async def test_mismatched_harness_result_is_never_published(
    fake_modal: _ModalRegistry,
    harness: _Harness,
    message: str,
) -> None:
    executor, capability, _selected = _executor(fake_modal, harness=harness)

    with pytest.raises(ValueError, match=message):
        await executor.execute(
            operation_id=f"missions.author:world-a:{message}",
            request=_request(f"dispatch-{message.replace(' ', '-')}"),
            attempt=1,
            fence=1,
            retry_guard=None,
        )

    assert capability.starts == harness.calls == 1
    assert capability.sessions[0].closed == 1
    assert fake_modal.result_values(executor) == ()


@pytest.mark.asyncio
async def test_recovery_binds_the_exact_sanitized_request(
    fake_modal: _ModalRegistry,
) -> None:
    executor, capability, harness = _executor(fake_modal)
    request = _request("dispatch-request-binding")
    operation_id = "missions.author:world-a:dispatch-request-binding"
    await executor.execute(
        operation_id=operation_id,
        request=request,
        attempt=1,
        fence=1,
        retry_guard=None,
    )
    changed = replace(request, prompt="a different committed prompt")
    restarted, restarted_capability, restarted_harness = _executor(fake_modal)

    recovery = await restarted.reconcile(
        operation_id=operation_id,
        request=changed,
    )

    assert isinstance(recovery, AuthorRecoveryUnknown)
    assert "exact operation and request" in recovery.reason
    assert capability.starts == harness.calls == 1
    assert restarted_capability.starts == restarted_harness.calls == 0


@pytest.mark.asyncio
async def test_noncanonical_provider_result_is_not_recovered(
    fake_modal: _ModalRegistry,
) -> None:
    executor, _capability, _harness = _executor(fake_modal)
    request = _request("dispatch-noncanonical")
    operation_id = "missions.author:world-a:dispatch-noncanonical"
    await executor.execute(
        operation_id=operation_id,
        request=request,
        attempt=1,
        fence=1,
        retry_guard=None,
    )
    data = fake_modal.dicts[(fake_modal.environment_name, executor.result_dict_name)]
    (key,) = data.values
    data.values[key] = str(data.values[key]) + " "

    recovery = await executor.reconcile(
        operation_id=operation_id,
        request=request,
    )

    assert isinstance(recovery, AuthorRecoveryUnknown)
    assert "canonically encoded" in recovery.reason


def test_family_codec_rejects_unbounded_and_noncanonical_results() -> None:
    redactor = RedactionService()
    codec = MissionAuthorValueCodec(redactor=redactor)
    durable = codec.sanitize_observation(_raw_observation())
    encoded = codec.encode_observation(durable)

    assert codec.redaction_policy_id == redactor.policy_id
    assert codec.max_result_bytes == 512 * 1024
    assert codec.decode_observation(encoded) == durable
    with pytest.raises(ValueError, match="canonical JSON"):
        codec.decode_observation(b"not-json")
    with pytest.raises(ValueError, match="incompatible envelope"):
        codec.decode_observation(b'{"kind":"other","schema_version":1,"value":{}}')
    with pytest.raises(ValueError, match="canonically encoded"):
        codec.decode_observation(encoded + b" ")
    with pytest.raises(ValueError, match="durability limit"):
        MissionAuthorValueCodec(
            redactor=redactor,
            max_result_bytes=16,
        ).sanitize_observation(_raw_observation("dispatch-too-large"))


def test_family_codec_rejects_invalid_redaction_and_metadata_bounds() -> None:
    class _EmptyPolicyRedactor:
        policy_id = ""

    with pytest.raises(ValueError, match="policy identity"):
        MissionAuthorValueCodec(redactor=_EmptyPolicyRedactor())  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="byte limit"):
        MissionAuthorValueCodec(redactor=RedactionService(), max_result_bytes=0)

    codec = MissionAuthorValueCodec(redactor=RedactionService())
    with pytest.raises(ValueError, match="4096"):
        codec.sanitize_request(
            replace(
                _request("dispatch-metadata-bound"),
                repository="x" * 4_097,
            )
        )
