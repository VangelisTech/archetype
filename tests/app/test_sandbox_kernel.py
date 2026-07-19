# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import ast
import asyncio
import hashlib
import json
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any

import pytest

from archetype.app.missions import (
    AttemptClaimAcquireOutcome,
    AttemptClaimStatus,
    AttemptRecoveryAction,
    FencedExecutionAuthorization,
    MissionAttemptClaimService,
    MissionAttemptExecution,
    MissionAttemptExecutionService,
    MissionService,
    ProviderExecutionCapabilities,
    attempt_invocation_fingerprint,
)
from archetype.app.redaction.service import RedactionService
from archetype.app.sandboxes import (
    GIT_TREE_CHANGE_GATE_NAME,
    AttemptPhase,
    CodingAgentSandboxClient,
    CommandResult,
    SandboxService,
    ValidatorSpec,
    iSandboxSession,
)
from archetype.app.sandboxes.models import (
    AgentExecution,
    CheckpointCapture,
    EvidenceCapture,
    PreparedAttempt,
    RepositoryFinalization,
    RepositoryPhaseReceipt,
    ValidationEvidence,
)
from archetype.app.sandboxes.versions import load_version_inventory
from archetype.app.storage.catalog import (
    AttemptClaimConflictError,
    AttemptClaimPendingError,
    AttemptClaimStaleError,
    SqliteControlCatalog,
)


def _claim_service(catalog: Any) -> MissionAttemptClaimService:
    return MissionAttemptClaimService(catalog, redaction_service=RedactionService())


@dataclass(frozen=True)
class _Spec:
    repo_url: str = "https://example.test/repo.git"
    branch: str = "agent/fix"
    base_ref: str = "main"
    harness: str = "codex"
    model: str = ""
    opencode_base_url: str = "https://endpoint.example/v1"
    opencode_provider_id: str = "test-endpoint"
    opencode_wire_api: str = "chat-completions"
    opencode_header_env: Mapping[str, str] = field(
        default_factory=lambda: {
            "Modal-Key": "MODAL_ENDPOINT_TOKEN_ID",
            "Modal-Secret": "MODAL_ENDPOINT_TOKEN_SECRET",
        }
    )
    workspace: str = "/workspace/repo"
    agent_timeout_seconds: int = 60
    snapshot_timeout_seconds: int = 30
    snapshot_ttl_seconds: int | None = 120
    snapshot_after_attempt: bool = True
    capture_filesystem_manifests: bool = True
    push: bool = False
    git_author_name: str = "Agent"
    git_author_email: str = "agent@example.test"


@dataclass
class _FakeClient(CodingAgentSandboxClient[_Spec]):
    spec: _Spec
    _sandbox: object = field(default_factory=object)
    _agent_secret: object | None = None
    files: dict[str, str] = field(default_factory=dict)
    events: list[tuple[str, dict[str, Any]]] = field(default_factory=list)
    commands: list[tuple[tuple[str, ...], dict[str, Any]]] = field(default_factory=list)
    phases: list[AttemptPhase] = field(default_factory=list)
    agent_calls: list[tuple[str, str]] = field(default_factory=list)
    agent_result: CommandResult = field(
        default_factory=lambda: CommandResult(
            ("codex",),
            0,
            '{"type":"thread.started","thread_id":"thread-1"}\n',
            "",
        )
    )
    agent_error: BaseException | None = None
    agent_head_after_run: str = ""
    validator_returncode: int = 0
    tree_changed: bool = True
    head: str = "baseline"
    checkpoint_ref: str = "fake-checkpoint://checkpoint-1"
    checkpoint_error: BaseException | None = None
    evidence_error: BaseException | None = None
    context_exists: bool = True
    close_calls: int = 0

    @property
    def sandbox_id(self) -> str:
        return "sandbox-1"

    async def close(self) -> None:
        self.close_calls += 1
        self._closed = True

    async def _exec(
        self,
        *args: str,
        workdir: str | None = None,
        timeout: int | None = None,
        secrets: Sequence[Any] = (),
        env: dict[str, str] | None = None,
    ) -> CommandResult:
        kwargs = {
            "workdir": workdir,
            "timeout": timeout,
            "secrets": tuple(secrets),
            "env": env,
        }
        self.commands.append((args, kwargs))
        if args[0] == "cat":
            value = self.files.get(args[1])
            return CommandResult(args, 0 if value is not None else 1, value or "", "")
        if args[:3] == ("test", "-d", f"{self.spec.workspace}/.context"):
            return CommandResult(args, 0 if self.context_exists else 1, "", "")
        if args[0] == "verify":
            return CommandResult(
                args,
                self.validator_returncode,
                "validator output",
                "validator failed" if self.validator_returncode else "",
            )
        if args[:3] == ("git", "rev-parse", "HEAD"):
            return CommandResult(args, 0, f"{self.head}\n", "")
        if args[:3] == ("git", "status", "--porcelain"):
            return CommandResult(args, 0, " M file.py\n" if self.tree_changed else "", "")
        if args[:3] == ("git", "reset", "--mixed"):
            self.head = args[3]
            return CommandResult(args, 0, "", "")
        if args[0] == "git" and "commit" in args:
            self.head = "committed"
            return CommandResult(args, 0, "committed", "")
        return CommandResult(args, 0, "", "")

    async def _write_text(self, path: str, value: str) -> None:
        self.files[path] = value

    async def _snapshot_if_configured(self, checkpoint_key: str = "") -> str:
        assert checkpoint_key
        if self.checkpoint_error is not None:
            raise self.checkpoint_error
        return self.checkpoint_ref if self.spec.snapshot_after_attempt else ""

    def _checkpoint_provider(self) -> str:
        return "fake"

    def _sandbox_uri(self, path: str) -> str:
        return f"fake-sandbox://{self.sandbox_id}{path}"

    async def _run_agent(self, prompt: str, *, session_id: str) -> CommandResult:
        self.agent_calls.append((prompt, session_id))
        if self.agent_error is not None:
            raise self.agent_error
        if self.agent_head_after_run:
            self.head = self.agent_head_after_run
        return self.agent_result

    async def _capture_git_recovery(self, attempt_id: str, baseline: str) -> dict[str, str]:
        assert attempt_id and baseline
        root = f"{self.spec.workspace}/.archetype-agent/recovery/{attempt_id}"
        return {
            "status": f"{root}-status.txt",
            "patch": f"{root}.patch",
            "bundle": f"{root}.bundle",
        }

    async def _ensure_start_manifest(self) -> str:
        return f"{self.spec.workspace}/.archetype-agent/filesystem/start.jsonl"

    async def _capture_attempt_filesystem(
        self, step_name: str, attempt_index: int
    ) -> tuple[str, str]:
        root = f"{self.spec.workspace}/.archetype-agent/filesystem/{step_name}-{attempt_index}"
        return f"{root}-end.jsonl", f"{root}-diff.jsonl"

    def _live_artifact_paths(self) -> tuple[str, str]:
        root = f"{self.spec.workspace}/.archetype-agent/live"
        return f"{root}/status.json", f"{root}/events.jsonl"

    async def _emit_live_event(self, event_type: str, **details: Any) -> None:
        self.events.append((event_type, details))

    async def _execution_phase(
        self,
        prepared: PreparedAttempt,
        previous_session_id: str,
        recovered: RepositoryPhaseReceipt | None = None,
    ) -> AgentExecution:
        self.phases.append(AttemptPhase.EXECUTION)
        return await super()._execution_phase(prepared, previous_session_id, recovered)

    async def _validation_phase(
        self,
        validators: Sequence[ValidatorSpec],
        recovered: RepositoryPhaseReceipt | None = None,
    ) -> ValidationEvidence:
        self.phases.append(AttemptPhase.VALIDATION)
        return await super()._validation_phase(validators, recovered)

    async def _repository_finalization_phase(
        self,
        prepared: PreparedAttempt,
        execution: AgentExecution,
        validation: ValidationEvidence,
        recovered: RepositoryPhaseReceipt | None = None,
    ) -> RepositoryFinalization:
        self.phases.append(AttemptPhase.REPOSITORY_FINALIZATION)
        return await super()._repository_finalization_phase(
            prepared, execution, validation, recovered
        )

    async def _evidence_phase(
        self,
        prepared: PreparedAttempt,
        execution: AgentExecution,
        repository: RepositoryFinalization,
    ) -> EvidenceCapture:
        self.phases.append(AttemptPhase.EVIDENCE)
        if self.evidence_error is not None:
            raise self.evidence_error
        return await super()._evidence_phase(prepared, execution, repository)

    async def _checkpoint_phase(self, prepared: PreparedAttempt) -> CheckpointCapture:
        self.phases.append(AttemptPhase.CHECKPOINT)
        return await super()._checkpoint_phase(prepared)

    async def _artifact_handoff_phase(
        self,
        prepared: PreparedAttempt,
        execution: AgentExecution,
        repository: RepositoryFinalization,
        evidence: EvidenceCapture,
        checkpoint: CheckpointCapture,
    ) -> dict[str, Any]:
        self.phases.append(AttemptPhase.ARTIFACT_HANDOFF)
        return await super()._artifact_handoff_phase(
            prepared, execution, repository, evidence, checkpoint
        )


def _authorization(
    idempotency_key: str,
    *,
    action: AttemptRecoveryAction = AttemptRecoveryAction.EXECUTE,
    prompt: str = "Fix the reported bug",
    validators: tuple[dict[str, Any], ...] = (
        {
            "name": "tests",
            "command": ["verify"],
            "timeout_seconds": 10,
        },
    ),
    step_name: str = "fix",
    attempt_index: int = 1,
    previous_session_id: str = "",
    previous_validator_details: tuple[dict[str, Any], ...] = (),
    correlation: dict[str, Any] | None = None,
    provider_session_id: str = "",
    provider_idempotency_key: str = "",
    lease_expires_at: float | None = None,
) -> FencedExecutionAuthorization:
    correlation = correlation or {
        "world_id": "world",
        "run_id": "run",
        "entity_id": "7",
        "step_index": 0,
    }
    return FencedExecutionAuthorization(
        action=action,
        claim_key="claim-1",
        world_id="world",
        run_id="run",
        mission_id="world:run:7",
        task_id="task-1",
        attempt_id=hashlib.sha256(idempotency_key.encode()).hexdigest(),
        idempotency_key=idempotency_key,
        request_fingerprint="request-fingerprint",
        sandbox_request_fingerprint=attempt_invocation_fingerprint(
            prompt=prompt,
            validators=validators,
            step_name=step_name,
            attempt_index=attempt_index,
            previous_session_id=previous_session_id,
            previous_validator_details=previous_validator_details,
            correlation=correlation,
        ),
        execution_nonce="execution-nonce-1",
        claimant="worker-1",
        fence_epoch=1,
        lease_expires_at=time.time() + 3600 if lease_expires_at is None else lease_expires_at,
        provider_session_id=provider_session_id,
        provider_idempotency_key=provider_idempotency_key,
    )


async def _acknowledge_provider(
    provider_session_id: str,
    provider_request_id: str,
) -> None:
    del provider_session_id, provider_request_id


async def _authorize_execution(
    authorization: FencedExecutionAuthorization,
) -> None:
    del authorization


def _attempt_kwargs() -> dict[str, Any]:
    idempotency_key = "world/run/attempt-1"
    return {
        "prompt": "Fix the reported bug",
        "validators": [ValidatorSpec("tests", ("verify",), timeout_seconds=10)],
        "step_name": "fix",
        "attempt_index": 1,
        "idempotency_key": idempotency_key,
        "authorization": _authorization(idempotency_key),
        "authorize_execution": _authorize_execution,
        "acknowledge_provider": _acknowledge_provider,
        "correlation": {
            "world_id": "world",
            "run_id": "run",
            "entity_id": "7",
            "step_index": 0,
        },
    }


def _mission_row() -> dict[str, Any]:
    plan = [
        {
            "name": "fix",
            "prompt": "Fix the reported bug",
            "validators": [{"name": "tests", "command": ["verify"]}],
        }
    ]
    return {
        "world_id": "world",
        "run_id": "run",
        "entity_id": 7,
        "mission__name": "mission",
        "mission__plan_json": json.dumps(plan),
        "mission__status": "ready",
        "mission__finished": False,
        "mission__succeeded": False,
        "mission__failure_reason": "",
        "mission__pr_ready": False,
        "mission__pr_url": "",
        "taskgate__step_index": 0,
        "taskgate__step_name": "fix",
        "taskgate__prompt": "Fix the reported bug",
        "taskgate__validators_json": json.dumps(plan[0]["validators"]),
        "taskgate__attempts": 0,
        "taskgate__max_attempts": 3,
        "taskgate__status": "ready",
        "taskgate__required_finalization_phase": "checkpointed",
        "taskgate__passed": False,
        "attempt__agent_session_id": "",
        "attempt__validator_details_json": "[]",
        "frictionlog__entries_json": "[]",
    }


def _provider_capabilities() -> ProviderExecutionCapabilities:
    return _FakeClient(_Spec()).provider_execution_capabilities


@pytest.mark.asyncio
async def test_attempt_runs_six_phases_and_returns_checkpoint_qualified_handoff() -> None:
    client = _FakeClient(_Spec())

    outcome = await client.run_attempt(**_attempt_kwargs())

    assert client.phases == list(AttemptPhase)
    assert outcome["accepted"] is True
    assert outcome["status"] == "accepted"
    assert outcome["sha"] == "committed"
    assert outcome["checkpoint_status"] == "ready"
    assert outcome["checkpoint_expires_at_ms"] > outcome["checkpoint_created_at_ms"]
    assert outcome["finalization_phase"] == "checkpointed"
    assert client._latest_checkpoint_ref == "fake-checkpoint://checkpoint-1"
    assert outcome["agent_session_id"] == "thread-1"
    assert outcome["correlation"] == {
        "world_id": "world",
        "run_id": "run",
        "entity_id": "7",
        "step_index": 0,
    }
    assert outcome["git_bundle_ref"].startswith("fake-checkpoint://checkpoint-1#")
    assert outcome["worktree_archive_ref"].startswith("fake-checkpoint://checkpoint-1#")
    assert outcome["live_events_ref"].startswith("fake-sandbox://sandbox-1/")
    assert any(path.endswith(".json") for path in client.files)
    initial_mkdir = next(call[0] for call in client.commands if call[0][:2] == ("mkdir", "-p"))
    assert f"{client.spec.workspace}/.archetype-agent/manifests" in initial_mkdir
    assert f"{client.spec.workspace}/.archetype-agent/gates" in initial_mkdir
    assert [event for event, _ in client.events][-3:] == [
        "artifact_handoff_started",
        "artifact_handoff_finished",
        "attempt_completed",
    ]
    validator_call = next(call for call in client.commands if call[0][0] == "verify")
    assert validator_call[1]["secrets"] == ()
    manifest_path = next(path for path in client.files if "/manifests/" in path)
    environment = json.loads(client.files[manifest_path])["environment"]
    inventory = load_version_inventory()
    harness_pin = inventory.harness_pin(client.spec.harness)
    assert environment["inventory_digest"] == inventory.digest
    assert environment["harness"]["version"] == harness_pin.version
    assert environment["harness"]["immutable_ref"] == harness_pin.immutable_ref
    assert environment["configuration_digest"] == (
        client.provider_execution_capabilities.request_fingerprint
    )
    version_probe = next(call for call in client.commands if call[0][1:] == ("--version",))
    assert version_probe[1]["secrets"] == ()
    assert isinstance(client, iSandboxSession)


@pytest.mark.asyncio
async def test_sandbox_receipt_replays_without_second_submission() -> None:
    client = _FakeClient(_Spec())
    first = await client.run_attempt(**_attempt_kwargs())
    client.agent_result = CommandResult(("codex",), 99, "", "must not run")

    second = await client.run_attempt(**_attempt_kwargs())

    assert second == first
    assert len(client.agent_calls) == 1
    assert client.phases == list(AttemptPhase)


@pytest.mark.asyncio
async def test_sandbox_receipt_rejects_idempotency_key_reuse_for_changed_request() -> None:
    client = _FakeClient(_Spec())
    await client.run_attempt(**_attempt_kwargs())
    changed = _attempt_kwargs()
    changed["prompt"] = "A materially different task"

    with pytest.raises(ValueError, match="does not match its durable attempt claim"):
        await client.run_attempt(**changed)

    assert len(client.agent_calls) == 1


@pytest.mark.parametrize(
    "changes",
    [
        {"prompt": "A different prompt"},
        {"validators": [ValidatorSpec("tests", ("verify",), timeout_seconds=11)]},
        {"step_name": "different-step"},
        {"attempt_index": 2},
        {"previous_session_id": "different-session"},
        {"previous_validator_details": ({"name": "tests", "passed": False},)},
        {
            "correlation": {
                "world_id": "world",
                "run_id": "run",
                "entity_id": "7",
                "step_index": 1,
            }
        },
    ],
)
@pytest.mark.asyncio
async def test_fenced_authorization_binds_every_sandbox_invocation_field(
    changes: dict[str, Any],
) -> None:
    client = _FakeClient(_Spec())
    kwargs = _attempt_kwargs()
    kwargs.update(changes)

    with pytest.raises(ValueError, match="does not match its durable attempt claim"):
        await client.run_attempt(**kwargs)

    assert client.agent_calls == []
    assert client.phases == []


def test_invocation_fingerprint_normalizes_validator_dict_defaults() -> None:
    correlation = {
        "world_id": "world",
        "run_id": "run",
        "entity_id": "7",
        "step_index": 0,
    }
    raw = attempt_invocation_fingerprint(
        prompt="fix",
        validators=({"name": "tests", "command": ["pytest", "-q"]},),
        step_name="fix",
        attempt_index=1,
        previous_session_id="",
        previous_validator_details=(),
        correlation=correlation,
    )
    normalized = _FakeClient._request_fingerprint(
        prompt="fix",
        validators=(ValidatorSpec("tests", ("pytest", "-q")),),
        step_name="fix",
        attempt_index=1,
        previous_session_id="",
        previous_validator_details=(),
        correlation=correlation,
    )
    assert raw == normalized


@pytest.mark.asyncio
async def test_nonzero_agent_does_not_bypass_validators_evidence_or_checkpoint() -> None:
    client = _FakeClient(
        _Spec(),
        agent_result=CommandResult(("codex",), 7, "partial", "agent failed"),
        validator_returncode=2,
    )

    outcome = await client.run_attempt(**_attempt_kwargs())

    assert outcome["accepted"] is False
    assert outcome["sha"] == ""
    assert outcome["agent_completed"] is False
    assert outcome["checkpoint_status"] == "ready"
    assert client.phases == list(AttemptPhase)
    assert [item["finding"] for item in outcome["friction"]] == [
        "codex exited with code 7; authoritative validators still ran",
        "Gate failed: tests",
    ]


@pytest.mark.asyncio
async def test_passing_validator_without_tree_change_is_rejected() -> None:
    client = _FakeClient(_Spec(), tree_changed=False)

    outcome = await client.run_attempt(**_attempt_kwargs())

    assert outcome["accepted"] is False
    assert outcome["results"] == {"tests": True, "git_tree_change": False}
    assert outcome["checkpoint_status"] == "ready"


@pytest.mark.asyncio
async def test_agent_authored_commit_is_rejected_before_trusted_finalization() -> None:
    client = _FakeClient(_Spec(), agent_head_after_run="agent-commit")

    outcome = await client.run_attempt(**_attempt_kwargs())

    assert outcome["accepted"] is False
    assert outcome["sha"] == ""
    assert outcome["pushed"] is False
    assert client.head == "baseline"
    assert outcome["results"] == {"tests": True, "git_tree_change": False}
    gate = next(item for item in outcome["validator_details"] if item["name"] == "git_tree_change")
    assert "agent-authored commits are not accepted" in gate["stderr"]
    assert any(call[0][:3] == ("git", "reset", "--mixed") for call in client.commands)
    assert not any(call[0][0] == "git" and "commit" in call[0] for call in client.commands)
    assert not any(call[0][0] == "git" and "push" in call[0] for call in client.commands)


@pytest.mark.asyncio
async def test_repository_receipt_resumes_after_post_commit_evidence_crash() -> None:
    client = _FakeClient(_Spec(), evidence_error=RuntimeError("evidence unavailable"))

    with pytest.raises(RuntimeError, match="evidence unavailable"):
        await client.run_attempt(**_attempt_kwargs())

    assert client.head == "committed"
    assert len(client.agent_calls) == 1
    assert client._repository_receipt_path(_attempt_kwargs()["idempotency_key"]) in client.files
    commit_calls = [call for call in client.commands if call[0][0] == "git" and "commit" in call[0]]
    assert len(commit_calls) == 1

    client.evidence_error = None
    client.agent_result = CommandResult(("codex",), 99, "", "must not run")
    recovered = _attempt_kwargs()
    recovered["authorization"] = _authorization(
        recovered["idempotency_key"],
        action=AttemptRecoveryAction.RECONCILE,
    )
    outcome = await client.run_attempt(**recovered)

    assert outcome["accepted"] is True
    assert outcome["sha"] == "committed"
    assert len(client.agent_calls) == 1
    assert (
        len([call for call in client.commands if call[0][0] == "git" and "commit" in call[0]]) == 1
    )
    assert any(event == "attempt_resumed" for event, _ in client.events)


@pytest.mark.asyncio
async def test_nonexecuting_claim_action_requires_a_repository_receipt() -> None:
    client = _FakeClient(_Spec())
    kwargs = _attempt_kwargs()
    kwargs["authorization"] = _authorization(
        kwargs["idempotency_key"],
        action=AttemptRecoveryAction.RECONCILE,
    )

    with pytest.raises(RuntimeError, match="forbids model execution"):
        await client.run_attempt(**kwargs)

    assert client.agent_calls == []
    assert client.phases == []


@pytest.mark.parametrize(
    ("authorization", "previous_session_id", "message"),
    [
        (
            _authorization("wrong-key"),
            "",
            "does not match the idempotency key",
        ),
        (
            replace(
                _authorization("world/run/attempt-1"),
                attempt_id="wrong-attempt",
            ),
            "",
            "does not match the attempt identity",
        ),
        (
            replace(
                _authorization("world/run/attempt-1"),
                fence_epoch=0,
            ),
            "",
            "positive owned fence",
        ),
        (
            replace(
                _authorization("world/run/attempt-1"),
                claim_key="",
            ),
            "",
            "claim and request identity",
        ),
        (
            _authorization(
                "world/run/attempt-1",
                lease_expires_at=0,
            ),
            "",
            "lease expired",
        ),
        (
            _authorization(
                "world/run/attempt-1",
                action=AttemptRecoveryAction.REPLAY_IDEMPOTENT,
                provider_idempotency_key="provider-operation-1",
            ),
            "",
            "no implemented provider transport",
        ),
        (
            _authorization(
                "world/run/attempt-1",
                action=AttemptRecoveryAction.RESUME_SESSION,
                previous_session_id="session-1",
                provider_session_id="session-1",
            ),
            "session-1",
            "no implemented provider transport",
        ),
    ],
)
@pytest.mark.asyncio
async def test_sandbox_rejects_invalid_fenced_authorization_before_execution(
    authorization: FencedExecutionAuthorization,
    previous_session_id: str,
    message: str,
) -> None:
    client = _FakeClient(_Spec())
    kwargs = _attempt_kwargs()
    kwargs["authorization"] = authorization
    kwargs["previous_session_id"] = previous_session_id

    with pytest.raises((ValueError, RuntimeError), match=message):
        await client.run_attempt(**kwargs)

    assert client.agent_calls == []
    assert client.phases == []


@pytest.mark.asyncio
async def test_checkpoint_failure_is_evidence_not_lost_attempt() -> None:
    client = _FakeClient(_Spec(), checkpoint_error=RuntimeError("snapshot unavailable"))
    client._latest_checkpoint_ref = "fake-checkpoint://previous"

    outcome = await client.run_attempt(**_attempt_kwargs())

    assert outcome["accepted"] is True
    assert outcome["checkpoint_status"] == "failed"
    assert outcome["checkpoint_restorable"] is False
    assert client._latest_checkpoint_ref == "fake-checkpoint://previous"
    assert outcome["finalization_phase"] == "captured"
    assert "snapshot unavailable" in outcome["finalization_error"]
    assert outcome["git_bundle_ref"].startswith("fake-sandbox://sandbox-1/")
    assert outcome["worktree_archive_ref"].startswith("fake-sandbox://sandbox-1/")
    assert outcome["friction"][-1]["finding"] == "Provider checkpoint failed"


@pytest.mark.asyncio
async def test_checkpoint_without_ttl_has_no_expiration_sentinel() -> None:
    client = _FakeClient(replace(_Spec(), snapshot_ttl_seconds=None))

    outcome = await client.run_attempt(**_attempt_kwargs())

    assert outcome["checkpoint_status"] == "ready"
    assert outcome["checkpoint_restorable"] is True
    assert outcome["checkpoint_expires_at_ms"] is None


@pytest.mark.parametrize(
    ("snapshot_after_attempt", "persisted_status", "attempt_status"),
    [(True, "created", "accepted"), (False, "disabled", "incomplete")],
)
@pytest.mark.asyncio
async def test_real_sandbox_checkpoint_outcome_crosses_the_mission_boundary(
    snapshot_after_attempt: bool,
    persisted_status: str,
    attempt_status: str,
) -> None:
    service = MissionService()
    row = _mission_row()
    request = service.prepare_attempt(row, tick=1)
    assert request is not None
    client = _FakeClient(replace(_Spec(), snapshot_after_attempt=snapshot_after_attempt))

    outcome = await client.run_attempt(
        prompt=request.prompt,
        validators=[ValidatorSpec.from_dict(dict(value)) for value in request.validators],
        step_name=request.step_name,
        attempt_index=request.attempt_index,
        idempotency_key=request.idempotency_key,
        authorization=_authorization(
            request.idempotency_key,
            prompt=request.prompt,
            validators=request.validators,
            step_name=request.step_name,
            attempt_index=request.attempt_index,
            previous_session_id=request.previous_session_id,
            previous_validator_details=request.previous_validator_details,
            correlation=request.correlation,
        ),
        authorize_execution=_authorize_execution,
        acknowledge_provider=_acknowledge_provider,
        previous_session_id=request.previous_session_id,
        previous_validator_details=request.previous_validator_details,
        correlation=request.correlation,
    )
    updated = service.apply_attempt(row, request, outcome)

    assert updated["checkpoint__status"] == persisted_status
    assert updated["attempt__status"] == attempt_status


@pytest.mark.asyncio
async def test_claim_acquisition_failure_precedes_every_sandbox_phase(
    tmp_path,
    monkeypatch,
) -> None:
    catalog = SqliteControlCatalog(tmp_path / "catalog.db")
    claims = _claim_service(catalog)
    executions = MissionAttemptExecutionService(claims, MissionService())
    client = _FakeClient(_Spec())
    row = _mission_row()
    request = MissionService().prepare_attempt(row, tick=1)
    assert request is not None

    async def fail_before_claim_commit(*args: Any, **kwargs: Any) -> None:
        del args, kwargs
        raise RuntimeError("claim commit failed")

    monkeypatch.setattr(claims, "acquire", fail_before_claim_commit)
    with pytest.raises(RuntimeError, match="claim commit failed"):
        await executions.run(
            row,
            tick=1,
            claimant="worker-before-commit",
            runner=client,
        )

    claim_key = claims.claim_key(
        world_id=request.correlation["world_id"],
        mission_id=request.mission_id,
        task_id=request.task_id,
        attempt_id=request.attempt_id,
    )
    assert await claims.get(request.correlation["world_id"], claim_key) is None
    assert client.agent_calls == []
    assert client.phases == []
    assert client.commands == []
    assert client.events == []
    await catalog.close()


@pytest.mark.asyncio
async def test_crash_after_claim_commit_recovers_claimed_then_executes_once(tmp_path) -> None:
    path = tmp_path / "catalog.db"
    row = _mission_row()
    request = MissionService().prepare_attempt(row, tick=1)
    assert request is not None

    original_catalog = SqliteControlCatalog(path)
    original = _claim_service(original_catalog)
    acquired = await original.acquire(
        request,
        _provider_capabilities(),
        claimant="worker-crashed-before-provider",
        lease_seconds=0,
    )
    persisted_before_crash = await original.get(
        acquired.claim.world_id,
        acquired.claim.claim_key,
    )
    assert persisted_before_crash == acquired.claim
    assert persisted_before_crash.status is AttemptClaimStatus.CLAIMED
    assert persisted_before_crash.possibly_submitted_at is None
    await original_catalog.close()

    recovered_catalog = SqliteControlCatalog(path)
    recovered_claims = _claim_service(recovered_catalog)
    takeover = await recovered_claims.acquire(
        request,
        _provider_capabilities(),
        claimant="worker-recovered-before-provider",
        lease_seconds=60,
    )
    assert takeover.outcome is AttemptClaimAcquireOutcome.RECOVERED
    assert takeover.claim.status is AttemptClaimStatus.CLAIMED
    assert takeover.claim.possibly_submitted_at is None
    assert takeover.claim.fence_epoch == acquired.claim.fence_epoch + 1

    client = _FakeClient(_Spec())
    completed = await MissionAttemptExecutionService(recovered_claims, MissionService()).run(
        row,
        tick=1,
        claimant="worker-recovered-before-provider",
        runner=client,
        lease_seconds=60,
    )

    assert completed is not None
    assert completed.acquisition.outcome is AttemptClaimAcquireOutcome.OWNED
    assert completed.decision.action is AttemptRecoveryAction.EXECUTE
    assert completed.claim.status is AttemptClaimStatus.SETTLED
    assert completed.replayed is False
    assert len(client.agent_calls) == 1
    assert client.phases == list(AttemptPhase)
    persisted_after_execution = await recovered_claims.get(
        completed.claim.world_id,
        completed.claim.claim_key,
    )
    assert persisted_after_execution == completed.claim
    await recovered_catalog.close()


@pytest.mark.asyncio
async def test_claim_fenced_execution_settles_and_replays_without_second_model_call(
    tmp_path,
) -> None:
    catalog = SqliteControlCatalog(tmp_path / "catalog.db")
    claims = _claim_service(catalog)
    executions = MissionAttemptExecutionService(claims, MissionService())
    client = _FakeClient(_Spec())
    row = _mission_row()

    first = await executions.run(
        row,
        tick=1,
        claimant="worker-incarnation-1",
        runner=client,
    )
    replay = await executions.run(
        row,
        tick=2,
        claimant="worker-incarnation-2",
        runner=client,
    )

    assert first is not None and replay is not None
    assert first.acquisition.outcome is AttemptClaimAcquireOutcome.ACQUIRED
    assert first.claim.status is AttemptClaimStatus.SETTLED
    assert first.replayed is False
    assert replay.acquisition.outcome is AttemptClaimAcquireOutcome.DUPLICATE
    assert replay.claim == first.claim
    assert replay.outcome == first.outcome
    assert replay.updated_row == first.updated_row
    assert replay.replayed is True
    assert len(client.agent_calls) == 1
    await catalog.close()


@pytest.mark.asyncio
async def test_crash_after_artifact_handoff_reconciles_receipt_without_second_model_call(
    tmp_path,
    monkeypatch,
) -> None:
    catalog = SqliteControlCatalog(tmp_path / "catalog.db")
    claims = _claim_service(catalog)
    executions = MissionAttemptExecutionService(claims, MissionService())
    client = _FakeClient(_Spec())
    original_settle = claims.settle

    async def crash_before_claim_settlement(*args: Any, **kwargs: Any):
        del args, kwargs
        raise RuntimeError("crash after artifact handoff")

    monkeypatch.setattr(claims, "settle", crash_before_claim_settlement)
    with pytest.raises(RuntimeError, match="after artifact handoff"):
        await executions.run(
            _mission_row(),
            tick=1,
            claimant="worker-incarnation",
            runner=client,
        )
    assert len(client.agent_calls) == 1

    monkeypatch.setattr(claims, "settle", original_settle)
    recovered = await executions.run(
        _mission_row(),
        tick=2,
        claimant="worker-incarnation",
        runner=client,
    )

    assert recovered is not None
    assert recovered.decision.action is AttemptRecoveryAction.RECONCILE
    assert recovered.claim.status is AttemptClaimStatus.SETTLED
    assert recovered.replayed is False
    assert len(client.agent_calls) == 1
    await catalog.close()


@pytest.mark.asyncio
async def test_settled_execute_authorization_cannot_reach_a_fresh_model_process(
    tmp_path,
) -> None:
    catalog = SqliteControlCatalog(tmp_path / "catalog.db")
    claims = _claim_service(catalog)
    row = _mission_row()
    first_client = _FakeClient(_Spec())
    first = await MissionAttemptExecutionService(claims, MissionService()).run(
        row,
        tick=1,
        claimant="worker-incarnation-1",
        runner=first_client,
    )
    assert first is not None
    assert first.claim.status is AttemptClaimStatus.SETTLED

    fresh_client = _FakeClient(_Spec())
    request = first.request
    with pytest.raises(AttemptClaimStaleError, match="stale or already consumed"):
        await fresh_client.run_attempt(
            prompt=request.prompt,
            validators=request.validators,
            step_name=request.step_name,
            attempt_index=request.attempt_index,
            idempotency_key=request.idempotency_key,
            authorization=first.decision.authorization,
            authorize_execution=claims.consume_execution,
            acknowledge_provider=_acknowledge_provider,
            previous_session_id=request.previous_session_id,
            previous_validator_details=request.previous_validator_details,
            correlation=request.correlation,
        )

    assert fresh_client.agent_calls == []
    assert fresh_client.phases == []
    await catalog.close()


@pytest.mark.asyncio
async def test_orchestration_derives_provider_identity_from_the_runner(tmp_path) -> None:
    catalog = SqliteControlCatalog(tmp_path / "catalog.db")
    claims = _claim_service(catalog)
    row = _mission_row()
    request = MissionService().prepare_attempt(row, tick=1)
    assert request is not None
    original = _FakeClient(_Spec())
    await claims.acquire(
        request,
        original.provider_execution_capabilities,
        claimant="expired-worker",
        lease_seconds=0,
    )
    different = _FakeClient(replace(_Spec(), model="different-model"))

    with pytest.raises(AttemptClaimConflictError, match="different immutable input"):
        await MissionAttemptExecutionService(claims, MissionService()).run(
            row,
            tick=2,
            claimant="replacement-worker",
            runner=different,
        )

    assert different.agent_calls == []
    assert different.phases == []
    await catalog.close()


@pytest.mark.asyncio
async def test_execution_service_rejects_runner_that_skips_claim_callbacks(
    tmp_path,
    monkeypatch,
) -> None:
    catalog = SqliteControlCatalog(tmp_path / "catalog.db")
    claims = _claim_service(catalog)
    client = _FakeClient(_Spec())
    original_run = client.run_attempt

    async def run_without_durable_callbacks(**kwargs: Any) -> dict[str, Any]:
        kwargs["authorize_execution"] = _authorize_execution
        kwargs["acknowledge_provider"] = _acknowledge_provider
        return await original_run(**kwargs)

    monkeypatch.setattr(client, "run_attempt", run_without_durable_callbacks)
    with pytest.raises(RuntimeError, match="without consuming its execution grant"):
        await MissionAttemptExecutionService(claims, MissionService()).run(
            _mission_row(),
            tick=1,
            claimant="worker",
            runner=client,
        )

    request = MissionService().prepare_attempt(_mission_row(), tick=1)
    assert request is not None
    claim_key = claims.claim_key(
        world_id=request.correlation["world_id"],
        mission_id=request.mission_id,
        task_id=request.task_id,
        attempt_id=request.attempt_id,
    )
    persisted = await claims.get(request.correlation["world_id"], claim_key)
    assert persisted is not None
    assert persisted.status is AttemptClaimStatus.POSSIBLY_SUBMITTED
    assert persisted.execution_consumed_at is None
    await catalog.close()


@pytest.mark.asyncio
async def test_execution_heartbeat_prevents_live_attempt_takeover(tmp_path, monkeypatch) -> None:
    path = tmp_path / "catalog.db"
    catalog = SqliteControlCatalog(path)
    contender_catalog = SqliteControlCatalog(path)
    claims = _claim_service(catalog)
    contender = _claim_service(contender_catalog)
    client = _FakeClient(_Spec())
    original_run_agent = client._run_agent
    original_renew = claims.renew
    provider_started = asyncio.Event()
    heartbeat_renewed = asyncio.Event()
    release_provider = asyncio.Event()

    async def slow_run_agent(prompt: str, *, session_id: str) -> CommandResult:
        provider_started.set()
        await release_provider.wait()
        return await original_run_agent(prompt, session_id=session_id)

    async def observed_renew(*args: Any, **kwargs: Any):
        renewed = await original_renew(*args, **kwargs)
        if provider_started.is_set():
            heartbeat_renewed.set()
        return renewed

    monkeypatch.setattr(client, "_run_agent", slow_run_agent)
    monkeypatch.setattr(claims, "renew", observed_renew)
    execution = asyncio.create_task(
        MissionAttemptExecutionService(claims, MissionService()).run(
            _mission_row(),
            tick=1,
            claimant="live-worker",
            runner=client,
            lease_seconds=3.0,
        )
    )
    await provider_started.wait()
    await heartbeat_renewed.wait()
    request = MissionService().prepare_attempt(_mission_row(), tick=1)
    assert request is not None

    with pytest.raises(AttemptClaimPendingError, match="live claimant 'live-worker'"):
        await contender.acquire(
            request,
            client.provider_execution_capabilities,
            claimant="contender",
        )

    release_provider.set()
    completed = await execution
    assert completed is not None
    assert completed.claim.status is AttemptClaimStatus.SETTLED
    await catalog.close()
    await contender_catalog.close()


@pytest.mark.asyncio
async def test_cancelling_execution_stops_runner_and_lease_heartbeat(tmp_path, monkeypatch) -> None:
    catalog = SqliteControlCatalog(tmp_path / "catalog.db")
    claims = _claim_service(catalog)
    client = _FakeClient(_Spec())
    provider_started = asyncio.Event()
    provider_cancelled = asyncio.Event()
    renewals = 0
    original_renew = claims.renew

    async def blocked_run_agent(prompt: str, *, session_id: str) -> CommandResult:
        del prompt, session_id
        provider_started.set()
        try:
            await asyncio.Event().wait()
            raise AssertionError("unreachable blocked provider")
        finally:
            provider_cancelled.set()

    async def counted_renew(*args: Any, **kwargs: Any):
        nonlocal renewals
        renewals += 1
        return await original_renew(*args, **kwargs)

    monkeypatch.setattr(client, "_run_agent", blocked_run_agent)
    monkeypatch.setattr(claims, "renew", counted_renew)
    execution = asyncio.create_task(
        MissionAttemptExecutionService(claims, MissionService()).run(
            _mission_row(),
            tick=1,
            claimant="cancelled-worker",
            runner=client,
            lease_seconds=0.06,
        )
    )
    await provider_started.wait()
    await asyncio.sleep(0.08)
    execution.cancel()
    with pytest.raises(asyncio.CancelledError):
        await execution
    assert provider_cancelled.is_set()
    renewals_after_cancel = renewals
    await asyncio.sleep(0.08)
    assert renewals == renewals_after_cancel
    await catalog.close()


@pytest.mark.asyncio
async def test_concurrent_execution_decisions_reach_the_model_exactly_once(tmp_path) -> None:
    catalog = SqliteControlCatalog(tmp_path / "catalog.db")
    claims = _claim_service(catalog)
    executions = MissionAttemptExecutionService(claims, MissionService())
    client = _FakeClient(_Spec())
    row = _mission_row()

    results = await asyncio.gather(
        executions.run(
            row,
            tick=1,
            claimant="shared-worker-incarnation",
            runner=client,
        ),
        executions.run(
            row,
            tick=1,
            claimant="shared-worker-incarnation",
            runner=client,
        ),
        return_exceptions=True,
    )

    assert len(client.agent_calls) == 1
    successful = [value for value in results if not isinstance(value, BaseException)]
    assert successful
    assert isinstance(successful[0], MissionAttemptExecution)
    persisted = await claims.get(
        successful[0].claim.world_id,
        successful[0].claim.claim_key,
    )
    assert persisted is not None
    assert persisted.status is AttemptClaimStatus.SETTLED
    await catalog.close()


@pytest.mark.asyncio
async def test_crash_after_send_before_ack_never_becomes_a_second_model_call(
    tmp_path,
) -> None:
    path = tmp_path / "catalog.db"
    catalog = SqliteControlCatalog(path)
    claims = _claim_service(catalog)
    request = MissionService().prepare_attempt(_mission_row(), tick=1)
    assert request is not None
    acquired = await claims.acquire(
        request,
        _provider_capabilities(),
        claimant="crashed-worker",
        lease_seconds=0.5,
    )
    decision = await claims.decide_recovery(acquired.claim, lease_seconds=0.5)
    client = _FakeClient(_Spec())

    async def crash_before_ack(
        provider_session_id: str,
        provider_request_id: str,
    ) -> None:
        del provider_session_id, provider_request_id
        raise RuntimeError("crash before acknowledgement commit")

    with pytest.raises(RuntimeError, match="before acknowledgement"):
        await client.run_attempt(
            prompt=request.prompt,
            validators=request.validators,
            step_name=request.step_name,
            attempt_index=request.attempt_index,
            idempotency_key=request.idempotency_key,
            authorization=decision.authorization,
            authorize_execution=claims.consume_execution,
            acknowledge_provider=crash_before_ack,
            previous_session_id=request.previous_session_id,
            previous_validator_details=request.previous_validator_details,
            correlation=request.correlation,
        )
    assert len(client.agent_calls) == 1
    uncertain = await claims.get(acquired.claim.world_id, acquired.claim.claim_key)
    assert uncertain is not None
    assert uncertain.status is AttemptClaimStatus.POSSIBLY_SUBMITTED
    await asyncio.sleep(0.51)
    await catalog.close()

    cold_catalog = SqliteControlCatalog(path)
    cold = _claim_service(cold_catalog)
    recovered = await cold.acquire(
        request,
        _provider_capabilities(),
        claimant="recovery-worker",
    )
    recovery = await cold.decide_recovery(recovered.claim)
    assert recovery.action is AttemptRecoveryAction.RECONCILE
    with pytest.raises(RuntimeError, match="forbids model execution"):
        await client.run_attempt(
            prompt=request.prompt,
            validators=request.validators,
            step_name=request.step_name,
            attempt_index=request.attempt_index,
            idempotency_key=request.idempotency_key,
            authorization=recovery.authorization,
            authorize_execution=_authorize_execution,
            acknowledge_provider=_acknowledge_provider,
            previous_session_id=request.previous_session_id,
            previous_validator_details=request.previous_validator_details,
            correlation=request.correlation,
        )
    assert len(client.agent_calls) == 1
    await cold_catalog.close()


@pytest.mark.asyncio
async def test_crash_after_provider_ack_is_restart_discoverable_without_resubmission(
    tmp_path,
    monkeypatch,
) -> None:
    path = tmp_path / "catalog.db"
    catalog = SqliteControlCatalog(path)
    claims = _claim_service(catalog)
    executions = MissionAttemptExecutionService(claims, MissionService())
    client = _FakeClient(_Spec())

    async def crash_before_validation(*args: Any, **kwargs: Any) -> ValidationEvidence:
        del args, kwargs
        raise RuntimeError("crash after provider acknowledgement")

    monkeypatch.setattr(client, "_validation_phase", crash_before_validation)
    with pytest.raises(RuntimeError, match="after provider acknowledgement"):
        await executions.run(
            _mission_row(),
            tick=1,
            claimant="crashed-worker",
            runner=client,
            lease_seconds=0.5,
        )
    assert len(client.agent_calls) == 1
    request = MissionService().prepare_attempt(_mission_row(), tick=1)
    assert request is not None
    claim_key = claims.claim_key(
        world_id=request.correlation["world_id"],
        mission_id=request.mission_id,
        task_id=request.task_id,
        attempt_id=request.attempt_id,
    )
    acknowledged = await claims.get(request.correlation["world_id"], claim_key)
    assert acknowledged is not None
    assert acknowledged.status is AttemptClaimStatus.PROVIDER_ACKNOWLEDGED
    await asyncio.sleep(0.51)
    await catalog.close()

    cold_catalog = SqliteControlCatalog(path)
    cold = _claim_service(cold_catalog)
    recovered = await cold.acquire(
        request,
        _provider_capabilities(),
        claimant="recovery-worker",
    )
    recovery = await cold.decide_recovery(recovered.claim)
    assert recovery.action is AttemptRecoveryAction.RECONCILE
    assert len(client.agent_calls) == 1
    await cold_catalog.close()


@pytest.mark.asyncio
async def test_transport_failure_stops_before_authoritative_side_effects() -> None:
    client = _FakeClient(_Spec(), agent_error=TimeoutError("transport timed out"))

    with pytest.raises(TimeoutError, match="transport timed out"):
        await client.run_attempt(**_attempt_kwargs())

    assert client.phases == [AttemptPhase.EXECUTION]
    assert not any(call[0][0] == "verify" for call in client.commands)
    assert client.events[-1][0] == "agent_transport_failed"


@pytest.mark.asyncio
async def test_closed_sandbox_and_missing_context_fail_closed_or_declare_no_ref() -> None:
    closed = _FakeClient(_Spec())
    await closed.close()
    with pytest.raises(RuntimeError, match="already closed"):
        await closed.run_attempt(**_attempt_kwargs())

    no_context = _FakeClient(
        replace(_Spec(), snapshot_after_attempt=False),
        context_exists=False,
    )
    outcome = await no_context.run_attempt(**_attempt_kwargs())
    assert outcome["checkpoint_status"] == "disabled"
    assert outcome["checkpoint_expires_at_ms"] is None
    assert outcome["context_ref"] == ""
    assert outcome["finalization_phase"] == "captured"


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        ({"attempt_index": 0}, "attempt_index"),
        ({"idempotency_key": ""}, "idempotency_key"),
        ({"prompt": ""}, "prompt"),
        ({"step_name": ""}, "step_name"),
        ({"validators": []}, "at least one validator"),
        (
            {
                "validators": [
                    ValidatorSpec("tests", ("verify",)),
                    ValidatorSpec("tests", ("verify",)),
                ]
            },
            "unique",
        ),
        ({"correlation": {"bad": object()}}, "JSON serializable"),
    ],
)
@pytest.mark.asyncio
async def test_attempt_request_fails_closed(changes: dict[str, Any], message: str) -> None:
    kwargs = _attempt_kwargs()
    kwargs.update(changes)

    with pytest.raises(ValueError, match=message):
        await _FakeClient(_Spec()).run_attempt(**kwargs)


def test_validator_spec_validates_its_boundary() -> None:
    assert ValidatorSpec.from_dict(
        {"name": "tests", "command": ["pytest", "-q"], "timeout_seconds": 12}
    ).to_dict() == {
        "name": "tests",
        "command": ["pytest", "-q"],
        "expected_returncode": 0,
        "timeout_seconds": 12,
    }
    with pytest.raises(ValueError, match="name"):
        ValidatorSpec("", ("pytest",))
    with pytest.raises(ValueError, match="command"):
        ValidatorSpec("tests", ())
    with pytest.raises(ValueError, match="timeout"):
        ValidatorSpec("tests", ("pytest",), timeout_seconds=0)
    with pytest.raises(ValueError, match="reserved"):
        ValidatorSpec(GIT_TREE_CHANGE_GATE_NAME, ("pytest",))


@pytest.mark.asyncio
async def test_common_repository_setup_and_agent_command_shapes() -> None:
    codex = _FakeClient(
        replace(_Spec(), model="gpt-test"),
        _agent_secret=object(),
        _github_secret=object(),
    )
    await CodingAgentSandboxClient._prepare_repository(codex)
    resumed = await CodingAgentSandboxClient._run_agent(codex, "repair", session_id="thread-1")
    fresh = await CodingAgentSandboxClient._run_agent(codex, "fix", session_id="")
    assert resumed.returncode == fresh.returncode == 0

    clone = next(call for call in codex.commands if call[0][:2] == ("git", "-c"))
    assert "clone" in clone[0]
    assert clone[1]["secrets"] == (codex._github_secret,)
    codex_calls = [call for call in codex.commands if call[0][:2] == ("codex", "exec")]
    assert "resume" in codex_calls[0][0]
    assert "--model" in codex_calls[0][0]
    assert "resume" not in codex_calls[1][0]
    assert codex_calls[0][1]["secrets"] == (codex._agent_secret,)

    claude = _FakeClient(
        replace(_Spec(), harness="claude-code", model="claude-test"),
        _agent_secret=object(),
    )
    await CodingAgentSandboxClient._run_agent(claude, "repair", session_id="session-1")
    claude_call = next(call for call in claude.commands if call[0][0] == "claude")
    assert claude_call[0][-3:] == ("--resume", "session-1", "repair")
    assert "--model" in claude_call[0]
    assert claude_call[1]["env"]["DISABLE_AUTOUPDATER"] == "1"

    unsupported = _FakeClient(replace(_Spec(), harness="unknown"))
    with pytest.raises(ValueError, match="unsupported coding-agent harness"):
        await CodingAgentSandboxClient._run_agent(unsupported, "fix", session_id="")


@pytest.mark.parametrize(
    ("wire_api", "provider_package"),
    [
        ("chat-completions", "@ai-sdk/openai-compatible"),
        ("responses", "@ai-sdk/openai"),
    ],
)
@pytest.mark.asyncio
async def test_common_opencode_command_and_secret_placeholder_contract(
    wire_api: str,
    provider_package: str,
) -> None:
    secret = object()
    client = _FakeClient(
        replace(
            _Spec(),
            harness="opencode",
            model="Qwen/Qwen3.6-35B-A3B-FP8",
            opencode_wire_api=wire_api,
        ),
        _agent_secret=secret,
    )

    await CodingAgentSandboxClient._run_agent(
        client,
        "repair the bug",
        session_id="open-session-1",
    )

    config_path = "/root/.config/archetype/opencode.json"
    config = json.loads(client.files[config_path])
    provider = config["provider"]["test-endpoint"]
    assert config["model"] == "test-endpoint/Qwen/Qwen3.6-35B-A3B-FP8"
    assert provider["npm"] == provider_package
    assert provider["options"] == {
        "baseURL": "https://endpoint.example/v1",
        "headers": {
            "Modal-Key": "{env:MODAL_ENDPOINT_TOKEN_ID}",
            "Modal-Secret": "{env:MODAL_ENDPOINT_TOKEN_SECRET}",
        },
    }
    assert "MODAL_ENDPOINT_TOKEN_SECRET" in client.files[config_path]

    command = next(call for call in client.commands if call[0][0] == "opencode")
    assert command[0] == (
        "opencode",
        "run",
        "--pure",
        "--format",
        "json",
        "--model",
        "test-endpoint/Qwen/Qwen3.6-35B-A3B-FP8",
        "--auto",
        "--session",
        "open-session-1",
        "repair the bug",
    )
    assert command[1]["secrets"] == (secret,)
    assert command[1]["env"] == {
        "NO_COLOR": "1",
        "OPENCODE_CONFIG": config_path,
        "OPENCODE_DISABLE_AUTOUPDATE": "1",
        "OPENCODE_DISABLE_PROJECT_CONFIG": "1",
    }


@pytest.mark.asyncio
async def test_common_recovery_capture_push_and_receipt_edges(monkeypatch) -> None:
    client = _FakeClient(
        replace(_Spec(), push=True),
        _github_secret=object(),
    )
    assert await CodingAgentSandboxClient._push_if_configured(client)
    recovery = await CodingAgentSandboxClient._capture_git_recovery(client, "attempt-1", "baseline")
    assert recovery == {
        "status": "/workspace/repo/.archetype-agent/recovery/attempt-1-status.txt",
        "patch": "/workspace/repo/.archetype-agent/recovery/attempt-1.patch",
        "bundle": "/workspace/repo/.archetype-agent/recovery/attempt-1.bundle",
    }
    assert recovery["status"] in client.files
    assert recovery["patch"] in client.files

    start = await CodingAgentSandboxClient._ensure_start_manifest(client)
    end, diff = await CodingAgentSandboxClient._capture_attempt_filesystem(client, "fix it", 2)
    assert start.endswith("/start.jsonl")
    assert end.endswith("/fix-it-2-end.jsonl")
    assert diff.endswith("/fix-it-2-diff.jsonl")
    assert any(call[0][:2] == ("python3", "-c") for call in client.commands)

    disabled = _FakeClient(replace(_Spec(), capture_filesystem_manifests=False))
    assert await CodingAgentSandboxClient._ensure_start_manifest(disabled) == ""
    assert await CodingAgentSandboxClient._capture_attempt_filesystem(disabled, "fix", 1) == (
        "",
        "",
    )

    corrupt_key = "corrupt-receipt"
    corrupt_path = client._receipt_path(corrupt_key)
    client.files[corrupt_path] = "{not-json"
    assert await CodingAgentSandboxClient._load_completed(client, corrupt_key) is None
    client.files[corrupt_path] = '{"accepted":true}'
    assert await CodingAgentSandboxClient._load_completed(client, corrupt_key) == {"accepted": True}

    corrupt_repository_path = client._repository_receipt_path(corrupt_key)
    client.files[corrupt_repository_path] = "{not-json"
    with pytest.raises(ValueError, match="refusing replay"):
        await CodingAgentSandboxClient._load_repository_receipt(client, corrupt_key)

    assert client._artifact_ref("checkpoint", "") == ""
    assert client._git_auth_args()
    assert client._git_secrets() == [client._github_secret]
    client._github_secret = None
    assert client._git_auth_args() == ()
    assert client._git_secrets() == []

    async def fail_event(*args: Any, **kwargs: Any) -> None:
        raise RuntimeError("telemetry unavailable")

    monkeypatch.setattr(client, "_emit_live_event", fail_event)
    await client._emit_live_event_safely("ignored")
    await CodingAgentSandboxClient._emit_live_event(client, "default-noop")
    assert CodingAgentSandboxClient._live_artifact_paths(client) == ("", "")


def test_common_identity_prompt_and_failure_helpers() -> None:
    codex = _FakeClient(_Spec())
    assert (
        codex._session_id('not-json\n{"type":"thread.started","thread_id":"thread-2"}')
        == "thread-2"
    )
    claude = _FakeClient(replace(_Spec(), harness="claude-code"))
    assert claude._session_id('{"session_id":"claude-1"}') == "claude-1"
    opencode = _FakeClient(replace(_Spec(), harness="opencode"))
    assert opencode._session_id('{"sessionID":"open-1"}') == "open-1"
    assert opencode._session_id('{"type":"message"}') == ""

    with pytest.raises(RuntimeError, match="probe failed with exit code 2"):
        codex._raise_for_result(CommandResult(("probe",), 2, "", "bad"), "probe")
    with pytest.raises(ValueError, match="attempt request values must be JSON serializable"):
        codex._request_fingerprint(
            prompt="fix",
            validators=(ValidatorSpec("tests", ("verify",)),),
            step_name="fix",
            attempt_index=1,
            previous_session_id="",
            previous_validator_details=({"bad": object()},),
            correlation={},
        )

    assert "Validator evidence" in codex._repair_prompt(
        "fix", ({"name": "tests", "passed": False},)
    )
    assert codex._safe_step(" !!! ") == "step"
    assert codex._subject("   ") == "complete task gate"
    assert codex._failure_summary(({"name": "tests", "passed": True},)) == "unknown failure"


@dataclass
class _Session:
    identity: str
    close_error: BaseException | None = None
    close_calls: int = 0

    @property
    def sandbox_id(self) -> str:
        return self.identity

    async def run_attempt(self, **kwargs: Any) -> dict[str, Any]:
        return kwargs

    async def close(self) -> None:
        self.close_calls += 1
        if self.close_error is not None:
            raise self.close_error


@dataclass
class _Backend:
    name: str = "fake"
    created: list[_Session] = field(default_factory=list)
    authenticated: list[Any] = field(default_factory=list)

    async def create(self, spec: Any) -> _Session:
        session = _Session(str(spec))
        self.created.append(session)
        return session

    async def restore(self, spec: Any, checkpoint_ref: str) -> _Session:
        return await self.create(f"{spec}:restore:{checkpoint_ref}")

    async def resume(self, spec: Any, checkpoint_ref: str) -> _Session:
        return await self.create(f"{spec}:resume:{checkpoint_ref}")

    async def authenticate(self, spec: Any) -> None:
        self.authenticated.append(spec)


@pytest.mark.asyncio
async def test_sandbox_service_owns_only_live_handle_lifetime() -> None:
    backend = _Backend()
    service = SandboxService([backend])

    created = await service.create("fake", "one")
    restored = await service.restore("fake", "two", "cp-1")
    resumed = await service.resume("fake", "three", "cp-2")
    await service.authenticate("fake", "auth")

    assert service.session(created.sandbox_id) is created
    assert service.session(restored.sandbox_id) is restored
    assert service.session(resumed.sandbox_id) is resumed
    assert backend.authenticated == ["auth"]
    await service.close(created.sandbox_id)
    assert created.close_calls == 1
    assert service.session(created.sandbox_id) is None
    await service.shutdown()
    assert restored.close_calls == resumed.close_calls == 1
    with pytest.raises(RuntimeError, match="shutting down"):
        await service.create("fake", "late")


@pytest.mark.asyncio
async def test_sandbox_service_rejects_unknown_duplicate_and_reports_shutdown_failures() -> None:
    service = SandboxService()
    with pytest.raises(ValueError, match="unknown sandbox provider"):
        await service.create("missing", object())
    with pytest.raises(ValueError, match="must not be empty"):
        service.register_backend(_Backend(name=" "))

    backend = _Backend()
    service.register_backend(backend)
    with pytest.raises(ValueError, match="already registered"):
        service.register_backend(backend)
    first = await service.create("fake", "same")
    with pytest.raises(RuntimeError, match="duplicate live sandbox id"):
        await service.create("fake", "same")
    assert backend.created[-1].close_calls == 1
    first.close_error = RuntimeError("close failed")
    with pytest.raises(ExceptionGroup, match="failed to close 1") as captured:
        await service.shutdown()
    assert len(captured.value.exceptions) == 1
    assert "close failed" in str(captured.value.exceptions[0])


@pytest.mark.asyncio
async def test_sandbox_service_preserves_cancellation_failure_identity() -> None:
    backend = _Backend()
    service = SandboxService([backend])
    session = await service.create("fake", "cancelled")
    session.close_error = asyncio.CancelledError("close cancelled")

    with pytest.raises(BaseExceptionGroup, match="failed to close 1") as captured:
        await service.shutdown()

    assert len(captured.value.exceptions) == 1
    assert isinstance(captured.value.exceptions[0], asyncio.CancelledError)


@pytest.mark.asyncio
async def test_sandbox_service_closes_session_that_loses_shutdown_race() -> None:
    service = SandboxService()
    service._accepting = False
    session = _Session("late")
    with pytest.raises(RuntimeError, match="shutting down"):
        await service._retain(session)
    assert session.close_calls == 1
    await service.close("missing")


def test_common_kernel_has_no_provider_sdk_or_adapter_imports() -> None:
    source_path = Path(__file__).parents[2] / "src/archetype/app/sandboxes/common.py"
    tree = ast.parse(source_path.read_text())
    imported = {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, (ast.Import, ast.ImportFrom))
        for alias in node.names
    }
    assert not {
        name
        for name in imported
        if name == "modal"
        or name.startswith("modal.")
        or "apple_container" in name
        or name == "docker"
        or name.startswith("docker.")
    }
