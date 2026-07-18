# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import ast
from collections.abc import Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest

from archetype.app.sandboxes import (
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
    ValidationEvidence,
)


@dataclass(frozen=True)
class _Spec:
    repo_url: str = "https://example.test/repo.git"
    branch: str = "agent/fix"
    base_ref: str = "main"
    harness: str = "codex"
    model: str = ""
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
    validator_returncode: int = 0
    tree_changed: bool = True
    head: str = "baseline"
    checkpoint_ref: str = "fake-checkpoint://checkpoint-1"
    checkpoint_error: BaseException | None = None
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
        if args[:2] == ("git", "commit"):
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
        self, prepared: PreparedAttempt, previous_session_id: str
    ) -> AgentExecution:
        self.phases.append(AttemptPhase.EXECUTION)
        return await super()._execution_phase(prepared, previous_session_id)

    async def _validation_phase(self, validators: Sequence[ValidatorSpec]) -> ValidationEvidence:
        self.phases.append(AttemptPhase.VALIDATION)
        return await super()._validation_phase(validators)

    async def _repository_finalization_phase(
        self,
        prepared: PreparedAttempt,
        execution: AgentExecution,
        validation: ValidationEvidence,
    ) -> RepositoryFinalization:
        self.phases.append(AttemptPhase.REPOSITORY_FINALIZATION)
        return await super()._repository_finalization_phase(prepared, execution, validation)

    async def _evidence_phase(
        self,
        prepared: PreparedAttempt,
        execution: AgentExecution,
        repository: RepositoryFinalization,
    ) -> EvidenceCapture:
        self.phases.append(AttemptPhase.EVIDENCE)
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


def _attempt_kwargs() -> dict[str, Any]:
    return {
        "prompt": "Fix the reported bug",
        "validators": [ValidatorSpec("tests", ("verify",), timeout_seconds=10)],
        "step_name": "fix",
        "attempt_index": 1,
        "idempotency_key": "world/run/attempt-1",
        "correlation": {"world_id": "world", "run_id": "run"},
    }


@pytest.mark.asyncio
async def test_attempt_runs_six_phases_and_returns_checkpoint_qualified_handoff() -> None:
    client = _FakeClient(_Spec())

    outcome = await client.run_attempt(**_attempt_kwargs())

    assert client.phases == list(AttemptPhase)
    assert outcome["accepted"] is True
    assert outcome["status"] == "accepted"
    assert outcome["sha"] == "committed"
    assert outcome["checkpoint_status"] == "ready"
    assert outcome["finalization_phase"] == "checkpointed"
    assert outcome["agent_session_id"] == "thread-1"
    assert outcome["correlation"] == {"world_id": "world", "run_id": "run"}
    assert outcome["git_bundle_ref"].startswith("fake-checkpoint://checkpoint-1#")
    assert outcome["live_events_ref"].startswith("fake-sandbox://sandbox-1/")
    assert any(path.endswith(".json") for path in client.files)
    assert [event for event, _ in client.events][-3:] == [
        "artifact_handoff_started",
        "artifact_handoff_finished",
        "attempt_completed",
    ]
    validator_call = next(call for call in client.commands if call[0][0] == "verify")
    assert validator_call[1]["secrets"] == ()
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

    with pytest.raises(ValueError, match="reused with a different"):
        await client.run_attempt(**changed)

    assert len(client.agent_calls) == 1


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
async def test_checkpoint_failure_is_evidence_not_lost_attempt() -> None:
    client = _FakeClient(_Spec(), checkpoint_error=RuntimeError("snapshot unavailable"))

    outcome = await client.run_attempt(**_attempt_kwargs())

    assert outcome["accepted"] is True
    assert outcome["checkpoint_status"] == "failed"
    assert outcome["checkpoint_restorable"] is False
    assert outcome["finalization_phase"] == "captured"
    assert "snapshot unavailable" in outcome["finalization_error"]
    assert outcome["git_bundle_ref"].startswith("fake-sandbox://sandbox-1/")
    assert outcome["friction"][-1]["finding"] == "Provider checkpoint failed"


@pytest.mark.asyncio
async def test_transport_failure_stops_before_authoritative_side_effects() -> None:
    client = _FakeClient(_Spec(), agent_error=TimeoutError("transport timed out"))

    with pytest.raises(TimeoutError, match="transport timed out"):
        await client.run_attempt(**_attempt_kwargs())

    assert client.phases == [AttemptPhase.EXECUTION]
    assert not any(call[0][0] == "verify" for call in client.commands)
    assert client.events[-1][0] == "agent_transport_failed"


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        ({"attempt_index": 0}, "attempt_index"),
        ({"idempotency_key": ""}, "idempotency_key"),
        ({"prompt": ""}, "prompt"),
        ({"step_name": ""}, "step_name"),
        ({"validators": []}, "at least one validator"),
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
    with pytest.raises(RuntimeError, match="failed to close 1") as captured:
        await service.shutdown()
    assert any("close failed" in note for note in captured.value.__notes__)


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
