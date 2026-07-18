# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Deterministic provider-neutral sandbox-kernel capability scenario."""

from __future__ import annotations

import asyncio
import hashlib
import time
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any, cast

from archetype.app.missions import (
    AttemptRecoveryAction,
    FencedExecutionAuthorization,
    attempt_invocation_fingerprint,
)
from archetype.app.sandboxes import (
    AgentHarness,
    CodingAgentSandboxClient,
    CommandResult,
    ValidatorSpec,
)
from evals.graders import exact_match, state_check
from evals.harness import EvalHarness
from evals.types import GraderResult

SUITE = "capability"


@dataclass(frozen=True)
class _Spec:
    repo_url: str = "https://example.test/repo.git"
    branch: str = "agent/eval"
    base_ref: str = "main"
    harness: AgentHarness = cast(AgentHarness, "codex")
    model: str = ""
    workspace: str = "/workspace/repo"
    agent_timeout_seconds: int = 60
    snapshot_timeout_seconds: int = 30
    snapshot_ttl_seconds: int | None = 60
    snapshot_after_attempt: bool = True
    capture_filesystem_manifests: bool = True
    push: bool = False
    git_author_name: str = "Eval Agent"
    git_author_email: str = "eval@example.test"


@dataclass
class _KernelClient(CodingAgentSandboxClient[_Spec]):
    spec: _Spec
    _sandbox: object = field(default_factory=object)
    _agent_secret: object | None = None
    files: dict[str, str] = field(default_factory=dict)
    events: list[str] = field(default_factory=list)
    agent_calls: int = 0
    head: str = "baseline"

    @property
    def sandbox_id(self) -> str:
        return "eval-sandbox"

    async def close(self) -> None:
        self._closed = True

    async def _exec(
        self,
        *args: str,
        workdir: str | None = None,
        timeout: int | None = None,
        secrets: Sequence[Any] = (),
        env: dict[str, str] | None = None,
    ) -> CommandResult:
        del workdir, timeout, secrets, env
        if args[0] == "cat":
            value = self.files.get(args[1])
            return CommandResult(args, 0 if value is not None else 1, value or "", "")
        if args[:3] == ("git", "rev-parse", "HEAD"):
            return CommandResult(args, 0, f"{self.head}\n", "")
        if args[:3] == ("git", "status", "--porcelain"):
            return CommandResult(args, 0, " M fix.py\n", "")
        if args[0] == "git" and "commit" in args:
            self.head = "verified-commit"
        return CommandResult(args, 0, "", "")

    async def _write_text(self, path: str, value: str) -> None:
        self.files[path] = value

    async def _snapshot_if_configured(self, checkpoint_key: str = "") -> str:
        assert checkpoint_key
        return "eval-checkpoint://one"

    def _checkpoint_provider(self) -> str:
        return "eval"

    def _sandbox_uri(self, path: str) -> str:
        return f"eval-sandbox://{self.sandbox_id}{path}"

    async def _run_agent(self, prompt: str, *, session_id: str) -> CommandResult:
        assert prompt and not session_id
        self.agent_calls += 1
        # A provider process exit is deliberately not the acceptance authority.
        return CommandResult(
            ("codex",),
            7,
            '{"type":"thread.started","thread_id":"eval-thread"}\n',
            "provider process exited after editing",
        )

    async def _capture_git_recovery(self, attempt_id: str, baseline: str) -> dict[str, str]:
        assert attempt_id and baseline
        root = f"{self.spec.workspace}/recovery/{attempt_id}"
        return {
            "status": f"{root}.status",
            "patch": f"{root}.patch",
            "bundle": f"{root}.bundle",
        }

    async def _ensure_start_manifest(self) -> str:
        return f"{self.spec.workspace}/filesystem/start.jsonl"

    async def _capture_attempt_filesystem(
        self, step_name: str, attempt_index: int
    ) -> tuple[str, str]:
        root = f"{self.spec.workspace}/filesystem/{step_name}-{attempt_index}"
        return f"{root}.end", f"{root}.diff"

    async def _emit_live_event(self, event_type: str, **details: Any) -> None:
        del details
        self.events.append(event_type)


def task_sandbox_kernel_phase_contract() -> list[GraderResult]:
    """Prove phase ordering, validator authority, handoff, and replay."""

    return asyncio.run(_task_sandbox_kernel_phase_contract())


async def _task_sandbox_kernel_phase_contract() -> list[GraderResult]:
    client = _KernelClient(_Spec())
    idempotency_key = "eval-attempt"
    correlation = {
        "world_id": "eval-world",
        "run_id": "eval-run",
        "entity_id": "7",
        "step_index": 0,
    }
    validators = [ValidatorSpec("tests", ("verify",))]

    async def acknowledge_provider(
        provider_session_id: str,
        provider_request_id: str,
    ) -> None:
        del provider_session_id, provider_request_id

    async def authorize_execution(
        authorization: FencedExecutionAuthorization,
    ) -> None:
        del authorization

    request = {
        "prompt": "Repair the bug",
        "validators": validators,
        "step_name": "fix",
        "attempt_index": 1,
        "idempotency_key": idempotency_key,
        "authorization": FencedExecutionAuthorization(
            action=AttemptRecoveryAction.EXECUTE,
            claim_key="eval-claim",
            world_id="eval-world",
            run_id="eval-run",
            mission_id="eval-world:eval-run:7",
            task_id="eval-task",
            attempt_id=hashlib.sha256(idempotency_key.encode()).hexdigest(),
            idempotency_key=idempotency_key,
            request_fingerprint="eval-request-fingerprint",
            sandbox_request_fingerprint=attempt_invocation_fingerprint(
                prompt="Repair the bug",
                validators=tuple(validator.to_dict() for validator in validators),
                step_name="fix",
                attempt_index=1,
                previous_session_id="",
                previous_validator_details=(),
                correlation=correlation,
            ),
            execution_nonce="eval-execution-nonce",
            claimant="eval-worker",
            fence_epoch=1,
            lease_expires_at=time.time() + 60,
        ),
        "authorize_execution": authorize_execution,
        "acknowledge_provider": acknowledge_provider,
        "correlation": correlation,
    }
    first = await client.run_attempt(**request)
    second = await client.run_attempt(**request)
    phase_events = [
        event
        for event in client.events
        if event
        in {
            "agent_started",
            "validator_started",
            "commit_started",
            "evidence_capture_started",
            "checkpoint_started",
            "artifact_handoff_started",
        }
    ]
    return [
        exact_match(
            phase_events,
            [
                "agent_started",
                "validator_started",
                "commit_started",
                "evidence_capture_started",
                "checkpoint_started",
                "artifact_handoff_started",
            ],
            name="six_phase_order",
        ),
        state_check(
            {
                "validator_is_authority": first["accepted"] is True,
                "nonzero_agent_retained": first["agent_returncode"] == 7,
                "checkpoint_qualified": first["finalization_phase"] == "checkpointed",
                "artifact_declared": first["git_bundle_ref"].startswith("eval-checkpoint://one#"),
                "correlated": first["correlation"]["world_id"] == "eval-world",
                "sandbox_receipt_replayed": second == first and client.agent_calls == 1,
            },
            name="sandbox_kernel_outcome",
        ),
    ]


def register(harness: EvalHarness) -> None:
    harness.add(
        "sandbox_attempt_phase_contract",
        suite=SUITE,
        fn=task_sandbox_kernel_phase_contract,
        desc="Provider-neutral six-phase attempt, validator authority, checkpoint handoff, replay",
    )
