# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Provider-neutral values for isolated coding-agent execution."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict, dataclass
from enum import StrEnum
from typing import Any, Literal, Protocol

AgentHarness = Literal["codex", "claude-code", "opencode"]
AgentAuthMode = Literal["api-key", "oauth"]
OpenCodeWireAPI = Literal["chat-completions", "responses"]


class AttemptPhase(StrEnum):
    """Stable phase names for one sandbox attempt."""

    EXECUTION = "execution"
    VALIDATION = "validation"
    REPOSITORY_FINALIZATION = "repository_finalization"
    EVIDENCE = "evidence"
    CHECKPOINT = "checkpoint"
    ARTIFACT_HANDOFF = "artifact_handoff"


@dataclass(frozen=True)
class ValidatorSpec:
    """One authoritative command that must return the expected exit code."""

    name: str
    command: tuple[str, ...]
    expected_returncode: int = 0
    timeout_seconds: int = 900

    def __post_init__(self) -> None:
        if not self.name.strip():
            raise ValueError("validator name must not be empty")
        if not self.command:
            raise ValueError("validator command must not be empty")
        if self.timeout_seconds < 1:
            raise ValueError("validator timeout_seconds must be at least 1")

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> ValidatorSpec:
        return cls(
            name=str(value["name"]),
            command=tuple(str(part) for part in value["command"]),
            expected_returncode=int(value.get("expected_returncode", 0)),
            timeout_seconds=int(value.get("timeout_seconds", 900)),
        )

    def to_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["command"] = list(self.command)
        return value


class CodingAgentSandboxSpec(Protocol):
    """Structural fields shared by provider-specific sandbox specifications."""

    repo_url: str
    branch: str
    base_ref: str
    harness: AgentHarness
    model: str
    opencode_base_url: str
    opencode_provider_id: str
    opencode_wire_api: OpenCodeWireAPI
    opencode_header_env: Mapping[str, str]
    workspace: str
    agent_timeout_seconds: int
    snapshot_timeout_seconds: int
    snapshot_ttl_seconds: int | None
    snapshot_after_attempt: bool
    capture_filesystem_manifests: bool
    push: bool
    git_author_name: str
    git_author_email: str


@dataclass(frozen=True)
class CommandResult:
    """Captured result from one command inside the sandbox."""

    argv: tuple[str, ...]
    returncode: int
    stdout: str
    stderr: str


@dataclass(frozen=True)
class PreparedAttempt:
    """Validated request and repository baseline for the phase pipeline."""

    attempt_id: str
    request_fingerprint: str
    idempotency_key: str
    attempt_index: int
    step_name: str
    prompt: str
    agent_prompt: str
    correlation: dict[str, Any]
    baseline_sha: str
    trace_dir: str
    trace_path: str
    trace_stderr_path: str
    live_status_path: str
    live_events_path: str
    filesystem_start_path: str


@dataclass(frozen=True)
class AgentExecution:
    """Untrusted agent-process output retained independently of acceptance."""

    result: CommandResult
    session_id: str
    metadata: dict[str, Any]
    friction: tuple[dict[str, Any], ...] = ()


@dataclass(frozen=True)
class ValidationEvidence:
    """Authoritative validator results."""

    details: tuple[dict[str, Any], ...]

    @property
    def accepted(self) -> bool:
        return bool(self.details) and all(bool(detail.get("passed")) for detail in self.details)


@dataclass(frozen=True)
class RepositoryFinalization:
    """Commit/push result after validator acceptance."""

    accepted: bool
    details: tuple[dict[str, Any], ...]
    commit_sha: str
    message: str
    pushed: bool
    friction: tuple[dict[str, Any], ...] = ()


@dataclass(frozen=True)
class RepositoryPhaseReceipt:
    """Replay-safe state captured immediately after repository finalization."""

    request_fingerprint: str
    prepared: PreparedAttempt
    execution: AgentExecution
    repository: RepositoryFinalization


@dataclass(frozen=True)
class EvidenceCapture:
    """Portable paths captured before a provider checkpoint."""

    attempt_manifest_path: str
    trace_path: str
    trace_stderr_path: str
    live_status_path: str
    live_events_path: str
    filesystem_start_path: str
    filesystem_end_path: str
    filesystem_diff_path: str
    git_status_path: str
    git_patch_path: str
    git_bundle_path: str
    context_path: str


@dataclass(frozen=True)
class CheckpointCapture:
    """Provider checkpoint result; failure remains queryable evidence."""

    ref: str
    status: Literal["ready", "failed", "disabled"]
    provider: str
    restorable: bool
    error: str
    created_at_ms: int
    expires_at_ms: int
    friction: tuple[dict[str, Any], ...] = ()


@dataclass(frozen=True)
class ArtifactHandoff:
    """Declared source references for authoritative publication by the caller."""

    refs: dict[str, str]
    finalization_phase: Literal["captured", "checkpointed"]
    finalization_error: str
