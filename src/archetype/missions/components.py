# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Persistent ECS state for the Agent Missions software factory."""

from __future__ import annotations

from pydantic import field_validator, model_validator

from archetype.core.component import Component
from archetype.missions.contracts import RepositoryPublicationPolicy
from archetype.missions.sandboxes.contracts import SandboxStatus
from archetype.missions.transitions import (
    AgentExecutionStatus,
    CriticConclusion,
    CriticExecutionStatus,
    MissionStatus,
    TaskStatus,
)


class Mission(Component):
    """Immutable repository identity for one submitted mission."""

    name: str = "agent-mission"
    repository: str = ""
    branch: str = ""
    base_ref: str = "main"


class MissionState(Component):
    """Current mission rollup derived from related task entities."""

    status: str = MissionStatus.RUNNING.value
    reason: str = ""

    @field_validator("status")
    @classmethod
    def _valid_status(cls, value: str) -> str:
        return MissionStatus(value).value


class Task(Component):
    """Immutable task name and atomic goal prompt."""

    name: str = ""
    prompt: str = ""


class TaskWorkspace(Component):
    """Repository coordinates shared by every dispatch of one task."""

    repository: str = ""
    branch: str = ""
    base_ref: str = "main"


class TaskPolicy(Component):
    """Dispatch budget and repository publication policy."""

    max_dispatches: int = 3
    publication_policy: str = RepositoryPublicationPolicy.COMMIT_AND_PUSH.value

    @field_validator("max_dispatches")
    @classmethod
    def _positive_dispatches(cls, value: int) -> int:
        if value < 1:
            raise ValueError("max_dispatches must be positive")
        return value

    @field_validator("publication_policy")
    @classmethod
    def _valid_publication_policy(cls, value: str) -> str:
        return RepositoryPublicationPolicy(value).value


class TaskCriticPolicy(Component):
    """Immutable independent-review policy materialized with one task."""

    policy_id: str = ""
    version: str = ""
    digest: str = ""
    perspective: str = ""
    information_view: str = ""
    driver: str = ""
    model: str = ""
    sampling: str = ""
    max_reviews: int = 1
    timeout_seconds: int = 2700
    output_schema_version: int = 1
    max_output_chars: int = 16_000

    @model_validator(mode="after")
    def _valid_policy(self) -> TaskCriticPolicy:
        required = (
            self.policy_id,
            self.version,
            self.digest,
            self.perspective,
            self.information_view,
            self.driver,
            self.sampling,
        )
        if any(not value.strip() for value in required):
            raise ValueError("critic policy identity fields must not be empty")
        if self.max_reviews < 1 or self.timeout_seconds < 1:
            raise ValueError("critic review budgets must be positive")
        if self.output_schema_version < 1 or self.max_output_chars < 1:
            raise ValueError("critic output schema and bound must be positive")
        if self.information_view != "task-diff-validators":
            raise ValueError("unsupported critic information_view")
        if self.sampling != "provider-default":
            raise ValueError("unsupported critic sampling policy")
        return self


class TaskState(Component):
    """Processor-owned task decision state."""

    status: str = TaskStatus.PENDING.value
    reason: str = ""

    @field_validator("status")
    @classmethod
    def _valid_status(cls, value: str) -> str:
        return TaskStatus(value).value


class TaskDispatch(Component):
    """Latest committed permission to execute a task; history retains prior intent."""

    dispatch_id: str = ""
    sequence: int = 0

    @model_validator(mode="after")
    def _valid_identity(self) -> TaskDispatch:
        if self.sequence < 0:
            raise ValueError("dispatch sequence must not be negative")
        if self.sequence == 0 and self.dispatch_id:
            raise ValueError("an undispatched task cannot have a dispatch identity")
        if self.sequence > 0 and not self.dispatch_id:
            raise ValueError("a dispatched task requires a dispatch identity")
        return self


class TaskValidator(Component):
    """One executable repository guard materialized as its own entity."""

    name: str = ""
    command: list[str] = []
    expected_returncode: int = 0
    timeout_seconds: int = 900

    @model_validator(mode="after")
    def _valid_spec(self) -> TaskValidator:
        if not self.name.strip():
            raise ValueError("validator name must not be empty")
        if not self.command or any(not argument for argument in self.command):
            raise ValueError("validator command must contain non-empty arguments")
        if self.timeout_seconds < 1:
            raise ValueError("validator timeout_seconds must be positive")
        return self


class Sandbox(Component):
    """Observed lifecycle of a filesystem and process container."""

    provider: str = ""
    sandbox_id: str = ""
    environment: str = ""
    worktree: str = ""
    status: str = SandboxStatus.PROVISIONING.value
    error: str = ""

    @field_validator("status")
    @classmethod
    def _valid_status(cls, value: str) -> str:
        return SandboxStatus(value).value


class AgentExecution(Component):
    """Factual process observation for one task dispatch."""

    task_id: int = 0
    dispatch_id: str = ""
    dispatch_sequence: int = 0
    status: str = AgentExecutionStatus.STARTING.value
    sandbox_id: str = ""
    agent_session_id: str = ""
    agent_returncode: int = -1
    agent_stdout: str = ""
    agent_stderr: str = ""
    trace_uri: str = ""
    redaction_policy_id: str = ""
    starting_revision: str = ""
    final_revision: str = ""
    error: str = ""

    @field_validator("status")
    @classmethod
    def _valid_status(cls, value: str) -> str:
        return AgentExecutionStatus(value).value


class AuthorActivityObservation(Component):
    """Complete committed fact bundle for one durable author result."""

    activity_id: str = ""
    task_id: int = 0
    dispatch_sequence: int = 0
    result_ref: str = ""
    result_digest: str = ""
    fact_bundle_digest: str = ""
    execution_id: int = 0
    validation_count: int = 0
    commit_count: int = 0
    friction_count: int = 0
    redaction_policy_id: str = ""

    @model_validator(mode="after")
    def _valid_observation(self) -> AuthorActivityObservation:
        required = (
            self.activity_id,
            self.result_ref,
            self.result_digest,
            self.fact_bundle_digest,
            self.redaction_policy_id,
        )
        if any(not value.strip() for value in required):
            raise ValueError("author activity observation identity cannot be empty")
        if self.task_id < 1 or self.dispatch_sequence < 1 or self.execution_id < 1:
            raise ValueError(
                "author activity observation requires dispatch and execution identities"
            )
        if min(self.validation_count, self.commit_count, self.friction_count) < 0:
            raise ValueError("author activity observation counts cannot be negative")
        return self


class CompleteAuthorActivityObservation(Component):
    """Complete v2 fact bundle for one durable author result."""

    schema_version: int = 2
    activity_id: str = ""
    task_id: int = 0
    dispatch_sequence: int = 0
    result_ref: str = ""
    result_digest: str = ""
    fact_bundle_digest: str = ""
    execution_id: int = 0
    validation_count: int = 0
    commit_count: int = 0
    friction_count: int = 0
    checkpoint_count: int = 0
    checkpoint_entity_id: int = 0
    sandbox_entity_id: int = 0
    sandbox_bound: bool = False
    candidate_entity_id: int = 0
    candidate_count: int = 0
    relation_count: int = 0
    redaction_policy_id: str = ""

    @model_validator(mode="after")
    def _valid_observation(self) -> CompleteAuthorActivityObservation:
        required = (
            self.activity_id,
            self.result_ref,
            self.result_digest,
            self.fact_bundle_digest,
            self.redaction_policy_id,
        )
        if any(not value.strip() for value in required):
            raise ValueError("complete author activity observation identity cannot be empty")
        if self.task_id < 1 or self.dispatch_sequence < 1 or self.execution_id < 1:
            raise ValueError(
                "complete author activity observation requires dispatch and execution identities"
            )
        if min(self.validation_count, self.commit_count, self.friction_count) < 0:
            raise ValueError("complete author activity observation counts cannot be negative")
        if self.checkpoint_count not in {0, 1}:
            raise ValueError("complete author activity observation has at most one checkpoint")
        if self.checkpoint_count != int(self.checkpoint_entity_id > 0):
            raise ValueError("complete author activity checkpoint identity is inconsistent")
        if self.schema_version != 2:
            raise ValueError("complete author activity observation requires schema version 2")
        if self.candidate_count not in {0, 1}:
            raise ValueError(
                "complete author activity observation candidate count must be zero or one"
            )
        if self.candidate_count != int(self.candidate_entity_id > 0):
            raise ValueError(
                "complete author activity observation candidate identity is inconsistent"
            )
        if self.sandbox_entity_id < 1:
            raise ValueError("complete author activity observation requires a sandbox")
        if self.relation_count < 2:
            raise ValueError("complete author activity observation requires provenance")
        return self


class ValidationResult(Component):
    """One validator observation bound to exact execution and revision identity."""

    task_id: int = 0
    validator_id: int = 0
    execution_id: int = 0
    dispatch_id: str = ""
    dispatch_sequence: int = 0
    revision: str = ""
    expected_returncode: int = 0
    actual_returncode: int = 0
    stdout: str = ""
    stderr: str = ""

    @property
    def passed(self) -> bool:
        return self.actual_returncode == self.expected_returncode


class Commit(Component):
    """One Git commit observed during a task dispatch."""

    task_id: int = 0
    execution_id: int = 0
    dispatch_id: str = ""
    sha: str = ""
    message: str = ""
    branch: str = ""
    pushed: bool = False
    final_revision: bool = False


class Candidate(Component):
    """Immutable authored-green exact-head review subject."""

    candidate_id: str = ""
    mission_id: int = 0
    task_id: int = 0
    dispatch_id: str = ""
    dispatch_sequence: int = 0
    author_execution_id: int = 0
    author_sandbox_id: str = ""
    repository: str = ""
    branch: str = ""
    base_ref: str = ""
    base_revision: str = ""
    head_revision: str = ""
    diff_digest: str = ""
    validator_bundle_digest: str = ""
    policy_digest: str = ""
    candidate_digest: str = ""
    created_at_ms: int = 0

    @model_validator(mode="after")
    def _valid_candidate(self) -> Candidate:
        required = (
            self.candidate_id,
            self.dispatch_id,
            self.author_sandbox_id,
            self.repository,
            self.branch,
            self.base_ref,
            self.base_revision,
            self.head_revision,
            self.diff_digest,
            self.validator_bundle_digest,
            self.policy_digest,
            self.candidate_digest,
        )
        if any(not value.strip() for value in required):
            raise ValueError("candidate identity fields must not be empty")
        if self.dispatch_sequence < 1 or self.created_at_ms < 0:
            raise ValueError("candidate sequence must be positive and time non-negative")
        return self


class CriticExecution(Component):
    """Factual lifecycle and bounded output for one review attempt."""

    candidate_entity_id: int = 0
    candidate_id: str = ""
    review_id: str = ""
    attempt: int = 0
    status: str = CriticExecutionStatus.STARTING.value
    sandbox_id: str = ""
    driver: str = ""
    model: str = ""
    started_at_ms: int = 0
    ended_at_ms: int = 0
    provision_started_at_ms: int = 0
    sandbox_ready_at_ms: int = 0
    base_hydrated_at_ms: int = 0
    candidate_published_at_ms: int = 0
    head_ready_at_ms: int = 0
    critic_started_at_ms: int = 0
    receipt_staged_at_ms: int = 0
    raw_output: str = ""
    trace_uri: str = ""
    redaction_policy_id: str = ""
    error: str = ""

    @field_validator("status")
    @classmethod
    def _valid_status(cls, value: str) -> str:
        return CriticExecutionStatus(value).value


class CriticFinding(Component):
    """One typed, provider-neutral critic finding."""

    candidate_entity_id: int = 0
    critic_execution_id: int = 0
    finding_id: str = ""
    severity: str = ""
    category: str = ""
    confidence: float = 0.0
    title: str = ""
    detail: str = ""
    evidence_location: str = ""
    reproduction: str = ""

    @model_validator(mode="after")
    def _valid_finding(self) -> CriticFinding:
        if self.severity not in {"blocking", "advisory"}:
            raise ValueError("critic finding severity must be blocking or advisory")
        if not 0.0 <= self.confidence <= 1.0:
            raise ValueError("critic finding confidence must be between zero and one")
        if not self.finding_id or not self.category or not self.title or not self.detail:
            raise ValueError("critic finding identity and description must not be empty")
        return self


class CriticReceipt(Component):
    """Verified exact-subject receipt consumed by task transition processors."""

    candidate_entity_id: int = 0
    critic_execution_id: int = 0
    critic_sandbox_id: str = ""
    review_id: str = ""
    conclusion: str = CriticConclusion.APPROVED.value
    candidate_digest: str = ""
    policy_digest: str = ""
    evidence_digest: str = ""
    reviewed_base_revision: str = ""
    reviewed_head_revision: str = ""
    reviewed_diff_digest: str = ""
    validator_bundle_digest: str = ""
    reviewed_scope: str = ""
    finding_count: int = 0
    blocking_count: int = 0
    output_schema_version: int = 1
    completed_at_ms: int = 0

    @field_validator("conclusion")
    @classmethod
    def _valid_conclusion(cls, value: str) -> str:
        return CriticConclusion(value).value


class Checkpoint(Component):
    """Optional provider-native recovery point for a sandbox."""

    task_id: int = 0
    execution_id: int = 0
    dispatch_id: str = ""
    provider: str = ""
    checkpoint_id: str = ""
    uri: str = ""
    created_at_ms: int = 0
    environment: str = ""
    source_sandbox_id: str = ""
    owner_id: str = ""
    locality: str = ""
    expires_at_ms: int = 0
    integrity: str = ""
    restorable: bool = False
    error: str = ""


class FilesystemManifest(Component):
    """Optional content-addressed observation of sandbox filesystem state."""

    task_id: int = 0
    execution_id: int = 0
    dispatch_id: str = ""
    digest: str = ""
    uri: str = ""
    entry_count: int = 0


class FrictionLog(Component):
    """One timestamped, queryable obstacle rather than an embedded JSON list."""

    task_id: int = 0
    execution_id: int = 0
    dispatch_id: str = ""
    kind: str = ""
    message: str = ""


class AgentArtifact(Component):
    """Content-addressed reference to a large output produced by an execution."""

    task_id: int = 0
    execution_id: int = 0
    dispatch_id: str = ""
    artifact_id: str = ""
    logical_path: str = ""
    digest: str = ""
    uri: str = ""
    media_type: str = "application/octet-stream"
    size_bytes: int = 0


TASK_COMPONENTS = (
    Task,
    TaskWorkspace,
    TaskPolicy,
    TaskCriticPolicy,
    TaskState,
    TaskDispatch,
)

OUTPUT_COMPONENTS = (
    AuthorActivityObservation,
    CompleteAuthorActivityObservation,
    ValidationResult,
    Commit,
    Candidate,
    CriticExecution,
    CriticFinding,
    CriticReceipt,
    Checkpoint,
    FilesystemManifest,
    FrictionLog,
    AgentArtifact,
)

MISSION_COMPONENTS = (
    Mission,
    MissionState,
    *TASK_COMPONENTS,
    TaskValidator,
    Sandbox,
    AgentExecution,
    *OUTPUT_COMPONENTS,
)
