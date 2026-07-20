# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Persistent ECS state for the Agent Missions software factory."""

from __future__ import annotations

from pydantic import field_validator, model_validator

from archetype.core.component import Component
from archetype.missions.contracts import RepositoryPublicationPolicy
from archetype.missions.sandboxes.contracts import SandboxStatus
from archetype.missions.transitions import AgentExecutionStatus, MissionStatus, TaskStatus


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
    starting_revision: str = ""
    final_revision: str = ""
    error: str = ""

    @field_validator("status")
    @classmethod
    def _valid_status(cls, value: str) -> str:
        return AgentExecutionStatus(value).value


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


class Checkpoint(Component):
    """Optional provider-native recovery point for a sandbox."""

    task_id: int = 0
    execution_id: int = 0
    dispatch_id: str = ""
    provider: str = ""
    checkpoint_id: str = ""
    uri: str = ""
    created_at_ms: int = 0
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
    digest: str = ""
    uri: str = ""
    media_type: str = "application/octet-stream"
    size_bytes: int = 0


TASK_COMPONENTS = (
    Task,
    TaskWorkspace,
    TaskPolicy,
    TaskState,
    TaskDispatch,
)

OUTPUT_COMPONENTS = (
    ValidationResult,
    Commit,
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
