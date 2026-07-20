# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Typed intent and observation values for coding-agent execution."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol, runtime_checkable

from archetype.missions.contracts import CommandValidator, RepositoryPublicationPolicy
from archetype.missions.sandboxes import SandboxIdentity, SandboxSession


class AgentExecutionStatus(StrEnum):
    """Factual lifecycle of one agent process and its repository harness."""

    STARTING = "starting"
    RUNNING = "running"
    EXITED = "exited"
    ERRORED = "errored"
    INTERRUPTED = "interrupted"


@dataclass(frozen=True)
class DispatchedValidator:
    """Validator entity identity plus the command authored for it."""

    validator_id: int
    spec: CommandValidator

    def __post_init__(self) -> None:
        if self.validator_id < 0:
            raise ValueError("validator_id must not be negative")


@dataclass(frozen=True)
class ValidationObservation:
    """Observed command result, bound to one dispatch and repository revision."""

    validator_id: int
    name: str
    command: tuple[str, ...]
    expected_returncode: int
    actual_returncode: int
    revision: str
    stdout: str = ""
    stderr: str = ""

    @property
    def passed(self) -> bool:
        """Derive the verdict; providers never supply this authority bit."""

        return self.actual_returncode == self.expected_returncode


@dataclass(frozen=True)
class CommitObservation:
    """One Git commit present in the task's validated history."""

    sha: str
    message: str
    branch: str
    pushed: bool
    final_revision: bool = False

    def __post_init__(self) -> None:
        if not self.sha.strip():
            raise ValueError("commit observation requires a SHA")


@dataclass(frozen=True)
class FrictionObservation:
    """One queryable obstacle encountered during execution."""

    kind: str
    message: str

    def __post_init__(self) -> None:
        if not self.kind.strip() or not self.message.strip():
            raise ValueError("friction requires a kind and message")


@dataclass(frozen=True)
class AgentProcessObservation:
    """Raw process facts returned by a coding-agent driver."""

    returncode: int
    stdout: str = ""
    stderr: str = ""
    session_id: str = ""


@dataclass(frozen=True)
class TaskDispatchRequest:
    """Committed permission to execute one task dispatch."""

    mission_id: int
    task_id: int
    task_name: str
    dispatch_id: str
    dispatch_sequence: int
    repository: str
    branch: str
    base_ref: str
    prompt: str
    validators: tuple[DispatchedValidator, ...]
    publication_policy: RepositoryPublicationPolicy
    task_base_revision: str = ""
    previous_agent_session_id: str = ""
    previous_validation: tuple[ValidationObservation, ...] = ()

    def __post_init__(self) -> None:
        if not self.dispatch_id.strip() or self.dispatch_sequence < 1:
            raise ValueError("task dispatch requires an identity and positive sequence")
        if not self.validators:
            raise ValueError("task dispatch requires at least one validator")
        validator_ids = [validator.validator_id for validator in self.validators]
        if len(validator_ids) != len(set(validator_ids)):
            raise ValueError("task dispatch validator identities must be unique")
        try:
            policy = RepositoryPublicationPolicy(self.publication_policy)
        except ValueError as exc:
            raise ValueError("unsupported repository publication policy") from exc
        object.__setattr__(self, "publication_policy", policy)


@dataclass(frozen=True)
class AgentExecutionResult:
    """Factual execution and repository observations returned by the harness."""

    mission_id: int
    task_id: int
    dispatch_id: str
    dispatch_sequence: int
    status: AgentExecutionStatus
    sandbox: SandboxIdentity
    worktree: str
    agent_session_id: str
    agent_returncode: int
    starting_revision: str
    final_revision: str
    validation: tuple[ValidationObservation, ...] = ()
    commits: tuple[CommitObservation, ...] = ()
    friction: tuple[FrictionObservation, ...] = ()
    error: str = ""

    def __post_init__(self) -> None:
        if not self.dispatch_id.strip() or self.dispatch_sequence < 1:
            raise ValueError("agent execution requires a dispatch identity")
        object.__setattr__(self, "status", AgentExecutionStatus(self.status))


@runtime_checkable
class CodingAgentDriver(Protocol):
    """Invoke one coding-agent process through a sandbox session."""

    async def run(
        self,
        session: SandboxSession,
        request: TaskDispatchRequest,
        prompt: str,
    ) -> AgentProcessObservation: ...


__all__ = [
    "AgentExecutionResult",
    "AgentExecutionStatus",
    "AgentProcessObservation",
    "CodingAgentDriver",
    "CommitObservation",
    "DispatchedValidator",
    "FrictionObservation",
    "TaskDispatchRequest",
    "ValidationObservation",
]
