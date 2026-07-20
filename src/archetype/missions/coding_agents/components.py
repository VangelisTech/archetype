# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Built-in ECS state for coding-agent missions."""

from __future__ import annotations

import json
from dataclasses import asdict

from pydantic import field_validator, model_validator

from archetype.core.component import Component
from archetype.missions.coding_agents.transitions import (
    AgentAttemptStatus,
    AgentMissionStatus,
    AgentTaskStatus,
)
from archetype.missions.contracts import (
    ArtifactRef,
    CommandValidator,
    Friction,
    RepositoryPublicationPolicy,
    TaskExecutionReceipt,
    ValidatorResult,
)


class AgentMissionRecord(Component):
    """Immutable repository identity for one submitted mission."""

    name: str = "agent-mission"
    repository: str = ""
    branch: str = ""
    base_ref: str = "main"


class AgentMissionState(Component):
    """Current mission rollup derived from its task entities."""

    status: str = AgentMissionStatus.RUNNING.value
    reason: str = ""

    @field_validator("status")
    @classmethod
    def _valid_status(cls, value: str) -> str:
        return AgentMissionStatus(value).value


class AgentTaskRecord(Component):
    """Immutable task name and goal prompt."""

    name: str = ""
    prompt: str = ""


class AgentTaskWorkspace(Component):
    """Repository work surface required by the sandbox resource."""

    repository: str = ""
    branch: str = ""
    base_ref: str = "main"


class AgentTaskPolicy(Component):
    """Attempt and repository-publication policy for one task."""

    max_attempts: int = 3
    publication_policy: str = RepositoryPublicationPolicy.COMMIT_AND_PUSH.value

    @field_validator("max_attempts")
    @classmethod
    def _positive_attempts(cls, value: int) -> int:
        if value < 1:
            raise ValueError("max_attempts must be positive")
        return value

    @field_validator("publication_policy")
    @classmethod
    def _valid_publication_policy(cls, value: str) -> str:
        return RepositoryPublicationPolicy(value).value


class AgentTaskValidators(Component):
    """Arrow-safe private encoding of the typed validator authoring contract."""

    specs_json: str = "[]"

    @classmethod
    def from_specs(cls, specs: tuple[CommandValidator, ...]) -> AgentTaskValidators:
        return cls(specs_json=json.dumps([asdict(spec) for spec in specs], sort_keys=True))

    def specs(self) -> tuple[CommandValidator, ...]:
        values = json.loads(self.specs_json)
        return tuple(
            CommandValidator(
                name=str(value["name"]),
                command=tuple(str(argument) for argument in value["command"]),
                expected_returncode=int(value["expected_returncode"]),
                timeout_seconds=int(value["timeout_seconds"]),
            )
            for value in values
        )


class AgentTaskState(Component):
    """Processor-owned task lifecycle."""

    status: str = AgentTaskStatus.PENDING.value
    reason: str = ""

    @field_validator("status")
    @classmethod
    def _valid_status(cls, value: str) -> str:
        return AgentTaskStatus(value).value


class AgentTaskAttempt(Component):
    """Latest task attempt; earlier values remain available by tick."""

    attempt_id: str = ""
    attempt_index: int = 0
    status: str = AgentAttemptStatus.IDLE.value
    settled: bool = True
    sandbox_id: str = ""
    worktree: str = ""
    agent_session_id: str = ""
    commit_sha: str = ""
    commit_message: str = ""
    pushed: bool = False
    error: str = ""

    @field_validator("status")
    @classmethod
    def _valid_status(cls, value: str) -> str:
        return AgentAttemptStatus(value).value

    @model_validator(mode="after")
    def _valid_attempt(self) -> AgentTaskAttempt:
        if self.attempt_index < 0:
            raise ValueError("attempt_index must not be negative")
        if self.attempt_index == 0 and self.attempt_id:
            raise ValueError("an idle attempt cannot have an attempt_id")
        if self.attempt_index > 0 and not self.attempt_id:
            raise ValueError("a started attempt requires an attempt_id")
        return self

    @classmethod
    def from_receipt(cls, receipt: TaskExecutionReceipt) -> AgentTaskAttempt:
        return cls(
            attempt_id=receipt.attempt_id,
            attempt_index=receipt.attempt_index,
            status=receipt.outcome.value,
            settled=False,
            sandbox_id=receipt.sandbox_id,
            worktree=receipt.worktree,
            agent_session_id=receipt.agent_session_id,
            commit_sha=receipt.commit_sha,
            commit_message=receipt.commit_message,
            pushed=receipt.pushed,
            error=receipt.error,
        )


class AgentTaskEvidence(Component):
    """Validator, artifact, and friction evidence for the latest attempt."""

    validator_results_json: str = "[]"
    artifacts_json: str = "[]"
    friction_json: str = "[]"

    @classmethod
    def from_receipt(cls, receipt: TaskExecutionReceipt) -> AgentTaskEvidence:
        return cls(
            validator_results_json=json.dumps(
                [asdict(result) for result in receipt.validator_results], sort_keys=True
            ),
            artifacts_json=json.dumps([asdict(ref) for ref in receipt.artifacts], sort_keys=True),
            friction_json=json.dumps(
                [asdict(finding) for finding in receipt.friction], sort_keys=True
            ),
        )

    def validator_results(self) -> tuple[ValidatorResult, ...]:
        return tuple(
            ValidatorResult(
                name=str(value["name"]),
                command=tuple(str(argument) for argument in value["command"]),
                returncode=int(value["returncode"]),
                passed=bool(value["passed"]),
                stdout=str(value.get("stdout", "")),
                stderr=str(value.get("stderr", "")),
            )
            for value in json.loads(self.validator_results_json)
        )

    def artifacts(self) -> tuple[ArtifactRef, ...]:
        return tuple(ArtifactRef(**value) for value in json.loads(self.artifacts_json))

    def friction(self) -> tuple[Friction, ...]:
        return tuple(Friction(**value) for value in json.loads(self.friction_json))


AGENT_TASK_COMPONENTS = (
    AgentTaskRecord,
    AgentTaskWorkspace,
    AgentTaskPolicy,
    AgentTaskValidators,
    AgentTaskState,
    AgentTaskAttempt,
    AgentTaskEvidence,
)
