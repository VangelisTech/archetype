# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Mission-owned values and provider protocol for durable author Activities."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from archetype.missions.coding_agents.contracts import (
    AgentExecutionResult,
    TaskDispatchRequest,
)
from archetype.missions.components import (
    AgentExecution,
    AuthorActivityObservation,
    Commit,
    FrictionLog,
    ValidationResult,
)
from archetype.missions.projections import author_activity_fact_bundle_digest
from archetype.missions.sandboxes import SandboxStatus

AUTHOR_ACTIVITY_KIND = "missions.author"


def author_provider_operation_id(world_id: str, activity_id: str) -> str:
    """Return one bounded stable provider identity for a world-local dispatch."""

    if not world_id.strip() or not activity_id.strip():
        raise ValueError("author provider operation requires world and activity identities")
    encoded = json.dumps(
        {
            "activity_id": activity_id,
            "kind": AUTHOR_ACTIVITY_KIND,
            "world_id": world_id,
        },
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    return f"{AUTHOR_ACTIVITY_KIND}:{hashlib.sha256(encoded).hexdigest()}"


@dataclass(frozen=True, slots=True)
class AuthorActivityRequestRef:
    """Content identity retained by generic Activity admission."""

    ref: str
    digest: str

    def __post_init__(self) -> None:
        if not self.ref.strip() or not self.digest.strip():
            raise ValueError("author activity request reference and digest cannot be empty")


@dataclass(frozen=True, slots=True)
class AuthorActivityResultRef:
    """Bounded content identity retained by generic Activity result recording."""

    ref: str
    digest: str
    media_type: str = "application/json"
    size_bytes: int = 0

    def __post_init__(self) -> None:
        if not self.ref.strip() or not self.digest.strip() or not self.media_type.strip():
            raise ValueError("author activity result reference and digest cannot be empty")
        if self.size_bytes < 0:
            raise ValueError("author activity result size cannot be negative")


@dataclass(frozen=True, slots=True)
class AuthorActivityRetryGuard:
    """Provider-side barrier authorizing one safe replay attempt."""

    ref: str
    digest: str

    def __post_init__(self) -> None:
        if not self.ref.strip() or not self.digest.strip():
            raise ValueError("author activity retry guard ref and digest cannot be empty")


@dataclass(frozen=True, slots=True)
class AuthorExecutionObservation:
    """Raw provider result that has not crossed the durability boundary."""

    result: AgentExecutionResult
    sandbox_status: SandboxStatus

    def __post_init__(self) -> None:
        object.__setattr__(self, "sandbox_status", SandboxStatus(self.sandbox_status))


@dataclass(frozen=True, slots=True)
class DurableAuthorExecutionObservation:
    """Bounded, redacted author evidence safe to persist and later stage."""

    result: AgentExecutionResult
    sandbox_status: SandboxStatus
    redaction_policy_id: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "sandbox_status", SandboxStatus(self.sandbox_status))
        if not self.redaction_policy_id.strip():
            raise ValueError("durable author observation requires a redaction policy")


@dataclass(frozen=True, slots=True)
class AuthorRecovered:
    """Provider evidence proves the prior operation completed."""

    observation: AuthorExecutionObservation


@dataclass(frozen=True, slots=True)
class AuthorConfirmedAbsent:
    """Provider evidence proves absence behind an atomic replay barrier."""

    guard: AuthorActivityRetryGuard


@dataclass(frozen=True, slots=True)
class AuthorRecoveryUnknown:
    """Provider evidence cannot prove completion or safe absence."""

    reason: str = ""


type AuthorReconciliation = AuthorRecovered | AuthorConfirmedAbsent | AuthorRecoveryUnknown


@runtime_checkable
class MissionAuthorExecutor(Protocol):
    """Execute and reconcile the provider-specific meaning of author work."""

    @property
    def provider(self) -> str: ...

    async def execute(
        self,
        *,
        operation_id: str,
        request: TaskDispatchRequest,
        attempt: int,
        fence: int,
        retry_guard: AuthorActivityRetryGuard | None,
    ) -> AuthorExecutionObservation: ...

    async def reconcile(
        self,
        *,
        operation_id: str,
        request: TaskDispatchRequest,
    ) -> AuthorReconciliation: ...


@dataclass(frozen=True, slots=True)
class AuthorActivityFactBundle:
    """Exact ECS facts expected from one bounded durable author result.

    This A3a execution-fact bundle is deliberately not the production
    completion contract. A3b must add every continuation and provenance fact
    before the Activity-backed Mission path can be wired.
    """

    execution_id: int
    execution: AgentExecution
    validations: tuple[ValidationResult, ...]
    commits: tuple[Commit, ...]
    friction: tuple[FrictionLog, ...]

    def __post_init__(self) -> None:
        if self.execution_id < 1:
            raise ValueError("author activity fact bundle requires an execution identity")

    @property
    def digest(self) -> str:
        """Digest the expected facts using the family-owned canonical codec."""

        return author_activity_fact_bundle_digest(
            execution_id=self.execution_id,
            execution=self.execution,
            validations=self.validations,
            commits=self.commits,
            friction=self.friction,
        )

    def marker(
        self,
        *,
        result: AuthorActivityResultRef,
        redaction_policy_id: str,
    ) -> AuthorActivityObservation:
        """Bind the complete A3a execution-fact bundle to its durable result."""

        return AuthorActivityObservation(
            activity_id=self.execution.dispatch_id,
            task_id=self.execution.task_id,
            dispatch_sequence=self.execution.dispatch_sequence,
            result_ref=result.ref,
            result_digest=result.digest,
            fact_bundle_digest=self.digest,
            execution_id=self.execution_id,
            validation_count=len(self.validations),
            commit_count=len(self.commits),
            friction_count=len(self.friction),
            redaction_policy_id=redaction_policy_id,
        )


def author_activity_fact_bundle(
    observation: DurableAuthorExecutionObservation,
    *,
    execution_id: int,
) -> AuthorActivityFactBundle:
    """Derive the A3a ECS execution facts from one durable author result."""

    result = observation.result
    execution = AgentExecution(
        task_id=result.task_id,
        dispatch_id=result.dispatch_id,
        dispatch_sequence=result.dispatch_sequence,
        status=result.status.value,
        sandbox_id=result.sandbox.sandbox_id,
        agent_session_id=result.agent_session_id,
        agent_returncode=result.agent_returncode,
        agent_stdout=result.agent_stdout,
        agent_stderr=result.agent_stderr,
        trace_uri=result.trace_uri,
        redaction_policy_id=observation.redaction_policy_id,
        starting_revision=result.starting_revision,
        final_revision=result.final_revision,
        error=result.error,
    )
    validations = tuple(
        ValidationResult(
            task_id=result.task_id,
            validator_id=item.validator_id,
            execution_id=execution_id,
            dispatch_id=result.dispatch_id,
            dispatch_sequence=result.dispatch_sequence,
            revision=item.revision,
            expected_returncode=item.expected_returncode,
            actual_returncode=item.actual_returncode,
            stdout=item.stdout,
            stderr=item.stderr,
        )
        for item in result.validation
    )
    commits = tuple(
        Commit(
            task_id=result.task_id,
            execution_id=execution_id,
            dispatch_id=result.dispatch_id,
            sha=item.sha,
            message=item.message,
            branch=item.branch,
            pushed=item.pushed,
            final_revision=item.final_revision,
        )
        for item in result.commits
    )
    friction = tuple(
        FrictionLog(
            task_id=result.task_id,
            execution_id=execution_id,
            dispatch_id=result.dispatch_id,
            kind=item.kind,
            message=item.message,
        )
        for item in result.friction
    )
    return AuthorActivityFactBundle(
        execution_id=execution_id,
        execution=execution,
        validations=validations,
        commits=commits,
        friction=friction,
    )


__all__ = [
    "AUTHOR_ACTIVITY_KIND",
    "AuthorActivityFactBundle",
    "AuthorActivityRequestRef",
    "AuthorActivityResultRef",
    "AuthorActivityRetryGuard",
    "AuthorConfirmedAbsent",
    "AuthorExecutionObservation",
    "AuthorRecovered",
    "AuthorRecoveryUnknown",
    "AuthorReconciliation",
    "DurableAuthorExecutionObservation",
    "MissionAuthorExecutor",
    "author_activity_fact_bundle",
    "author_provider_operation_id",
]
