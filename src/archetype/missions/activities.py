# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Mission-owned values and provider protocol for durable author Activities."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from archetype.core.component import Component
from archetype.graph import Relation
from archetype.missions.coding_agents.contracts import (
    AgentExecutionResult,
    TaskDispatchRequest,
)
from archetype.missions.components import (
    AgentExecution,
    AuthorActivityObservation,
    Candidate,
    Checkpoint,
    Commit,
    CompleteAuthorActivityObservation,
    FrictionLog,
    Sandbox,
    ValidationResult,
)
from archetype.missions.critics.contracts import (
    candidate_subject_digest,
    canonical_digest,
)
from archetype.missions.relations import (
    AuthoredBy,
    CandidateFor,
    Executes,
    PartOfMission,
    ProducedBy,
    RunsIn,
    Supersedes,
)
from archetype.missions.sandboxes import CheckpointRef, SandboxStatus
from archetype.missions.transitions import AgentExecutionStatus

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
    """Workflow binding for one provider adapter's atomic retry route.

    This value is never transferable provider execution authority. The
    adapter must still acquire its own non-transferable provider barrier while
    performing the retry.
    """

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
    bind_mission: bool = True
    checkpoint: CheckpointRef | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "sandbox_status", SandboxStatus(self.sandbox_status))


@dataclass(frozen=True, slots=True)
class DurableAuthorExecutionObservation:
    """Bounded, redacted author evidence safe to persist and later stage."""

    result: AgentExecutionResult
    sandbox_status: SandboxStatus
    redaction_policy_id: str
    bind_mission: bool = True
    checkpoint: CheckpointRef | None = None

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
    """Provider evidence permits an attempt through an atomic retry route."""

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


def author_activity_fact_bundle_digest(
    *,
    execution_id: int,
    execution: AgentExecution,
    validations: Sequence[ValidationResult],
    commits: Sequence[Commit],
    friction: Sequence[FrictionLog],
) -> str:
    """Digest the execution-only A3a fact scaffold using its canonical codec."""

    def ordered(values: Sequence[Component]) -> list[dict[str, object]]:
        payloads = [value.model_dump(mode="json") for value in values]
        return sorted(
            payloads,
            key=lambda value: json.dumps(
                value,
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            ),
        )

    payload = {
        "commits": ordered(commits),
        "execution": execution.model_dump(mode="json"),
        "execution_id": execution_id,
        "friction": ordered(friction),
        "schema_version": 1,
        "validations": ordered(validations),
    }
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


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


@dataclass(frozen=True, slots=True)
class AuthorActivityEntityFact:
    """One entity identity and one immutable component value in an observation."""

    entity_id: int
    component: Component

    def __post_init__(self) -> None:
        if self.entity_id < 1:
            raise ValueError("author activity fact requires a positive entity identity")

    def canonical_value(self) -> dict[str, object]:
        """Return the family-owned canonical digest value for this fact."""

        component_type = type(self.component)
        return {
            "component": f"{component_type.__module__}.{component_type.__qualname__}",
            "entity_id": self.entity_id,
            "value": self.component.model_dump(mode="json"),
        }


@dataclass(frozen=True, slots=True)
class CompleteAuthorActivityFactBundle:
    """Complete result-derived Mission facts staged by A3b as one batch."""

    facts: tuple[AuthorActivityEntityFact, ...]
    execution_id: int
    sandbox_entity_id: int
    candidate_entity_id: int = 0
    checkpoint_entity_id: int = 0

    def __post_init__(self) -> None:
        identities = [fact.entity_id for fact in self.facts]
        if len(identities) != len(set(identities)):
            raise ValueError("complete author activity facts require unique entity identities")
        if self.execution_id < 1 or self.sandbox_entity_id < 1:
            raise ValueError("complete author activity facts require execution and sandbox ids")
        if self.candidate_entity_id < 0:
            raise ValueError("complete author activity candidate identity cannot be negative")
        if self.checkpoint_entity_id < 0:
            raise ValueError("complete author activity checkpoint identity cannot be negative")
        by_id = {fact.entity_id: fact.component for fact in self.facts}
        if not isinstance(by_id.get(self.execution_id), AgentExecution):
            raise ValueError("complete author activity execution identity is not an execution")
        if not isinstance(by_id.get(self.sandbox_entity_id), Sandbox):
            raise ValueError("complete author activity sandbox identity is not a sandbox")
        if self.candidate_entity_id and not isinstance(
            by_id.get(self.candidate_entity_id),
            Candidate,
        ):
            raise ValueError("complete author activity candidate identity is not a candidate")
        if self.checkpoint_entity_id and not isinstance(
            by_id.get(self.checkpoint_entity_id),
            Checkpoint,
        ):
            raise ValueError("complete author activity checkpoint identity is not a checkpoint")

    def components(self, component_type: type[Component]) -> tuple[AuthorActivityEntityFact, ...]:
        """Return facts of one exact component type in stable entity order."""

        return tuple(
            sorted(
                (fact for fact in self.facts if type(fact.component) is component_type),
                key=lambda fact: fact.entity_id,
            )
        )

    @property
    def digest(self) -> str:
        """Digest every semantic fact and provenance edge, including entity ids."""

        payload = {
            "facts": [
                fact.canonical_value()
                for fact in sorted(self.facts, key=lambda fact: fact.entity_id)
            ],
            "schema_version": 2,
        }
        encoded = json.dumps(
            payload,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
        return hashlib.sha256(encoded).hexdigest()

    def marker(
        self,
        *,
        result: AuthorActivityResultRef,
        redaction_policy_id: str,
    ) -> CompleteAuthorActivityObservation:
        """Bind this entire Mission observation to the exact durable result."""

        execution = self.components(AgentExecution)
        if len(execution) != 1:
            raise ValueError("complete author activity requires exactly one execution")
        value = execution[0].component
        assert isinstance(value, AgentExecution)
        relations = tuple(fact for fact in self.facts if isinstance(fact.component, Relation))
        return CompleteAuthorActivityObservation(
            schema_version=2,
            activity_id=value.dispatch_id,
            task_id=value.task_id,
            dispatch_sequence=value.dispatch_sequence,
            result_ref=result.ref,
            result_digest=result.digest,
            fact_bundle_digest=self.digest,
            execution_id=self.execution_id,
            validation_count=len(self.components(ValidationResult)),
            commit_count=len(self.components(Commit)),
            friction_count=len(self.components(FrictionLog)),
            checkpoint_count=len(self.components(Checkpoint)),
            checkpoint_entity_id=self.checkpoint_entity_id,
            sandbox_entity_id=self.sandbox_entity_id,
            sandbox_bound=bool(self.components(PartOfMission)),
            candidate_entity_id=self.candidate_entity_id,
            candidate_count=int(self.candidate_entity_id > 0),
            relation_count=len(relations),
            redaction_policy_id=redaction_policy_id,
        )

    def staged_entities(
        self,
        *,
        marker_entity_id: int,
        result: AuthorActivityResultRef,
        redaction_policy_id: str,
    ) -> list[list[Component]]:
        """Return the exact mixed-signature batch with its completion marker last."""

        if marker_entity_id < 1 or marker_entity_id in {fact.entity_id for fact in self.facts}:
            raise ValueError("author activity marker requires a fresh entity identity")
        ordered = [[fact.component] for fact in sorted(self.facts, key=lambda fact: fact.entity_id)]
        ordered.append(
            [
                self.marker(
                    result=result,
                    redaction_policy_id=redaction_policy_id,
                )
            ]
        )
        return ordered


def author_result_is_green(
    request: TaskDispatchRequest,
    result: AgentExecutionResult,
) -> bool:
    """Return whether exact author evidence requires one Candidate fact."""

    expected_validator_ids = {item.validator_id for item in request.validators}
    observed_validator_ids = {item.validator_id for item in result.validation}
    exact_validation = (
        expected_validator_ids == observed_validator_ids
        and len(result.validation) == len(request.validators)
        and all(
            item.passed and item.revision == result.final_revision for item in result.validation
        )
    )
    final_publications = [
        item
        for item in result.commits
        if item.sha == result.final_revision and item.pushed and item.final_revision
    ]
    return bool(
        result.status is AgentExecutionStatus.EXITED
        and exact_validation
        and len(final_publications) == 1
        and result.final_revision
        and result.starting_revision
        and result.diff_digest
        and result.validator_bundle_digest
    )


def complete_author_activity_fact_count(
    request: TaskDispatchRequest,
    observation: DurableAuthorExecutionObservation,
    *,
    prior_candidate_id: int | None,
) -> int:
    """Return required non-marker entity identities for one complete bundle."""

    output_count = (
        len(observation.result.validation)
        + len(observation.result.commits)
        + len(observation.result.friction)
        + int(observation.checkpoint is not None)
    )
    base = 4 + int(observation.bind_mission)  # sandbox, execution, Executes, RunsIn
    candidate = (
        3 + int(prior_candidate_id is not None)
        if author_result_is_green(request, observation.result)
        else 0
    )
    return base + (2 * output_count) + candidate


def complete_author_activity_fact_bundle(
    request: TaskDispatchRequest,
    observation: DurableAuthorExecutionObservation,
    *,
    entity_ids: Sequence[int],
    prior_candidate_id: int | None,
    candidate_created_at_ms: int,
) -> CompleteAuthorActivityFactBundle:
    """Construct every Mission fact and provenance edge for one author result."""

    expected_count = complete_author_activity_fact_count(
        request,
        observation,
        prior_candidate_id=prior_candidate_id,
    )
    if len(entity_ids) != expected_count or len(set(entity_ids)) != len(entity_ids):
        raise ValueError("author activity entity identities do not match the complete fact count")
    if any(entity_id < 1 for entity_id in entity_ids):
        raise ValueError("author activity entity identities must be positive")
    if candidate_created_at_ms < 0:
        raise ValueError("candidate creation time cannot be negative")

    selected = iter(entity_ids)
    result = observation.result
    sandbox_entity_id = next(selected)
    facts: list[AuthorActivityEntityFact] = [
        AuthorActivityEntityFact(
            sandbox_entity_id,
            Sandbox(
                provider=result.sandbox.provider,
                sandbox_id=result.sandbox.sandbox_id,
                environment=result.sandbox.environment,
                worktree=result.worktree,
                status=observation.sandbox_status.value,
                error=(result.error if observation.sandbox_status is SandboxStatus.ERRORED else ""),
            ),
        )
    ]
    if observation.bind_mission:
        facts.append(
            AuthorActivityEntityFact(
                next(selected),
                PartOfMission(
                    source=sandbox_entity_id,
                    target=request.mission_id,
                ),
            )
        )

    execution_id = next(selected)
    scaffold = author_activity_fact_bundle(
        observation,
        execution_id=execution_id,
    )
    facts.extend(
        (
            AuthorActivityEntityFact(execution_id, scaffold.execution),
            AuthorActivityEntityFact(
                next(selected),
                Executes(source=execution_id, target=request.task_id),
            ),
            AuthorActivityEntityFact(
                next(selected),
                RunsIn(source=execution_id, target=sandbox_entity_id),
            ),
        )
    )

    for component in (*scaffold.validations, *scaffold.commits, *scaffold.friction):
        output_id = next(selected)
        facts.append(AuthorActivityEntityFact(output_id, component))
        facts.append(
            AuthorActivityEntityFact(
                next(selected),
                ProducedBy(source=output_id, target=execution_id),
            )
        )

    checkpoint_entity_id = 0
    if observation.checkpoint is not None:
        checkpoint = observation.checkpoint
        checkpoint_entity_id = next(selected)
        facts.append(
            AuthorActivityEntityFact(
                checkpoint_entity_id,
                Checkpoint(
                    task_id=result.task_id,
                    execution_id=execution_id,
                    dispatch_id=result.dispatch_id,
                    provider=checkpoint.provider,
                    checkpoint_id=checkpoint.checkpoint_id,
                    uri=checkpoint.uri,
                    created_at_ms=checkpoint.created_at_ms,
                    environment=checkpoint.environment,
                    source_sandbox_id=checkpoint.source_sandbox_id,
                    owner_id=checkpoint.owner_id,
                    locality=checkpoint.locality.value,
                    expires_at_ms=checkpoint.expires_at_ms or 0,
                    integrity=checkpoint.integrity,
                    restorable=checkpoint.restorable,
                ),
            )
        )
        facts.append(
            AuthorActivityEntityFact(
                next(selected),
                ProducedBy(source=checkpoint_entity_id, target=execution_id),
            )
        )

    candidate_entity_id = 0
    if author_result_is_green(request, result):
        candidate_entity_id = next(selected)
        candidate_id = canonical_digest(
            {
                "mission_id": request.mission_id,
                "task_id": request.task_id,
                "dispatch_id": request.dispatch_id,
                "author_execution_id": execution_id,
                "head_revision": result.final_revision,
                "policy_digest": request.critic_policy.digest,
            }
        )
        subject_digest = candidate_subject_digest(
            candidate_id=candidate_id,
            mission_id=request.mission_id,
            task_id=request.task_id,
            dispatch_id=request.dispatch_id,
            author_execution_id=execution_id,
            repository=request.repository,
            branch=request.branch,
            base_ref=request.base_ref,
            base_revision=result.starting_revision,
            head_revision=result.final_revision,
            diff_digest=result.diff_digest,
            validator_bundle_digest=result.validator_bundle_digest,
            policy_digest=request.critic_policy.digest,
        )
        facts.extend(
            (
                AuthorActivityEntityFact(
                    candidate_entity_id,
                    Candidate(
                        candidate_id=candidate_id,
                        mission_id=request.mission_id,
                        task_id=request.task_id,
                        dispatch_id=request.dispatch_id,
                        dispatch_sequence=request.dispatch_sequence,
                        author_execution_id=execution_id,
                        author_sandbox_id=result.sandbox.sandbox_id,
                        repository=request.repository,
                        branch=request.branch,
                        base_ref=request.base_ref,
                        base_revision=result.starting_revision,
                        head_revision=result.final_revision,
                        diff_digest=result.diff_digest,
                        validator_bundle_digest=result.validator_bundle_digest,
                        policy_digest=request.critic_policy.digest,
                        candidate_digest=subject_digest,
                        created_at_ms=candidate_created_at_ms,
                    ),
                ),
                AuthorActivityEntityFact(
                    next(selected),
                    CandidateFor(
                        source=candidate_entity_id,
                        target=request.task_id,
                    ),
                ),
                AuthorActivityEntityFact(
                    next(selected),
                    AuthoredBy(
                        source=candidate_entity_id,
                        target=execution_id,
                    ),
                ),
            )
        )
        if prior_candidate_id is not None:
            facts.append(
                AuthorActivityEntityFact(
                    next(selected),
                    Supersedes(
                        source=candidate_entity_id,
                        target=prior_candidate_id,
                    ),
                )
            )

    try:
        next(selected)
    except StopIteration:
        pass
    else:
        raise AssertionError("author activity fact builder did not consume every entity id")
    return CompleteAuthorActivityFactBundle(
        facts=tuple(facts),
        execution_id=execution_id,
        sandbox_entity_id=sandbox_entity_id,
        candidate_entity_id=candidate_entity_id,
        checkpoint_entity_id=checkpoint_entity_id,
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
    "AuthorActivityEntityFact",
    "AuthorActivityRequestRef",
    "AuthorActivityResultRef",
    "AuthorActivityRetryGuard",
    "AuthorConfirmedAbsent",
    "AuthorExecutionObservation",
    "AuthorRecovered",
    "AuthorRecoveryUnknown",
    "AuthorReconciliation",
    "CompleteAuthorActivityFactBundle",
    "DurableAuthorExecutionObservation",
    "MissionAuthorExecutor",
    "author_activity_fact_bundle",
    "author_activity_fact_bundle_digest",
    "author_provider_operation_id",
    "author_result_is_green",
    "complete_author_activity_fact_bundle",
    "complete_author_activity_fact_count",
]
