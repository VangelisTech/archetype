# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Complete ECS fact bundles derived from durable critic Activity results."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Sequence
from dataclasses import dataclass

from archetype.core.component import Component
from archetype.graph import Relation
from archetype.missions.components import (
    CompleteCriticActivityObservation,
    CriticExecution,
    CriticFinding,
    CriticReceipt,
    Sandbox,
)
from archetype.missions.critics.activities import (
    CriticActivityRequest,
    CriticActivityResult,
)
from archetype.missions.critics.activity_execution import CriticActivityResultRef
from archetype.missions.relations import ProducedBy, Reviews, RunsIn
from archetype.missions.sandboxes import SandboxStatus


@dataclass(frozen=True, slots=True)
class CriticActivityEntityFact:
    """One entity identity and one immutable critic observation Component."""

    entity_id: int
    component: Component

    def __post_init__(self) -> None:
        if self.entity_id < 1:
            raise ValueError("critic Activity fact requires a positive entity identity")

    def canonical_value(self) -> dict[str, object]:
        """Return the canonical value included in the complete-bundle digest."""

        component_type = type(self.component)
        return {
            "component": f"{component_type.__module__}.{component_type.__qualname__}",
            "entity_id": self.entity_id,
            "value": self.component.model_dump(mode="json"),
        }


@dataclass(frozen=True, slots=True)
class CompleteCriticActivityFactBundle:
    """All result-derived critic facts staged atomically before one marker."""

    facts: tuple[CriticActivityEntityFact, ...]
    execution_id: int
    sandbox_entity_id: int
    receipt_entity_id: int = 0

    def __post_init__(self) -> None:
        identities = [fact.entity_id for fact in self.facts]
        if len(identities) != len(set(identities)):
            raise ValueError("complete critic Activity facts require unique entity identities")
        if self.execution_id < 1 or self.sandbox_entity_id < 1:
            raise ValueError("complete critic Activity facts require execution and sandbox ids")
        if self.receipt_entity_id < 0:
            raise ValueError("complete critic Activity receipt identity cannot be negative")
        by_id = {fact.entity_id: fact.component for fact in self.facts}
        if not isinstance(by_id.get(self.execution_id), CriticExecution):
            raise ValueError("complete critic Activity execution identity is not an execution")
        if not isinstance(by_id.get(self.sandbox_entity_id), Sandbox):
            raise ValueError("complete critic Activity sandbox identity is not a sandbox")
        if self.receipt_entity_id and not isinstance(
            by_id.get(self.receipt_entity_id),
            CriticReceipt,
        ):
            raise ValueError("complete critic Activity receipt identity is not a receipt")

    def components(
        self,
        component_type: type[Component],
    ) -> tuple[CriticActivityEntityFact, ...]:
        """Return facts of one exact Component type in stable entity order."""

        return tuple(
            sorted(
                (fact for fact in self.facts if type(fact.component) is component_type),
                key=lambda fact: fact.entity_id,
            )
        )

    @property
    def digest(self) -> str:
        """Digest every Component value, provenance edge, and entity identity."""

        payload = {
            "facts": [
                fact.canonical_value()
                for fact in sorted(self.facts, key=lambda fact: fact.entity_id)
            ],
            "schema_version": 1,
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
        request: CriticActivityRequest,
        result: CriticActivityResult,
        result_ref: CriticActivityResultRef,
    ) -> CompleteCriticActivityObservation:
        """Bind this complete ECS observation to one exact durable result."""

        _validate_result_identity(request, result)
        relations = tuple(fact for fact in self.facts if isinstance(fact.component, Relation))
        receipt = result.receipt
        subject = receipt.subject if receipt is not None else None
        return CompleteCriticActivityObservation(
            activity_id=request.review_id,
            candidate_entity_id=request.candidate_entity_id,
            domain_review_attempt=request.domain_review_attempt,
            result_ref=result_ref.ref,
            result_digest=result_ref.digest,
            fact_bundle_digest=self.digest,
            execution_id=self.execution_id,
            sandbox_entity_id=self.sandbox_entity_id,
            finding_count=len(self.components(CriticFinding)),
            receipt_entity_id=self.receipt_entity_id,
            receipt_count=int(self.receipt_entity_id > 0),
            relation_count=len(relations),
            author_sandbox_id=request.author_sandbox_id,
            critic_sandbox_id=result.sandbox.sandbox_id,
            subject_content_digest=(subject.content_digest if subject is not None else ""),
            subject_metadata_digest=(subject.metadata_digest if subject is not None else ""),
            subject_digest=(subject.subject_digest if subject is not None else ""),
            subject_content_size_bytes=(subject.content_size_bytes if subject is not None else 0),
            subject_metadata_size_bytes=(subject.metadata_size_bytes if subject is not None else 0),
            subject_size_bytes=(subject.total_size_bytes if subject is not None else 0),
            subject_media_type=(subject.media_type if subject is not None else ""),
            subject_transport=(subject.transport.value if subject is not None else ""),
            subject_ref=(subject.ref if subject is not None else ""),
            redaction_policy_id=result.redaction_policy_id,
        )

    def staged_entities(
        self,
        *,
        marker_entity_id: int,
        request: CriticActivityRequest,
        result: CriticActivityResult,
        result_ref: CriticActivityResultRef,
    ) -> list[list[Component]]:
        """Return the exact mixed-signature batch with its completion marker last."""

        if marker_entity_id < 1 or marker_entity_id in {fact.entity_id for fact in self.facts}:
            raise ValueError("critic Activity marker requires a fresh entity identity")
        ordered = [[fact.component] for fact in sorted(self.facts, key=lambda fact: fact.entity_id)]
        ordered.append(
            [
                self.marker(
                    request=request,
                    result=result,
                    result_ref=result_ref,
                )
            ]
        )
        return ordered


def complete_critic_activity_fact_count(result: CriticActivityResult) -> int:
    """Return required non-marker entity identities for one critic result."""

    return 4 + (2 * len(result.findings)) + (2 * int(result.receipt is not None))


def complete_critic_activity_fact_bundle(
    request: CriticActivityRequest,
    result: CriticActivityResult,
    *,
    entity_ids: Sequence[int],
    receipt_staged_at_ms: int,
) -> CompleteCriticActivityFactBundle:
    """Construct every critic fact and provenance edge for one durable result."""

    _validate_result_identity(request, result)
    expected_count = complete_critic_activity_fact_count(result)
    if len(entity_ids) != expected_count or len(set(entity_ids)) != len(entity_ids):
        raise ValueError("critic Activity entity identities do not match the complete fact count")
    if any(entity_id < 1 for entity_id in entity_ids):
        raise ValueError("critic Activity entity identities must be positive")
    if receipt_staged_at_ms < 0:
        raise ValueError("critic receipt staging time cannot be negative")
    if (result.receipt is None) != (receipt_staged_at_ms == 0):
        raise ValueError("critic receipt staging time does not match result completeness")

    selected = iter(entity_ids)
    sandbox_entity_id = next(selected)
    sandbox_error = result.error if result.sandbox_status is SandboxStatus.ERRORED else ""
    facts: list[CriticActivityEntityFact] = [
        CriticActivityEntityFact(
            sandbox_entity_id,
            Sandbox(
                provider=result.sandbox.provider,
                sandbox_id=result.sandbox.sandbox_id,
                environment=result.sandbox.environment,
                status=result.sandbox_status.value,
                error=sandbox_error,
            ),
        )
    ]

    execution_id = next(selected)
    facts.extend(
        (
            CriticActivityEntityFact(
                execution_id,
                CriticExecution(
                    candidate_entity_id=request.candidate_entity_id,
                    candidate_id=request.candidate_id,
                    review_id=request.review_id,
                    attempt=request.domain_review_attempt,
                    status=result.status.value,
                    sandbox_id=result.sandbox.sandbox_id,
                    driver=request.policy.driver,
                    model=request.policy.model,
                    started_at_ms=result.started_at_ms,
                    ended_at_ms=result.ended_at_ms,
                    provision_started_at_ms=result.provision_started_at_ms,
                    sandbox_ready_at_ms=result.sandbox_ready_at_ms,
                    base_hydrated_at_ms=result.base_hydrated_at_ms,
                    candidate_published_at_ms=request.candidate_published_at_ms,
                    head_ready_at_ms=result.head_ready_at_ms,
                    critic_started_at_ms=result.critic_started_at_ms,
                    receipt_staged_at_ms=receipt_staged_at_ms,
                    raw_output=result.raw_output,
                    trace_uri=result.trace_uri,
                    redaction_policy_id=result.redaction_policy_id,
                    error=result.error,
                ),
            ),
            CriticActivityEntityFact(
                next(selected),
                Reviews(source=execution_id, target=request.candidate_entity_id),
            ),
            CriticActivityEntityFact(
                next(selected),
                RunsIn(source=execution_id, target=sandbox_entity_id),
            ),
        )
    )

    for finding in result.findings:
        finding_entity_id = next(selected)
        facts.extend(
            (
                CriticActivityEntityFact(
                    finding_entity_id,
                    CriticFinding(
                        candidate_entity_id=request.candidate_entity_id,
                        critic_execution_id=execution_id,
                        finding_id=finding.finding_id,
                        severity=finding.severity,
                        category=finding.category,
                        confidence=finding.confidence,
                        title=finding.title,
                        detail=finding.detail,
                        evidence_location=finding.evidence_location,
                        reproduction=finding.reproduction,
                    ),
                ),
                CriticActivityEntityFact(
                    next(selected),
                    ProducedBy(source=finding_entity_id, target=execution_id),
                ),
            )
        )

    receipt_entity_id = 0
    if result.receipt is not None:
        receipt = result.receipt
        receipt_entity_id = next(selected)
        facts.extend(
            (
                CriticActivityEntityFact(
                    receipt_entity_id,
                    CriticReceipt(
                        candidate_entity_id=request.candidate_entity_id,
                        critic_execution_id=execution_id,
                        critic_sandbox_id=result.sandbox.sandbox_id,
                        review_id=receipt.review_id,
                        conclusion=receipt.conclusion.value,
                        candidate_digest=receipt.candidate_digest,
                        policy_digest=receipt.policy_digest,
                        evidence_digest=receipt.evidence_digest,
                        reviewed_base_revision=receipt.reviewed_base_revision,
                        reviewed_head_revision=receipt.reviewed_head_revision,
                        reviewed_diff_digest=receipt.reviewed_diff_digest,
                        validator_bundle_digest=receipt.validator_bundle_digest,
                        reviewed_scope=receipt.reviewed_scope,
                        finding_count=receipt.finding_count,
                        blocking_count=receipt.blocking_count,
                        output_schema_version=receipt.output_schema_version,
                        completed_at_ms=receipt.completed_at_ms,
                    ),
                ),
                CriticActivityEntityFact(
                    next(selected),
                    ProducedBy(source=receipt_entity_id, target=execution_id),
                ),
            )
        )

    try:
        next(selected)
    except StopIteration:
        pass
    else:
        raise AssertionError("critic Activity fact builder did not consume every entity id")
    return CompleteCriticActivityFactBundle(
        facts=tuple(facts),
        execution_id=execution_id,
        sandbox_entity_id=sandbox_entity_id,
        receipt_entity_id=receipt_entity_id,
    )


def _validate_result_identity(
    request: CriticActivityRequest,
    result: CriticActivityResult,
) -> None:
    expected = (
        request.review_id,
        request.domain_review_attempt,
        request.candidate_digest,
        request.policy.digest,
        request.base_revision,
        request.head_revision,
        request.diff_digest,
        request.validator_bundle_digest,
        request.author_sandbox_id,
        request.redaction_policy_id,
    )
    observed = (
        result.review_id,
        result.domain_review_attempt,
        result.candidate_digest,
        result.policy_digest,
        result.base_revision,
        result.head_revision,
        result.diff_digest,
        result.validator_bundle_digest,
        result.author_sandbox_id,
        result.redaction_policy_id,
    )
    if observed != expected:
        raise ValueError("critic Activity result does not match its admitted request")
    if result.sandbox.sandbox_id == request.author_sandbox_id:
        raise ValueError("critic Activity result reused the author sandbox identity")


__all__ = [
    "CompleteCriticActivityFactBundle",
    "CriticActivityEntityFact",
    "complete_critic_activity_fact_bundle",
    "complete_critic_activity_fact_count",
]
