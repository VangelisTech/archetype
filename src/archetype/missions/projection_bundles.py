# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Exact selection of complete author/critic activity fact bundles.

The input is already Python: the post-tick path hydrates Components from the
snapshot frames, and the pending-world path reads them from the spawn cache,
which was never a DataFrame. Selection here is therefore a bounded per-marker
walk over staged Component values, not a query plan. The relational work that
does belong to Daft happens upstream in `projections.py`, on the live frames.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping, Sequence

from archetype.core.component import Component
from archetype.graph import Relation
from archetype.missions.activities import (
    AuthorActivityEntityFact,
    CompleteAuthorActivityFactBundle,
)
from archetype.missions.components import (
    AgentExecution,
    Candidate,
    Checkpoint,
    Commit,
    CompleteAuthorActivityObservation,
    CompleteCriticActivityObservation,
    CriticExecution,
    CriticFinding,
    CriticReceipt,
    FrictionLog,
    Sandbox,
    ValidationResult,
)
from archetype.missions.critics.activity_facts import (
    CompleteCriticActivityFactBundle,
    CriticActivityEntityFact,
)
from archetype.missions.relations import (
    AuthoredBy,
    CandidateFor,
    Executes,
    PartOfMission,
    ProducedBy,
    Reviews,
    RunsIn,
    Supersedes,
)

COMPLETE_AUTHOR_ACTIVITY_FACT_TYPES: tuple[type[Component], ...] = (
    Sandbox,
    AgentExecution,
    ValidationResult,
    Commit,
    Checkpoint,
    FrictionLog,
    Candidate,
    PartOfMission,
    Executes,
    RunsIn,
    ProducedBy,
    CandidateFor,
    AuthoredBy,
    Supersedes,
)

COMPLETE_CRITIC_ACTIVITY_FACT_TYPES: tuple[type[Component], ...] = (
    Sandbox,
    CriticExecution,
    CriticFinding,
    CriticReceipt,
    Reviews,
    RunsIn,
    ProducedBy,
)

type _ActivityFact = AuthorActivityEntityFact | CriticActivityEntityFact


def _fact_by_entity_id[FactT: _ActivityFact](
    facts: Sequence[FactT],
    component_type: type[Component],
    entity_id: int,
) -> FactT | None:
    """Return the one staged fact of this exact type at this entity identity."""

    for fact in facts:
        if fact.entity_id == entity_id and isinstance(fact.component, component_type):
            return fact
    return None


def _edges_from[FactT: _ActivityFact](
    facts: Sequence[FactT],
    relation_type: type[Relation],
    source: int,
) -> tuple[FactT, ...]:
    """Return the staged edges of one relation type leaving one entity."""

    return tuple(
        fact
        for fact in facts
        if isinstance(fact.component, relation_type) and fact.component.source == source
    )


def _edges_by_source[FactT: _ActivityFact](
    facts: Sequence[FactT],
    relation_type: type[Relation],
) -> dict[int, list[FactT]]:
    """Index staged edges of one relation type by their source entity."""

    indexed: dict[int, list[FactT]] = defaultdict(list)
    for fact in facts:
        component = fact.component
        if isinstance(component, relation_type):
            indexed[component.source].append(fact)
    return indexed


def _relation_count(facts: Sequence[_ActivityFact]) -> int:
    return sum(isinstance(fact.component, Relation) for fact in facts)


def _names_author_activity(
    component: Component,
    marker: CompleteAuthorActivityObservation,
    *,
    exact_sequence: bool,
) -> bool:
    """Return whether one output component names this marker's exact activity."""

    if getattr(component, "execution_id", None) != marker.execution_id:
        return False
    if getattr(component, "task_id", None) != marker.task_id:
        return False
    if getattr(component, "dispatch_id", None) != marker.activity_id:
        return False
    if exact_sequence:
        return getattr(component, "dispatch_sequence", None) == marker.dispatch_sequence
    return True


def _select_author_outputs(
    marker: CompleteAuthorActivityObservation,
    facts_by_type: Mapping[type[Component], Sequence[AuthorActivityEntityFact]],
) -> list[AuthorActivityEntityFact] | None:
    """Select every output fact the marker's declared counts name exactly."""

    groups: tuple[tuple[type[Component], int, bool], ...] = (
        (ValidationResult, marker.validation_count, True),
        (Commit, marker.commit_count, False),
        (FrictionLog, marker.friction_count, False),
        (Checkpoint, marker.checkpoint_count, False),
    )
    selected: list[AuthorActivityEntityFact] = []
    for component_type, expected_count, exact_sequence in groups:
        matching = tuple(
            fact
            for fact in facts_by_type.get(component_type, ())
            if _names_author_activity(fact.component, marker, exact_sequence=exact_sequence)
        )
        if len(matching) != int(expected_count):
            return None
        selected.extend(matching)
    return selected


def _select_author_provenance(
    facts_by_type: Mapping[type[Component], Sequence[AuthorActivityEntityFact]],
    outputs: Sequence[AuthorActivityEntityFact],
    execution_id: int,
) -> list[AuthorActivityEntityFact] | None:
    """Require exactly one ProducedBy edge from each output to the execution."""

    to_execution: dict[int, list[AuthorActivityEntityFact]] = defaultdict(list)
    for fact in facts_by_type.get(ProducedBy, ()):
        component = fact.component
        if isinstance(component, ProducedBy) and component.target == execution_id:
            to_execution[component.source].append(fact)
    selected: list[AuthorActivityEntityFact] = []
    for output in outputs:
        edges = to_execution.get(output.entity_id, ())
        if len(edges) != 1:
            return None
        selected.extend(edges)
    return selected


def _select_author_continuation(
    marker: CompleteAuthorActivityObservation,
    facts_by_type: Mapping[type[Component], Sequence[AuthorActivityEntityFact]],
) -> list[AuthorActivityEntityFact] | None:
    """Select the optional candidate continuation the marker's count names."""

    matching = tuple(
        fact
        for fact in facts_by_type.get(Candidate, ())
        if isinstance(fact.component, Candidate)
        and fact.component.task_id == marker.task_id
        and fact.component.dispatch_id == marker.activity_id
        and fact.component.dispatch_sequence == marker.dispatch_sequence
    )
    if not marker.candidate_count:
        return [] if not matching else None
    if len(matching) != 1 or matching[0].entity_id != marker.candidate_entity_id:
        return None
    candidate_id = marker.candidate_entity_id
    candidate_for = _edges_from(facts_by_type.get(CandidateFor, ()), CandidateFor, candidate_id)
    authored_by = _edges_from(facts_by_type.get(AuthoredBy, ()), AuthoredBy, candidate_id)
    supersedes = _edges_from(facts_by_type.get(Supersedes, ()), Supersedes, candidate_id)
    if len(candidate_for) != 1 or len(authored_by) != 1 or len(supersedes) > 1:
        return None
    return [matching[0], *candidate_for, *authored_by, *supersedes]


def select_complete_author_activity_facts(
    marker: CompleteAuthorActivityObservation,
    facts_by_type: Mapping[type[Component], Sequence[AuthorActivityEntityFact]],
) -> tuple[AuthorActivityEntityFact, ...] | None:
    """Select the exact fact set one author completion marker names."""

    sandbox = _fact_by_entity_id(facts_by_type.get(Sandbox, ()), Sandbox, marker.sandbox_entity_id)
    execution = _fact_by_entity_id(
        facts_by_type.get(AgentExecution, ()), AgentExecution, marker.execution_id
    )
    if sandbox is None or execution is None:
        return None

    membership = _edges_from(
        facts_by_type.get(PartOfMission, ()), PartOfMission, marker.sandbox_entity_id
    )
    if len(membership) != int(marker.sandbox_bound):
        return None

    executes = _edges_from(facts_by_type.get(Executes, ()), Executes, marker.execution_id)
    runs_in = _edges_from(facts_by_type.get(RunsIn, ()), RunsIn, marker.execution_id)
    if len(executes) != 1 or len(runs_in) != 1:
        return None

    outputs = _select_author_outputs(marker, facts_by_type)
    if outputs is None:
        return None
    provenance = _select_author_provenance(facts_by_type, outputs, marker.execution_id)
    if provenance is None:
        return None
    continuation = _select_author_continuation(marker, facts_by_type)
    if continuation is None:
        return None

    selected = (
        sandbox,
        execution,
        *membership,
        *executes,
        *runs_in,
        *outputs,
        *provenance,
        *continuation,
    )
    if _relation_count(selected) != int(marker.relation_count):
        return None
    return selected


def reconstruct_complete_author_activity_fact_bundle(
    marker: CompleteAuthorActivityObservation,
    facts_by_type: Mapping[
        type[Component],
        Sequence[AuthorActivityEntityFact],
    ],
) -> CompleteAuthorActivityFactBundle | None:
    """Select the exact v2 fact bundle named by one completion marker."""

    selected = select_complete_author_activity_facts(marker, facts_by_type)
    if selected is None:
        return None
    try:
        bundle = CompleteAuthorActivityFactBundle(
            facts=selected,
            execution_id=marker.execution_id,
            sandbox_entity_id=marker.sandbox_entity_id,
            candidate_entity_id=marker.candidate_entity_id,
            checkpoint_entity_id=marker.checkpoint_entity_id,
        )
    except ValueError:
        return None
    return bundle if bundle.digest == marker.fact_bundle_digest else None


def _critic_head_names_marker(
    sandbox: CriticActivityEntityFact,
    execution: CriticActivityEntityFact,
    marker: CompleteCriticActivityObservation,
) -> bool:
    """Return whether the staged sandbox and execution match the marker exactly."""

    sandbox_value = sandbox.component
    execution_value = execution.component
    if not isinstance(sandbox_value, Sandbox) or not isinstance(execution_value, CriticExecution):
        return False
    return (
        sandbox_value.sandbox_id == marker.critic_sandbox_id
        and execution_value.candidate_entity_id == marker.candidate_entity_id
        and execution_value.review_id == marker.activity_id
        and execution_value.attempt == marker.domain_review_attempt
        and execution_value.sandbox_id == marker.critic_sandbox_id
        and execution_value.redaction_policy_id == marker.redaction_policy_id
    )


def _sole_produced_edge(
    edges_by_source: Mapping[int, Sequence[CriticActivityEntityFact]],
    source: int,
    execution_id: int,
) -> CriticActivityEntityFact | None:
    """Require the source to publish exactly one edge, naming the execution."""

    edges = edges_by_source.get(source, ())
    if len(edges) != 1:
        return None
    component = edges[0].component
    if not isinstance(component, ProducedBy) or component.target != execution_id:
        return None
    return edges[0]


def _select_critic_findings(
    marker: CompleteCriticActivityObservation,
    facts_by_type: Mapping[type[Component], Sequence[CriticActivityEntityFact]],
    produced_by_source: Mapping[int, Sequence[CriticActivityEntityFact]],
) -> list[CriticActivityEntityFact] | None:
    """Select every finding the marker counts, each with its provenance edge."""

    findings = tuple(
        fact
        for fact in facts_by_type.get(CriticFinding, ())
        if isinstance(fact.component, CriticFinding)
        and fact.component.critic_execution_id == marker.execution_id
        and fact.component.candidate_entity_id == marker.candidate_entity_id
    )
    if len(findings) != int(marker.finding_count):
        return None
    selected: list[CriticActivityEntityFact] = list(findings)
    for finding in findings:
        edge = _sole_produced_edge(produced_by_source, finding.entity_id, marker.execution_id)
        if edge is None:
            return None
        selected.append(edge)
    return selected


def _select_critic_receipt(
    marker: CompleteCriticActivityObservation,
    facts_by_type: Mapping[type[Component], Sequence[CriticActivityEntityFact]],
    produced_by_source: Mapping[int, Sequence[CriticActivityEntityFact]],
) -> list[CriticActivityEntityFact] | None:
    """Select the optional settled receipt the marker's count names."""

    receipts = tuple(
        fact
        for fact in facts_by_type.get(CriticReceipt, ())
        if isinstance(fact.component, CriticReceipt)
        and fact.component.critic_execution_id == marker.execution_id
        and fact.component.candidate_entity_id == marker.candidate_entity_id
        and fact.component.review_id == marker.activity_id
    )
    if len(receipts) != int(marker.receipt_count):
        return None
    if not marker.receipt_count:
        return []
    if receipts[0].entity_id != marker.receipt_entity_id:
        return None
    edge = _sole_produced_edge(produced_by_source, marker.receipt_entity_id, marker.execution_id)
    if edge is None:
        return None
    return [receipts[0], edge]


def select_complete_critic_activity_facts(
    marker: CompleteCriticActivityObservation,
    facts_by_type: Mapping[type[Component], Sequence[CriticActivityEntityFact]],
) -> tuple[CriticActivityEntityFact, ...] | None:
    """Select the exact fact set one critic completion marker names."""

    sandbox = _fact_by_entity_id(facts_by_type.get(Sandbox, ()), Sandbox, marker.sandbox_entity_id)
    execution = _fact_by_entity_id(
        facts_by_type.get(CriticExecution, ()), CriticExecution, marker.execution_id
    )
    if sandbox is None or execution is None:
        return None
    if not _critic_head_names_marker(sandbox, execution, marker):
        return None

    reviews = _edges_from(facts_by_type.get(Reviews, ()), Reviews, marker.execution_id)
    runs_in = _edges_from(facts_by_type.get(RunsIn, ()), RunsIn, marker.execution_id)
    if len(reviews) != 1 or len(runs_in) != 1:
        return None
    review_edge = reviews[0].component
    runs_in_edge = runs_in[0].component
    if not isinstance(review_edge, Reviews) or review_edge.target != marker.candidate_entity_id:
        return None
    if not isinstance(runs_in_edge, RunsIn) or runs_in_edge.target != marker.sandbox_entity_id:
        return None

    produced_by_source = _edges_by_source(facts_by_type.get(ProducedBy, ()), ProducedBy)
    findings = _select_critic_findings(marker, facts_by_type, produced_by_source)
    if findings is None:
        return None
    receipt = _select_critic_receipt(marker, facts_by_type, produced_by_source)
    if receipt is None:
        return None

    selected = (sandbox, execution, *reviews, *runs_in, *findings, *receipt)
    if _relation_count(selected) != int(marker.relation_count):
        return None
    return selected


def reconstruct_complete_critic_activity_fact_bundle(
    marker: CompleteCriticActivityObservation,
    facts_by_type: Mapping[
        type[Component],
        Sequence[CriticActivityEntityFact],
    ],
) -> CompleteCriticActivityFactBundle | None:
    """Select the exact critic fact bundle named by one completion marker."""

    selected = select_complete_critic_activity_facts(marker, facts_by_type)
    if selected is None:
        return None
    try:
        bundle = CompleteCriticActivityFactBundle(
            facts=selected,
            execution_id=marker.execution_id,
            sandbox_entity_id=marker.sandbox_entity_id,
            receipt_entity_id=marker.receipt_entity_id,
        )
    except ValueError:
        return None
    return bundle if bundle.digest == marker.fact_bundle_digest else None
