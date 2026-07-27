# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Post-commit dispatch and terminal result projections for Agent Missions."""

from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, cast

from daft import DataFrame, Expression, col

from archetype.core.component import Component
from archetype.core.hooks import PostTick
from archetype.graph import GraphView, Relation
from archetype.missions.activities import (
    AuthorActivityEntityFact,
    CompleteAuthorActivityFactBundle,
    author_activity_fact_bundle_digest,
)
from archetype.missions.coding_agents.contracts import (
    CriticRepairFinding,
    DispatchedValidator,
    TaskDispatchRequest,
    ValidationObservation,
)
from archetype.missions.components import (
    AgentExecution,
    AuthorActivityObservation,
    Candidate,
    Checkpoint,
    Commit,
    CompleteAuthorActivityObservation,
    CompleteCriticActivityObservation,
    CriticExecution,
    CriticFinding,
    CriticReceipt,
    FrictionLog,
    Mission,
    MissionState,
    Sandbox,
    Task,
    TaskCriticPolicy,
    TaskCriticSubjectPolicy,
    TaskDispatch,
    TaskPolicy,
    TaskState,
    TaskValidator,
    TaskWorkspace,
    ValidationResult,
)
from archetype.missions.contracts import (
    CommandValidator,
    CriticPolicy,
    MissionResult,
    RepositoryPublicationPolicy,
    SubmittedMission,
    TaskResult,
)
from archetype.missions.critics.activity_facts import (
    CompleteCriticActivityFactBundle,
    CriticActivityEntityFact,
)
from archetype.missions.critics.contracts import (
    CandidateReviewRequest,
    CriticValidationEvidence,
    validator_bundle_digest,
)
from archetype.missions.relations import (
    AuthoredBy,
    CandidateFor,
    Executes,
    Guards,
    PartOfMission,
    ProducedBy,
    Reviews,
    RunsIn,
    Supersedes,
)
from archetype.missions.transitions import MissionStatus, TaskStatus


def _live_frame(event: PostTick, *components: type[Component]) -> DataFrame | None:
    columns = ["entity_id", "tick"]
    for component in components:
        prefix = component.get_prefix()
        columns.extend(f"{prefix}{field}" for field in component.model_fields)

    matches: list[DataFrame] = []
    for signature, frame in event.results.items():
        if all(component in signature for component in components):
            selected = frame
            if "is_active" in selected.column_names:
                selected = selected.where(col("is_active"))
            matches.append(selected.select(*columns))
    if not matches:
        return None
    result = matches[0]
    for match in matches[1:]:
        result = result.concat(match)
    return result


def _critic_subject_bounds(event: PostTick) -> dict[int, int]:
    """Return task-owned subject bounds without changing the legacy policy schema."""

    frame = _live_frame(event, TaskCriticSubjectPolicy)
    if frame is None:
        return {}
    subject = TaskCriticSubjectPolicy.get_prefix()
    return {
        entity_id: int(row[f"{subject}max_subject_bytes"])
        for entity_id, row in _latest_entity_rows(
            frame.to_pylist(),
            label="critic subject policy",
        ).items()
    }


def _latest_entity_rows(
    rows: Sequence[dict[str, Any]],
    *,
    label: str,
    allow_updates: bool = False,
) -> dict[int, dict[str, Any]]:
    """Index the latest committed row and reject ambiguity at one tick."""

    indexed: dict[int, dict[str, Any]] = {}
    for row in rows:
        entity_id = int(row["entity_id"])
        prior = indexed.get(entity_id)
        if prior is not None and not allow_updates:
            prior_value = {key: value for key, value in prior.items() if key != "tick"}
            row_value = {key: value for key, value in row.items() if key != "tick"}
            if prior_value != row_value:
                raise ValueError(f"{label} has conflicting committed rows")
        if prior is None or int(row["tick"]) > int(prior["tick"]):
            indexed[entity_id] = row
            continue
        if int(row["tick"]) == int(prior["tick"]) and prior != row:
            raise ValueError(f"{label} has conflicting committed rows")
    return indexed


class _TaskDispatchProjection:
    """Build bounded author requests from one committed snapshot."""

    _TASK_COMPONENTS = (
        Task,
        TaskWorkspace,
        TaskPolicy,
        TaskCriticPolicy,
        TaskState,
        TaskDispatch,
    )

    def __init__(self) -> None:
        self.requests: list[TaskDispatchRequest] = []

    async def on_post_tick(self, event: PostTick) -> None:
        task_frame = _live_frame(event, *self._TASK_COMPONENTS)
        mission_edges = _live_frame(event, PartOfMission)
        validator_frame = _live_frame(event, TaskValidator)
        guard_edges = _live_frame(event, Guards)
        if any(
            frame is None for frame in (task_frame, mission_edges, validator_frame, guard_edges)
        ):
            return
        assert task_frame is not None
        assert mission_edges is not None
        assert validator_frame is not None
        assert guard_edges is not None

        state = TaskState.get_prefix()
        dispatch = TaskDispatch.get_prefix()
        dispatched = task_frame.where(
            cast(
                Expression,
                col(f"{state}status") == TaskStatus.DISPATCHED.value,
            )
        )
        membership = PartOfMission.get_prefix()
        dispatched = dispatched.join(
            mission_edges.select(f"{membership}source", f"{membership}target"),
            left_on="entity_id",
            right_on=f"{membership}source",
        )
        task_rows = dispatched.to_pylist()
        if not task_rows:
            return
        subject_bounds = _critic_subject_bounds(event)

        validator = TaskValidator.get_prefix()
        guard = Guards.get_prefix()
        guarded_validators = guard_edges.join(
            validator_frame.select(
                col("entity_id").alias("_validator_id"),
                col(f"{validator}name").alias("_validator_name"),
                col(f"{validator}command").alias("_validator_command"),
                col(f"{validator}expected_returncode").alias("_validator_expected"),
                col(f"{validator}timeout_seconds").alias("_validator_timeout"),
            ),
            left_on=f"{guard}source",
            right_on="_validator_id",
        ).select(
            f"{guard}target",
            "_validator_id",
            "_validator_name",
            "_validator_command",
            "_validator_expected",
            "_validator_timeout",
        )
        validators_by_id: dict[int, CommandValidator] = {}
        validators_by_task: dict[int, list[DispatchedValidator]] = defaultdict(list)
        for row in guarded_validators.to_pylist():
            validator_id = int(row["_validator_id"])
            task_id = int(row[f"{guard}target"])
            spec = CommandValidator(
                name=str(row["_validator_name"]),
                command=tuple(str(argument) for argument in row["_validator_command"]),
                expected_returncode=int(row["_validator_expected"]),
                timeout_seconds=int(row["_validator_timeout"]),
            )
            validators_by_id[validator_id] = spec
            validators_by_task[task_id].append(DispatchedValidator(validator_id, spec))

        executions = _live_frame(event, AgentExecution)
        validation_results = _live_frame(event, ValidationResult)
        execution = AgentExecution.get_prefix()
        validation = ValidationResult.get_prefix()
        if executions is None:
            observation_rows: list[dict[str, Any]] = []
        else:
            observations = executions.select(
                col("entity_id").alias("_prior_execution_id"),
                col(f"{execution}task_id").alias("_prior_task_id"),
                col(f"{execution}dispatch_sequence").alias("_prior_sequence"),
                col(f"{execution}starting_revision").alias("_prior_starting_revision"),
                col(f"{execution}agent_session_id").alias("_prior_agent_session_id"),
            )
            if validation_results is not None:
                observations = observations.join(
                    validation_results.select(
                        col(f"{validation}execution_id").alias("_validation_execution_id"),
                        col(f"{validation}validator_id").alias("_validation_validator_id"),
                        col(f"{validation}expected_returncode").alias("_validation_expected"),
                        col(f"{validation}actual_returncode").alias("_validation_actual"),
                        col(f"{validation}revision").alias("_validation_revision"),
                        col(f"{validation}stdout").alias("_validation_stdout"),
                        col(f"{validation}stderr").alias("_validation_stderr"),
                    ),
                    left_on="_prior_execution_id",
                    right_on="_validation_execution_id",
                    how="left",
                )
            observation_rows = observations.to_pylist()
        record = Task.get_prefix()
        workspace = TaskWorkspace.get_prefix()
        policy = TaskPolicy.get_prefix()
        critic_policy = TaskCriticPolicy.get_prefix()
        candidate_history = _live_frame(event, Candidate)
        finding_history = _live_frame(event, CriticFinding)
        candidate_rows = candidate_history.to_pylist() if candidate_history is not None else []
        finding_rows = finding_history.to_pylist() if finding_history is not None else []
        candidate_record = Candidate.get_prefix()
        finding_record = CriticFinding.get_prefix()
        for row in task_rows:
            dispatch_id = str(row[f"{dispatch}dispatch_id"])
            task_id = int(row["entity_id"])
            sequence = int(row[f"{dispatch}sequence"])
            prior = [
                candidate
                for candidate in observation_rows
                if int(candidate["_prior_task_id"]) == task_id
                and int(candidate["_prior_sequence"]) < sequence
            ]
            previous = max(
                prior,
                key=lambda candidate: int(candidate["_prior_sequence"]),
                default=None,
            )
            previous_sequence = int(previous["_prior_sequence"]) if previous is not None else 0
            previous_validation = tuple(
                ValidationObservation(
                    validator_id=int(candidate["_validation_validator_id"]),
                    name=validators_by_id[int(candidate["_validation_validator_id"])].name,
                    command=validators_by_id[int(candidate["_validation_validator_id"])].command,
                    expected_returncode=int(candidate["_validation_expected"]),
                    actual_returncode=int(candidate["_validation_actual"]),
                    revision=str(candidate["_validation_revision"]),
                    stdout=str(candidate["_validation_stdout"]),
                    stderr=str(candidate["_validation_stderr"]),
                )
                for candidate in observation_rows
                if int(candidate["_prior_task_id"]) == task_id
                and int(candidate["_prior_sequence"]) == previous_sequence
                and candidate.get("_validation_validator_id") is not None
            )
            validators = tuple(
                sorted(validators_by_task[task_id], key=lambda item: item.validator_id)
            )
            prior_candidates = [
                item
                for item in candidate_rows
                if int(item[f"{candidate_record}task_id"]) == task_id
                and int(item[f"{candidate_record}dispatch_sequence"]) < sequence
            ]
            prior_candidate = max(
                prior_candidates,
                key=lambda item: int(item[f"{candidate_record}dispatch_sequence"]),
                default=None,
            )
            previous_critic_findings = (
                tuple(
                    CriticRepairFinding(
                        candidate_id=str(prior_candidate[f"{candidate_record}candidate_id"]),
                        finding_id=str(item[f"{finding_record}finding_id"]),
                        severity=str(item[f"{finding_record}severity"]),
                        category=str(item[f"{finding_record}category"]),
                        title=str(item[f"{finding_record}title"]),
                        detail=str(item[f"{finding_record}detail"]),
                        evidence_location=str(item[f"{finding_record}evidence_location"]),
                        reproduction=str(item[f"{finding_record}reproduction"]),
                    )
                    for item in finding_rows
                    if int(item[f"{finding_record}candidate_entity_id"])
                    == int(prior_candidate["entity_id"])
                )
                if prior_candidate is not None
                else ()
            )
            self.requests.append(
                TaskDispatchRequest(
                    mission_id=int(row[f"{membership}target"]),
                    task_id=task_id,
                    task_name=str(row[f"{record}name"]),
                    dispatch_id=dispatch_id,
                    dispatch_sequence=sequence,
                    repository=str(row[f"{workspace}repository"]),
                    branch=str(row[f"{workspace}branch"]),
                    base_ref=str(row[f"{workspace}base_ref"]),
                    prompt=str(row[f"{record}prompt"]),
                    validators=validators,
                    publication_policy=RepositoryPublicationPolicy(
                        str(row[f"{policy}publication_policy"])
                    ),
                    critic_policy=CriticPolicy(
                        policy_id=str(row[f"{critic_policy}policy_id"]),
                        version=str(row[f"{critic_policy}version"]),
                        perspective=str(row[f"{critic_policy}perspective"]),
                        information_view=str(row[f"{critic_policy}information_view"]),
                        driver=str(row[f"{critic_policy}driver"]),
                        model=str(row[f"{critic_policy}model"]),
                        sampling=str(row[f"{critic_policy}sampling"]),
                        max_reviews=int(row[f"{critic_policy}max_reviews"]),
                        timeout_seconds=int(row[f"{critic_policy}timeout_seconds"]),
                        output_schema_version=int(row[f"{critic_policy}output_schema_version"]),
                        max_output_chars=int(row[f"{critic_policy}max_output_chars"]),
                        max_subject_bytes=subject_bounds.get(
                            task_id,
                            CriticPolicy().max_subject_bytes,
                        ),
                    ),
                    prior_candidate_entity_id=(
                        int(prior_candidate["entity_id"]) if prior_candidate is not None else 0
                    ),
                    task_base_revision=(
                        str(previous["_prior_starting_revision"]) if previous is not None else ""
                    ),
                    previous_agent_session_id=(
                        str(previous["_prior_agent_session_id"]) if previous is not None else ""
                    ),
                    previous_validation=previous_validation,
                    previous_critic_findings=previous_critic_findings,
                )
            )


async def project_task_dispatch_requests(event: PostTick) -> tuple[TaskDispatchRequest, ...]:
    """Project author requests from one exact committed mission snapshot."""

    projection = _TaskDispatchProjection()
    await projection.on_post_tick(event)
    return tuple(projection.requests)


def project_complete_author_activity_observations(
    event: PostTick,
) -> tuple[AuthorActivityObservation, ...]:
    """Return markers whose exact factual bundle is present in the snapshot."""

    markers = _live_frame(event, AuthorActivityObservation)
    executions = _live_frame(event, AgentExecution)
    if markers is None or executions is None:
        return ()
    validations = _live_frame(event, ValidationResult)
    commits = _live_frame(event, Commit)
    friction = _live_frame(event, FrictionLog)
    marker = AuthorActivityObservation.get_prefix()
    execution = AgentExecution.get_prefix()
    validation = ValidationResult.get_prefix()
    commit = Commit.get_prefix()
    friction_record = FrictionLog.get_prefix()
    execution_rows = executions.to_pylist()
    validation_rows = validations.to_pylist() if validations is not None else []
    commit_rows = commits.to_pylist() if commits is not None else []
    friction_rows = friction.to_pylist() if friction is not None else []
    complete: list[AuthorActivityObservation] = []
    for row in markers.to_pylist():
        activity_id = str(row[f"{marker}activity_id"])
        task_id = int(row[f"{marker}task_id"])
        execution_id = int(row[f"{marker}execution_id"])
        sequence = int(row[f"{marker}dispatch_sequence"])
        matching_execution = [
            candidate
            for candidate in execution_rows
            if int(candidate["entity_id"]) == execution_id
            and int(candidate[f"{execution}task_id"]) == task_id
            and str(candidate[f"{execution}dispatch_id"]) == activity_id
            and int(candidate[f"{execution}dispatch_sequence"]) == sequence
        ]
        matching_validations = [
            candidate
            for candidate in validation_rows
            if int(candidate[f"{validation}execution_id"]) == execution_id
            and int(candidate[f"{validation}task_id"]) == task_id
            and str(candidate[f"{validation}dispatch_id"]) == activity_id
            and int(candidate[f"{validation}dispatch_sequence"]) == sequence
        ]
        matching_commits = [
            candidate
            for candidate in commit_rows
            if int(candidate[f"{commit}execution_id"]) == execution_id
            and int(candidate[f"{commit}task_id"]) == task_id
            and str(candidate[f"{commit}dispatch_id"]) == activity_id
        ]
        matching_friction = [
            candidate
            for candidate in friction_rows
            if int(candidate[f"{friction_record}execution_id"]) == execution_id
            and int(candidate[f"{friction_record}task_id"]) == task_id
            and str(candidate[f"{friction_record}dispatch_id"]) == activity_id
        ]
        if (
            len(matching_execution) != 1
            or len(matching_validations) != int(row[f"{marker}validation_count"])
            or len(matching_commits) != int(row[f"{marker}commit_count"])
            or len(matching_friction) != int(row[f"{marker}friction_count"])
            or str(matching_execution[0][f"{execution}redaction_policy_id"])
            != str(row[f"{marker}redaction_policy_id"])
        ):
            continue
        execution_fact = AgentExecution(
            **{
                field: matching_execution[0][f"{execution}{field}"]
                for field in AgentExecution.model_fields
            }
        )
        validation_facts = tuple(
            ValidationResult(
                **{
                    field: candidate[f"{validation}{field}"]
                    for field in ValidationResult.model_fields
                }
            )
            for candidate in matching_validations
        )
        commit_facts = tuple(
            Commit(**{field: candidate[f"{commit}{field}"] for field in Commit.model_fields})
            for candidate in matching_commits
        )
        friction_facts = tuple(
            FrictionLog(
                **{
                    field: candidate[f"{friction_record}{field}"]
                    for field in FrictionLog.model_fields
                }
            )
            for candidate in matching_friction
        )
        bundle_digest = author_activity_fact_bundle_digest(
            execution_id=execution_id,
            execution=execution_fact,
            validations=validation_facts,
            commits=commit_facts,
            friction=friction_facts,
        )
        if bundle_digest != str(row[f"{marker}fact_bundle_digest"]):
            continue
        complete.append(
            AuthorActivityObservation(
                activity_id=activity_id,
                task_id=task_id,
                dispatch_sequence=sequence,
                result_ref=str(row[f"{marker}result_ref"]),
                result_digest=str(row[f"{marker}result_digest"]),
                fact_bundle_digest=bundle_digest,
                execution_id=execution_id,
                validation_count=int(row[f"{marker}validation_count"]),
                commit_count=int(row[f"{marker}commit_count"]),
                friction_count=int(row[f"{marker}friction_count"]),
                redaction_policy_id=str(row[f"{marker}redaction_policy_id"]),
            )
        )
    return tuple(sorted(complete, key=lambda value: value.activity_id))


@dataclass(frozen=True, slots=True)
class ProjectedAuthorActivityFactBundle:
    """One v2 completion marker and the exact facts named by its digest."""

    marker: CompleteAuthorActivityObservation
    bundle: CompleteAuthorActivityFactBundle


def _component_facts(
    event: PostTick,
    component_type: type[Component],
) -> tuple[AuthorActivityEntityFact, ...]:
    frame = _live_frame(event, component_type)
    if frame is None:
        return ()
    prefix = component_type.get_prefix()
    return tuple(
        AuthorActivityEntityFact(
            entity_id=int(row["entity_id"]),
            component=component_type(
                **{field: row[f"{prefix}{field}"] for field in component_type.model_fields}
            ),
        )
        for row in frame.to_pylist()
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


def reconstruct_complete_author_activity_fact_bundle(
    marker: CompleteAuthorActivityObservation,
    facts_by_type: Mapping[
        type[Component],
        Sequence[AuthorActivityEntityFact],
    ],
) -> CompleteAuthorActivityFactBundle | None:
    """Select the exact v2 fact bundle named by one completion marker."""

    facts = {
        component_type: tuple(facts_by_type.get(component_type, ()))
        for component_type in COMPLETE_AUTHOR_ACTIVITY_FACT_TYPES
    }
    by_type_and_id = {
        component_type: {fact.entity_id: fact for fact in component_facts}
        for component_type, component_facts in facts.items()
    }

    sandbox = by_type_and_id[Sandbox].get(marker.sandbox_entity_id)
    execution = by_type_and_id[AgentExecution].get(marker.execution_id)
    if sandbox is None or execution is None:
        return None
    selected: list[AuthorActivityEntityFact] = [sandbox, execution]

    membership = tuple(
        fact
        for fact in facts[PartOfMission]
        if isinstance(fact.component, PartOfMission)
        and fact.component.source == marker.sandbox_entity_id
    )
    if len(membership) != int(marker.sandbox_bound):
        return None
    selected.extend(membership)

    executes = tuple(
        fact
        for fact in facts[Executes]
        if isinstance(fact.component, Executes) and fact.component.source == marker.execution_id
    )
    runs_in = tuple(
        fact
        for fact in facts[RunsIn]
        if isinstance(fact.component, RunsIn) and fact.component.source == marker.execution_id
    )
    if len(executes) != 1 or len(runs_in) != 1:
        return None
    selected.extend((*executes, *runs_in))

    output_groups: tuple[tuple[type[Component], int], ...] = (
        (ValidationResult, marker.validation_count),
        (Commit, marker.commit_count),
        (FrictionLog, marker.friction_count),
        (Checkpoint, marker.checkpoint_count),
    )
    output_facts: list[AuthorActivityEntityFact] = []
    for component_type, expected_count in output_groups:
        matching = tuple(
            fact
            for fact in facts[component_type]
            if getattr(fact.component, "execution_id", None) == marker.execution_id
            and getattr(fact.component, "task_id", None) == marker.task_id
            and getattr(fact.component, "dispatch_id", None) == marker.activity_id
            and (
                component_type is not ValidationResult
                or getattr(fact.component, "dispatch_sequence", None) == marker.dispatch_sequence
            )
        )
        if len(matching) != expected_count:
            return None
        output_facts.extend(matching)

    produced: list[AuthorActivityEntityFact] = []
    for output in output_facts:
        matching_edges = tuple(
            fact
            for fact in facts[ProducedBy]
            if isinstance(fact.component, ProducedBy)
            and fact.component.source == output.entity_id
            and fact.component.target == marker.execution_id
        )
        if len(matching_edges) != 1:
            return None
        produced.extend(matching_edges)
    selected.extend((*output_facts, *produced))

    matching_candidates = tuple(
        fact
        for fact in facts[Candidate]
        if isinstance(fact.component, Candidate)
        and fact.component.task_id == marker.task_id
        and fact.component.dispatch_id == marker.activity_id
        and fact.component.dispatch_sequence == marker.dispatch_sequence
    )
    if marker.candidate_count:
        if (
            len(matching_candidates) != 1
            or matching_candidates[0].entity_id != marker.candidate_entity_id
        ):
            return None
        candidate_id = marker.candidate_entity_id
        candidate_for = tuple(
            fact
            for fact in facts[CandidateFor]
            if isinstance(fact.component, CandidateFor) and fact.component.source == candidate_id
        )
        authored_by = tuple(
            fact
            for fact in facts[AuthoredBy]
            if isinstance(fact.component, AuthoredBy) and fact.component.source == candidate_id
        )
        supersedes = tuple(
            fact
            for fact in facts[Supersedes]
            if isinstance(fact.component, Supersedes) and fact.component.source == candidate_id
        )
        if len(candidate_for) != 1 or len(authored_by) != 1 or len(supersedes) > 1:
            return None
        selected.extend(
            (
                matching_candidates[0],
                *candidate_for,
                *authored_by,
                *supersedes,
            )
        )
    elif matching_candidates:
        return None

    relation_count = sum(isinstance(fact.component, Relation) for fact in selected)
    if relation_count != marker.relation_count:
        return None
    try:
        bundle = CompleteAuthorActivityFactBundle(
            facts=tuple(selected),
            execution_id=marker.execution_id,
            sandbox_entity_id=marker.sandbox_entity_id,
            candidate_entity_id=marker.candidate_entity_id,
            checkpoint_entity_id=marker.checkpoint_entity_id,
        )
    except ValueError:
        return None
    return bundle if bundle.digest == marker.fact_bundle_digest else None


def project_complete_author_activity_fact_bundles(
    event: PostTick,
) -> tuple[ProjectedAuthorActivityFactBundle, ...]:
    """Reconstruct v2 author observations only when every named fact is present.

    The marker is not proof by itself. Its exact entity identities, counts, and
    digest must select one sandbox, one execution, every output/provenance
    pair, and the optional candidate continuation from the same snapshot.
    Semantic equality with the durable request/result is checked by the
    Mission projector after this structural reconstruction.
    """

    marker_facts = _component_facts(event, CompleteAuthorActivityObservation)
    if not marker_facts:
        return ()
    facts_by_type = {
        component_type: _component_facts(event, component_type)
        for component_type in COMPLETE_AUTHOR_ACTIVITY_FACT_TYPES
    }
    marker_counts = Counter(
        fact.component.activity_id
        for fact in marker_facts
        if isinstance(fact.component, CompleteAuthorActivityObservation)
    )

    projected: list[ProjectedAuthorActivityFactBundle] = []
    for marker_fact in marker_facts:
        marker = marker_fact.component
        assert isinstance(marker, CompleteAuthorActivityObservation)
        if marker_counts[marker.activity_id] != 1:
            continue
        bundle = reconstruct_complete_author_activity_fact_bundle(marker, facts_by_type)
        if bundle is not None:
            projected.append(ProjectedAuthorActivityFactBundle(marker=marker, bundle=bundle))

    return tuple(sorted(projected, key=lambda value: value.marker.activity_id))


@dataclass(frozen=True, slots=True)
class ProjectedCriticActivityFactBundle:
    """One complete critic marker and the exact facts named by its digest."""

    marker: CompleteCriticActivityObservation
    bundle: CompleteCriticActivityFactBundle


def _critic_component_facts(
    event: PostTick,
    component_type: type[Component],
) -> tuple[CriticActivityEntityFact, ...]:
    frame = _live_frame(event, component_type)
    if frame is None:
        return ()
    prefix = component_type.get_prefix()
    return tuple(
        CriticActivityEntityFact(
            entity_id=int(row["entity_id"]),
            component=component_type(
                **{field: row[f"{prefix}{field}"] for field in component_type.model_fields}
            ),
        )
        for row in frame.to_pylist()
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


def reconstruct_complete_critic_activity_fact_bundle(
    marker: CompleteCriticActivityObservation,
    facts_by_type: Mapping[
        type[Component],
        Sequence[CriticActivityEntityFact],
    ],
) -> CompleteCriticActivityFactBundle | None:
    """Select the exact critic fact bundle named by one completion marker."""

    facts = {
        component_type: tuple(facts_by_type.get(component_type, ()))
        for component_type in COMPLETE_CRITIC_ACTIVITY_FACT_TYPES
    }
    by_type_and_id = {
        component_type: {fact.entity_id: fact for fact in component_facts}
        for component_type, component_facts in facts.items()
    }
    sandbox = by_type_and_id[Sandbox].get(marker.sandbox_entity_id)
    execution = by_type_and_id[CriticExecution].get(marker.execution_id)
    if sandbox is None or execution is None:
        return None
    sandbox_value = sandbox.component
    execution_value = execution.component
    assert isinstance(sandbox_value, Sandbox)
    assert isinstance(execution_value, CriticExecution)
    if (
        sandbox_value.sandbox_id != marker.critic_sandbox_id
        or execution_value.candidate_entity_id != marker.candidate_entity_id
        or execution_value.review_id != marker.activity_id
        or execution_value.attempt != marker.domain_review_attempt
        or execution_value.sandbox_id != marker.critic_sandbox_id
        or execution_value.redaction_policy_id != marker.redaction_policy_id
    ):
        return None
    selected: list[CriticActivityEntityFact] = [sandbox, execution]

    reviews = tuple(
        fact
        for fact in facts[Reviews]
        if isinstance(fact.component, Reviews) and fact.component.source == marker.execution_id
    )
    runs_in = tuple(
        fact
        for fact in facts[RunsIn]
        if isinstance(fact.component, RunsIn) and fact.component.source == marker.execution_id
    )
    review_edge = reviews[0].component if len(reviews) == 1 else None
    runs_in_edge = runs_in[0].component if len(runs_in) == 1 else None
    if (
        not isinstance(review_edge, Reviews)
        or review_edge.target != marker.candidate_entity_id
        or not isinstance(runs_in_edge, RunsIn)
        or runs_in_edge.target != marker.sandbox_entity_id
    ):
        return None
    selected.extend((*reviews, *runs_in))

    findings = tuple(
        fact
        for fact in facts[CriticFinding]
        if isinstance(fact.component, CriticFinding)
        and fact.component.critic_execution_id == marker.execution_id
        and fact.component.candidate_entity_id == marker.candidate_entity_id
    )
    if len(findings) != marker.finding_count:
        return None
    finding_edges: list[CriticActivityEntityFact] = []
    for finding in findings:
        produced = tuple(
            fact
            for fact in facts[ProducedBy]
            if isinstance(fact.component, ProducedBy) and fact.component.source == finding.entity_id
        )
        produced_edge = produced[0].component if len(produced) == 1 else None
        if not isinstance(produced_edge, ProducedBy) or produced_edge.target != marker.execution_id:
            return None
        finding_edges.extend(produced)
    selected.extend((*findings, *finding_edges))

    receipts = tuple(
        fact
        for fact in facts[CriticReceipt]
        if isinstance(fact.component, CriticReceipt)
        and fact.component.critic_execution_id == marker.execution_id
        and fact.component.candidate_entity_id == marker.candidate_entity_id
        and fact.component.review_id == marker.activity_id
    )
    if len(receipts) != marker.receipt_count:
        return None
    if marker.receipt_count:
        if receipts[0].entity_id != marker.receipt_entity_id:
            return None
        produced = tuple(
            fact
            for fact in facts[ProducedBy]
            if isinstance(fact.component, ProducedBy)
            and fact.component.source == marker.receipt_entity_id
        )
        produced_edge = produced[0].component if len(produced) == 1 else None
        if not isinstance(produced_edge, ProducedBy) or produced_edge.target != marker.execution_id:
            return None
        selected.extend((receipts[0], produced[0]))

    relation_count = sum(isinstance(fact.component, Relation) for fact in selected)
    if relation_count != marker.relation_count:
        return None
    try:
        bundle = CompleteCriticActivityFactBundle(
            facts=tuple(selected),
            execution_id=marker.execution_id,
            sandbox_entity_id=marker.sandbox_entity_id,
            receipt_entity_id=marker.receipt_entity_id,
        )
    except ValueError:
        return None
    return bundle if bundle.digest == marker.fact_bundle_digest else None


def project_complete_critic_activity_fact_bundles(
    event: PostTick,
) -> tuple[ProjectedCriticActivityFactBundle, ...]:
    """Reconstruct critic observations only when every named fact is present."""

    marker_facts = _critic_component_facts(event, CompleteCriticActivityObservation)
    if not marker_facts:
        return ()
    facts_by_type = {
        component_type: _critic_component_facts(event, component_type)
        for component_type in COMPLETE_CRITIC_ACTIVITY_FACT_TYPES
    }
    marker_counts = Counter(
        fact.component.activity_id
        for fact in marker_facts
        if isinstance(fact.component, CompleteCriticActivityObservation)
    )
    projected: list[ProjectedCriticActivityFactBundle] = []
    for marker_fact in marker_facts:
        marker = marker_fact.component
        assert isinstance(marker, CompleteCriticActivityObservation)
        if marker_counts[marker.activity_id] != 1:
            continue
        bundle = reconstruct_complete_critic_activity_fact_bundle(
            marker,
            facts_by_type,
        )
        if bundle is not None:
            projected.append(
                ProjectedCriticActivityFactBundle(
                    marker=marker,
                    bundle=bundle,
                )
            )
    return tuple(sorted(projected, key=lambda value: value.marker.activity_id))


@dataclass(frozen=True)
class CriticReviewBudgetExhausted:
    """One candidate remains pending after its independent review budget."""

    mission_id: int
    task_id: int
    candidate_id: str
    attempts: int
    max_reviews: int


@dataclass(frozen=True, slots=True)
class CriticActivityIntentProjection:
    """Pure committed-snapshot projection of critic requests and exhaustion."""

    requests: tuple[CandidateReviewRequest, ...]
    exhausted: tuple[CriticReviewBudgetExhausted, ...]


async def project_critic_activity_intents(
    event: PostTick,
) -> CriticActivityIntentProjection:
    """Project exact-candidate review intent without process-local queue state."""

    tasks = _live_frame(event, Task, TaskCriticPolicy, TaskState)
    candidates = _live_frame(event, Candidate)
    author_executions = _live_frame(event, AgentExecution)
    validators = _live_frame(event, TaskValidator)
    validation_results = _live_frame(event, ValidationResult)
    if any(
        frame is None
        for frame in (
            tasks,
            candidates,
            author_executions,
            validators,
            validation_results,
        )
    ):
        return CriticActivityIntentProjection((), ())
    assert tasks is not None
    assert candidates is not None
    assert author_executions is not None
    assert validators is not None
    assert validation_results is not None

    state = TaskState.get_prefix()
    task = Task.get_prefix()
    policy = TaskCriticPolicy.get_prefix()
    candidate = Candidate.get_prefix()
    author = AgentExecution.get_prefix()
    validator = TaskValidator.get_prefix()
    validation = ValidationResult.get_prefix()

    candidate_tasks = tasks.where(
        cast(Expression, col(f"{state}status") == TaskStatus.CANDIDATE.value)
    )
    candidate_task_rows = _latest_entity_rows(
        candidate_tasks.to_pylist(),
        label="candidate task",
        allow_updates=True,
    )
    candidate_rows = _latest_entity_rows(
        candidates.to_pylist(),
        label="candidate",
    )
    rows: list[dict[str, Any]] = []
    for candidate_entity_id, candidate_row in candidate_rows.items():
        task_id = int(candidate_row[f"{candidate}task_id"])
        task_row = candidate_task_rows.get(task_id)
        if task_row is None:
            continue
        joined = dict(task_row)
        joined.update(
            {key: value for key, value in candidate_row.items() if key not in {"entity_id", "tick"}}
        )
        joined["_candidate_entity_id"] = candidate_entity_id
        rows.append(joined)
    if not rows:
        return CriticActivityIntentProjection((), ())
    subject_bounds = _critic_subject_bounds(event)

    candidates_by_task: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        candidates_by_task[int(row["entity_id"])].append(row)

    current_by_task: dict[int, dict[str, Any]] = {}
    for task_id, task_candidates in candidates_by_task.items():
        current_sequence = max(int(row[f"{candidate}dispatch_sequence"]) for row in task_candidates)
        current = tuple(
            row
            for row in task_candidates
            if int(row[f"{candidate}dispatch_sequence"]) == current_sequence
        )
        current_ids = {int(row["_candidate_entity_id"]) for row in current}
        if len(current_ids) != 1:
            raise ValueError("task has multiple current candidates at one dispatch sequence")
        first = current[0]
        if any(row != first for row in current[1:]):
            raise ValueError("task current candidate has conflicting committed rows")
        current_by_task[task_id] = first

    author_rows = _latest_entity_rows(
        author_executions.to_pylist(),
        label="author execution",
    )
    validator_rows = _latest_entity_rows(
        validators.to_pylist(),
        label="task validator",
    )
    validation_rows = tuple(
        _latest_entity_rows(
            validation_results.to_pylist(),
            label="validation result",
        ).values()
    )
    critic_executions = _live_frame(event, CriticExecution)
    critic_receipts = _live_frame(event, CriticReceipt)
    critic_execution_rows = (
        tuple(
            _latest_entity_rows(
                critic_executions.to_pylist(),
                label="critic execution",
            ).values()
        )
        if critic_executions is not None
        else ()
    )
    receipt_rows = (
        tuple(
            _latest_entity_rows(
                critic_receipts.to_pylist(),
                label="critic receipt",
            ).values()
        )
        if critic_receipts is not None
        else ()
    )
    critic_execution = CriticExecution.get_prefix()
    receipt = CriticReceipt.get_prefix()

    requests: list[CandidateReviewRequest] = []
    exhausted: list[CriticReviewBudgetExhausted] = []
    for row in current_by_task.values():
        task_id = int(row["entity_id"])
        candidate_entity_id = int(row["_candidate_entity_id"])
        candidate_id = str(row[f"{candidate}candidate_id"])
        if any(
            int(item[f"{receipt}candidate_entity_id"]) == candidate_entity_id
            and str(item[f"{receipt}critic_sandbox_id"])
            != str(row[f"{candidate}author_sandbox_id"])
            and str(item[f"{receipt}candidate_digest"]) == str(row[f"{candidate}candidate_digest"])
            and str(item[f"{receipt}policy_digest"]) == str(row[f"{candidate}policy_digest"])
            and str(item[f"{receipt}reviewed_base_revision"])
            == str(row[f"{candidate}base_revision"])
            and str(item[f"{receipt}reviewed_head_revision"])
            == str(row[f"{candidate}head_revision"])
            and str(item[f"{receipt}reviewed_diff_digest"]) == str(row[f"{candidate}diff_digest"])
            and str(item[f"{receipt}validator_bundle_digest"])
            == str(row[f"{candidate}validator_bundle_digest"])
            for item in receipt_rows
        ):
            continue
        attempts = sum(
            int(item[f"{critic_execution}candidate_entity_id"]) == candidate_entity_id
            for item in critic_execution_rows
        )
        max_reviews = int(row[f"{policy}max_reviews"])
        if attempts >= max_reviews:
            exhausted.append(
                CriticReviewBudgetExhausted(
                    mission_id=int(row[f"{candidate}mission_id"]),
                    task_id=int(row[f"{candidate}task_id"]),
                    candidate_id=candidate_id,
                    attempts=attempts,
                    max_reviews=max_reviews,
                )
            )
            continue
        attempt = attempts + 1
        critic_policy = CriticPolicy(
            policy_id=str(row[f"{policy}policy_id"]),
            version=str(row[f"{policy}version"]),
            perspective=str(row[f"{policy}perspective"]),
            information_view=str(row[f"{policy}information_view"]),
            driver=str(row[f"{policy}driver"]),
            model=str(row[f"{policy}model"]),
            sampling=str(row[f"{policy}sampling"]),
            max_reviews=max_reviews,
            timeout_seconds=int(row[f"{policy}timeout_seconds"]),
            output_schema_version=int(row[f"{policy}output_schema_version"]),
            max_output_chars=int(row[f"{policy}max_output_chars"]),
            max_subject_bytes=subject_bounds.get(
                task_id,
                CriticPolicy().max_subject_bytes,
            ),
        )
        persisted_policy_digests = {
            str(row[f"{policy}digest"]),
            str(row[f"{candidate}policy_digest"]),
        }
        if persisted_policy_digests != {critic_policy.digest}:
            raise ValueError("critic policy digest does not match the committed task and candidate")
        author_execution_id = int(row[f"{candidate}author_execution_id"])
        try:
            author_row = author_rows[author_execution_id]
        except KeyError:
            raise ValueError("candidate author execution is not committed") from None
        candidate_author_sandbox_id = str(row[f"{candidate}author_sandbox_id"])
        expected_author_identity = (
            task_id,
            str(row[f"{candidate}dispatch_id"]),
            int(row[f"{candidate}dispatch_sequence"]),
            candidate_author_sandbox_id,
            str(row[f"{candidate}base_revision"]),
            str(row[f"{candidate}head_revision"]),
        )
        observed_author_identity = (
            int(author_row[f"{author}task_id"]),
            str(author_row[f"{author}dispatch_id"]),
            int(author_row[f"{author}dispatch_sequence"]),
            str(author_row[f"{author}sandbox_id"]),
            str(author_row[f"{author}starting_revision"]),
            str(author_row[f"{author}final_revision"]),
        )
        if observed_author_identity != expected_author_identity:
            raise ValueError("candidate identity does not match its committed author execution")

        exact_validation_by_id: dict[int, CriticValidationEvidence] = {}
        for item in validation_rows:
            if int(item[f"{validation}execution_id"]) != author_execution_id:
                continue
            validator_id = int(item[f"{validation}validator_id"])
            if validator_id in exact_validation_by_id:
                raise ValueError("candidate has duplicate validator observations for one execution")
            try:
                validator_row = validator_rows[validator_id]
            except KeyError:
                raise ValueError("candidate validation refers to an unknown validator") from None
            if int(item[f"{validation}expected_returncode"]) != int(
                validator_row[f"{validator}expected_returncode"]
            ):
                raise ValueError(
                    "candidate validation expectation does not match its validator definition"
                )
            validation_identity = (
                int(item[f"{validation}task_id"]),
                str(item[f"{validation}dispatch_id"]),
                int(item[f"{validation}dispatch_sequence"]),
                str(item[f"{validation}revision"]),
            )
            expected_validation_identity = (
                task_id,
                str(row[f"{candidate}dispatch_id"]),
                int(row[f"{candidate}dispatch_sequence"]),
                str(row[f"{candidate}head_revision"]),
            )
            if validation_identity != expected_validation_identity:
                raise ValueError(
                    "candidate validation does not match its committed author execution"
                )
            exact_validation_by_id[validator_id] = CriticValidationEvidence(
                validator_id=int(item[f"{validation}validator_id"]),
                name=str(validator_row[f"{validator}name"]),
                command=tuple(str(argument) for argument in validator_row[f"{validator}command"]),
                expected_returncode=int(item[f"{validation}expected_returncode"]),
                actual_returncode=int(item[f"{validation}actual_returncode"]),
                revision=str(item[f"{validation}revision"]),
                stdout=str(item[f"{validation}stdout"]),
                stderr=str(item[f"{validation}stderr"]),
            )
        exact_validation = tuple(
            exact_validation_by_id[validator_id] for validator_id in sorted(exact_validation_by_id)
        )
        observed_validator_bundle_digest = validator_bundle_digest(
            tuple(
                (
                    validator_id,
                    str(validator_rows[validator_id][f"{validator}name"]),
                    tuple(
                        str(argument)
                        for argument in validator_rows[validator_id][f"{validator}command"]
                    ),
                    int(validator_rows[validator_id][f"{validator}expected_returncode"]),
                    int(validator_rows[validator_id][f"{validator}timeout_seconds"]),
                )
                for validator_id in sorted(exact_validation_by_id)
            )
        )
        if observed_validator_bundle_digest != str(row[f"{candidate}validator_bundle_digest"]):
            raise ValueError(
                "candidate validator bundle does not match its committed validation evidence"
            )
        request = CandidateReviewRequest(
            candidate_entity_id=candidate_entity_id,
            candidate_id=candidate_id,
            mission_id=int(row[f"{candidate}mission_id"]),
            task_id=int(row[f"{candidate}task_id"]),
            task_name=str(row[f"{task}name"]),
            task_prompt=str(row[f"{task}prompt"]),
            dispatch_id=str(row[f"{candidate}dispatch_id"]),
            dispatch_sequence=int(row[f"{candidate}dispatch_sequence"]),
            author_execution_id=author_execution_id,
            author_sandbox_id=candidate_author_sandbox_id,
            repository=str(row[f"{candidate}repository"]),
            branch=str(row[f"{candidate}branch"]),
            base_ref=str(row[f"{candidate}base_ref"]),
            base_revision=str(row[f"{candidate}base_revision"]),
            head_revision=str(row[f"{candidate}head_revision"]),
            diff_digest=str(row[f"{candidate}diff_digest"]),
            validator_bundle_digest=str(row[f"{candidate}validator_bundle_digest"]),
            policy=critic_policy,
            validation=exact_validation,
            candidate_published_at_ms=int(row[f"{candidate}created_at_ms"]),
            attempt=attempt,
        )
        if request.candidate_digest != str(row[f"{candidate}candidate_digest"]):
            raise ValueError("candidate digest does not match its committed subject")
        requests.append(request)

    return CriticActivityIntentProjection(
        requests=tuple(sorted(requests, key=lambda request: request.review_id)),
        exhausted=tuple(
            sorted(
                exhausted,
                key=lambda item: (item.task_id, item.candidate_id),
            )
        ),
    )


async def project_critic_activity_requests(
    event: PostTick,
) -> tuple[CandidateReviewRequest, ...]:
    """Return idempotent exact-candidate requests for generic Activity admission."""

    return (await project_critic_activity_intents(event)).requests


def current_mission_status(view: GraphView, mission_id: int) -> MissionStatus | None:
    """Project one mission's current persisted decision state."""

    frame = view.frame(MissionState)
    if frame is None:
        return None
    state = MissionState.get_prefix()
    rows = (
        frame.where(cast(Expression, col("entity_id") == mission_id))
        .select(f"{state}status")
        .to_pylist()
    )
    if not rows:
        return None
    return MissionStatus(rows[0][f"{state}status"])


def project_mission_result(
    view: GraphView,
    submitted: SubmittedMission,
    *,
    ticks_completed: int,
) -> MissionResult:
    """Materialize the bounded terminal projection returned to an author."""

    mission_frame = view.frame(Mission, MissionState)
    task_frame = view.frame(Task, TaskState, TaskDispatch)
    if mission_frame is None or task_frame is None:
        raise RuntimeError("terminal mission state is not queryable")

    mission_rows = mission_frame.where(
        cast(Expression, col("entity_id") == submitted.mission_id)
    ).to_pylist()
    if len(mission_rows) != 1:
        raise RuntimeError("terminal mission projection is not unique")
    mission_row = mission_rows[0]

    task_ids = dict(submitted.task_ids)
    task_projection = task_frame.where(col("entity_id").is_in(list(task_ids.values())))
    commits = view.frame(Commit)
    commit = Commit.get_prefix()
    if commits is not None:
        task_projection = task_projection.join(
            commits.select(
                col(f"{commit}task_id").alias("_commit_task_id"),
                col(f"{commit}sha").alias("_commit_sha"),
            ),
            left_on="entity_id",
            right_on="_commit_task_id",
            how="left",
        )
    projection_rows = task_projection.to_pylist()
    task_rows: dict[int, dict[str, Any]] = {}
    commits_by_task: dict[int, list[str]] = {}
    for row in projection_rows:
        task_id = int(row["entity_id"])
        task_rows.setdefault(task_id, row)
        if row.get("_commit_sha") is not None:
            sha = str(row["_commit_sha"])
            task_commits = commits_by_task.setdefault(task_id, [])
            if sha not in task_commits:
                task_commits.append(sha)

    task = Task.get_prefix()
    state = TaskState.get_prefix()
    dispatch = TaskDispatch.get_prefix()
    tasks = tuple(
        TaskResult(
            task_id=task_id,
            name=str(task_rows[task_id][f"{task}name"]),
            status=str(task_rows[task_id][f"{state}status"]),
            dispatches=int(task_rows[task_id][f"{dispatch}sequence"]),
            commit_shas=tuple(commits_by_task.get(task_id, ())),
            reason=str(task_rows[task_id][f"{state}reason"]),
        )
        for _, task_id in submitted.task_ids
    )
    mission = Mission.get_prefix()
    mission_state = MissionState.get_prefix()
    return MissionResult(
        mission_id=submitted.mission_id,
        status=str(mission_row[f"{mission_state}status"]),
        repository=str(mission_row[f"{mission}repository"]),
        branch=str(mission_row[f"{mission}branch"]),
        ticks_completed=ticks_completed,
        tasks=tasks,
        reason=str(mission_row[f"{mission_state}reason"]),
    )
