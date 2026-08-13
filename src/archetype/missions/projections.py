# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Post-commit dispatch and terminal result projections for Agent Missions."""

from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, cast

import daft
from daft import DataFrame, Expression, col

from archetype.core.component import Component
from archetype.core.hooks import PostTick
from archetype.errors import AvailabilityError
from archetype.graph import GraphView
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
    Commit,
    CompleteAuthorActivityObservation,
    CompleteCriticActivityObservation,
    CriticExecution,
    CriticFinding,
    CriticReceipt,
    FrictionLog,
    Mission,
    MissionState,
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
from archetype.missions.projection_bundles import (
    COMPLETE_AUTHOR_ACTIVITY_FACT_TYPES,
    COMPLETE_CRITIC_ACTIVITY_FACT_TYPES,
    reconstruct_complete_author_activity_fact_bundle,
    reconstruct_complete_critic_activity_fact_bundle,
)
from archetype.missions.relations import (
    Guards,
    PartOfMission,
)
from archetype.missions.transitions import MissionStatus, TaskStatus


def _latest_prior_sequence(
    task_keys: DataFrame,
    history: DataFrame,
    *,
    task_column: str,
    sequence_column: str,
    alias: str,
) -> DataFrame:
    """Per dispatched task, the greatest history sequence strictly before it.

    Tasks with no earlier history are absent from the result; callers decide
    whether that means "no previous dispatch" or the sequence-zero default.
    """

    return (
        task_keys.join(history, left_on="_task_id", right_on=task_column)
        .where(col(sequence_column) < col("_task_sequence"))
        .groupby("_task_id")
        .agg(col(sequence_column).max().alias(alias))
    )


def _sequence_with_zero_default(
    task_keys: DataFrame,
    prior: DataFrame,
    *,
    alias: str,
) -> DataFrame:
    """Give every dispatched task a previous sequence, defaulting to zero."""

    return (
        task_keys.select("_task_id")
        .distinct()
        .join(prior, on="_task_id", how="left")
        .with_column(alias, col(alias).fill_null(0))
    )


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
    bounds = _daft_latest(frame, label="critic subject policy").select(
        "entity_id",
        f"{subject}max_subject_bytes",
    )
    return {
        int(row["entity_id"]): int(row[f"{subject}max_subject_bytes"]) for row in bounds.to_pylist()
    }


def _latest_accepted_mission_checkout_revision(
    *,
    mission_id: int,
    mission_task_ids: Sequence[int],
    task_rows: Mapping[int, Mapping[str, Any]],
    candidate_rows: Sequence[Mapping[str, Any]],
) -> str:
    """Resolve the latest accepted head on one serialized mission branch."""

    state = TaskState.get_prefix()
    dispatch = TaskDispatch.get_prefix()
    candidate = Candidate.get_prefix()
    accepted: list[Mapping[str, Any]] = []
    for task_id in mission_task_ids:
        task = task_rows.get(task_id)
        if task is None or str(task[f"{state}status"]) != TaskStatus.ACCEPTED.value:
            continue
        task_candidates = [
            row
            for row in candidate_rows
            if int(row[f"{candidate}mission_id"]) == mission_id
            and int(row[f"{candidate}task_id"]) == task_id
            and str(row[f"{candidate}dispatch_id"]) == str(task[f"{dispatch}dispatch_id"])
        ]
        if len(task_candidates) != 1:
            raise ValueError("accepted mission task has no unique published candidate")
        accepted.extend(task_candidates)
    if not accepted:
        return ""
    # One outstanding task plus monotonic world allocation makes candidate
    # identity the durable branch-order authority; wall clocks are not.
    latest = max(accepted, key=lambda row: int(row["entity_id"]))
    return str(latest[f"{candidate}head_revision"])


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

    @staticmethod
    def _previous_executions(
        task_keys: DataFrame,
        observations: DataFrame | None,
    ) -> dict[int, dict[str, Any]]:
        """Index the execution each task dispatched most recently before now."""

        if observations is None:
            return {}
        prior = _latest_prior_sequence(
            task_keys,
            observations.select("_prior_task_id", "_prior_sequence").distinct(),
            task_column="_prior_task_id",
            sequence_column="_prior_sequence",
            alias="_previous_sequence",
        )
        winners = (
            observations.join(
                prior,
                left_on=["_prior_task_id", "_prior_sequence"],
                right_on=["_task_id", "_previous_sequence"],
            )
            .select(
                "_prior_task_id",
                "_prior_starting_revision",
                "_prior_agent_session_id",
            )
            .distinct()
        )
        indexed: dict[int, dict[str, Any]] = {}
        for row in winners.to_pylist():
            indexed.setdefault(int(row["_prior_task_id"]), row)
        return indexed

    @staticmethod
    def _previous_validations(
        task_keys: DataFrame,
        observations: DataFrame | None,
        *,
        has_validations: bool,
    ) -> dict[int, list[dict[str, Any]]]:
        """Index the validations observed at each task's previous sequence.

        A task with no earlier dispatch falls back to sequence zero, which is
        what the per-task scan this replaces did with its `default=None`.
        """

        if observations is None or not has_validations:
            return {}
        prior = _latest_prior_sequence(
            task_keys,
            observations.select("_prior_task_id", "_prior_sequence").distinct(),
            task_column="_prior_task_id",
            sequence_column="_prior_sequence",
            alias="_previous_sequence",
        )
        settled = observations.join(
            _sequence_with_zero_default(task_keys, prior, alias="_previous_sequence"),
            left_on=["_prior_task_id", "_prior_sequence"],
            right_on=["_task_id", "_previous_sequence"],
        ).where(col("_validation_validator_id").not_null())
        indexed: dict[int, list[dict[str, Any]]] = defaultdict(list)
        for row in settled.to_pylist():
            indexed[int(row["_prior_task_id"])].append(row)
        return indexed

    @staticmethod
    def _prior_candidates(
        task_keys: DataFrame,
        candidate_history: DataFrame | None,
        finding_history: DataFrame | None,
        *,
        candidate_prefix: str,
        finding_prefix: str,
    ) -> tuple[dict[int, dict[str, Any]], dict[int, list[dict[str, Any]]]]:
        """Index each task's latest earlier candidate and that candidate's findings."""

        if candidate_history is None:
            return {}, {}
        candidate_keys = candidate_history.select(
            col("entity_id").alias("_prior_candidate_entity_id"),
            col(f"{candidate_prefix}task_id").alias("_prior_candidate_task_id"),
            col(f"{candidate_prefix}dispatch_sequence").alias("_prior_candidate_sequence"),
            col(f"{candidate_prefix}candidate_id").alias("_prior_candidate_id"),
            col(f"{candidate_prefix}head_revision").alias("_prior_candidate_head_revision"),
        )
        prior = _latest_prior_sequence(
            task_keys,
            candidate_keys.select(
                "_prior_candidate_task_id",
                "_prior_candidate_sequence",
            ).distinct(),
            task_column="_prior_candidate_task_id",
            sequence_column="_prior_candidate_sequence",
            alias="_previous_candidate_sequence",
        )
        winners = candidate_keys.join(
            prior,
            left_on=["_prior_candidate_task_id", "_prior_candidate_sequence"],
            right_on=["_task_id", "_previous_candidate_sequence"],
        )
        by_task: dict[int, dict[str, Any]] = {}
        for row in winners.to_pylist():
            by_task.setdefault(int(row["_prior_candidate_task_id"]), row)
        if finding_history is None or not by_task:
            return by_task, {}
        # Only findings the winning candidates own cross into Python.
        named = winners.select(
            col("_prior_candidate_entity_id").alias("_named_candidate")
        ).distinct()
        kept = finding_history.join(
            named,
            left_on=f"{finding_prefix}candidate_entity_id",
            right_on="_named_candidate",
            how="semi",
        )
        findings: dict[int, list[dict[str, Any]]] = defaultdict(list)
        for row in kept.to_pylist():
            findings[int(row[f"{finding_prefix}candidate_entity_id"])].append(row)
        return by_task, findings

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
        all_task_rows = {
            int(row["entity_id"]): row
            for row in _daft_latest(
                task_frame,
                label="task dispatch state",
                allow_updates=True,
            ).to_pylist()
        }
        membership = PartOfMission.get_prefix()
        mission_task_ids: dict[int, set[int]] = defaultdict(set)
        for row in (
            mission_edges.select(
                f"{membership}source",
                f"{membership}target",
            )
            .distinct()
            .to_pylist()
        ):
            mission_task_ids[int(row[f"{membership}target"])].add(int(row[f"{membership}source"]))
        dispatched = task_frame.where(
            cast(
                Expression,
                col(f"{state}status") == TaskStatus.DISPATCHED.value,
            )
        )
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

        task_keys = dispatched.select(
            col("entity_id").alias("_task_id"),
            col(f"{dispatch}sequence").alias("_task_sequence"),
        )

        executions = _live_frame(event, AgentExecution)
        validation_results = _live_frame(event, ValidationResult)
        execution = AgentExecution.get_prefix()
        validation = ValidationResult.get_prefix()
        observations: DataFrame | None = None
        if executions is not None:
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
        record = Task.get_prefix()
        workspace = TaskWorkspace.get_prefix()
        policy = TaskPolicy.get_prefix()
        critic_policy = TaskCriticPolicy.get_prefix()
        candidate_history = _live_frame(event, Candidate)
        finding_history = _live_frame(event, CriticFinding)
        candidate_record = Candidate.get_prefix()
        finding_record = CriticFinding.get_prefix()
        candidate_rows = (
            _daft_latest(candidate_history, label="candidate").to_pylist()
            if candidate_history is not None
            else []
        )

        previous_execution_by_task = self._previous_executions(task_keys, observations)
        previous_validations_by_task = self._previous_validations(
            task_keys,
            observations,
            has_validations=validation_results is not None,
        )
        prior_candidate_by_task, findings_by_candidate = self._prior_candidates(
            task_keys,
            candidate_history,
            finding_history,
            candidate_prefix=candidate_record,
            finding_prefix=finding_record,
        )

        for row in task_rows:
            dispatch_id = str(row[f"{dispatch}dispatch_id"])
            task_id = int(row["entity_id"])
            sequence = int(row[f"{dispatch}sequence"])
            previous = previous_execution_by_task.get(task_id)
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
                for candidate in previous_validations_by_task.get(task_id, ())
            )
            validators = tuple(
                sorted(validators_by_task[task_id], key=lambda item: item.validator_id)
            )
            prior_candidate = prior_candidate_by_task.get(task_id)
            checkout_revision = (
                str(prior_candidate["_prior_candidate_head_revision"])
                if prior_candidate is not None
                else _latest_accepted_mission_checkout_revision(
                    mission_id=int(row[f"{membership}target"]),
                    mission_task_ids=tuple(mission_task_ids[int(row[f"{membership}target"])]),
                    task_rows=all_task_rows,
                    candidate_rows=candidate_rows,
                )
            )
            previous_critic_findings = (
                tuple(
                    CriticRepairFinding(
                        candidate_id=str(prior_candidate["_prior_candidate_id"]),
                        finding_id=str(item[f"{finding_record}finding_id"]),
                        severity=str(item[f"{finding_record}severity"]),
                        category=str(item[f"{finding_record}category"]),
                        title=str(item[f"{finding_record}title"]),
                        detail=str(item[f"{finding_record}detail"]),
                        evidence_location=str(item[f"{finding_record}evidence_location"]),
                        reproduction=str(item[f"{finding_record}reproduction"]),
                    )
                    for item in findings_by_candidate.get(
                        int(prior_candidate["_prior_candidate_entity_id"]), ()
                    )
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
                        int(prior_candidate["_prior_candidate_entity_id"])
                        if prior_candidate is not None
                        else 0
                    ),
                    task_base_revision=(
                        str(previous["_prior_starting_revision"]) if previous is not None else ""
                    ),
                    checkout_revision=checkout_revision,
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


def _count_by_keys(
    frame: DataFrame | None,
    *,
    keys: tuple[str, ...],
    count_alias: str,
) -> DataFrame:
    """Aggregate row counts by join keys; empty input yields an empty count frame."""

    if frame is None:
        return daft.from_pydict({**{key: [] for key in keys}, count_alias: []})
    return frame.groupby(*keys).agg(col(keys[0]).count().alias(count_alias))


def _outputs_by_activity(
    frame: DataFrame | None,
    markers: DataFrame,
    *,
    prefix: str,
    marker_prefix: str,
    exact_sequence: bool,
) -> dict[tuple[Any, ...], list[dict[str, Any]]]:
    """Index only the output rows a surviving marker names, by that activity.

    The semi-join keeps unnamed history out of Python; the remaining rows still
    have to become typed Components for the bundle digest.
    """

    if frame is None:
        return {}
    fact_keys: list[Expression | str] = [
        f"{prefix}execution_id",
        f"{prefix}task_id",
        f"{prefix}dispatch_id",
    ]
    marker_keys: list[str] = [
        f"{marker_prefix}execution_id",
        f"{marker_prefix}task_id",
        f"{marker_prefix}activity_id",
    ]
    if exact_sequence:
        fact_keys.append(f"{prefix}dispatch_sequence")
        marker_keys.append(f"{marker_prefix}dispatch_sequence")
    named = markers.select(
        *[col(name).alias(f"_named_{index}") for index, name in enumerate(marker_keys)]
    ).distinct()
    kept = frame.join(
        named,
        left_on=fact_keys,
        right_on=[f"_named_{index}" for index in range(len(marker_keys))],
        how="semi",
    )
    indexed: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
    for row in kept.to_pylist():
        key = (
            int(row[f"{prefix}execution_id"]),
            int(row[f"{prefix}task_id"]),
            str(row[f"{prefix}dispatch_id"]),
        )
        if exact_sequence:
            key = (*key, int(row[f"{prefix}dispatch_sequence"]))
        indexed[key].append(row)
    return indexed


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

    execution_keys = executions.select(
        col("entity_id").alias("_execution_entity_id"),
        *[
            col(f"{execution}{field}").alias(f"_execution_{field}")
            for field in AgentExecution.model_fields
        ],
    ).distinct()
    # One execution identity must name one committed value. Dropping the
    # ambiguous identities keeps the join from fanning a marker into several
    # observations, which is what the per-marker exact-count gate did before.
    execution_keys = execution_keys.join(
        _count_by_keys(
            execution_keys,
            keys=("_execution_entity_id",),
            count_alias="_execution_versions",
        ).where(
            cast(
                Expression,
                col("_execution_versions") > 1,  # ty: ignore[unsupported-operator]
            )
        ),
        on="_execution_entity_id",
        how="anti",
    )
    candidates = markers.join(
        execution_keys,
        left_on=[
            f"{marker}execution_id",
            f"{marker}task_id",
            f"{marker}activity_id",
            f"{marker}dispatch_sequence",
        ],
        right_on=[
            "_execution_entity_id",
            "_execution_task_id",
            "_execution_dispatch_id",
            "_execution_dispatch_sequence",
        ],
    ).where(col("_execution_redaction_policy_id") == col(f"{marker}redaction_policy_id"))

    validation_counts = _count_by_keys(
        None
        if validations is None
        else validations.select(
            col(f"{validation}execution_id").alias("_v_execution_id"),
            col(f"{validation}task_id").alias("_v_task_id"),
            col(f"{validation}dispatch_id").alias("_v_dispatch_id"),
            col(f"{validation}dispatch_sequence").alias("_v_dispatch_sequence"),
        ),
        keys=("_v_execution_id", "_v_task_id", "_v_dispatch_id", "_v_dispatch_sequence"),
        count_alias="_validation_seen",
    )
    commit_counts = _count_by_keys(
        None
        if commits is None
        else commits.select(
            col(f"{commit}execution_id").alias("_c_execution_id"),
            col(f"{commit}task_id").alias("_c_task_id"),
            col(f"{commit}dispatch_id").alias("_c_dispatch_id"),
        ),
        keys=("_c_execution_id", "_c_task_id", "_c_dispatch_id"),
        count_alias="_commit_seen",
    )
    friction_counts = _count_by_keys(
        None
        if friction is None
        else friction.select(
            col(f"{friction_record}execution_id").alias("_f_execution_id"),
            col(f"{friction_record}task_id").alias("_f_task_id"),
            col(f"{friction_record}dispatch_id").alias("_f_dispatch_id"),
        ),
        keys=("_f_execution_id", "_f_task_id", "_f_dispatch_id"),
        count_alias="_friction_seen",
    )

    complete_markers = (
        candidates.join(
            validation_counts,
            left_on=[
                f"{marker}execution_id",
                f"{marker}task_id",
                f"{marker}activity_id",
                f"{marker}dispatch_sequence",
            ],
            right_on=[
                "_v_execution_id",
                "_v_task_id",
                "_v_dispatch_id",
                "_v_dispatch_sequence",
            ],
            how="left",
        )
        .with_column("_validation_seen", col("_validation_seen").fill_null(0))
        .join(
            commit_counts,
            left_on=[f"{marker}execution_id", f"{marker}task_id", f"{marker}activity_id"],
            right_on=["_c_execution_id", "_c_task_id", "_c_dispatch_id"],
            how="left",
        )
        .with_column("_commit_seen", col("_commit_seen").fill_null(0))
        .join(
            friction_counts,
            left_on=[f"{marker}execution_id", f"{marker}task_id", f"{marker}activity_id"],
            right_on=["_f_execution_id", "_f_task_id", "_f_dispatch_id"],
            how="left",
        )
        .with_column("_friction_seen", col("_friction_seen").fill_null(0))
        .where(
            (col("_validation_seen") == col(f"{marker}validation_count"))
            & (col("_commit_seen") == col(f"{marker}commit_count"))
            & (col("_friction_seen") == col(f"{marker}friction_count"))
        )
    )

    # Only facts a surviving marker names cross into Python, where digest
    # verification needs typed Component values.
    validation_by_activity = _outputs_by_activity(
        validations,
        complete_markers,
        prefix=validation,
        marker_prefix=marker,
        exact_sequence=True,
    )
    commit_by_activity = _outputs_by_activity(
        commits,
        complete_markers,
        prefix=commit,
        marker_prefix=marker,
        exact_sequence=False,
    )
    friction_by_activity = _outputs_by_activity(
        friction,
        complete_markers,
        prefix=friction_record,
        marker_prefix=marker,
        exact_sequence=False,
    )
    complete: list[AuthorActivityObservation] = []
    for row in complete_markers.to_pylist():
        activity_id = str(row[f"{marker}activity_id"])
        task_id = int(row[f"{marker}task_id"])
        execution_id = int(row[f"{marker}execution_id"])
        sequence = int(row[f"{marker}dispatch_sequence"])
        execution_fact = AgentExecution(
            **{field: row[f"_execution_{field}"] for field in AgentExecution.model_fields}
        )
        exact_key = (execution_id, task_id, activity_id, sequence)
        activity_key = (execution_id, task_id, activity_id)
        validation_facts = tuple(
            ValidationResult(
                **{
                    field: candidate[f"{validation}{field}"]
                    for field in ValidationResult.model_fields
                }
            )
            for candidate in validation_by_activity.get(exact_key, ())
        )
        commit_facts = tuple(
            Commit(**{field: candidate[f"{commit}{field}"] for field in Commit.model_fields})
            for candidate in commit_by_activity.get(activity_key, ())
        )
        friction_facts = tuple(
            FrictionLog(
                **{
                    field: candidate[f"{friction_record}{field}"]
                    for field in FrictionLog.model_fields
                }
            )
            for candidate in friction_by_activity.get(activity_key, ())
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


class CriticReviewBudgetExhaustedError(AvailabilityError):
    """Candidates remain pending review after their whole independent budget."""

    public_detail = "Independent review is still pending; the review budget is exhausted"

    def __init__(
        self,
        mission_id: int,
        pending: tuple[CriticReviewBudgetExhausted, ...],
    ) -> None:
        self.mission_id = mission_id
        self.pending = pending
        described = ", ".join(
            f"task {item.task_id} candidate {item.candidate_id!r} "
            f"({item.attempts}/{item.max_reviews} reviews)"
            for item in pending
        )
        super().__init__(
            f"mission {mission_id} candidates remain pending independent review "
            f"after their whole review budget: {described}"
        )


@dataclass(frozen=True, slots=True)
class CriticActivityIntentProjection:
    """Pure committed-snapshot projection of critic requests and exhaustion."""

    requests: tuple[CandidateReviewRequest, ...]
    exhausted: tuple[CriticReviewBudgetExhausted, ...]


def _reject_ambiguous_history(frame: DataFrame, *, label: str, allow_updates: bool) -> None:
    """Fail closed when one entity committed more than one value.

    An immutable component may hold exactly one value across its whole history;
    a mutable one may hold exactly one value per tick. Either way the distinct
    committed values must equal the number of keys they are indexed by.
    """

    if allow_updates:
        keys = ["entity_id", "tick"]
        columns = list(frame.column_names)
    else:
        keys = ["entity_id"]
        columns = [name for name in frame.column_names if name != "tick"]
    values = frame.select(*columns).distinct()
    if values.count_rows() != values.select(*keys).distinct().count_rows():
        raise ValueError(f"{label} has conflicting committed rows")


def _daft_latest(frame: DataFrame, *, label: str, allow_updates: bool = False) -> DataFrame:
    """Keep the max-tick row per entity_id, rejecting ambiguous history first."""

    _reject_ambiguous_history(frame, label=label, allow_updates=allow_updates)
    heads = frame.groupby("entity_id").agg(col("tick").max().alias("_latest_tick"))
    latest = frame.join(
        heads,
        left_on=["entity_id", "tick"],
        right_on=["entity_id", "_latest_tick"],
    )
    keep = [name for name in latest.column_names if name != "_latest_tick"]
    return latest.select(*keep)


def _current_candidate_frame(
    *,
    tasks: DataFrame,
    candidates: DataFrame,
    state_prefix: str,
    candidate_prefix: str,
) -> DataFrame:
    """Join CANDIDATE tasks to candidates and keep the current dispatch sequence."""

    candidate_tasks = _daft_latest(
        tasks.where(cast(Expression, col(f"{state_prefix}status") == TaskStatus.CANDIDATE.value)),
        label="candidate task",
        allow_updates=True,
    ).with_column("_task_entity_id", col("entity_id"))
    candidate_tasks = candidate_tasks.select(
        *[name for name in candidate_tasks.column_names if name not in {"entity_id", "tick"}]
    )

    candidate_latest = _daft_latest(candidates, label="candidate").with_column(
        "_candidate_entity_id",
        col("entity_id"),
    )
    joined = candidate_latest.join(
        candidate_tasks,
        left_on=f"{candidate_prefix}task_id",
        right_on="_task_entity_id",
    )
    max_seq = joined.groupby("_task_entity_id").agg(
        col(f"{candidate_prefix}dispatch_sequence").max().alias("_max_seq")
    )
    current = joined.join(max_seq, on="_task_entity_id").where(
        col(f"{candidate_prefix}dispatch_sequence") == col("_max_seq")
    )
    distinct_ids = current.select("_task_entity_id", "_candidate_entity_id").distinct()
    id_counts = distinct_ids.groupby("_task_entity_id").agg(
        col("_candidate_entity_id").count().alias("_n_current_ids")
    )
    ambiguous = cast(
        Expression,
        col("_n_current_ids") > 1,  # ty: ignore[unsupported-operator]
    )
    if id_counts.where(ambiguous).count_rows() > 0:
        raise ValueError("task has multiple current candidates at one dispatch sequence")
    return current


def _admit_critic_request_from_current_row(
    row: Mapping[str, Any],
    *,
    attempt: int,
    subject_bounds: Mapping[int, int],
    author_rows: Mapping[int, Mapping[str, Any]],
    validator_rows: Mapping[int, Mapping[str, Any]],
    validation_rows: Sequence[Mapping[str, Any]],
    prefixes: Mapping[str, str],
) -> CandidateReviewRequest:
    """Build one exact-candidate request from an already-selected current row."""

    task = prefixes["task"]
    policy = prefixes["policy"]
    candidate = prefixes["candidate"]
    author = prefixes["author"]
    validator = prefixes["validator"]
    validation = prefixes["validation"]

    task_id = int(row["_task_entity_id"])
    candidate_entity_id = int(row["_candidate_entity_id"])
    candidate_id = str(row[f"{candidate}candidate_id"])
    max_reviews = int(row[f"{policy}max_reviews"])
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
            raise ValueError("candidate validation does not match its committed author execution")
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
    return request


def _settled_candidate_ids(
    current: DataFrame,
    receipts: DataFrame,
    *,
    candidate_prefix: str,
    receipt_prefix: str,
) -> set[int]:
    """Return the current candidates an independent critic already settled.

    The exact-subject match is a join on the candidate identity and every
    reviewed digest; a receipt written from the author's own sandbox does not
    settle anything, so it is filtered out rather than joined away.
    """

    settled = current.join(
        receipts,
        left_on=[
            "_candidate_entity_id",
            f"{candidate_prefix}candidate_digest",
            f"{candidate_prefix}policy_digest",
            f"{candidate_prefix}base_revision",
            f"{candidate_prefix}head_revision",
            f"{candidate_prefix}diff_digest",
            f"{candidate_prefix}validator_bundle_digest",
        ],
        right_on=[
            f"{receipt_prefix}candidate_entity_id",
            f"{receipt_prefix}candidate_digest",
            f"{receipt_prefix}policy_digest",
            f"{receipt_prefix}reviewed_base_revision",
            f"{receipt_prefix}reviewed_head_revision",
            f"{receipt_prefix}reviewed_diff_digest",
            f"{receipt_prefix}validator_bundle_digest",
        ],
    ).where(
        col(f"{receipt_prefix}critic_sandbox_id") != col(f"{candidate_prefix}author_sandbox_id")
    )
    return {
        int(row["_candidate_entity_id"])
        for row in settled.select("_candidate_entity_id").distinct().to_pylist()
    }


def project_pending_review_exhaustion(
    view: GraphView,
    mission_id: int,
) -> tuple[CriticReviewBudgetExhausted, ...]:
    """Report candidates that cannot admit another independent review."""

    tasks = view.frame(Task, TaskCriticPolicy, TaskState)
    candidates = view.frame(Candidate)
    if tasks is None or candidates is None:
        return ()

    state = TaskState.get_prefix()
    candidate = Candidate.get_prefix()
    policy = TaskCriticPolicy.get_prefix()
    current = _current_candidate_frame(
        tasks=tasks,
        candidates=candidates,
        state_prefix=state,
        candidate_prefix=candidate,
    ).where(cast(Expression, col(f"{candidate}mission_id") == mission_id))
    current_rows = current.to_pylist()
    if not current_rows:
        return ()

    critic_execution = CriticExecution.get_prefix()
    critic_executions = view.frame(CriticExecution)
    attempt_counts: dict[int, int] = {}
    if critic_executions is not None:
        attempts_frame = (
            _daft_latest(critic_executions, label="critic execution")
            .groupby(f"{critic_execution}candidate_entity_id")
            .agg(col("entity_id").count_distinct().alias("_attempts"))
        )
        attempt_counts = {
            int(row[f"{critic_execution}candidate_entity_id"]): int(row["_attempts"])
            for row in attempts_frame.to_pylist()
        }

    receipt = CriticReceipt.get_prefix()
    critic_receipts = view.frame(CriticReceipt)
    settled_candidate_ids: set[int] = set()
    if critic_receipts is not None:
        settled_candidate_ids = _settled_candidate_ids(
            current,
            _daft_latest(critic_receipts, label="critic receipt"),
            candidate_prefix=candidate,
            receipt_prefix=receipt,
        )

    exhausted: list[CriticReviewBudgetExhausted] = []
    for row in current_rows:
        candidate_entity_id = int(row["_candidate_entity_id"])
        if candidate_entity_id in settled_candidate_ids:
            continue
        attempts = attempt_counts.get(candidate_entity_id, 0)
        max_reviews = int(row[f"{policy}max_reviews"])
        if attempts >= max_reviews:
            exhausted.append(
                CriticReviewBudgetExhausted(
                    mission_id=mission_id,
                    task_id=int(row[f"{candidate}task_id"]),
                    candidate_id=str(row[f"{candidate}candidate_id"]),
                    attempts=attempts,
                    max_reviews=max_reviews,
                )
            )
    return tuple(sorted(exhausted, key=lambda item: item.task_id))


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
    prefixes = {
        "task": task,
        "policy": policy,
        "candidate": candidate,
        "author": author,
        "validator": validator,
        "validation": validation,
    }

    current = _current_candidate_frame(
        tasks=tasks,
        candidates=candidates,
        state_prefix=state,
        candidate_prefix=candidate,
    )
    current_rows = current.to_pylist()
    if not current_rows:
        return CriticActivityIntentProjection((), ())

    current_by_task: dict[int, dict[str, Any]] = {}
    for row in current_rows:
        task_id = int(row["_task_entity_id"])
        prior = current_by_task.get(task_id)
        if prior is not None and prior != row:
            raise ValueError("task current candidate has conflicting committed rows")
        current_by_task[task_id] = row

    subject_bounds = _critic_subject_bounds(event)

    author_latest = _daft_latest(author_executions, label="author execution")
    validator_latest = _daft_latest(validators, label="task validator")
    validation_latest = _daft_latest(validation_results, label="validation result")
    author_rows = {int(row["entity_id"]): row for row in author_latest.to_pylist()}
    validator_rows = {int(row["entity_id"]): row for row in validator_latest.to_pylist()}
    validation_rows = tuple(validation_latest.to_pylist())

    critic_executions = _live_frame(event, CriticExecution)
    critic_receipts = _live_frame(event, CriticReceipt)
    critic_execution = CriticExecution.get_prefix()
    receipt = CriticReceipt.get_prefix()

    attempt_counts: dict[int, int] = {}
    if critic_executions is not None:
        attempts_frame = (
            _daft_latest(critic_executions, label="critic execution")
            .groupby(f"{critic_execution}candidate_entity_id")
            .agg(col("entity_id").count_distinct().alias("_attempts"))
        )
        attempt_counts = {
            int(row[f"{critic_execution}candidate_entity_id"]): int(row["_attempts"])
            for row in attempts_frame.to_pylist()
        }

    settled_candidate_ids: set[int] = set()
    if critic_receipts is not None:
        settled_candidate_ids = _settled_candidate_ids(
            current,
            _daft_latest(critic_receipts, label="critic receipt"),
            candidate_prefix=candidate,
            receipt_prefix=receipt,
        )

    requests: list[CandidateReviewRequest] = []
    exhausted: list[CriticReviewBudgetExhausted] = []
    for row in current_by_task.values():
        candidate_entity_id = int(row["_candidate_entity_id"])
        if candidate_entity_id in settled_candidate_ids:
            continue
        attempts = attempt_counts.get(candidate_entity_id, 0)
        max_reviews = int(row[f"{policy}max_reviews"])
        if attempts >= max_reviews:
            exhausted.append(
                CriticReviewBudgetExhausted(
                    mission_id=int(row[f"{candidate}mission_id"]),
                    task_id=int(row[f"{candidate}task_id"]),
                    candidate_id=str(row[f"{candidate}candidate_id"]),
                    attempts=attempts,
                    max_reviews=max_reviews,
                )
            )
            continue
        requests.append(
            _admit_critic_request_from_current_row(
                row,
                attempt=attempts + 1,
                subject_bounds=subject_bounds,
                author_rows=author_rows,
                validator_rows=validator_rows,
                validation_rows=validation_rows,
                prefixes=prefixes,
            )
        )

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
    episode_id = str(mission_row[f"{mission}episode_id"])
    if not episode_id or episode_id != submitted.episode_id:
        raise RuntimeError("terminal mission episode identity does not match its submission")
    return MissionResult(
        mission_id=submitted.mission_id,
        episode_id=episode_id,
        status=str(mission_row[f"{mission_state}status"]),
        repository=str(mission_row[f"{mission}repository"]),
        branch=str(mission_row[f"{mission}branch"]),
        ticks_completed=ticks_completed,
        tasks=tasks,
        reason=str(mission_row[f"{mission_state}reason"]),
    )
