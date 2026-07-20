# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Post-commit dispatch and terminal result projections for Agent Missions."""

from __future__ import annotations

from collections import defaultdict
from typing import Any, cast

from daft import DataFrame, Expression, col

from archetype.core.component import Component
from archetype.core.hooks import PostTick
from archetype.graph import GraphView
from archetype.missions.coding_agents.contracts import (
    DispatchedValidator,
    TaskDispatchRequest,
    ValidationObservation,
)
from archetype.missions.components import (
    AgentExecution,
    Commit,
    Mission,
    MissionState,
    Task,
    TaskDispatch,
    TaskPolicy,
    TaskState,
    TaskValidator,
    TaskWorkspace,
    ValidationResult,
)
from archetype.missions.contracts import (
    CommandValidator,
    MissionResult,
    RepositoryPublicationPolicy,
    SubmittedMission,
    TaskResult,
)
from archetype.missions.relations import Guards, PartOfMission
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


class TaskDispatchOutbox:
    """Project newly committed dispatch rows into bounded execution requests."""

    _TASK_COMPONENTS = (
        Task,
        TaskWorkspace,
        TaskPolicy,
        TaskState,
        TaskDispatch,
    )

    def __init__(self) -> None:
        self._queued: list[TaskDispatchRequest] = []
        self._seen_dispatches: set[str] = set()

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
        for row in task_rows:
            dispatch_id = str(row[f"{dispatch}dispatch_id"])
            if dispatch_id in self._seen_dispatches:
                continue
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
            self._queued.append(
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
                    task_base_revision=(
                        str(previous["_prior_starting_revision"]) if previous is not None else ""
                    ),
                    previous_agent_session_id=(
                        str(previous["_prior_agent_session_id"]) if previous is not None else ""
                    ),
                    previous_validation=previous_validation,
                )
            )
            self._seen_dispatches.add(dispatch_id)

    def drain(self) -> tuple[TaskDispatchRequest, ...]:
        requests = tuple(self._queued)
        self._queued.clear()
        return requests


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
