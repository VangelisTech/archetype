# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Post-commit dispatch projection for coding-agent missions."""

from __future__ import annotations

from collections import defaultdict
from typing import Any, cast

from daft import DataFrame, Expression, col

from archetype.core.component import Component
from archetype.core.hooks import PostTick
from archetype.missions.coding_agents.components import (
    AgentExecution,
    AgentTaskPolicy,
    AgentTaskRecord,
    AgentTaskState,
    AgentTaskWorkspace,
    TaskDispatch,
    TaskValidator,
    ValidationResult,
)
from archetype.missions.coding_agents.contracts import (
    DispatchedValidator,
    TaskDispatchRequest,
    ValidationObservation,
)
from archetype.missions.coding_agents.transitions import AgentTaskStatus
from archetype.missions.contracts import CommandValidator, RepositoryPublicationPolicy
from archetype.missions.relationships import Guards, PartOfMission


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
        AgentTaskRecord,
        AgentTaskWorkspace,
        AgentTaskPolicy,
        AgentTaskState,
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

        state = AgentTaskState.get_prefix()
        dispatch = TaskDispatch.get_prefix()
        dispatched = task_frame.where(
            cast(
                Expression,
                col(f"{state}status") == AgentTaskStatus.DISPATCHED.value,
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
        record = AgentTaskRecord.get_prefix()
        workspace = AgentTaskWorkspace.get_prefix()
        policy = AgentTaskPolicy.get_prefix()
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
