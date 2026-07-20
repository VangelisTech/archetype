# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Data-centric transition authority for Agent Missions."""

from __future__ import annotations

import hashlib
from typing import Any, cast

import daft
from daft import DataFrame, DataType, Expression, col, lit
from daft.functions import when

from archetype.core.aio.async_processor import AsyncProcessor
from archetype.core.resources import Resources
from archetype.graph import GraphView
from archetype.missions.coding_agents.components import (
    AgentCommit,
    AgentExecution,
    AgentMissionRecord,
    AgentMissionState,
    AgentTaskPolicy,
    AgentTaskRecord,
    AgentTaskState,
    TaskDispatch,
    ValidationResult,
)
from archetype.missions.coding_agents.contracts import AgentExecutionStatus
from archetype.missions.coding_agents.transitions import (
    AgentMissionStatus,
    AgentTaskStatus,
)
from archetype.missions.relationships import DependsOn, Guards, PartOfMission


class TaskDecisionProcessor(AsyncProcessor):
    """Accept, retry, or exhaust from current revision-bound observations."""

    components = (AgentTaskState, TaskDispatch, AgentTaskPolicy)
    priority = 10

    async def process(
        self,
        df: DataFrame,
        resources: Resources | None = None,
        **_: Any,
    ) -> DataFrame:
        if resources is None:
            raise KeyError("TaskDecisionProcessor requires world resources")
        view = resources.require(GraphView)
        executions = view.frame(AgentExecution)
        guards = view.frame(Guards)
        if executions is None or guards is None:
            return df

        execution = AgentExecution.get_prefix()
        validation = ValidationResult.get_prefix()
        commit = AgentCommit.get_prefix()
        guard = Guards.get_prefix()
        terminal = executions.where(
            col(f"{execution}status").is_in(
                [
                    AgentExecutionStatus.EXITED.value,
                    AgentExecutionStatus.ERRORED.value,
                    AgentExecutionStatus.INTERRUPTED.value,
                ]
            )
        ).select(
            col("entity_id").alias("_execution_id"),
            col(f"{execution}task_id").alias("_summary_task_id"),
            col(f"{execution}dispatch_id").alias("_summary_dispatch_id"),
            col(f"{execution}status").alias("_execution_status"),
            col(f"{execution}final_revision").alias("_final_revision"),
            col(f"{execution}error").alias("_execution_error"),
        )

        guard_counts = guards.groupby(f"{guard}target").agg(
            col("entity_id").count().alias("_guard_count")
        )
        summary = terminal.join(
            guard_counts,
            left_on="_summary_task_id",
            right_on=f"{guard}target",
            how="left",
        )

        validations = view.frame(ValidationResult)
        if validations is not None:
            guarded_results = validations.join(
                guards.select(f"{guard}source", f"{guard}target"),
                left_on=f"{validation}validator_id",
                right_on=f"{guard}source",
            ).where(col(f"{validation}task_id") == col(f"{guard}target"))
            guarded_results = guarded_results.join(
                terminal,
                left_on=f"{validation}execution_id",
                right_on="_execution_id",
            ).where(
                (col(f"{validation}task_id") == col("_summary_task_id"))
                & (col(f"{validation}dispatch_id") == col("_summary_dispatch_id"))
                & (col(f"{validation}revision") == col("_final_revision"))
            )
            guarded_results = guarded_results.with_column(
                "_passed",
                when(
                    col(f"{validation}actual_returncode")
                    == col(f"{validation}expected_returncode"),
                    then=lit(1),
                ).otherwise(lit(0)),
            )
            validation_counts = guarded_results.groupby(
                "_summary_task_id",
                "_summary_dispatch_id",
                "_execution_id",
            ).agg(
                col("entity_id").count().alias("_validation_count"),
                col("_passed").sum().alias("_passed_count"),
            )
            summary = summary.join(
                validation_counts,
                left_on=["_summary_task_id", "_summary_dispatch_id", "_execution_id"],
                right_on=["_summary_task_id", "_summary_dispatch_id", "_execution_id"],
                how="left",
            )
        else:
            summary = summary.with_column("_validation_count", lit(0))
            summary = summary.with_column("_passed_count", lit(0))

        commits = view.frame(AgentCommit)
        if commits is not None:
            final_commits = commits.join(
                terminal,
                left_on=f"{commit}execution_id",
                right_on="_execution_id",
            ).where(
                (col(f"{commit}task_id") == col("_summary_task_id"))
                & (col(f"{commit}dispatch_id") == col("_summary_dispatch_id"))
                & (col(f"{commit}sha") == col("_final_revision"))
                & col(f"{commit}pushed")
                & col(f"{commit}final_revision")
            )
            commit_counts = final_commits.groupby(
                "_summary_task_id",
                "_summary_dispatch_id",
                "_execution_id",
            ).agg(col("entity_id").count().alias("_final_commit_count"))
            summary = summary.join(
                commit_counts,
                left_on=["_summary_task_id", "_summary_dispatch_id", "_execution_id"],
                right_on=["_summary_task_id", "_summary_dispatch_id", "_execution_id"],
                how="left",
            )
        else:
            summary = summary.with_column("_final_commit_count", lit(0))

        original = tuple(df.column_names)
        dispatch = TaskDispatch.get_prefix()
        df = df.join(
            summary,
            left_on=["entity_id", f"{dispatch}dispatch_id"],
            right_on=["_summary_task_id", "_summary_dispatch_id"],
            how="left",
        )
        state = AgentTaskState.get_prefix()
        policy = AgentTaskPolicy.get_prefix()
        dispatched = cast(
            Expression,
            col(f"{state}status") == AgentTaskStatus.DISPATCHED.value,
        )
        has_terminal = col("_execution_id").not_null()
        exited = cast(
            Expression,
            col("_execution_status") == AgentExecutionStatus.EXITED.value,
        )
        guard_count = col("_guard_count").fill_null(0)
        validation_count = col("_validation_count").fill_null(0)
        passed_count = col("_passed_count").fill_null(0)
        final_commit_count = col("_final_commit_count").fill_null(0)
        positive_guard_count = cast(
            Expression,
            guard_count > 0,  # ty: ignore[unsupported-operator]
        )
        complete_validation = validation_count == guard_count
        passing_validation = passed_count == guard_count
        published_revision = cast(Expression, final_commit_count == 1)
        accepted = (
            dispatched
            & has_terminal
            & exited
            & positive_guard_count
            & complete_validation
            & passing_validation
            & published_revision
        )
        rejected = dispatched & has_terminal & ~accepted
        retryable = rejected & (col(f"{dispatch}sequence") < col(f"{policy}max_dispatches"))
        exhausted = rejected & ~retryable
        failure_reason = when(
            col("_execution_error").fill_null("") != "",
            then=col("_execution_error"),
        ).otherwise(lit("validation or repository publication failed"))
        df = df.with_columns(
            {
                f"{state}status": when(accepted, then=lit(AgentTaskStatus.ACCEPTED.value))
                .when(retryable, then=lit(AgentTaskStatus.READY.value))
                .when(exhausted, then=lit(AgentTaskStatus.FAILED.value))
                .otherwise(col(f"{state}status")),
                f"{state}reason": when(exhausted, then=failure_reason).otherwise(
                    col(f"{state}reason")
                ),
            }
        )
        return df.select(*original)


class TaskReadinessProcessor(AsyncProcessor):
    """Move pending tasks to ready when every prerequisite is accepted."""

    components = (AgentTaskRecord, AgentTaskState)
    priority = 20

    async def process(
        self,
        df: DataFrame,
        resources: Resources | None = None,
        **_: Any,
    ) -> DataFrame:
        if resources is None:
            raise KeyError("TaskReadinessProcessor requires world resources")
        view = resources.require(GraphView)
        previous_tasks = view.frame(AgentTaskRecord, AgentTaskState)
        if previous_tasks is None:
            return df
        dependencies = view.frame(DependsOn)
        original = tuple(df.column_names)
        state = AgentTaskState.get_prefix()

        if dependencies is None:
            unblocked = df
            marker = None
        else:
            relation = DependsOn.get_prefix()
            nonaccepted = previous_tasks.where(
                cast(
                    Expression,
                    col(f"{state}status") != AgentTaskStatus.ACCEPTED.value,
                )
            ).select(col("entity_id").alias("_prerequisite_id"))
            blocking = (
                dependencies.join(
                    nonaccepted,
                    left_on=f"{relation}target",
                    right_on="_prerequisite_id",
                )
                .select(col(f"{relation}source").alias("_blocked_task_id"))
                .distinct()
            )
            unblocked = df.join(
                blocking,
                left_on="entity_id",
                right_on="_blocked_task_id",
                how="left",
            )
            marker = col("_blocked_task_id").is_null()

        pending = cast(Expression, col(f"{state}status") == AgentTaskStatus.PENDING.value)
        eligible = pending if marker is None else pending & marker
        unblocked = unblocked.with_column(
            f"{state}status",
            when(eligible, then=lit(AgentTaskStatus.READY.value)).otherwise(col(f"{state}status")),
        )
        return unblocked.select(*original)


_BEGIN_DISPATCH = DataType.struct(
    {
        "task_status": DataType.string(),
        "dispatch_id": DataType.string(),
        "sequence": DataType.int64(),
    }
)


@daft.func(return_dtype=_BEGIN_DISPATCH)
def _begin_dispatch(
    entity_id: int,
    task_status: str,
    dispatch_id: str,
    sequence: int,
) -> dict[str, Any]:
    if task_status != AgentTaskStatus.READY.value:
        return {
            "task_status": task_status,
            "dispatch_id": dispatch_id,
            "sequence": sequence,
        }
    next_sequence = sequence + 1
    identity = hashlib.sha256(f"{entity_id}:{next_sequence}".encode()).hexdigest()
    return {
        "task_status": AgentTaskStatus.DISPATCHED.value,
        "dispatch_id": identity,
        "sequence": next_sequence,
    }


class TaskDispatchProcessor(AsyncProcessor):
    """Turn ready tasks into durable external-work intent."""

    components = (AgentTaskRecord, AgentTaskState, TaskDispatch)
    priority = 30

    async def process(self, df: DataFrame, **_: Any) -> DataFrame:
        state = AgentTaskState.get_prefix()
        dispatch = TaskDispatch.get_prefix()
        df = df.with_column(
            "_agent_mission_dispatch",
            _begin_dispatch(
                col("entity_id"),
                col(f"{state}status"),
                col(f"{dispatch}dispatch_id"),
                col(f"{dispatch}sequence"),
            ),
        )
        df = df.with_column(f"{state}status", col("_agent_mission_dispatch")["task_status"])
        for field in ("dispatch_id", "sequence"):
            df = df.with_column(f"{dispatch}{field}", col("_agent_mission_dispatch")[field])
        return df.exclude("_agent_mission_dispatch")


class MissionRollupProcessor(AsyncProcessor):
    """Derive mission success or failure from related previous-tick tasks."""

    components = (AgentMissionRecord, AgentMissionState)
    priority = 40

    async def process(
        self,
        df: DataFrame,
        resources: Resources | None = None,
        **_: Any,
    ) -> DataFrame:
        if resources is None:
            raise KeyError("MissionRollupProcessor requires world resources")
        view = resources.require(GraphView)
        tasks = view.frame(AgentTaskState)
        memberships = view.frame(PartOfMission)
        if tasks is None or memberships is None:
            return df

        task_state = AgentTaskState.get_prefix()
        relation = PartOfMission.get_prefix()
        joined = memberships.join(
            tasks,
            left_on=f"{relation}source",
            right_on="entity_id",
        )
        joined = joined.with_column(
            "_accepted",
            when(
                cast(
                    Expression,
                    col(f"{task_state}status") == AgentTaskStatus.ACCEPTED.value,
                ),
                then=lit(1),
            ).otherwise(lit(0)),
        )
        joined = joined.with_column(
            "_failed",
            when(
                cast(
                    Expression,
                    col(f"{task_state}status") == AgentTaskStatus.FAILED.value,
                ),
                then=lit(1),
            ).otherwise(lit(0)),
        )
        rollups = joined.groupby(f"{relation}target").agg(
            col("entity_id").count().alias("_task_count"),
            col("_accepted").sum().alias("_accepted_count"),
            col("_failed").sum().alias("_failed_count"),
        )
        original = tuple(df.column_names)
        df = df.join(
            rollups,
            left_on="entity_id",
            right_on=f"{relation}target",
            how="left",
        )
        state = AgentMissionState.get_prefix()
        running = cast(
            Expression,
            col(f"{state}status") == AgentMissionStatus.RUNNING.value,
        )
        failed_count = cast(Expression, col("_failed_count") > 0)  # ty: ignore[unsupported-operator]
        task_count = cast(Expression, col("_task_count") > 0)  # ty: ignore[unsupported-operator]
        failed = running & failed_count
        succeeded = running & task_count & (col("_accepted_count") == col("_task_count"))
        df = df.with_columns(
            {
                f"{state}status": when(failed, then=lit(AgentMissionStatus.FAILED.value))
                .when(succeeded, then=lit(AgentMissionStatus.SUCCEEDED.value))
                .otherwise(col(f"{state}status")),
                f"{state}reason": when(failed, then=lit("a mission task failed")).otherwise(
                    col(f"{state}reason")
                ),
            }
        )
        return df.select(*original)


def agent_mission_processors() -> tuple[AsyncProcessor, ...]:
    """Return the complete built-in task transition pipeline."""

    return (
        TaskDecisionProcessor(),
        TaskReadinessProcessor(),
        TaskDispatchProcessor(),
        MissionRollupProcessor(),
    )
