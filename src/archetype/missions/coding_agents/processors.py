# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Built-in data-centric state transition processors for Agent Missions."""

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
    AgentMissionRecord,
    AgentMissionState,
    AgentTaskAttempt,
    AgentTaskPolicy,
    AgentTaskRecord,
    AgentTaskState,
)
from archetype.missions.coding_agents.transitions import (
    AgentAttemptStatus,
    AgentMissionStatus,
    AgentTaskStatus,
)
from archetype.missions.relationships import DependsOn, PartOfMission


class TaskGateProcessor(AsyncProcessor):
    """Consume one sandbox receipt and accept, retry, or fail the task."""

    components = (AgentTaskState, AgentTaskAttempt, AgentTaskPolicy)
    priority = 10

    async def process(self, df: DataFrame, **_: Any) -> DataFrame:
        state = AgentTaskState.get_prefix()
        attempt = AgentTaskAttempt.get_prefix()
        policy = AgentTaskPolicy.get_prefix()
        original = tuple(df.column_names)

        dispatched = cast(Expression, col(f"{state}status") == AgentTaskStatus.DISPATCHED.value)
        unsettled = ~col(f"{attempt}settled")
        accepted = (
            dispatched
            & unsettled
            & cast(
                Expression,
                col(f"{attempt}status") == AgentAttemptStatus.ACCEPTED.value,
            )
        )
        rejected = (
            dispatched
            & unsettled
            & col(f"{attempt}status").is_in(
                [AgentAttemptStatus.REJECTED.value, AgentAttemptStatus.FAILED.value]
            )
        )
        retryable = rejected & (col(f"{attempt}attempt_index") < col(f"{policy}max_attempts"))
        exhausted = rejected & ~retryable
        consumed = accepted | rejected

        reason = when(exhausted, then=col(f"{attempt}error").fill_null(""))
        reason = reason.otherwise(col(f"{state}reason"))
        df = df.with_columns(
            {
                f"{state}status": when(accepted, then=lit(AgentTaskStatus.ACCEPTED.value))
                .when(retryable, then=lit(AgentTaskStatus.READY.value))
                .when(exhausted, then=lit(AgentTaskStatus.FAILED.value))
                .otherwise(col(f"{state}status")),
                f"{state}reason": reason,
                f"{attempt}settled": when(consumed, then=lit(True)).otherwise(
                    col(f"{attempt}settled")
                ),
            }
        )
        return df.select(*original)


class TaskReadinessProcessor(AsyncProcessor):
    """Move PENDING tasks to READY when every DependsOn target is accepted."""

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


_BEGIN_ATTEMPT = DataType.struct(
    {
        "task_status": DataType.string(),
        "attempt_id": DataType.string(),
        "attempt_index": DataType.int64(),
        "attempt_status": DataType.string(),
        "settled": DataType.bool(),
    }
)


@daft.func(return_dtype=_BEGIN_ATTEMPT)
def _begin_attempt(
    entity_id: int,
    task_status: str,
    attempt_id: str,
    attempt_index: int,
    attempt_status: str,
    settled: bool,
) -> dict[str, Any]:
    if task_status != AgentTaskStatus.READY.value:
        return {
            "task_status": task_status,
            "attempt_id": attempt_id,
            "attempt_index": attempt_index,
            "attempt_status": attempt_status,
            "settled": settled,
        }
    next_index = attempt_index + 1
    identity = hashlib.sha256(f"{entity_id}:{next_index}".encode()).hexdigest()
    return {
        "task_status": AgentTaskStatus.DISPATCHED.value,
        "attempt_id": identity,
        "attempt_index": next_index,
        "attempt_status": AgentAttemptStatus.PENDING.value,
        "settled": False,
    }


class TaskDispatchProcessor(AsyncProcessor):
    """Turn READY rows into durable execution intents after gate processing."""

    components = (AgentTaskRecord, AgentTaskState, AgentTaskAttempt)
    priority = 30

    async def process(self, df: DataFrame, **_: Any) -> DataFrame:
        state = AgentTaskState.get_prefix()
        attempt = AgentTaskAttempt.get_prefix()
        df = df.with_column(
            "_agent_mission_dispatch",
            _begin_attempt(
                col("entity_id"),
                col(f"{state}status"),
                col(f"{attempt}attempt_id"),
                col(f"{attempt}attempt_index"),
                col(f"{attempt}status"),
                col(f"{attempt}settled"),
            ),
        )
        df = df.with_column(f"{state}status", col("_agent_mission_dispatch")["task_status"])
        for field in ("attempt_id", "attempt_index", "status", "settled"):
            source = "attempt_status" if field == "status" else field
            df = df.with_column(f"{attempt}{field}", col("_agent_mission_dispatch")[source])
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
    """Return the complete built-in V1 transition pipeline."""

    return (
        TaskGateProcessor(),
        TaskReadinessProcessor(),
        TaskDispatchProcessor(),
        MissionRollupProcessor(),
    )
