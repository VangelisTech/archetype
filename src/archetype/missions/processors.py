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
from archetype.missions.components import (
    AgentExecution,
    Candidate,
    Commit,
    CriticExecution,
    CriticReceipt,
    Mission,
    MissionState,
    Task,
    TaskCriticPolicy,
    TaskDispatch,
    TaskPolicy,
    TaskState,
    ValidationResult,
)
from archetype.missions.relations import DependsOn, Guards, PartOfMission
from archetype.missions.transitions import (
    AgentExecutionStatus,
    CriticConclusion,
    MissionStatus,
    TaskStatus,
)


class TaskDecisionProcessor(AsyncProcessor):
    """Create candidates, then accept or repair from exact independent review."""

    components = (TaskState, TaskDispatch, TaskPolicy, TaskCriticPolicy)
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
        commit = Commit.get_prefix()
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

        commits = view.frame(Commit)
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
        state = TaskState.get_prefix()
        policy = TaskPolicy.get_prefix()
        dispatched = cast(
            Expression,
            col(f"{state}status") == TaskStatus.DISPATCHED.value,
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
        author_green = (
            dispatched
            & has_terminal
            & exited
            & positive_guard_count
            & complete_validation
            & passing_validation
            & published_revision
        )
        author_rejected = dispatched & has_terminal & ~author_green

        candidate_frame = view.frame(Candidate)
        candidate = Candidate.get_prefix()
        review_conclusion = "_review_conclusion"
        review_policy_digest = "_review_policy_digest"
        review_attempts = "_review_attempt_count"
        receipts = view.frame(CriticReceipt)
        critic_executions = view.frame(CriticExecution)
        candidates = (
            candidate_frame.select(
                col("entity_id").alias("_candidate_entity_id"),
                col(f"{candidate}task_id").alias("_candidate_task_id"),
                col(f"{candidate}dispatch_id").alias("_candidate_dispatch_id"),
                col(f"{candidate}author_sandbox_id").alias("_candidate_author_sandbox_id"),
                col(f"{candidate}base_revision").alias("_candidate_base_revision"),
                col(f"{candidate}head_revision").alias("_candidate_head_revision"),
                col(f"{candidate}diff_digest").alias("_candidate_diff_digest"),
                col(f"{candidate}validator_bundle_digest").alias(
                    "_candidate_validator_bundle_digest"
                ),
                col(f"{candidate}policy_digest").alias("_candidate_policy_digest"),
                col(f"{candidate}candidate_digest").alias("_candidate_digest"),
            )
            if candidate_frame is not None
            else None
        )
        if candidates is not None and receipts is not None:
            receipt = CriticReceipt.get_prefix()
            reviews = receipts.join(
                candidates,
                left_on=f"{receipt}candidate_entity_id",
                right_on="_candidate_entity_id",
            )
            reviews = reviews.where(
                (col(f"{receipt}critic_sandbox_id") != col("_candidate_author_sandbox_id"))
                & (col(f"{receipt}candidate_digest") == col("_candidate_digest"))
                & (col(f"{receipt}policy_digest") == col("_candidate_policy_digest"))
                & (col(f"{receipt}reviewed_base_revision") == col("_candidate_base_revision"))
                & (col(f"{receipt}reviewed_head_revision") == col("_candidate_head_revision"))
                & (col(f"{receipt}reviewed_diff_digest") == col("_candidate_diff_digest"))
                & (
                    col(f"{receipt}validator_bundle_digest")
                    == col("_candidate_validator_bundle_digest")
                )
            ).select(
                col("_candidate_task_id").alias("_review_task_id"),
                col("_candidate_dispatch_id").alias("_review_dispatch_id"),
                col(f"{receipt}conclusion").alias(review_conclusion),
                col("_candidate_policy_digest").alias(review_policy_digest),
            )
            df = df.join(
                reviews,
                left_on=["entity_id", f"{dispatch}dispatch_id"],
                right_on=["_review_task_id", "_review_dispatch_id"],
                how="left",
            )
        else:
            df = df.with_column(review_conclusion, lit(None))
            df = df.with_column(review_policy_digest, lit(None))
        if candidates is not None and critic_executions is not None:
            # Committed CriticExecution facts are the consumed review budget —
            # the same domain counter the critic-Activity projection reads, so
            # a candidate the projection stops admitting also resolves here
            # instead of waiting out the caller's tick budget.
            critic_execution = CriticExecution.get_prefix()
            attempt_counts = critic_executions.groupby(
                f"{critic_execution}candidate_entity_id"
            ).agg(col("entity_id").count().alias(review_attempts))
            attempted = candidates.join(
                attempt_counts,
                left_on="_candidate_entity_id",
                right_on=f"{critic_execution}candidate_entity_id",
            ).select(
                col("_candidate_task_id").alias("_attempt_task_id"),
                col("_candidate_dispatch_id").alias("_attempt_dispatch_id"),
                col(review_attempts),
            )
            df = df.join(
                attempted,
                left_on=["entity_id", f"{dispatch}dispatch_id"],
                right_on=["_attempt_task_id", "_attempt_dispatch_id"],
                how="left",
            )
        else:
            df = df.with_column(review_attempts, lit(0))

        critic_policy = TaskCriticPolicy.get_prefix()
        candidate_created = dispatched & author_green
        awaiting_review = cast(
            Expression,
            col(f"{state}status") == TaskStatus.CANDIDATE.value,
        )
        current_policy_review = (
            col(review_policy_digest) == col(f"{critic_policy}digest")
        ).fill_null(False)
        approved = (
            awaiting_review
            & current_policy_review
            & cast(Expression, col(review_conclusion) == CriticConclusion.APPROVED.value)
        )
        approved = approved.fill_null(False)
        blocked = (
            awaiting_review
            & current_policy_review
            & cast(Expression, col(review_conclusion) == CriticConclusion.BLOCKING.value)
        )
        blocked = blocked.fill_null(False)
        # A candidate with no matching independent receipt after its whole
        # committed review budget can never be approved or repaired; without a
        # terminal decision it waits out the caller's tick budget instead.
        review_budget_exhausted = (
            awaiting_review
            & col(review_conclusion).is_null()
            & (col(review_attempts).fill_null(0) >= col(f"{critic_policy}max_reviews"))
        )
        author_retryable = author_rejected & (
            col(f"{dispatch}sequence") < col(f"{policy}max_dispatches")
        )
        author_exhausted = author_rejected & ~author_retryable
        repairable = blocked & (col(f"{dispatch}sequence") < col(f"{policy}max_dispatches"))
        repair_exhausted = blocked & ~repairable
        failure_reason = when(
            col("_execution_error").fill_null("") != "",
            then=col("_execution_error"),
        ).otherwise(lit("validation or repository publication failed"))
        df = df.with_columns(
            {
                f"{state}status": when(
                    candidate_created,
                    then=lit(TaskStatus.CANDIDATE.value),
                )
                .when(author_retryable, then=lit(TaskStatus.READY.value))
                .when(author_exhausted, then=lit(TaskStatus.FAILED.value))
                .when(approved, then=lit(TaskStatus.ACCEPTED.value))
                .when(repairable, then=lit(TaskStatus.READY.value))
                .when(repair_exhausted, then=lit(TaskStatus.FAILED.value))
                .when(review_budget_exhausted, then=lit(TaskStatus.FAILED.value))
                .otherwise(col(f"{state}status")),
                f"{state}reason": when(author_exhausted, then=failure_reason)
                .when(
                    repair_exhausted,
                    then=lit("independent critic found blocking issues"),
                )
                .when(
                    review_budget_exhausted,
                    then=lit("independent critic review budget exhausted"),
                )
                .otherwise(col(f"{state}reason")),
            }
        )
        return df.select(*original)


class TaskReadinessProcessor(AsyncProcessor):
    """Move pending tasks to ready when every prerequisite is accepted."""

    components = (Task, TaskState)
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
        previous_tasks = view.frame(Task, TaskState)
        if previous_tasks is None:
            return df
        dependencies = view.frame(DependsOn)
        original = tuple(df.column_names)
        state = TaskState.get_prefix()

        if dependencies is None:
            unblocked = df
            marker = None
        else:
            relation = DependsOn.get_prefix()
            nonaccepted = previous_tasks.where(
                cast(
                    Expression,
                    col(f"{state}status") != TaskStatus.ACCEPTED.value,
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

        pending = cast(Expression, col(f"{state}status") == TaskStatus.PENDING.value)
        eligible = pending if marker is None else pending & marker
        unblocked = unblocked.with_column(
            f"{state}status",
            when(eligible, then=lit(TaskStatus.READY.value)).otherwise(col(f"{state}status")),
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
    if task_status != TaskStatus.READY.value:
        return {
            "task_status": task_status,
            "dispatch_id": dispatch_id,
            "sequence": sequence,
        }
    next_sequence = sequence + 1
    identity = hashlib.sha256(f"{entity_id}:{next_sequence}".encode()).hexdigest()
    return {
        "task_status": TaskStatus.DISPATCHED.value,
        "dispatch_id": identity,
        "sequence": next_sequence,
    }


class TaskDispatchProcessor(AsyncProcessor):
    """Turn ready tasks into durable external-work intent."""

    components = (Task, TaskState, TaskDispatch)
    priority = 30

    async def process(self, df: DataFrame, **_: Any) -> DataFrame:
        state = TaskState.get_prefix()
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

    components = (Mission, MissionState)
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
        tasks = view.frame(TaskState)
        memberships = view.frame(PartOfMission)
        if tasks is None or memberships is None:
            return df

        task_state = TaskState.get_prefix()
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
                    col(f"{task_state}status") == TaskStatus.ACCEPTED.value,
                ),
                then=lit(1),
            ).otherwise(lit(0)),
        )
        joined = joined.with_column(
            "_failed",
            when(
                cast(
                    Expression,
                    col(f"{task_state}status") == TaskStatus.FAILED.value,
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
        state = MissionState.get_prefix()
        running = cast(
            Expression,
            col(f"{state}status") == MissionStatus.RUNNING.value,
        )
        failed_count = cast(Expression, col("_failed_count") > 0)  # ty: ignore[unsupported-operator]
        task_count = cast(Expression, col("_task_count") > 0)  # ty: ignore[unsupported-operator]
        failed = running & failed_count
        succeeded = running & task_count & (col("_accepted_count") == col("_task_count"))
        df = df.with_columns(
            {
                f"{state}status": when(failed, then=lit(MissionStatus.FAILED.value))
                .when(succeeded, then=lit(MissionStatus.SUCCEEDED.value))
                .otherwise(col(f"{state}status")),
                f"{state}reason": when(failed, then=lit("a mission task failed")).otherwise(
                    col(f"{state}reason")
                ),
            }
        )
        return df.select(*original)


def mission_processors() -> tuple[AsyncProcessor, ...]:
    """Return the complete built-in task transition pipeline."""

    return (
        TaskDecisionProcessor(),
        TaskReadinessProcessor(),
        TaskDispatchProcessor(),
        MissionRollupProcessor(),
    )
