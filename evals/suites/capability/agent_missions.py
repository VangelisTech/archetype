# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Credential-free checks for the Agent Missions V1 state protocol."""

from __future__ import annotations

from archetype.missions import (
    TASK_TRANSITIONS,
    AgentExecution,
    AgentExecutionStatus,
    AgentTask,
    CommandValidator,
    Commit,
    DependsOn,
    MissionSubmission,
    RepositoryPublicationPolicy,
    TaskDispatch,
    TaskStatus,
    ValidationResult,
)
from evals.graders import state_check
from evals.harness import EvalHarness
from evals.types import GraderResult

SUITE = "capability"


def task_agent_mission_transition_authority() -> list[GraderResult]:
    """Grade the explicit DAG, committed intent, and revision-bound evidence."""

    validator = CommandValidator("focused", ("pytest", "-q"))
    submission = MissionSubmission(
        repository="VangelisTech/archetype",
        branch="agent/eval",
        tasks=(
            AgentTask("regression", "Write the regression.", (validator,)),
            AgentTask(
                "implementation",
                "Implement the fix.",
                (validator,),
                depends_on=("regression",),
            ),
        ),
    )
    dependency = DependsOn(source=2, target=1)
    dispatch = TaskDispatch(dispatch_id="dispatch-1", sequence=1)
    execution = AgentExecution(
        task_id=2,
        dispatch_id=dispatch.dispatch_id,
        dispatch_sequence=dispatch.sequence,
        status=AgentExecutionStatus.EXITED.value,
        final_revision="abc123",
        agent_returncode=0,
    )
    validation = ValidationResult(
        task_id=execution.task_id,
        validator_id=3,
        execution_id=4,
        dispatch_id=execution.dispatch_id,
        dispatch_sequence=execution.dispatch_sequence,
        revision=execution.final_revision,
        expected_returncode=0,
        actual_returncode=0,
    )
    commit = Commit(
        task_id=execution.task_id,
        execution_id=4,
        dispatch_id=execution.dispatch_id,
        sha=execution.final_revision,
        branch=submission.branch,
        pushed=True,
        final_revision=True,
    )

    return [
        state_check(
            {
                "tasks_are_explicit": [task.name for task in submission.tasks]
                == ["regression", "implementation"],
                "dependency_is_explicit": submission.tasks[1].depends_on == ("regression",),
                "dependency_model_is_relational": (
                    dependency.source == 2 and dependency.target == 1
                ),
                "publication_is_required": all(
                    task.publication_policy is RepositoryPublicationPolicy.COMMIT_AND_PUSH
                    for task in submission.tasks
                ),
            },
            name="mission_graph_is_data",
        ),
        state_check(
            {
                "dispatch_is_committed_intent": (
                    execution.dispatch_id == dispatch.dispatch_id
                    and execution.dispatch_sequence == dispatch.sequence
                ),
                "validation_is_revision_bound": validation.revision == execution.final_revision,
                "validator_passed": validation.passed,
                "commit_is_exact_final_revision": (
                    commit.sha == execution.final_revision
                    and commit.pushed
                    and commit.final_revision
                ),
                "decision_edges_are_typed": TASK_TRANSITIONS[TaskStatus.DISPATCHED]
                == frozenset({TaskStatus.READY, TaskStatus.ACCEPTED, TaskStatus.FAILED}),
            },
            name="mission_evidence_is_bound",
        ),
    ]


def register(harness: EvalHarness) -> None:
    harness.add(
        "agent_mission_transition_authority",
        suite=SUITE,
        fn=task_agent_mission_transition_authority,
        desc="Explicit task graphs advance from dispatch-bound validation and pushed revisions",
    )
