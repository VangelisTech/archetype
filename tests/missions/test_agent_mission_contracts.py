# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Focused contracts for the explicit-task Agent Missions V1 boundary."""

from __future__ import annotations

import pytest

from archetype.missions import (
    AgentTask,
    CommandValidator,
    MissionSubmission,
    RepositoryPublicationPolicy,
)
from archetype.missions.coding_agents import (
    AGENT_OUTPUT_COMPONENTS,
    AGENT_TASK_COMPONENTS,
    AgentExecution,
    AgentTaskPolicy,
    TaskDispatch,
    TaskValidator,
    ValidationResult,
    agent_mission_processors,
)


def _validator(name: str = "focused") -> CommandValidator:
    return CommandValidator(name=name, command=("pytest", "-q"))


def test_submission_is_an_explicit_typed_dag_without_a_planner_switch() -> None:
    submission = MissionSubmission(
        repository="VangelisTech/archetype",
        branch="agent/v1",
        tasks=(
            AgentTask("regression", "Commit the regression.", (_validator(),)),
            AgentTask(
                "implementation",
                "Implement the fix.",
                (_validator(),),
                depends_on=("regression",),
            ),
        ),
    )

    assert submission.tasks[1].depends_on == ("regression",)
    assert all(
        task.publication_policy is RepositoryPublicationPolicy.COMMIT_AND_PUSH
        for task in submission.tasks
    )
    assert AgentTaskPolicy().publication_policy == RepositoryPublicationPolicy.COMMIT_AND_PUSH.value
    assert "decompose" not in AgentTask.__dataclass_fields__
    assert "max_attempts" not in AgentTask.__dataclass_fields__
    assert [type(processor).__name__ for processor in agent_mission_processors()] == [
        "TaskDecisionProcessor",
        "TaskReadinessProcessor",
        "TaskDispatchProcessor",
        "MissionRollupProcessor",
    ]


@pytest.mark.parametrize(
    "tasks, message",
    [
        (
            (AgentTask("task", "Do it.", (_validator(),), depends_on=("missing",)),),
            "unknown task",
        ),
        (
            (
                AgentTask("one", "One.", (_validator(),), depends_on=("two",)),
                AgentTask("two", "Two.", (_validator(),), depends_on=("one",)),
            ),
            "acyclic",
        ),
    ],
)
def test_submission_rejects_invalid_relationships(tasks, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        MissionSubmission(repository="repo", branch="agent/test", tasks=tasks)


def test_validators_dispatches_and_outputs_are_first_class_state() -> None:
    validator = TaskValidator(
        name="expected-red",
        command=["pytest", "-q", "tests/test_bug.py"],
        expected_returncode=1,
    )
    dispatch = TaskDispatch(dispatch_id="dispatch", sequence=1)
    result = ValidationResult(
        task_id=2,
        validator_id=3,
        execution_id=4,
        dispatch_id=dispatch.dispatch_id,
        dispatch_sequence=dispatch.sequence,
        revision="abc123",
        expected_returncode=validator.expected_returncode,
        actual_returncode=1,
    )

    assert result.passed is True
    assert "passed" not in ValidationResult.model_fields
    assert "specs_json" not in TaskValidator.model_fields
    assert "attempt" not in " ".join(
        component.__name__.lower()
        for component in (*AGENT_TASK_COMPONENTS, *AGENT_OUTPUT_COMPONENTS)
    )
    assert AgentExecution.model_fields["status"].default == "starting"
