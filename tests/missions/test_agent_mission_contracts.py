# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Focused contracts for the explicit-task Agent Missions V1 boundary."""

from __future__ import annotations

import pytest

from archetype.missions import (
    AgentMissionSandbox,
    AgentTask,
    CommandValidator,
    MissionSubmission,
    RepositoryPublicationPolicy,
)
from archetype.missions.coding_agents import (
    AgentTaskEvidence,
    AgentTaskPolicy,
    AgentTaskValidators,
    agent_mission_processors,
)
from archetype.missions.contracts import (
    ExecutionOutcome,
    TaskExecutionReceipt,
    ValidatorResult,
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
    assert [type(processor).__name__ for processor in agent_mission_processors()] == [
        "TaskGateProcessor",
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


def test_typed_validators_and_receipts_are_arrow_encoded_only_inside_components() -> None:
    validator = CommandValidator(
        name="red",
        command=("pytest", "-q", "tests/test_bug.py"),
        expected_returncode=1,
    )
    component = AgentTaskValidators.from_specs((validator,))
    assert component.specs() == (validator,)

    receipt = TaskExecutionReceipt(
        mission_id=1,
        task_id=2,
        attempt_id="attempt",
        attempt_index=1,
        outcome=ExecutionOutcome.ACCEPTED,
        validator_results=(
            ValidatorResult(
                name=validator.name,
                command=validator.command,
                returncode=1,
                passed=True,
                stdout="expected red test",
            ),
        ),
        commit_sha="abc123",
    )
    evidence = AgentTaskEvidence.from_receipt(receipt)
    assert evidence.validator_results() == receipt.validator_results


class _Sandbox:
    async def run_many(self, requests):
        return ()

    async def close_mission(self, mission_id: int) -> None:
        return None

    async def close(self) -> None:
        return None


def test_sandbox_is_a_narrow_world_resource_protocol() -> None:
    assert isinstance(_Sandbox(), AgentMissionSandbox)
