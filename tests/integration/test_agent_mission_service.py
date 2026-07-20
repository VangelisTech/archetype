# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Dogfood contract for the batteries-included Agent Missions V1 surface."""

from __future__ import annotations

import pytest

from archetype import ArchetypeRuntime
from archetype.app.missions.agent_service import AgentMissionService
from archetype.core.config import StorageConfig
from archetype.missions import (
    AgentMissionConfig,
    AgentTask,
    CommandValidator,
    DependsOn,
    ExecutionOutcome,
    RepositoryPublicationPolicy,
    TaskExecutionReceipt,
    TaskExecutionRequest,
    ValidatorResult,
)
from archetype.missions.coding_agents import AgentTaskPolicy, AgentTaskState


class _Sandbox:
    """Reject the first regression attempt, then accept verified commits."""

    def __init__(self) -> None:
        self.batches = []
        self.closed_missions: list[int] = []

    async def run_many(self, requests):
        self.batches.append(requests)
        receipts = []
        for request in requests:
            accepted = not (request.task_name == "regression" and request.attempt_index == 1)
            results = tuple(
                ValidatorResult(
                    name=validator.name,
                    command=validator.command,
                    returncode=(
                        validator.expected_returncode
                        if accepted
                        else validator.expected_returncode + 1
                    ),
                    passed=accepted,
                    stderr="" if accepted else "focused gate failed",
                )
                for validator in request.validators
            )
            receipts.append(
                TaskExecutionReceipt(
                    mission_id=request.mission_id,
                    task_id=request.task_id,
                    attempt_id=request.attempt_id,
                    attempt_index=request.attempt_index,
                    outcome=(ExecutionOutcome.ACCEPTED if accepted else ExecutionOutcome.REJECTED),
                    validator_results=results,
                    sandbox_id="sandbox-contract",
                    worktree=f"/worktrees/{request.task_name}",
                    agent_session_id=f"session-{request.task_name}",
                    commit_sha=(
                        f"{request.task_name}-sha-{request.attempt_index}" if accepted else ""
                    ),
                    pushed=accepted,
                    error="" if accepted else "focused gate failed",
                )
            )
        return tuple(receipts)

    async def close_mission(self, mission_id: int) -> None:
        self.closed_missions.append(mission_id)

    async def close(self) -> None:
        return None


@pytest.mark.asyncio
async def test_explicit_task_relationships_drive_retry_and_downstream_readiness(
    tmp_path,
) -> None:
    sandbox = _Sandbox()
    storage = StorageConfig(uri=str(tmp_path / "agent_missions"), namespace="contract")

    async with ArchetypeRuntime() as runtime:
        missions = runtime.missions(
            "agent-mission-contract",
            config=AgentMissionConfig(sandbox=sandbox, max_ticks=30),
            storage=storage,
        )
        submitted = await missions.submit(
            repository="VangelisTech/archetype",
            branch="agent/explicit-task-graph",
            tasks=[
                AgentTask(
                    name="regression",
                    prompt="Commit a deterministic regression test.",
                    validators=(CommandValidator("focused", ("pytest", "-q")),),
                    max_attempts=2,
                ),
                AgentTask(
                    name="implementation",
                    prompt="Implement the smallest fix.",
                    validators=(CommandValidator("focused", ("pytest", "-q")),),
                    depends_on=("regression",),
                ),
            ],
        )

        result = await missions.run(submitted)

        assert result.status == "succeeded"
        assert [(task.name, task.attempts) for task in result.tasks] == [
            ("regression", 2),
            ("implementation", 1),
        ]
        assert sandbox.closed_missions == [submitted.mission_id]
        assert [(batch[0].task_name, batch[0].attempt_index) for batch in sandbox.batches] == [
            ("regression", 1),
            ("regression", 2),
            ("implementation", 1),
        ]
        assert all(
            request.publication_policy is RepositoryPublicationPolicy.COMMIT_AND_PUSH
            for batch in sandbox.batches
            for request in batch
        )

        policy_rows = (await missions.query(AgentTaskPolicy)).to_pylist()
        policy = AgentTaskPolicy.get_prefix()
        assert {
            str(row[f"{policy}publication_policy"]) for row in policy_rows if row["is_active"]
        } == {RepositoryPublicationPolicy.COMMIT_AND_PUSH.value}

        relationships = (await missions.query(DependsOn)).to_pylist()
        dependency = DependsOn.get_prefix()
        assert {
            (row[f"{dependency}source"], row[f"{dependency}target"])
            for row in relationships
            if row["is_active"]
        } == {
            (
                submitted.task_id("implementation"),
                submitted.task_id("regression"),
            )
        }

        history = (await missions.query(AgentTaskState)).to_pylist()
        state_ticks: dict[tuple[int, str], list[int]] = {}
        state = AgentTaskState.get_prefix()
        for row in sorted(history, key=lambda value: value["tick"]):
            if row["is_active"]:
                key = (int(row["entity_id"]), str(row[f"{state}status"]))
                state_ticks.setdefault(key, []).append(int(row["tick"]))
        regression_accepted = min(state_ticks[(submitted.task_id("regression"), "accepted")])
        implementation_dispatched = min(
            state_ticks[(submitted.task_id("implementation"), "dispatched")]
        )
        assert implementation_dispatched > regression_accepted


def test_commit_and_push_policy_rejects_an_unpushed_accepted_receipt() -> None:
    validator = CommandValidator("focused", ("pytest", "-q"))
    request = TaskExecutionRequest(
        mission_id=1,
        task_id=2,
        task_name="implementation",
        repository="VangelisTech/archetype",
        branch="agent/policy",
        base_ref="main",
        prompt="Implement the fix.",
        validators=(validator,),
        publication_policy=RepositoryPublicationPolicy.COMMIT_AND_PUSH,
        attempt_id="attempt-1",
        attempt_index=1,
    )
    receipt = TaskExecutionReceipt(
        mission_id=1,
        task_id=2,
        attempt_id="attempt-1",
        attempt_index=1,
        outcome=ExecutionOutcome.ACCEPTED,
        validator_results=(
            ValidatorResult(
                name=validator.name,
                command=validator.command,
                returncode=0,
                passed=True,
            ),
        ),
        commit_sha="abc123",
        pushed=False,
    )

    with pytest.raises(ValueError, match="commit-and-push policy"):
        AgentMissionService._validate_receipt(request, receipt)
