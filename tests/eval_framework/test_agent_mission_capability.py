# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Counterfactual strength checks for the Agent Missions capability eval."""

from __future__ import annotations

from dataclasses import replace

import pytest

from archetype.app.missions.service import MissionService
from archetype.missions.coding_agents.harness import CodingAgentHarness
from archetype.missions.critics import CriticExecutionResult
from archetype.missions.processors import TaskDecisionProcessor
from archetype.missions.sandboxes import ProcessResult, SandboxIdentity
from evals.harness import EvalHarness
from evals.suites.capability import agent_missions
from evals.types import TaskResult


def _evaluate_transition_authority() -> TaskResult:
    harness = EvalHarness()
    agent_missions.register(harness)
    [result] = harness.run()
    assert result.task_id == "agent_mission_transition_authority"
    return result


def test_agent_mission_capability_executes_the_real_closed_loop() -> None:
    result = _evaluate_transition_authority()

    assert result.all_passed
    assert result.trials[0].error is None
    assert [grader.grader_name for grader in result.trials[0].grader_results] == [
        "mission_processors_own_transitions",
        "mission_retry_uses_repository_evidence",
        "mission_exact_head_critic_gates_promotion",
        "mission_publication_and_cleanup_are_real",
    ]


def test_agent_mission_capability_fails_without_task_decision_authority(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    async def fail_decision(*args: object, **kwargs: object) -> None:
        del args, kwargs
        raise RuntimeError("counterfactual task decision failure")

    monkeypatch.setattr(TaskDecisionProcessor, "process", fail_decision)

    result = _evaluate_transition_authority()

    assert not result.all_passed
    assert result.trials[0].error is not None
    assert "compute phase" in result.trials[0].error
    assert "counterfactual task decision failure" in caplog.text


def test_agent_mission_capability_fails_without_coding_agent_harness(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fail_execution(*args: object, **kwargs: object) -> None:
        del args, kwargs
        raise RuntimeError("counterfactual coding harness failure")

    monkeypatch.setattr(CodingAgentHarness, "execute", fail_execution)

    result = _evaluate_transition_authority()

    assert not result.all_passed
    assert result.trials[0].error is None
    assert {
        grader.grader_name for grader in result.trials[0].grader_results if not grader.passed
    } == {
        "mission_processors_own_transitions",
        "mission_retry_uses_repository_evidence",
        "mission_exact_head_critic_gates_promotion",
        "mission_publication_and_cleanup_are_real",
    }


def test_agent_mission_capability_fails_when_validators_are_bypassed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def bypass_validators(
        _self: object,
        _session: object,
        request: object,
        *,
        task_base_revision: str = "",
    ) -> tuple[tuple[object, ProcessResult], ...]:
        del request, task_base_revision
        return ()

    monkeypatch.setattr(CodingAgentHarness, "_run_validators", bypass_validators)

    result = _evaluate_transition_authority()

    assert not result.all_passed
    assert {
        grader.grader_name for grader in result.trials[0].grader_results if not grader.passed
    } == {
        "mission_processors_own_transitions",
        "mission_retry_uses_repository_evidence",
        "mission_exact_head_critic_gates_promotion",
        "mission_publication_and_cleanup_are_real",
    }


def test_agent_mission_capability_fails_when_publication_is_skipped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def skip_push(_self: object, _session: object, _branch: str) -> None:
        return None

    monkeypatch.setattr(CodingAgentHarness, "_push", skip_push)

    result = _evaluate_transition_authority()

    assert not result.all_passed
    assert result.trials[0].error is not None
    assert "critic review budget exhausted" in result.trials[0].error


def test_agent_mission_capability_fails_on_stale_validation_revision(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stage_result = MissionService._stage_result

    async def stage_stale_validation(
        self: MissionService,
        result: object,
        sandbox_status: object,
        *,
        bind_mission: bool,
    ) -> int:
        stale = replace(
            result,
            validation=tuple(
                replace(observation, revision="stale-revision") for observation in result.validation
            ),
        )
        return await stage_result(
            self,
            stale,
            sandbox_status,
            bind_mission=bind_mission,
        )

    monkeypatch.setattr(MissionService, "_stage_result", stage_stale_validation)

    result = _evaluate_transition_authority()

    assert not result.all_passed
    assert {
        grader.grader_name for grader in result.trials[0].grader_results if not grader.passed
    } == {
        "mission_processors_own_transitions",
        "mission_retry_uses_repository_evidence",
        "mission_exact_head_critic_gates_promotion",
        "mission_publication_and_cleanup_are_real",
    }


def test_agent_mission_capability_fails_without_staged_critic_evidence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stage_result = MissionService._stage_critic_result

    async def drop_critic_evidence(
        self: MissionService,
        result: CriticExecutionResult,
    ) -> int:
        return await stage_result(
            self,
            replace(result, findings=(), receipt=None),
        )

    monkeypatch.setattr(MissionService, "_stage_critic_result", drop_critic_evidence)

    result = _evaluate_transition_authority()

    assert not result.all_passed
    assert result.trials[0].error is not None
    assert "critic review budget exhausted" in result.trials[0].error


def test_agent_mission_capability_rejects_same_author_critic_evidence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stage_result = MissionService._stage_critic_result

    async def stage_same_author(
        self: MissionService,
        result: CriticExecutionResult,
    ) -> int:
        same_author = SandboxIdentity(
            result.sandbox.provider,
            result.request.author_sandbox_id,
            result.sandbox.environment,
        )
        return await stage_result(self, replace(result, sandbox=same_author))

    monkeypatch.setattr(MissionService, "_stage_critic_result", stage_same_author)

    result = _evaluate_transition_authority()

    assert not result.all_passed
    assert result.trials[0].error is not None
    assert "did not terminate" in result.trials[0].error


def test_agent_mission_capability_rejects_wrong_candidate_receipt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stage_result = MissionService._stage_critic_result

    async def stage_wrong_candidate(
        self: MissionService,
        result: CriticExecutionResult,
    ) -> int:
        if result.receipt is None:
            return await stage_result(self, result)
        wrong_receipt = replace(result.receipt, candidate_digest="wrong-candidate")
        return await stage_result(self, replace(result, receipt=wrong_receipt))

    monkeypatch.setattr(MissionService, "_stage_critic_result", stage_wrong_candidate)

    result = _evaluate_transition_authority()

    assert not result.all_passed
    assert result.trials[0].error is not None
    assert "did not terminate" in result.trials[0].error


def test_agent_mission_capability_rejects_wrong_head_receipt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stage_result = MissionService._stage_critic_result

    async def stage_wrong_head(
        self: MissionService,
        result: CriticExecutionResult,
    ) -> int:
        wrong_request = replace(result.request, head_revision="wrong-head")
        return await stage_result(self, replace(result, request=wrong_request))

    monkeypatch.setattr(MissionService, "_stage_critic_result", stage_wrong_head)

    result = _evaluate_transition_authority()

    assert not result.all_passed
    assert result.trials[0].error is not None
    assert "did not terminate" in result.trials[0].error


def test_agent_mission_capability_fails_when_blocking_findings_are_dropped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stage_result = MissionService._stage_critic_result

    async def stage_without_findings(
        self: MissionService,
        result: CriticExecutionResult,
    ) -> int:
        return await stage_result(self, replace(result, findings=()))

    monkeypatch.setattr(MissionService, "_stage_critic_result", stage_without_findings)

    result = _evaluate_transition_authority()

    assert not result.all_passed
    assert result.trials[0].error is None
    assert {
        grader.grader_name for grader in result.trials[0].grader_results if not grader.passed
    } == {"mission_exact_head_critic_gates_promotion"}
