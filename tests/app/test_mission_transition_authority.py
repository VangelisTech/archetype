# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import json
from typing import Any

import pytest
from pydantic import ValidationError

from archetype.app.missions import (
    MISSION_TRANSITION_GRAPH,
    Attempt,
    AttemptStatus,
    Checkpoint,
    Finalization,
    Mission,
    MissionService,
    MissionStatus,
    MissionTaskState,
    MissionTransitionEvent,
    MissionTransitionGraph,
    TaskGate,
    TaskStatus,
)


def _row(*, max_attempts: int = 3, plan: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    plan = plan or [
        {
            "name": "fix",
            "prompt": "Fix the bug",
            "validators": [{"name": "tests", "command": ["pytest"]}],
        }
    ]
    return {
        "world_id": "world-test",
        "run_id": "run-test",
        "entity_id": 7,
        "mission__name": "mission",
        "mission__plan_json": json.dumps(plan),
        "mission__status": MissionStatus.READY.value,
        "mission__finished": False,
        "mission__succeeded": False,
        "mission__failure_reason": "",
        "mission__pr_ready": False,
        "mission__pr_url": "",
        "taskgate__step_index": 0,
        "taskgate__step_name": plan[0]["name"],
        "taskgate__prompt": plan[0]["prompt"],
        "taskgate__validators_json": json.dumps(plan[0]["validators"]),
        "taskgate__attempts": 0,
        "taskgate__max_attempts": max_attempts,
        "taskgate__status": TaskStatus.READY.value,
        "taskgate__required_finalization_phase": "checkpointed",
        "taskgate__passed": False,
        "attempt__agent_session_id": "",
        "attempt__validator_details_json": "[]",
        "frictionlog__entries_json": "[]",
    }


def _outcome(
    request: Any,
    *,
    accepted: bool,
    checkpoint: bool = True,
    status: str | None = None,
    phase: str = "checkpointed",
) -> dict[str, Any]:
    provider_status = status or ("accepted" if accepted else "rejected")
    return {
        "attempt_id": f"attempt-{request.attempt_index}",
        "attempt_index": request.attempt_index,
        "idempotency_key": request.idempotency_key,
        "status": provider_status,
        "accepted": accepted,
        "harness": "fake",
        "agent_session_id": "session-test",
        "validator_details": [{"name": "tests", "passed": accepted}],
        "checkpoint_provider": "fake",
        "checkpoint_status": "created" if checkpoint else "failed",
        "checkpoint_restorable": checkpoint,
        "checkpoint_created_at_ms": 1,
        "checkpoint_expires_at_ms": 2,
        "sandbox_state_ref": "fake://checkpoint" if checkpoint else "",
        "finalization_phase": phase,
        "finalization_manifest_ref": "fake://manifest",
        "finalization_error": "" if checkpoint else "checkpoint unavailable",
        "results": {"tests": accepted},
        "trace_ref": "fake://trace",
        "traces_ref": "fake://traces",
        "filesystem_start_ref": "fake://fs-start",
        "filesystem_end_ref": "fake://fs-end",
        "filesystem_diff_ref": "fake://fs-diff",
        "git_status_ref": "fake://status",
        "git_patch_ref": "fake://patch",
        "git_bundle_ref": "fake://bundle",
        "context_ref": "fake://context",
        "friction": [{"kind": "note", "message": "fixture"}],
        "sha": "abc123" if accepted else "",
        "message": "fix: resolve bug" if accepted else "",
        "pushed": False,
    }


def test_transition_graph_is_complete_for_every_active_source_and_event() -> None:
    active = {
        MissionTaskState(MissionStatus.READY, TaskStatus.READY),
        MissionTaskState(MissionStatus.RUNNING, TaskStatus.READY),
        MissionTaskState(MissionStatus.RUNNING, TaskStatus.RETRYABLE),
    }
    assert len(MISSION_TRANSITION_GRAPH) == len(active) * len(MissionTransitionEvent)
    for source in active:
        for event in MissionTransitionEvent:
            edge = MissionTransitionGraph.transition(source, event)
            assert edge.source == source
            assert edge.event is event
            assert edge.attempt is not AttemptStatus.PENDING

    terminal = MissionTaskState(MissionStatus.SUCCEEDED, TaskStatus.PASSED)
    with pytest.raises(ValueError, match="illegal mission transition"):
        MissionTransitionGraph.transition(terminal, MissionTransitionEvent.MISSION_SUCCEEDED)
    with pytest.raises(ValueError, match="invalid persisted mission/task state"):
        MissionTransitionGraph.state("invented", "ready")
    with pytest.raises(ValueError, match="unknown mission transition event"):
        MissionTransitionGraph.transition(next(iter(active)), "invented")


def test_component_state_strings_are_enum_validated_without_breaking_arrow() -> None:
    assert Mission.to_pyarrow_schema().field("status").type == "string"
    assert TaskGate.to_pyarrow_schema().field("status").type == "string"
    assert Attempt.to_pyarrow_schema().field("status").type == "string"

    with pytest.raises(ValidationError, match="mission status"):
        Mission(status="succeeded")
    with pytest.raises(ValidationError):
        Mission(status="invented")
    with pytest.raises(ValidationError, match="passed flag"):
        TaskGate(status="passed", passed=False)
    with pytest.raises(ValidationError):
        Attempt(status="invented")
    with pytest.raises(ValidationError):
        Checkpoint(status="invented")
    with pytest.raises(ValidationError):
        Finalization(phase="invented")


def test_rejection_and_incomplete_evidence_commit_edges_before_success() -> None:
    service = MissionService()
    initial = _row()

    first = service.prepare_attempt(initial, tick=1)
    assert first is not None
    rejected = service.apply_attempt(initial, first, _outcome(first, accepted=False))
    assert rejected["mission__status"] == "running"
    assert rejected["taskgate__status"] == "retryable"
    assert rejected["attempt__status"] == "rejected"
    assert rejected["attempt__transition_event"] == "rejected_retry"
    assert rejected["attempt__mission_status_before"] == "ready"
    assert rejected["attempt__mission_status_after"] == "running"
    assert rejected["taskgate__step_index"] == 0

    second = service.prepare_attempt(rejected, tick=2)
    assert second is not None
    incomplete = service.apply_attempt(
        rejected,
        second,
        _outcome(second, accepted=True, checkpoint=False),
    )
    assert incomplete["attempt__provider_status"] == "accepted"
    assert incomplete["attempt__status"] == "incomplete"
    assert incomplete["attempt__transition_event"] == "incomplete_retry"
    assert incomplete["mission__finished"] is False

    third = service.prepare_attempt(incomplete, tick=3)
    assert third is not None
    completed = service.apply_attempt(incomplete, third, _outcome(third, accepted=True))
    assert completed["mission__status"] == "succeeded"
    assert completed["mission__finished"] is True
    assert completed["mission__succeeded"] is True
    assert completed["taskgate__status"] == "passed"
    assert completed["taskgate__passed"] is True
    assert completed["attempt__status"] == "accepted"
    assert completed["attempt__transition_event"] == "mission_succeeded"
    assert completed["commit__sha"] == "abc123"
    assert service.prepare_attempt(completed, tick=4) is None


@pytest.mark.parametrize(
    ("accepted", "status", "checkpoint", "attempt_status", "event"),
    [
        (False, "rejected", True, "rejected", "rejected_exhausted"),
        (False, "failed", False, "failed", "failed_exhausted"),
        (True, "accepted", False, "incomplete", "incomplete_exhausted"),
    ],
)
def test_each_failed_attempt_kind_exhausts_through_the_typed_graph(
    accepted: bool,
    status: str,
    checkpoint: bool,
    attempt_status: str,
    event: str,
) -> None:
    service = MissionService()
    row = _row(max_attempts=1)
    request = service.prepare_attempt(row, tick=0)
    assert request is not None
    result = service.apply_attempt(
        row,
        request,
        _outcome(request, accepted=accepted, checkpoint=checkpoint, status=status),
    )
    assert result["mission__status"] == "failed"
    assert result["mission__finished"] is True
    assert result["mission__succeeded"] is False
    assert result["taskgate__status"] == "exhausted"
    assert result["attempt__status"] == attempt_status
    assert result["attempt__transition_event"] == event


def test_multistep_acceptance_advances_to_a_typed_ready_state() -> None:
    service = MissionService()
    row = _row(
        plan=[
            {
                "name": "fix",
                "prompt": "Fix the bug",
                "validators": [{"name": "tests", "command": ["pytest"]}],
            },
            {
                "name": "review",
                "prompt": "Review the fix",
                "validators": [{"name": "lint", "command": ["ruff"]}],
            },
        ]
    )
    request = service.prepare_attempt(row, tick=0)
    assert request is not None
    advanced = service.apply_attempt(row, request, _outcome(request, accepted=True))
    assert advanced["mission__status"] == "running"
    assert advanced["taskgate__status"] == "ready"
    assert advanced["taskgate__step_index"] == 1
    assert advanced["taskgate__step_name"] == "review"
    assert advanced["taskgate__attempts"] == 0
    assert advanced["attempt__transition_event"] == "task_advanced"

    next_request = service.prepare_attempt(advanced, tick=1)
    assert next_request is not None
    assert next_request.step_index == 1
    assert next_request.source == MissionTaskState(MissionStatus.RUNNING, TaskStatus.READY)


def test_prepare_attempt_fails_closed_on_corrupt_or_inactive_state() -> None:
    service = MissionService()
    cases = []

    invalid_state = _row()
    invalid_state["taskgate__status"] = "invented"
    cases.append((invalid_state, "invalid persisted"))

    inconsistent_terminal = _row()
    inconsistent_terminal["mission__status"] = "succeeded"
    cases.append((inconsistent_terminal, "finished flag"))

    exhausted_counter = _row(max_attempts=1)
    exhausted_counter["taskgate__attempts"] = 1
    cases.append((exhausted_counter, "attempt counters"))

    outside_plan = _row()
    outside_plan["taskgate__step_index"] = 2
    cases.append((outside_plan, "outside its plan"))

    invalid_previous = _row()
    invalid_previous["taskgate__attempts"] = 1
    invalid_previous["mission__status"] = "running"
    invalid_previous["taskgate__status"] = "retryable"
    invalid_previous["attempt__validator_details_json"] = "{}"
    cases.append((invalid_previous, "validator details"))

    for row, message in cases:
        with pytest.raises(ValueError, match=message):
            service.prepare_attempt(row, tick=0)


def test_apply_attempt_rejects_stale_identity_and_vacuous_evidence() -> None:
    service = MissionService()
    row = _row()
    request = service.prepare_attempt(row, tick=0)
    assert request is not None

    stale = dict(row, mission__status="running")
    with pytest.raises(ValueError, match="state changed"):
        service.apply_attempt(stale, request, _outcome(request, accepted=False))

    mismatch = _outcome(request, accepted=False)
    mismatch["attempt_index"] = 99
    with pytest.raises(ValueError, match="attempt_index"):
        service.apply_attempt(row, request, mismatch)

    mismatch = _outcome(request, accepted=False)
    mismatch["idempotency_key"] = "wrong"
    with pytest.raises(ValueError, match="idempotency_key"):
        service.apply_attempt(row, request, mismatch)

    vacuous = _outcome(request, accepted=False)
    vacuous["validator_details"] = []
    with pytest.raises(ValueError, match="validator details"):
        service.apply_attempt(row, request, vacuous)

    wrong_status = _outcome(request, accepted=True, status="rejected")
    with pytest.raises(ValueError, match="accepted status"):
        service.apply_attempt(row, request, wrong_status)

    bad_checkpoint = _outcome(request, accepted=True)
    bad_checkpoint["checkpoint_status"] = "failed"
    with pytest.raises(ValueError, match="restorable checkpoint"):
        service.apply_attempt(row, request, bad_checkpoint)

    no_commit = _outcome(request, accepted=True)
    no_commit["sha"] = ""
    with pytest.raises(ValueError, match="commit SHA"):
        service.apply_attempt(row, request, no_commit)


def test_attempt_identity_binds_typed_source_state() -> None:
    service = MissionService()
    ready = _row()
    first = service.prepare_attempt(ready, tick=0)
    assert first is not None

    running = dict(ready, mission__status="running")
    second = service.prepare_attempt(running, tick=0)
    assert second is not None
    assert first.idempotency_key != second.idempotency_key
    assert first.source.mission is MissionStatus.READY
    assert second.source.mission is MissionStatus.RUNNING
