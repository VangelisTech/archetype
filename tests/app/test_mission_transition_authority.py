# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import json
from typing import Any

import pytest
from pydantic import ValidationError

from archetype.app.limits import MAX_ICEBERG_SNAPSHOT_ID
from archetype.app.missions import (
    MissionService,
    attempt_invocation_fingerprint,
)
from archetype.missions import (
    MISSION_TRANSITION_GRAPH,
    Attempt,
    AttemptStatus,
    Checkpoint,
    Finalization,
    FinalizationPhase,
    Mission,
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
    checkpoint_expires_at_ms: int | None = 2,
) -> dict[str, Any]:
    provider_status = status or ("accepted" if accepted else "rejected")
    return {
        "attempt_id": request.attempt_id,
        "attempt_index": request.attempt_index,
        "idempotency_key": request.idempotency_key,
        "request_fingerprint": attempt_invocation_fingerprint(
            prompt=request.prompt,
            validators=request.validators,
            step_name=request.step_name,
            attempt_index=request.attempt_index,
            previous_session_id=request.previous_session_id,
            previous_validator_details=request.previous_validator_details,
            correlation=request.correlation,
        ),
        "status": provider_status,
        "accepted": accepted,
        "harness": "fake",
        "agent_session_id": "session-test",
        "validator_details": [
            {
                "name": "tests",
                "command": ["pytest"],
                "expected_returncode": 0,
                "returncode": 0 if accepted else 1,
                "passed": accepted,
            }
        ],
        "checkpoint_provider": "fake",
        "checkpoint_status": "created" if checkpoint else "failed",
        "checkpoint_restorable": checkpoint,
        "checkpoint_created_at_ms": 1,
        "checkpoint_expires_at_ms": checkpoint_expires_at_ms,
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
    assert Checkpoint(status="disabled").status == "disabled"
    with pytest.raises(ValidationError):
        Finalization(phase="invented")


def test_indexed_finalization_requires_authoritative_bundle_evidence() -> None:
    with pytest.raises(ValidationError, match="indexed finalization requires"):
        Finalization(phase=FinalizationPhase.INDEXED.value)

    finalized = Finalization(
        phase=FinalizationPhase.INDEXED.value,
        manifest_ref="s3://artifacts/manifest.json",
        bundle_id="b" * 64,
        request_digest="c" * 64,
        producer_digest="d" * 64,
        redaction_policy_id="policy-v1",
        index_snapshot_id=17,
    )
    assert finalized.phase == "indexed"
    assert finalized.index_snapshot_id == 17
    assert (
        Finalization(
            phase=FinalizationPhase.INDEXED.value,
            manifest_ref="s3://artifacts/manifest.json",
            bundle_id="b" * 64,
            request_digest="c" * 64,
            producer_digest="d" * 64,
            redaction_policy_id="policy-v1",
            index_snapshot_id=MAX_ICEBERG_SNAPSHOT_ID,
        ).index_snapshot_id
        == MAX_ICEBERG_SNAPSHOT_ID
    )
    for invalid_snapshot in (MAX_ICEBERG_SNAPSHOT_ID + 1, 1.5, True):
        with pytest.raises(ValidationError, match="index_snapshot_id|snapshot"):
            Finalization(
                phase=FinalizationPhase.INDEXED.value,
                manifest_ref="s3://artifacts/manifest.json",
                bundle_id="b" * 64,
                request_digest="c" * 64,
                producer_digest="d" * 64,
                redaction_policy_id="policy-v1",
                index_snapshot_id=invalid_snapshot,
            )

    for phase in (FinalizationPhase.PUBLISHED, FinalizationPhase.INDEXED):
        legacy = Finalization(
            phase=phase.value,
            manifest_ref="legacy://manifest",
            legacy_unbound=True,
        )
        assert legacy.legacy_unbound is True
        assert legacy.index_snapshot_id == 0
    with pytest.raises(ValidationError, match="published or indexed"):
        Finalization(
            phase=FinalizationPhase.CHECKPOINTED.value,
            legacy_unbound=True,
        )
    with pytest.raises(ValidationError, match="cannot contain current authority"):
        Finalization(
            phase=FinalizationPhase.INDEXED.value,
            bundle_id="b" * 64,
            legacy_unbound=True,
        )


def test_uploaded_and_legacy_published_never_impersonate_indexed_authority() -> None:
    service = MissionService()
    indexed_row = _row()
    indexed_row["taskgate__required_finalization_phase"] = "indexed"

    uploaded_request = service.prepare_attempt(indexed_row, tick=0)
    assert uploaded_request is not None
    uploaded = service.apply_attempt(
        indexed_row,
        uploaded_request,
        _outcome(uploaded_request, accepted=True, phase="uploaded"),
    )
    assert uploaded["attempt__status"] == "incomplete"
    assert uploaded["taskgate__status"] == "retryable"

    published_request = service.prepare_attempt(indexed_row, tick=0)
    assert published_request is not None
    legacy_but_not_indexed = service.apply_attempt(
        indexed_row,
        published_request,
        _outcome(published_request, accepted=True, phase="published"),
    )
    assert legacy_but_not_indexed["attempt__status"] == "incomplete"

    legacy_row = _row()
    legacy_request = service.prepare_attempt(legacy_row, tick=0)
    assert legacy_request is not None
    compatible = service.apply_attempt(
        legacy_row,
        legacy_request,
        _outcome(legacy_request, accepted=True, phase="published"),
    )
    assert compatible["attempt__status"] == "accepted"
    assert compatible["mission__status"] == "succeeded"


@pytest.mark.parametrize("invalid_tick", [-1, 1.5, True])
def test_prepare_attempt_requires_an_exact_non_negative_observation_tick(
    invalid_tick: Any,
) -> None:
    with pytest.raises((TypeError, ValueError), match="observation tick"):
        MissionService().prepare_attempt(_row(), tick=invalid_tick)

    request = MissionService().prepare_attempt(_row(), tick=23)
    assert request is not None
    assert request.observation_tick == 23


@pytest.mark.parametrize("required_phase", list(FinalizationPhase))
def test_current_apply_attempt_cannot_self_assert_indexed_authority(
    required_phase: FinalizationPhase,
) -> None:
    service = MissionService()
    row = _row()
    row["taskgate__required_finalization_phase"] = required_phase.value
    request = service.prepare_attempt(row, tick=0)
    assert request is not None
    outcome = _outcome(request, accepted=True, phase="indexed")

    with pytest.raises(ValueError, match="claim-bound settled projection"):
        service.apply_attempt(row, request, outcome)

    outcome.update(
        {
            "finalization_bundle_id": "b" * 64,
            "finalization_request_digest": "c" * 64,
            "finalization_producer_digest": "d" * 64,
            "finalization_redaction_policy_id": "policy-v1",
            "finalization_index_snapshot_id": 17,
        }
    )

    with pytest.raises(ValueError, match="claim-bound settled projection"):
        service.apply_attempt(row, request, outcome)

    outcome.update(
        {
            "artifact_publication_key": outcome["finalization_bundle_id"],
            "artifact_request_digest": outcome["finalization_request_digest"],
            "artifact_producer_digest": outcome["finalization_producer_digest"],
            "artifact_redaction_policy_id": outcome["finalization_redaction_policy_id"],
        }
    )
    with pytest.raises(ValueError, match="claim-bound settled projection"):
        service.apply_attempt(row, request, outcome)


@pytest.mark.parametrize(
    "snapshot",
    [MAX_ICEBERG_SNAPSHOT_ID, MAX_ICEBERG_SNAPSHOT_ID + 1, 1.5, True],
)
def test_current_apply_attempt_rejects_every_indexed_snapshot_representation(
    snapshot: Any,
) -> None:
    service = MissionService()
    row = _row()
    row["taskgate__required_finalization_phase"] = FinalizationPhase.INDEXED.value
    request = service.prepare_attempt(row, tick=0)
    assert request is not None
    outcome = _outcome(request, accepted=True, phase=FinalizationPhase.INDEXED.value)
    outcome.update(
        {
            "finalization_bundle_id": "b" * 64,
            "finalization_request_digest": "c" * 64,
            "finalization_producer_digest": "d" * 64,
            "finalization_redaction_policy_id": "policy-v1",
            "finalization_index_snapshot_id": snapshot,
            "artifact_publication_key": "b" * 64,
            "artifact_request_digest": "c" * 64,
            "artifact_producer_digest": "d" * 64,
            "artifact_redaction_policy_id": "policy-v1",
        }
    )

    with pytest.raises(ValueError, match="claim-bound settled projection"):
        service.apply_attempt(row, request, outcome)


@pytest.mark.parametrize(
    "staged_field",
    [
        "artifact_publication_key",
        "artifact_request_digest",
        "artifact_producer_digest",
        "artifact_redaction_policy_id",
    ],
)
def test_current_apply_attempt_rejects_each_staged_authority_marker(
    staged_field: str,
) -> None:
    service = MissionService()
    row = _row()
    row["taskgate__required_finalization_phase"] = FinalizationPhase.INDEXED.value
    request = service.prepare_attempt(row, tick=0)
    assert request is not None
    outcome = _outcome(request, accepted=True, phase=FinalizationPhase.INDEXED.value)
    outcome.update(
        {
            "finalization_bundle_id": "b" * 64,
            "finalization_request_digest": "c" * 64,
            "finalization_producer_digest": "d" * 64,
            "finalization_redaction_policy_id": "policy-v1",
            "finalization_index_snapshot_id": 17,
            "artifact_publication_key": "b" * 64,
            "artifact_request_digest": "c" * 64,
            "artifact_producer_digest": "d" * 64,
            "artifact_redaction_policy_id": "policy-v1",
        }
    )
    outcome[staged_field] = "changed-policy" if staged_field.endswith("policy_id") else "e" * 64

    with pytest.raises(ValueError, match="claim-bound settled projection"):
        service.apply_attempt(row, request, outcome)


def test_mission_projection_preserves_a_non_expiring_checkpoint() -> None:
    service = MissionService()
    row = _row()
    request = service.prepare_attempt(row, tick=0)
    assert request is not None

    updated = service.apply_attempt(
        row,
        request,
        _outcome(request, accepted=True, checkpoint_expires_at_ms=None),
    )

    assert updated["checkpoint__expires_at_ms"] is None
    assert Checkpoint(expires_at_ms=None).expires_at_ms is None


def test_mission_projection_canonicalizes_legacy_zero_and_rejects_invalid_expiry() -> None:
    service = MissionService()
    row = _row()
    request = service.prepare_attempt(row, tick=0)
    assert request is not None

    legacy = service.apply_attempt(
        row,
        request,
        _outcome(request, accepted=True, checkpoint_expires_at_ms=0),
    )
    assert legacy["checkpoint__expires_at_ms"] is None

    with pytest.raises(ValueError, match="expiration must be after creation"):
        service.apply_attempt(
            row,
            request,
            _outcome(request, accepted=True, checkpoint_expires_at_ms=1),
        )


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

    empty_command = _row(
        plan=[
            {
                "name": "fix",
                "prompt": "Fix the bug",
                "validators": [{"name": "tests", "command": []}],
            }
        ]
    )
    cases.append((empty_command, "non-empty string command"))

    invalid_timeout = _row(
        plan=[
            {
                "name": "fix",
                "prompt": "Fix the bug",
                "validators": [{"name": "tests", "command": ["pytest"], "timeout_seconds": 0}],
            }
        ]
    )
    cases.append((invalid_timeout, "timeout_seconds"))

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

    changed_plan = json.loads(row["mission__plan_json"])
    changed_plan.append(
        {
            "name": "review",
            "prompt": "Review the fix",
            "validators": [{"name": "lint", "command": ["ruff"]}],
        }
    )
    stale = dict(row, mission__plan_json=json.dumps(changed_plan))
    with pytest.raises(ValueError, match="plan changed"):
        service.apply_attempt(stale, request, _outcome(request, accepted=True))

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

    with pytest.raises(ValueError, match="max_attempts changed"):
        service.apply_attempt(
            dict(row, taskgate__max_attempts=4),
            request,
            _outcome(request, accepted=False),
        )

    with pytest.raises(ValueError, match="finalization gate changed"):
        service.apply_attempt(
            dict(row, taskgate__required_finalization_phase="indexed"),
            request,
            _outcome(request, accepted=True),
        )


def test_attempt_identity_binds_typed_source_state() -> None:
    service = MissionService()
    ready = _row()
    first = service.prepare_attempt(ready, tick=0)
    assert first is not None

    running = dict(ready, mission__status="running")
    second = service.prepare_attempt(running, tick=0)
    assert second is not None
    assert first.idempotency_key != second.idempotency_key
    assert first.plan_digest == second.plan_digest
    assert first.source.mission is MissionStatus.READY
    assert second.source.mission is MissionStatus.RUNNING


def test_attempt_identity_binds_the_canonical_full_plan() -> None:
    service = MissionService()
    initial = _row()
    first = service.prepare_attempt(initial, tick=0)
    assert first is not None

    equivalent = dict(
        initial, mission__plan_json=json.dumps(json.loads(initial["mission__plan_json"]), indent=2)
    )
    same = service.prepare_attempt(equivalent, tick=0)
    assert same is not None
    assert same.plan_digest == first.plan_digest
    assert same.idempotency_key == first.idempotency_key

    extended_plan = json.loads(initial["mission__plan_json"])
    extended_plan.append(
        {
            "name": "review",
            "prompt": "Review the fix",
            "validators": [{"name": "lint", "command": ["ruff"]}],
        }
    )
    extended = service.prepare_attempt(
        dict(initial, mission__plan_json=json.dumps(extended_plan)), tick=0
    )
    assert extended is not None
    assert extended.plan_digest != first.plan_digest
    assert extended.idempotency_key != first.idempotency_key
