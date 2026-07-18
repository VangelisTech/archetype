# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Graded, credential-free mission-transition capability checks.

This suite proves typed transition authority over rows that the world persists:
validator rejection and an incomplete checkpoint produce explicit graph edges
without advancing the task, while complete evidence does. It does not claim a
durable pre-execution claim, crash recovery, resumption, or exactly-once model
submission; those require a control-authority claim before provider execution.
"""

from __future__ import annotations

import json
from typing import Any

from archetype.app.missions import MissionService
from evals.graders import state_check
from evals.harness import EvalHarness
from evals.types import GraderResult

SUITE = "capability"


def _row() -> dict[str, Any]:
    return {
        "world_id": "world-eval",
        "run_id": "run-eval",
        "entity_id": 7,
        "mission__status": "ready",
        "mission__finished": False,
        "mission__succeeded": False,
        "mission__failure_reason": "",
        "mission__pr_ready": False,
        "mission__pr_url": "",
        "mission__plan_json": json.dumps(
            [
                {
                    "name": "fix-bug",
                    "prompt": "Fix the issue.",
                    "validators": [{"name": "regression", "command": ["pytest"]}],
                }
            ]
        ),
        "taskgate__step_index": 0,
        "taskgate__attempts": 0,
        "taskgate__max_attempts": 5,
        "taskgate__status": "ready",
        "taskgate__required_finalization_phase": "checkpointed",
        "attempt__agent_session_id": "",
        "attempt__validator_details_json": "[]",
        "frictionlog__entries_json": "[]",
    }


def _outcome(
    request: Any,
    *,
    accepted: bool,
    checkpoint_restorable: bool,
) -> dict[str, Any]:
    status = "accepted" if accepted else "rejected"
    return {
        "attempt_id": f"attempt-{request.attempt_index}",
        "attempt_index": request.attempt_index,
        "idempotency_key": request.idempotency_key,
        "status": status,
        "accepted": accepted,
        "harness": "fake",
        "agent_session_id": "session-eval",
        "validator_details": [{"name": "regression", "passed": accepted}],
        "checkpoint_provider": "fake",
        "checkpoint_status": "created" if checkpoint_restorable else "failed",
        "checkpoint_restorable": checkpoint_restorable,
        "checkpoint_created_at_ms": 1,
        "checkpoint_expires_at_ms": 2,
        "sandbox_state_ref": "fake://checkpoint" if checkpoint_restorable else "",
        "finalization_phase": "checkpointed",
        "finalization_manifest_ref": "fake://manifest",
        "finalization_error": "" if checkpoint_restorable else "checkpoint unavailable",
        "results": {"regression": accepted},
        "trace_ref": "fake://trace",
        "traces_ref": "fake://traces",
        "filesystem_start_ref": "fake://fs-start",
        "filesystem_end_ref": "fake://fs-end",
        "filesystem_diff_ref": "fake://fs-diff",
        "git_status_ref": "fake://status",
        "git_patch_ref": "fake://patch",
        "git_bundle_ref": "fake://bundle",
        "context_ref": "fake://context",
        "friction": [],
        "sha": "abc123" if accepted else "",
        "message": "fix: resolve bug" if accepted else "",
        "pushed": False,
    }


def task_agent_mission_transition_authority() -> list[GraderResult]:
    """Reject twice for different reasons, then advance on complete evidence."""
    service = MissionService()
    initial = _row()

    first_request = service.prepare_attempt(initial, tick=1)
    assert first_request is not None
    rejected = service.apply_attempt(
        initial,
        first_request,
        _outcome(first_request, accepted=False, checkpoint_restorable=True),
    )

    second_request = service.prepare_attempt(rejected, tick=2)
    assert second_request is not None
    uncheckpointed = service.apply_attempt(
        rejected,
        second_request,
        _outcome(second_request, accepted=True, checkpoint_restorable=False),
    )

    third_request = service.prepare_attempt(uncheckpointed, tick=3)
    assert third_request is not None
    completed = service.apply_attempt(
        uncheckpointed,
        third_request,
        _outcome(third_request, accepted=True, checkpoint_restorable=True),
    )

    return [
        state_check(
            {
                "rejection_is_durable": rejected["attempt__status"] == "rejected",
                "rejection_edge_is_typed": (
                    rejected["attempt__transition_event"] == "rejected_retry"
                    and rejected["mission__status"] == "running"
                    and rejected["taskgate__status"] == "retryable"
                ),
                "rejection_does_not_advance": rejected["taskgate__step_index"] == 0,
                "accepted_without_checkpoint_does_not_advance": (
                    uncheckpointed["taskgate__step_index"] == 0
                    and not uncheckpointed["mission__finished"]
                ),
                "attempt_identity_changes": len(
                    {
                        first_request.idempotency_key,
                        second_request.idempotency_key,
                        third_request.idempotency_key,
                    }
                )
                == 3,
            },
            name="mission_gate_holds",
        ),
        state_check(
            {
                "complete_evidence_finishes": completed["mission__finished"],
                "complete_evidence_succeeds": completed["mission__succeeded"],
                "complete_evidence_is_pr_ready": completed["mission__pr_ready"],
                "commit_is_recorded": completed["commit__sha"] == "abc123",
                "success_edge_is_typed": (
                    completed["attempt__transition_event"] == "mission_succeeded"
                    and completed["mission__status"] == "succeeded"
                    and completed["taskgate__status"] == "passed"
                ),
                "attempt_count_is_three": completed["taskgate__attempts"] == 3,
            },
            name="mission_gate_advances",
        ),
    ]


def register(harness: EvalHarness) -> None:
    harness.add(
        "agent_mission_transition_authority",
        suite=SUITE,
        fn=task_agent_mission_transition_authority,
        desc="Mission progress requires validator, commit, checkpoint, and finalization evidence",
    )
