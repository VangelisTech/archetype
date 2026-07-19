# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Credential-free durable mission-attempt claim recovery proof."""

from __future__ import annotations

import asyncio
import json
import tempfile
import time
from pathlib import Path
from typing import Any

from archetype.app.missions import (
    AttemptClaimAcquireOutcome,
    AttemptClaimStatus,
    AttemptRecoveryAction,
    MissionAttemptClaimService,
    MissionService,
    ProviderExecutionCapabilities,
    attempt_invocation_fingerprint,
)
from archetype.app.redaction.service import RedactionService
from archetype.app.storage.catalog import SqliteControlCatalog
from archetype.missions import AttemptStatus
from evals.graders import state_check
from evals.harness import EvalHarness
from evals.types import GraderResult

SUITE = "capability"


def _claim_service(catalog: Any) -> MissionAttemptClaimService:
    return MissionAttemptClaimService(catalog, redaction_service=RedactionService())


def _mission_row() -> dict[str, Any]:
    return {
        "world_id": "world-claim-eval",
        "run_id": "run-claim-eval",
        "entity_id": 7,
        "mission__plan_json": json.dumps(
            [
                {
                    "name": "repair",
                    "prompt": "Repair the reported defect.",
                    "validators": [{"name": "tests", "command": ["pytest"]}],
                }
            ]
        ),
        "mission__status": "ready",
        "mission__finished": False,
        "mission__succeeded": False,
        "taskgate__step_index": 0,
        "taskgate__attempts": 0,
        "taskgate__max_attempts": 3,
        "taskgate__status": "ready",
        "taskgate__required_finalization_phase": "checkpointed",
        "attempt__agent_session_id": "",
        "attempt__validator_details_json": "[]",
    }


def task_mission_attempt_claim_recovery() -> list[GraderResult]:
    """Recover an uncertain attempt without implicitly submitting it twice."""

    return asyncio.run(_task_mission_attempt_claim_recovery())


async def _task_mission_attempt_claim_recovery() -> list[GraderResult]:
    request = MissionService().prepare_attempt(_mission_row(), tick=1)
    assert request is not None
    capabilities = ProviderExecutionCapabilities(
        provider="eval-provider",
        request_fingerprint="eval-provider-request-v1",
    )
    terminal_outcome = {
        "accepted": False,
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
        "status": AttemptStatus.REJECTED.value,
        "validator_details": [
            {
                "name": "tests",
                "command": ["pytest"],
                "expected_returncode": 0,
                "returncode": 1,
                "passed": False,
            }
        ],
        "harness": "eval",
        "agent_session_id": "",
        "checkpoint_provider": "eval-provider",
        "checkpoint_status": "ready",
        "checkpoint_restorable": True,
        "checkpoint_created_at_ms": 1,
        "checkpoint_expires_at_ms": 2,
        "sandbox_state_ref": "eval://checkpoint",
        "finalization_phase": "checkpointed",
        "finalization_manifest_ref": "eval://manifest",
        "finalization_error": "",
        "results": {"tests": False},
        "trace_ref": "eval://trace",
        "traces_ref": "eval://traces",
        "filesystem_start_ref": "eval://filesystem-start",
        "filesystem_end_ref": "eval://filesystem-end",
        "filesystem_diff_ref": "eval://filesystem-diff",
        "git_status_ref": "eval://git-status",
        "git_patch_ref": "eval://git-patch",
        "git_bundle_ref": "eval://git-bundle",
        "context_ref": "eval://context",
        "friction": [],
        "sha": "",
        "message": "",
        "pushed": False,
    }

    with tempfile.TemporaryDirectory(prefix="archetype-attempt-claim-eval-") as root:
        catalog_path = Path(root) / "control.db"

        original_catalog = SqliteControlCatalog(catalog_path)
        original = _claim_service(original_catalog)
        acquired = await original.acquire(
            request,
            capabilities,
            claimant="worker-before-crash",
            lease_seconds=0.1,
        )
        armed = await original.decide_recovery(acquired.claim, lease_seconds=0.05)
        await asyncio.sleep(0.06)
        await original_catalog.close()

        restarted_catalog = SqliteControlCatalog(catalog_path)
        restarted = _claim_service(restarted_catalog)
        due = await restarted.list_due(request.correlation["world_id"], now=time.time() + 1)
        recovered = await restarted.acquire(
            request,
            capabilities,
            claimant="worker-after-crash",
        )
        recovery = await restarted.decide_recovery(recovered.claim)
        settled = await restarted.settle(
            recovery.claim,
            attempt_status=AttemptStatus.REJECTED,
            outcome=terminal_outcome,
            last_error="validator rejected the recovered attempt",
        )
        await restarted_catalog.close()

        replay_catalog = SqliteControlCatalog(catalog_path)
        replay = _claim_service(replay_catalog)
        persisted = await replay.get(settled.world_id, settled.claim_key)
        assert persisted is not None
        duplicate = await replay.acquire(
            request,
            capabilities,
            claimant="worker-after-settlement",
        )
        recovered_request = replay.recover_request(persisted)
        recovered_outcome = replay.settled_outcome(persisted)
        remaining_due = await replay.list_due(settled.world_id, now=time.time() + 1)
        await replay_catalog.close()

    return [
        state_check(
            {
                "claim_precedes_submission": (
                    acquired.outcome is AttemptClaimAcquireOutcome.ACQUIRED
                    and acquired.claim.status is AttemptClaimStatus.CLAIMED
                    and armed.claim.status is AttemptClaimStatus.POSSIBLY_SUBMITTED
                    and armed.action is AttemptRecoveryAction.EXECUTE
                ),
                "cold_restart_discovers_due_work": (
                    len(due) == 1 and due[0].claim_key == armed.claim.claim_key
                ),
                "expired_owner_is_fenced": (
                    recovered.outcome is AttemptClaimAcquireOutcome.RECOVERED
                    and recovered.claim.claimant == "worker-after-crash"
                    and recovered.claim.fence_epoch == armed.claim.fence_epoch + 1
                ),
                "uncertainty_is_conservative": (
                    recovery.action is AttemptRecoveryAction.RECONCILE
                    and recovery.authorization.action
                    not in {
                        AttemptRecoveryAction.EXECUTE,
                        AttemptRecoveryAction.REPLAY_IDEMPOTENT,
                        AttemptRecoveryAction.RESUME_SESSION,
                    }
                ),
            },
            name="attempt_claim_recovery_authority",
        ),
        state_check(
            {
                "terminal_claim_is_durable": (
                    persisted.status is AttemptClaimStatus.SETTLED
                    and persisted.settlement_status == AttemptStatus.REJECTED.value
                ),
                "terminal_replay_is_duplicate": (
                    duplicate.outcome is AttemptClaimAcquireOutcome.DUPLICATE
                    and duplicate.claim == persisted
                ),
                "request_replays_exactly": recovered_request == request,
                "outcome_replays_exactly": recovered_outcome == terminal_outcome,
                "settled_claim_is_not_due": remaining_due == [],
            },
            name="attempt_claim_terminal_replay",
        ),
    ]


def register(harness: EvalHarness) -> None:
    harness.add(
        "mission_attempt_claim_recovery",
        suite=SUITE,
        fn=task_mission_attempt_claim_recovery,
        desc=(
            "Durable claim fencing, conservative uncertain recovery, and terminal replay "
            "survive a cold restart"
        ),
    )
