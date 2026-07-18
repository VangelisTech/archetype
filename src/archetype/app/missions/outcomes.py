# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Shared, authoritative validation for replayable mission attempt outcomes."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from archetype.app.missions.models import (
    MissionAttemptRequest,
    attempt_invocation_fingerprint,
)
from archetype.app.missions.transitions import (
    AttemptStatus,
    CheckpointStatus,
    FinalizationPhase,
)

REPOSITORY_CHANGE_GATE_NAME = "git_tree_change"
REPLAY_REQUIRED_OUTCOME_FIELDS = frozenset(
    {
        "attempt_id",
        "attempt_index",
        "idempotency_key",
        "request_fingerprint",
        "status",
        "accepted",
        "harness",
        "agent_session_id",
        "validator_details",
        "checkpoint_provider",
        "checkpoint_status",
        "checkpoint_restorable",
        "checkpoint_created_at_ms",
        "checkpoint_expires_at_ms",
        "sandbox_state_ref",
        "finalization_phase",
        "finalization_manifest_ref",
        "finalization_error",
        "results",
        "trace_ref",
        "traces_ref",
        "filesystem_start_ref",
        "filesystem_end_ref",
        "filesystem_diff_ref",
        "git_status_ref",
        "git_patch_ref",
        "git_bundle_ref",
        "context_ref",
        "sha",
        "message",
        "pushed",
    }
)


@dataclass(frozen=True)
class MissionAttemptAssessment:
    """The mission-derived meaning of one complete provider outcome."""

    provider_status: AttemptStatus
    checkpoint_status: CheckpointStatus
    checkpoint_expires_at_ms: int | None
    finalization_phase: FinalizationPhase
    gate_passed: bool
    attempt_status: AttemptStatus


def assess_attempt_outcome(
    request: MissionAttemptRequest,
    outcome: Mapping[str, Any],
) -> MissionAttemptAssessment:
    """Validate a replayable outcome and derive its authoritative attempt status."""

    missing = sorted(REPLAY_REQUIRED_OUTCOME_FIELDS - outcome.keys())
    if missing:
        raise ValueError("attempt outcome is not replayable; missing fields: " + ", ".join(missing))
    if str(outcome["attempt_id"]) != request.attempt_id:
        raise ValueError("attempt outcome does not match the claimed attempt_id")
    if str(outcome["idempotency_key"]) != request.idempotency_key:
        raise ValueError(
            "attempt outcome does not match the claimed idempotency key (idempotency_key)"
        )
    try:
        attempt_index = int(outcome["attempt_index"])
    except (TypeError, ValueError) as exc:
        raise ValueError("attempt outcome has an invalid attempt_index") from exc
    if attempt_index != request.attempt_index:
        raise ValueError("attempt outcome does not match the claimed attempt_index")

    expected_invocation = attempt_invocation_fingerprint(
        prompt=request.prompt,
        validators=request.validators,
        step_name=request.step_name,
        attempt_index=request.attempt_index,
        previous_session_id=request.previous_session_id,
        previous_validator_details=request.previous_validator_details,
        correlation=request.correlation,
    )
    if str(outcome["request_fingerprint"]) != expected_invocation:
        raise ValueError("attempt outcome does not match the claimed sandbox request")

    accepted = outcome["accepted"]
    if not isinstance(accepted, bool):
        raise ValueError("attempt outcome accepted flag must be boolean")
    try:
        provider_status = AttemptStatus(str(outcome["status"]))
    except ValueError as exc:
        raise ValueError("attempt outcome has an invalid provider status") from exc
    if accepted and provider_status is not AttemptStatus.ACCEPTED:
        raise ValueError("accepted sandbox outcome must have accepted status")
    if not accepted and provider_status not in {AttemptStatus.REJECTED, AttemptStatus.FAILED}:
        raise ValueError("unaccepted sandbox outcome must be rejected or failed")

    details = outcome["validator_details"]
    if (
        not isinstance(details, (list, tuple))
        or not details
        or any(not isinstance(value, dict) for value in details)
    ):
        raise ValueError("attempt outcome requires non-empty validator details")
    requested_names = [str(value.get("name", "")).strip() for value in request.validators]
    if any(not name for name in requested_names) or len(set(requested_names)) != len(
        requested_names
    ):
        raise ValueError("mission attempt validators require unique non-empty names")
    detail_names: list[str] = []
    passed_by_name: dict[str, bool] = {}
    for detail in details:
        name = str(detail.get("name", "")).strip()
        if not name or name in passed_by_name:
            raise ValueError("attempt validator evidence requires unique non-empty names")
        passed = detail.get("passed")
        if not isinstance(passed, bool):
            raise ValueError("attempt validator passed flags must be boolean")
        detail_names.append(name)
        passed_by_name[name] = passed
    requested = set(requested_names)
    actual = set(detail_names)
    # Repository finalization may append this reserved negative gate when the
    # validators pass but the agent produced no commit-worthy tree change.
    allowed = requested | {REPOSITORY_CHANGE_GATE_NAME}
    if not requested.issubset(actual) or not actual.issubset(allowed):
        raise ValueError("attempt validator evidence does not match the requested validators")
    if REPOSITORY_CHANGE_GATE_NAME in actual and passed_by_name[REPOSITORY_CHANGE_GATE_NAME]:
        raise ValueError("repository tree-change evidence may only record a failed gate")
    requested_by_name = {str(value["name"]): value for value in request.validators}
    details_by_name = {str(value["name"]): value for value in details}
    for name, validator in requested_by_name.items():
        detail = details_by_name[name]
        command = detail.get("command")
        if not isinstance(command, (list, tuple)) or list(command) != validator["command"]:
            raise ValueError(
                f"attempt validator {name!r} evidence does not match its requested command"
            )
        try:
            expected_returncode = int(detail["expected_returncode"])
            returncode = int(detail["returncode"])
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError(
                f"attempt validator {name!r} evidence requires numeric return codes"
            ) from exc
        if expected_returncode != validator["expected_returncode"]:
            raise ValueError(
                f"attempt validator {name!r} evidence changed its expected return code"
            )
        if passed_by_name[name] != (returncode == expected_returncode):
            raise ValueError(
                f"attempt validator {name!r} passed flag disagrees with its return code"
            )
    validators_accepted = all(passed_by_name.values())
    if accepted != validators_accepted:
        raise ValueError("attempt accepted flag disagrees with authoritative validator evidence")
    friction = outcome.get("friction") or ()
    if not isinstance(friction, (list, tuple)) or any(
        not isinstance(value, dict) for value in friction
    ):
        raise ValueError("attempt outcome friction must be a list of objects")
    results = outcome["results"]
    if not isinstance(results, Mapping):
        raise ValueError("attempt outcome results must be an object")
    if set(results) != actual or any(
        not isinstance(results[name], bool) or results[name] is not passed
        for name, passed in passed_by_name.items()
    ):
        raise ValueError("attempt results do not exactly project validator evidence")

    raw_checkpoint_status = str(outcome["checkpoint_status"])
    mission_checkpoint_status = (
        "created" if raw_checkpoint_status == "ready" else raw_checkpoint_status
    )
    try:
        checkpoint_status = CheckpointStatus(mission_checkpoint_status)
    except ValueError as exc:
        raise ValueError(f"unknown checkpoint status: {raw_checkpoint_status!r}") from exc
    restorable = outcome["checkpoint_restorable"]
    if not isinstance(restorable, bool):
        raise ValueError("attempt checkpoint restorable flag must be boolean")
    state_ref = str(outcome["sandbox_state_ref"])
    if restorable and (checkpoint_status is not CheckpointStatus.CREATED or not state_ref):
        raise ValueError("restorable checkpoint requires created status and state reference")
    if not restorable and checkpoint_status is CheckpointStatus.CREATED:
        raise ValueError("created checkpoint must be restorable")
    if checkpoint_status is CheckpointStatus.DISABLED and state_ref:
        raise ValueError("disabled checkpoint cannot have a state reference")

    try:
        created_at_ms = int(outcome["checkpoint_created_at_ms"])
        expires_raw = outcome["checkpoint_expires_at_ms"]
        expires_at_ms = None if expires_raw in {None, 0} else int(expires_raw)
    except (TypeError, ValueError) as exc:
        raise ValueError("attempt checkpoint timestamps are invalid") from exc
    if expires_at_ms is not None and expires_at_ms <= created_at_ms:
        raise ValueError("checkpoint expiration must be after creation")

    try:
        actual_phase = FinalizationPhase(str(outcome["finalization_phase"]))
    except ValueError as exc:
        raise ValueError(
            f"unknown outcome finalization phase: {outcome['finalization_phase']!r}"
        ) from exc
    gate_passed = (
        accepted and restorable and actual_phase.rank >= request.required_finalization_phase.rank
    )
    attempt_status = (
        AttemptStatus.ACCEPTED
        if gate_passed
        else AttemptStatus.INCOMPLETE
        if provider_status is AttemptStatus.ACCEPTED
        else provider_status
    )
    if gate_passed and not str(outcome["sha"]):
        raise ValueError("an accepted, finalized attempt requires a commit SHA")

    return MissionAttemptAssessment(
        provider_status=provider_status,
        checkpoint_status=checkpoint_status,
        checkpoint_expires_at_ms=expires_at_ms,
        finalization_phase=actual_phase,
        gate_passed=gate_passed,
        attempt_status=attempt_status,
    )
