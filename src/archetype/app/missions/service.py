# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Deterministic, provider-neutral mission transition authority."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from typing import Any

from archetype.app.missions.models import MissionAttemptRequest
from archetype.app.missions.transitions import (
    AttemptStatus,
    CheckpointStatus,
    FinalizationPhase,
    MissionStatus,
    MissionTaskState,
    MissionTransitionEvent,
    MissionTransitionGraph,
    TaskStatus,
    retry_event,
)


class MissionService:
    """Gate mission progress on validators and durable evidence.

    A tick records at most one completed attempt. Rejection and incomplete
    finalization are ordinary committed states; only this service traverses
    the typed mission/task/attempt graph.
    """

    def __init__(self, graph: MissionTransitionGraph | None = None) -> None:
        self._graph = graph or MissionTransitionGraph()

    def prepare_attempt(self, row: Mapping[str, Any], *, tick: int) -> MissionAttemptRequest | None:
        source = self._state(row)
        finished = bool(row.get("mission__finished"))
        terminal = source.mission in {MissionStatus.SUCCEEDED, MissionStatus.FAILED}
        if finished != terminal:
            raise ValueError("mission finished flag does not agree with its typed status")
        if terminal:
            return None
        self._graph.require_active(source)

        plan = self._plan(row)
        step_index = int(row["taskgate__step_index"])
        if step_index < 0:
            raise ValueError("mission step_index must be non-negative")
        if step_index >= len(plan):
            raise ValueError("active mission step_index is outside its plan")

        step = plan[step_index]
        name = str(step.get("name", "")).strip()
        prompt = str(step.get("prompt", "")).strip()
        validators = tuple(step.get("validators") or ())
        if not name or not prompt:
            raise ValueError("mission tasks require non-empty name and prompt")
        if not validators:
            raise ValueError(f"mission task {name!r} requires at least one validator")
        if any(not isinstance(value, dict) for value in validators):
            raise TypeError("mission validators must be JSON objects")

        attempts = int(row["taskgate__attempts"])
        max_attempts = int(row["taskgate__max_attempts"])
        if attempts < 0 or max_attempts < 1 or attempts >= max_attempts:
            raise ValueError("active task attempt counters are inconsistent")
        attempt_index = attempts + 1
        plan_digest = self._plan_digest(plan)
        gate_material = json.dumps(
            {
                "world_id": str(row["world_id"]),
                "run_id": str(row["run_id"]),
                "entity_id": str(row["entity_id"]),
                "mission_status": source.mission.value,
                "task_status": source.task.value,
                "step_index": step_index,
                "attempt_index": attempt_index,
                "plan_digest": plan_digest,
                "step": step,
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        prior = (
            json.loads(str(row.get("attempt__validator_details_json") or "[]"))
            if attempt_index > 1
            else []
        )
        if not isinstance(prior, list) or any(not isinstance(value, dict) for value in prior):
            raise ValueError("persisted validator details must be a JSON list of objects")
        return MissionAttemptRequest(
            prompt=prompt,
            validators=validators,
            step_name=name,
            step_index=step_index,
            attempt_index=attempt_index,
            plan_digest=plan_digest,
            idempotency_key=hashlib.sha256(gate_material.encode()).hexdigest(),
            previous_session_id=str(row.get("attempt__agent_session_id") or ""),
            previous_validator_details=tuple(prior),
            correlation={
                "world_id": str(row["world_id"]),
                "run_id": str(row["run_id"]),
                "entity_id": str(row["entity_id"]),
                "tick": tick,
                "step_index": step_index,
            },
            source=source,
        )

    def apply_attempt(
        self,
        row: Mapping[str, Any],
        request: MissionAttemptRequest,
        outcome: Mapping[str, Any],
    ) -> dict[str, Any]:
        source = self._state(row)
        if bool(row.get("mission__finished")):
            raise ValueError("a terminal mission cannot accept another attempt")
        self._graph.require_active(source)
        if source != request.source:
            raise ValueError("mission state changed after this attempt was prepared")
        if int(row["taskgate__step_index"]) != request.step_index:
            raise ValueError("mission step changed after this attempt was prepared")
        if int(row["taskgate__attempts"]) + 1 != request.attempt_index:
            raise ValueError("mission attempt counter changed after this attempt was prepared")
        plan = self._plan(row)
        if self._plan_digest(plan) != request.plan_digest:
            raise ValueError("mission plan changed after this attempt was prepared")
        if int(outcome["attempt_index"]) != request.attempt_index:
            raise ValueError("sandbox outcome attempt_index does not match the request")
        if str(outcome["idempotency_key"]) != request.idempotency_key:
            raise ValueError("sandbox outcome idempotency_key does not match the request")

        details = list(outcome["validator_details"])
        if not details or any(not isinstance(value, dict) for value in details):
            raise ValueError("sandbox outcomes require non-empty validator details")
        friction = list(outcome.get("friction") or ())
        if any(not isinstance(value, dict) for value in friction):
            raise ValueError("sandbox friction entries must be JSON objects")
        prior_friction = json.loads(str(row.get("frictionlog__entries_json") or "[]"))
        if not isinstance(prior_friction, list) or any(
            not isinstance(value, dict) for value in prior_friction
        ):
            raise ValueError("persisted friction log must be a JSON list of objects")
        prior_friction.extend(friction)

        provider_status = self._provider_status(outcome)
        checkpoint_status = self._checkpoint_status(outcome)
        checkpoint_expires_at_ms = self._checkpoint_expiry(outcome)
        required_phase = self._phase(row["taskgate__required_finalization_phase"], "required")
        actual_phase = self._phase(outcome["finalization_phase"], "outcome")
        gate_passed = (
            bool(outcome["accepted"])
            and bool(outcome["checkpoint_restorable"])
            and actual_phase.rank >= required_phase.rank
        )
        attempt_status = self._attempt_status(provider_status, gate_passed=gate_passed)
        exhausted = request.attempt_index >= int(row["taskgate__max_attempts"])

        if gate_passed:
            event = (
                MissionTransitionEvent.MISSION_SUCCEEDED
                if request.step_index + 1 >= len(plan)
                else MissionTransitionEvent.TASK_ADVANCED
            )
        else:
            event = retry_event(attempt_status, exhausted=exhausted)
        transition = self._graph.transition(source, event)

        updated = dict(row)
        updated.update(
            {
                "mission__status": transition.target.mission.value,
                "taskgate__step_name": request.step_name,
                "taskgate__prompt": request.prompt,
                "taskgate__validators_json": self._json(list(request.validators)),
                "taskgate__attempts": request.attempt_index,
                "taskgate__status": transition.target.task.value,
                "taskgate__passed": transition.target.task is TaskStatus.PASSED,
                "attempt__attempt_id": str(outcome["attempt_id"]),
                "attempt__attempt_index": request.attempt_index,
                "attempt__status": transition.attempt.value,
                "attempt__provider_status": provider_status.value,
                "attempt__harness": str(outcome["harness"]),
                "attempt__agent_session_id": str(outcome["agent_session_id"]),
                "attempt__validator_details_json": self._json(details),
                "attempt__transition_event": transition.event.value,
                "attempt__mission_status_before": transition.source.mission.value,
                "attempt__task_status_before": transition.source.task.value,
                "attempt__mission_status_after": transition.target.mission.value,
                "attempt__task_status_after": transition.target.task.value,
                "checkpoint__provider": str(outcome["checkpoint_provider"]),
                "checkpoint__status": checkpoint_status.value,
                "checkpoint__state_ref": str(outcome["sandbox_state_ref"]),
                "checkpoint__restorable": bool(outcome["checkpoint_restorable"]),
                "checkpoint__created_at_ms": int(outcome["checkpoint_created_at_ms"]),
                "checkpoint__expires_at_ms": checkpoint_expires_at_ms,
                "finalization__phase": actual_phase.value,
                "finalization__idempotency_key": request.idempotency_key,
                "finalization__manifest_ref": str(outcome["finalization_manifest_ref"]),
                "finalization__error": str(outcome["finalization_error"]),
                "evidence__results_json": self._json(outcome["results"]),
                "evidence__trace_ref": str(outcome["trace_ref"]),
                "evidence__traces_ref": str(outcome["traces_ref"]),
                "evidence__live_status_ref": str(outcome.get("live_status_ref", "")),
                "evidence__live_events_ref": str(outcome.get("live_events_ref", "")),
                "evidence__sandbox_state_ref": str(outcome["sandbox_state_ref"]),
                "evidence__filesystem_start_ref": str(outcome["filesystem_start_ref"]),
                "evidence__filesystem_end_ref": str(outcome["filesystem_end_ref"]),
                "evidence__filesystem_diff_ref": str(outcome["filesystem_diff_ref"]),
                "evidence__git_status_ref": str(outcome["git_status_ref"]),
                "evidence__git_patch_ref": str(outcome["git_patch_ref"]),
                "evidence__git_bundle_ref": str(outcome["git_bundle_ref"]),
                "evidence__context_ref": str(outcome["context_ref"]),
                "frictionlog__entries_json": self._json(prior_friction),
            }
        )

        if gate_passed:
            sha = str(outcome["sha"])
            if not sha:
                raise ValueError("an accepted, finalized attempt requires a commit SHA")
            updated["commit__sha"] = sha
            updated["commit__message"] = str(outcome["message"])
            updated["commit__pushed"] = bool(outcome["pushed"])
            if transition.advances_task:
                next_index = request.step_index + 1
                nxt = plan[next_index]
                updated.update(
                    {
                        "taskgate__step_index": next_index,
                        "taskgate__step_name": str(nxt["name"]),
                        "taskgate__prompt": str(nxt["prompt"]),
                        "taskgate__validators_json": self._json(nxt["validators"]),
                        "taskgate__attempts": 0,
                    }
                )
            else:
                updated.update(
                    {
                        "mission__finished": True,
                        "mission__succeeded": True,
                        "mission__pr_ready": True,
                        "mission__pr_url": str(
                            outcome.get("pr_url") or row.get("mission__pr_url", "")
                        ),
                    }
                )
        elif transition.terminal:
            updated.update(
                {
                    "mission__finished": True,
                    "mission__succeeded": False,
                    "mission__failure_reason": (
                        f"task {request.step_name!r} exhausted {request.attempt_index} attempts; "
                        f"latest status={attempt_status.value} phase={actual_phase.value}"
                    ),
                }
            )
        return updated

    def _state(self, row: Mapping[str, Any]) -> MissionTaskState:
        return self._graph.state(row.get("mission__status"), row.get("taskgate__status"))

    @staticmethod
    def _provider_status(outcome: Mapping[str, Any]) -> AttemptStatus:
        try:
            status = AttemptStatus(str(outcome["status"]))
        except ValueError as exc:
            raise ValueError(f"unknown sandbox outcome status: {outcome['status']!r}") from exc
        accepted = bool(outcome["accepted"])
        if accepted and status is not AttemptStatus.ACCEPTED:
            raise ValueError("accepted sandbox outcome must have accepted status")
        if not accepted and status not in {AttemptStatus.REJECTED, AttemptStatus.FAILED}:
            raise ValueError("unaccepted sandbox outcome must be rejected or failed")
        return status

    @staticmethod
    def _checkpoint_status(outcome: Mapping[str, Any]) -> CheckpointStatus:
        try:
            status = CheckpointStatus(str(outcome["checkpoint_status"]))
        except ValueError as exc:
            raise ValueError(
                f"unknown checkpoint status: {outcome['checkpoint_status']!r}"
            ) from exc
        restorable = bool(outcome["checkpoint_restorable"])
        state_ref = str(outcome["sandbox_state_ref"])
        if restorable and (status is not CheckpointStatus.CREATED or not state_ref):
            raise ValueError("restorable checkpoint requires created status and state reference")
        if not restorable and status is CheckpointStatus.CREATED:
            raise ValueError("created checkpoint must be restorable")
        return status

    @staticmethod
    def _checkpoint_expiry(outcome: Mapping[str, Any]) -> int | None:
        created_at_ms = int(outcome["checkpoint_created_at_ms"])
        value = outcome["checkpoint_expires_at_ms"]
        if value is None or value == 0:
            return None
        expires_at_ms = int(value)
        if expires_at_ms <= created_at_ms:
            raise ValueError("checkpoint expiration must be after creation")
        return expires_at_ms

    @staticmethod
    def _attempt_status(provider: AttemptStatus, *, gate_passed: bool) -> AttemptStatus:
        if gate_passed:
            return AttemptStatus.ACCEPTED
        if provider is AttemptStatus.ACCEPTED:
            return AttemptStatus.INCOMPLETE
        return provider

    @staticmethod
    def _phase(value: object, label: str) -> FinalizationPhase:
        try:
            return FinalizationPhase(str(value))
        except ValueError as exc:
            raise ValueError(f"unknown {label} finalization phase: {value!r}") from exc

    @staticmethod
    def _json(value: Any) -> str:
        return json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        )

    @classmethod
    def _plan_digest(cls, plan: list[dict[str, Any]]) -> str:
        return hashlib.sha256(cls._json(plan).encode()).hexdigest()

    @staticmethod
    def _plan(row: Mapping[str, Any]) -> list[dict[str, Any]]:
        plan = json.loads(str(row.get("mission__plan_json") or "[]"))
        if not isinstance(plan, list) or any(not isinstance(step, dict) for step in plan):
            raise ValueError("mission plan must be a JSON list of task objects")
        if not plan:
            raise ValueError("mission plan must contain at least one task")
        return plan
