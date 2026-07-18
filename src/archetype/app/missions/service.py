# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Deterministic, provider-neutral mission transition authority."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from typing import Any

from archetype.app.missions.models import (
    MissionAttemptRequest,
    mission_attempt_request_fingerprint,
    normalize_attempt_validators,
)
from archetype.app.missions.outcomes import (
    assess_attempt_outcome,
)
from archetype.app.missions.transitions import (
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
        # A world tick observes an attempt; it is not part of provider-submission
        # identity. Keeping it out of the durable request lets recovery converge
        # on the same claim when the unchanged task is revisited on a later tick.
        _ = tick
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
        validators = normalize_attempt_validators(tuple(step.get("validators") or ()))
        if not name or not prompt:
            raise ValueError("mission tasks require non-empty name and prompt")

        attempts = int(row["taskgate__attempts"])
        max_attempts = int(row["taskgate__max_attempts"])
        if attempts < 0 or max_attempts < 1 or attempts >= max_attempts:
            raise ValueError("active task attempt counters are inconsistent")
        attempt_index = attempts + 1
        try:
            required_phase = FinalizationPhase(str(row["taskgate__required_finalization_phase"]))
        except ValueError as exc:
            raise ValueError("unknown required finalization phase") from exc
        plan_digest = self._plan_digest(plan)
        world_id = str(row["world_id"])
        run_id = str(row["run_id"])
        entity_id = str(row["entity_id"])
        gate_material = json.dumps(
            {
                "world_id": world_id,
                "run_id": run_id,
                "entity_id": entity_id,
                "mission_status": source.mission.value,
                "task_status": source.task.value,
                "step_index": step_index,
                "attempt_index": attempt_index,
                "max_attempts": max_attempts,
                "required_finalization_phase": required_phase.value,
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
        idempotency_key = hashlib.sha256(gate_material.encode()).hexdigest()
        mission_id = f"{world_id}:{run_id}:{entity_id}"
        task_id = hashlib.sha256(f"{plan_digest}:{step_index}:{name}".encode()).hexdigest()
        attempt_id = hashlib.sha256(idempotency_key.encode()).hexdigest()
        correlation = {
            "world_id": world_id,
            "run_id": run_id,
            "entity_id": entity_id,
            "step_index": step_index,
        }
        request_fingerprint = mission_attempt_request_fingerprint(
            idempotency_key=idempotency_key,
            prompt=prompt,
            validators=validators,
            step_name=name,
            step_index=step_index,
            attempt_index=attempt_index,
            plan_digest=plan_digest,
            max_attempts=max_attempts,
            required_finalization_phase=required_phase,
            previous_session_id=str(row.get("attempt__agent_session_id") or ""),
            previous_validator_details=tuple(prior),
            correlation=correlation,
        )
        return MissionAttemptRequest(
            prompt=prompt,
            validators=validators,
            step_name=name,
            step_index=step_index,
            attempt_index=attempt_index,
            plan_digest=plan_digest,
            max_attempts=max_attempts,
            required_finalization_phase=required_phase,
            idempotency_key=idempotency_key,
            mission_id=mission_id,
            task_id=task_id,
            attempt_id=attempt_id,
            request_fingerprint=request_fingerprint,
            previous_session_id=str(row.get("attempt__agent_session_id") or ""),
            previous_validator_details=tuple(prior),
            correlation=correlation,
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
        if int(row["taskgate__max_attempts"]) != request.max_attempts:
            raise ValueError("mission max_attempts changed after this attempt was prepared")
        try:
            required_phase = FinalizationPhase(str(row["taskgate__required_finalization_phase"]))
        except ValueError as exc:
            raise ValueError("unknown required finalization phase") from exc
        if required_phase is not request.required_finalization_phase:
            raise ValueError("mission finalization gate changed after this attempt was prepared")

        assessment = assess_attempt_outcome(request, outcome)

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

        provider_status = assessment.provider_status
        checkpoint_status = assessment.checkpoint_status
        checkpoint_expires_at_ms = assessment.checkpoint_expires_at_ms
        actual_phase = assessment.finalization_phase
        gate_passed = assessment.gate_passed
        attempt_status = assessment.attempt_status
        exhausted = request.attempt_index >= request.max_attempts

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
