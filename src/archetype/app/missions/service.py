# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Deterministic, provider-neutral mission transition authority."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from typing import Any

from archetype.app.missions.models import MissionAttemptRequest


class MissionService:
    """Gate multi-task mission progress on validators and durable evidence.

    A tick records at most one attempt. Rejection and incomplete finalization
    are ordinary committed states; only this service may advance ``step_index``.
    """

    _FINALIZATION_PHASES = {
        "pending": 0,
        "captured": 1,
        "checkpointed": 2,
        "indexed": 3,
        "published": 4,
    }

    def prepare_attempt(self, row: Mapping[str, Any], *, tick: int) -> MissionAttemptRequest | None:
        if bool(row.get("mission__finished")):
            return None
        plan = self._plan(row)
        step_index = int(row["taskgate__step_index"])
        if step_index < 0:
            raise ValueError("mission step_index must be non-negative")
        if step_index >= len(plan):
            return None

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

        attempt_index = int(row["taskgate__attempts"]) + 1
        if attempt_index < 1:
            raise ValueError("mission attempt index must be positive")
        gate_material = json.dumps(
            {
                "world_id": str(row["world_id"]),
                "run_id": str(row["run_id"]),
                "entity_id": str(row["entity_id"]),
                "step_index": step_index,
                "attempt_index": attempt_index,
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
            attempt_index=attempt_index,
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
        )

    def apply_attempt(
        self,
        row: Mapping[str, Any],
        request: MissionAttemptRequest,
        outcome: Mapping[str, Any],
    ) -> dict[str, Any]:
        if bool(row.get("mission__finished")):
            raise ValueError("a terminal mission cannot accept another attempt")
        if int(outcome["attempt_index"]) != request.attempt_index:
            raise ValueError("sandbox outcome attempt_index does not match the request")
        if str(outcome["idempotency_key"]) != request.idempotency_key:
            raise ValueError("sandbox outcome idempotency_key does not match the request")

        plan = self._plan(row)
        step_index = int(row["taskgate__step_index"])
        details = list(outcome["validator_details"])
        if not details or any(not isinstance(value, dict) for value in details):
            raise ValueError("sandbox outcomes require non-empty validator details")
        prior_friction = json.loads(str(row.get("frictionlog__entries_json") or "[]"))
        prior_friction.extend(list(outcome.get("friction") or ()))

        updated = dict(row)
        updated.update(
            {
                "taskgate__step_name": request.step_name,
                "taskgate__prompt": request.prompt,
                "taskgate__validators_json": json.dumps(list(request.validators)),
                "taskgate__attempts": request.attempt_index,
                "taskgate__status": str(outcome["status"]),
                "taskgate__passed": False,
                "attempt__attempt_id": str(outcome["attempt_id"]),
                "attempt__attempt_index": request.attempt_index,
                "attempt__status": str(outcome["status"]),
                "attempt__harness": str(outcome["harness"]),
                "attempt__agent_session_id": str(outcome["agent_session_id"]),
                "attempt__validator_details_json": json.dumps(details),
                "checkpoint__provider": str(outcome["checkpoint_provider"]),
                "checkpoint__status": str(outcome["checkpoint_status"]),
                "checkpoint__state_ref": str(outcome["sandbox_state_ref"]),
                "checkpoint__restorable": bool(outcome["checkpoint_restorable"]),
                "checkpoint__created_at_ms": int(outcome["checkpoint_created_at_ms"]),
                "checkpoint__expires_at_ms": int(outcome["checkpoint_expires_at_ms"]),
                "finalization__phase": str(outcome["finalization_phase"]),
                "finalization__idempotency_key": request.idempotency_key,
                "finalization__manifest_ref": str(outcome["finalization_manifest_ref"]),
                "finalization__error": str(outcome["finalization_error"]),
                "evidence__results_json": json.dumps(outcome["results"]),
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
                "frictionlog__entries_json": json.dumps(prior_friction),
            }
        )

        required_phase = str(row["taskgate__required_finalization_phase"])
        required_rank = self._FINALIZATION_PHASES.get(required_phase)
        actual_phase = str(outcome["finalization_phase"])
        actual_rank = self._FINALIZATION_PHASES.get(actual_phase)
        if required_rank is None:
            raise ValueError(f"unknown required finalization phase: {required_phase!r}")
        if actual_rank is None:
            raise ValueError(f"unknown outcome finalization phase: {actual_phase!r}")
        gate_passed = (
            bool(outcome["accepted"])
            and bool(outcome["checkpoint_restorable"])
            and actual_rank >= required_rank
        )

        if gate_passed:
            sha = str(outcome["sha"])
            if not sha:
                raise ValueError("an accepted, finalized attempt requires a commit SHA")
            updated["taskgate__passed"] = True
            updated["taskgate__status"] = "passed"
            updated["commit__sha"] = sha
            updated["commit__message"] = str(outcome["message"])
            updated["commit__pushed"] = bool(outcome["pushed"])
            next_index = step_index + 1
            if next_index >= len(plan):
                updated["mission__finished"] = True
                updated["mission__succeeded"] = True
                updated["mission__pr_ready"] = True
                updated["mission__pr_url"] = str(
                    outcome.get("pr_url") or row.get("mission__pr_url", "")
                )
            else:
                nxt = plan[next_index]
                updated.update(
                    {
                        "taskgate__step_index": next_index,
                        "taskgate__step_name": str(nxt["name"]),
                        "taskgate__prompt": str(nxt["prompt"]),
                        "taskgate__validators_json": json.dumps(nxt["validators"]),
                        "taskgate__status": "ready",
                        "taskgate__passed": False,
                        "taskgate__attempts": 0,
                    }
                )
        elif request.attempt_index >= int(row["taskgate__max_attempts"]):
            updated["taskgate__status"] = "exhausted"
            updated["mission__finished"] = True
            updated["mission__succeeded"] = False
            updated["mission__failure_reason"] = (
                f"task {request.step_name!r} exhausted {request.attempt_index} attempts; "
                f"latest status={outcome['status']} phase={actual_phase}"
            )
        return updated

    @staticmethod
    def _plan(row: Mapping[str, Any]) -> list[dict[str, Any]]:
        plan = json.loads(str(row.get("mission__plan_json") or "[]"))
        if not isinstance(plan, list) or any(not isinstance(step, dict) for step in plan):
            raise ValueError("mission plan must be a JSON list of task objects")
        return plan
