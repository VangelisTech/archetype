# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Persisted mission state and provider-neutral attempt requests."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from archetype.core.component import Component


class Mission(Component):
    """Episode-level mission; ``finished`` is its terminal latch."""

    name: str = ""
    repo: str = ""
    branch: str = "agent/mission"
    plan_json: str = "[]"
    finished: bool = False
    succeeded: bool = False
    failure_reason: str = ""
    pr_ready: bool = False
    pr_url: str = ""


class TaskGate(Component):
    """Current task and the durable evidence threshold required to advance."""

    step_index: int = 0
    step_name: str = ""
    prompt: str = ""
    validators_json: str = "[]"
    attempts: int = 0
    max_attempts: int = 5
    status: str = "ready"
    required_finalization_phase: str = "checkpointed"
    passed: bool = False


class Attempt(Component):
    """Exactly one submission, persisted whether accepted or rejected."""

    attempt_id: str = ""
    attempt_index: int = 0
    status: str = "pending"
    harness: str = ""
    agent_session_id: str = ""
    validator_details_json: str = "[]"


class Checkpoint(Component):
    """Provider-native recovery point captured after an attempt."""

    provider: str = ""
    status: str = "pending"
    state_ref: str = ""
    restorable: bool = False
    created_at_ms: int = 0
    expires_at_ms: int = 0


class Finalization(Component):
    """Progress from evidence capture through durable publication."""

    phase: str = "pending"
    idempotency_key: str = ""
    manifest_ref: str = ""
    error: str = ""


class Commit(Component):
    """Verified Git identity produced by the task gate."""

    sha: str = ""
    message: str = ""
    pushed: bool = False


class Evidence(Component):
    """Queryable references to portable and provider-native attempt evidence."""

    results_json: str = "{}"
    trace_ref: str = ""
    traces_ref: str = ""
    live_status_ref: str = ""
    live_events_ref: str = ""
    sandbox_state_ref: str = ""
    filesystem_start_ref: str = ""
    filesystem_end_ref: str = ""
    filesystem_diff_ref: str = ""
    git_status_ref: str = ""
    git_patch_ref: str = ""
    git_bundle_ref: str = ""
    context_ref: str = ""


class FrictionLog(Component):
    """Agent-reported operational friction retained as episode evidence."""

    entries_json: str = "[]"


@dataclass(frozen=True)
class MissionAttemptRequest:
    """One deterministic submission requested by the mission state machine."""

    prompt: str
    validators: tuple[dict[str, Any], ...]
    step_name: str
    attempt_index: int
    idempotency_key: str
    previous_session_id: str
    previous_validator_details: tuple[dict[str, Any], ...]
    correlation: dict[str, Any]


MISSION_COMPONENTS = (
    Mission,
    TaskGate,
    Attempt,
    Checkpoint,
    Finalization,
    Commit,
    Evidence,
    FrictionLog,
)
