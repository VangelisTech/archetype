# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Mission, sandbox, and coding-agent service contracts."""

from __future__ import annotations

import asyncio
import json
from typing import Any

import daft
import pytest

from archetype.app.coding_agents import (
    CodingAgentEpisode,
    CodingAgentProcessor,
    CodingAgentService,
)
from archetype.app.container import ServiceContainer
from archetype.app.missions import (
    Attempt,
    Checkpoint,
    Commit,
    Evidence,
    Finalization,
    FrictionLog,
    Mission,
    MissionService,
    TaskGate,
)
from archetype.app.sandboxes import SandboxService
from archetype.core.resources import Resources

pytestmark = [
    pytest.mark.contract("missions.transition.evidence_gated"),
    pytest.mark.contract("sandboxes.lifecycle.resumable"),
]


def _outcome(*, accepted: bool, checkpoint: bool = True) -> dict[str, Any]:
    status = "accepted" if accepted else "rejected"
    details = [
        {
            "name": "tests",
            "returncode": 0 if accepted else 1,
            "passed": accepted,
            "stdout": "",
            "stderr": "" if accepted else "still failing",
        }
    ]
    snapshot = "modal-image://im-attempt"
    return {
        "attempt_id": "attempt",
        "status": status,
        "accepted": accepted,
        "sha": "verified-sha" if accepted else "",
        "message": "fix: issue",
        "pushed": False,
        "results": {"tests": accepted},
        "validator_details": details,
        "trace_ref": f"{snapshot}#/workspace/trace.jsonl",
        "traces_ref": f"{snapshot}#/workspace/traces",
        "live_status_ref": "modal-sandbox://sb-test/workspace/live/session.json",
        "live_events_ref": "modal-sandbox://sb-test/workspace/live/events.jsonl",
        "sandbox_state_ref": snapshot if checkpoint else "",
        "checkpoint_status": "ready" if checkpoint else "failed",
        "checkpoint_provider": "modal",
        "checkpoint_restorable": checkpoint,
        "checkpoint_error": "" if checkpoint else "snapshot unavailable",
        "checkpoint_created_at_ms": 1,
        "checkpoint_expires_at_ms": 1001 if checkpoint else 0,
        "finalization_phase": "checkpointed" if checkpoint else "captured",
        "finalization_error": "" if checkpoint else "snapshot unavailable",
        "finalization_manifest_ref": f"{snapshot}#/workspace/manifest.json",
        "filesystem_start_ref": f"{snapshot}#/workspace/start.jsonl",
        "filesystem_end_ref": f"{snapshot}#/workspace/end.jsonl",
        "filesystem_diff_ref": f"{snapshot}#/workspace/diff.jsonl",
        "git_status_ref": f"{snapshot}#/workspace/status.txt",
        "git_patch_ref": f"{snapshot}#/workspace/attempt.patch",
        "git_bundle_ref": f"{snapshot}#/workspace/repository.bundle",
        "context_ref": f"{snapshot}#/workspace/.context",
        "harness": "codex",
        "agent_session_id": "thread-1",
        "friction": [] if accepted else [{"finding": "tests failed"}],
        "pr_url": "",
    }


def _mission_row(*, max_attempts: int = 3) -> dict[str, Any]:
    plan = [
        {
            "name": "fix",
            "prompt": "Fix the bug",
            "validators": [{"name": "tests", "command": ["make", "test"]}],
        }
    ]
    row: dict[str, Any] = {
        "world_id": "world-test",
        "run_id": "run-test",
        "entity_id": "entity-test",
        "tick": 0,
        "is_active": True,
        "commit_token": "commit-test",
        "writer_epoch": 0,
    }
    for component in (
        Mission(name="mission", plan_json=json.dumps(plan)),
        TaskGate(max_attempts=max_attempts),
        Attempt(),
        Checkpoint(),
        Finalization(),
        Commit(),
        Evidence(),
        FrictionLog(),
        CodingAgentEpisode(
            mission_id="world-test:entity-test:mission",
            provider="modal",
            harness="codex",
        ),
    ):
        row.update(component.to_row_dict())
    return row


class _Session:
    def __init__(self, sandbox_id: str, outcomes: list[dict[str, Any]] | None = None) -> None:
        self.sandbox_id = sandbox_id
        self.outcomes = list(outcomes or [])
        self.calls: list[dict[str, Any]] = []
        self.closed = 0

    async def run_attempt(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(kwargs)
        outcome = dict(self.outcomes.pop(0))
        outcome["attempt_index"] = kwargs["attempt_index"]
        outcome["idempotency_key"] = kwargs["idempotency_key"]
        return outcome

    async def close(self) -> None:
        self.closed += 1


class _Backend:
    name = "test"

    def __init__(self, sessions: list[_Session]) -> None:
        self.sessions = list(sessions)
        self.calls: list[tuple[str, str]] = []

    async def _next(self, operation: str, checkpoint_ref: str = "") -> _Session:
        self.calls.append((operation, checkpoint_ref))
        return self.sessions.pop(0)

    async def create(self, spec: Any) -> _Session:
        return await self._next("create")

    async def restore(self, spec: Any, checkpoint_ref: str) -> _Session:
        return await self._next("restore", checkpoint_ref)

    async def resume(self, spec: Any, checkpoint_ref: str) -> _Session:
        return await self._next("resume", checkpoint_ref)

    async def authenticate(self, spec: Any) -> None:
        self.calls.append(("authenticate", ""))


def test_mission_rejection_commits_attempt_without_advancing_then_accepts() -> None:
    service = MissionService()
    row = _mission_row()
    first = service.prepare_attempt(row, tick=0)
    assert first is not None
    rejected = _outcome(accepted=False)
    rejected.update(attempt_index=1, idempotency_key=first.idempotency_key)
    row = service.apply_attempt(row, first, rejected)

    assert row["taskgate__attempts"] == 1
    assert row["taskgate__step_index"] == 0
    assert row["taskgate__status"] == "rejected"
    assert row["mission__finished"] is False
    assert row["checkpoint__restorable"] is True

    second = service.prepare_attempt(row, tick=1)
    assert second is not None
    assert second.attempt_index == 2
    assert second.previous_session_id == "thread-1"
    assert second.previous_validator_details[0]["name"] == "tests"
    accepted = _outcome(accepted=True)
    accepted.update(attempt_index=2, idempotency_key=second.idempotency_key)
    row = service.apply_attempt(row, second, accepted)

    assert row["taskgate__status"] == "passed"
    assert row["mission__finished"] is True
    assert row["mission__succeeded"] is True
    assert row["mission__pr_ready"] is True
    assert row["commit__sha"] == "verified-sha"


def test_accepted_attempt_cannot_advance_without_required_checkpoint() -> None:
    service = MissionService()
    row = _mission_row(max_attempts=1)
    request = service.prepare_attempt(row, tick=0)
    assert request is not None
    outcome = _outcome(accepted=True, checkpoint=False)
    outcome.update(attempt_index=1, idempotency_key=request.idempotency_key)
    row = service.apply_attempt(row, request, outcome)

    assert row["attempt__status"] == "accepted"
    assert row["taskgate__status"] == "exhausted"
    assert row["taskgate__step_index"] == 0
    assert row["checkpoint__restorable"] is False
    assert row["mission__succeeded"] is False
    assert row["commit__sha"] == ""


def test_mission_rejects_mismatched_or_vacuous_evidence() -> None:
    service = MissionService()
    row = _mission_row()
    request = service.prepare_attempt(row, tick=0)
    assert request is not None
    outcome = _outcome(accepted=True)
    outcome.update(attempt_index=1, idempotency_key="wrong")
    with pytest.raises(ValueError, match="idempotency_key"):
        service.apply_attempt(row, request, outcome)
    outcome.update(idempotency_key=request.idempotency_key, validator_details=[])
    with pytest.raises(ValueError, match="validator details"):
        service.apply_attempt(row, request, outcome)


@pytest.mark.asyncio
async def test_sandbox_service_owns_lifetime_and_resume_modes() -> None:
    created = _Session("sb-created")
    restored = _Session("sb-restored")
    resumed = _Session("sb-resumed")
    backend = _Backend([created, restored, resumed])
    service = SandboxService([backend])

    assert (await service.create("test", object())).sandbox_id == "sb-created"
    assert (await service.restore("test", object(), "checkpoint-a")).sandbox_id == "sb-restored"
    assert (await service.resume("test", object(), "checkpoint-b")).sandbox_id == "sb-resumed"
    await service.authenticate("test", object())
    await service.close("sb-created")
    await service.shutdown()

    assert created.closed == 1
    assert restored.closed == 1
    assert resumed.closed == 1
    assert backend.calls == [
        ("create", ""),
        ("restore", "checkpoint-a"),
        ("resume", "checkpoint-b"),
        ("authenticate", ""),
    ]
    with pytest.raises(RuntimeError, match="shutting down"):
        await service.create("test", object())


@pytest.mark.asyncio
async def test_container_registers_only_host_selected_sandbox_backends() -> None:
    session = _Session("sb-configured")
    backend = _Backend([session])
    container = ServiceContainer(sandbox_backends=[backend])

    try:
        created = await container.sandbox_service.create("test", object())
        assert created is session
        with pytest.raises(ValueError, match="unknown sandbox provider"):
            await container.sandbox_service.create("modal", object())
    finally:
        await container.shutdown()

    assert session.closed == 1


@pytest.mark.asyncio
async def test_coding_agent_service_and_processor_run_exactly_one_attempt_per_tick() -> None:
    session = _Session(
        "sb-agent",
        [_outcome(accepted=False), _outcome(accepted=True)],
    )
    backend = _Backend([session])
    sandboxes = SandboxService([backend])
    service = CodingAgentService(MissionService(), sandboxes)
    mission_id = "world-test:entity-test:mission"
    assert await service.start_episode(mission_id, "test", object()) == "sb-agent"

    resources = Resources()
    resources.insert(service)
    processor = CodingAgentProcessor()
    frame = daft.from_pylist([_mission_row()])
    first = await processor.process(frame, resources, tick=0)
    first_row = first.collect().to_pylist()[0]

    assert len(session.calls) == 1
    assert session.calls[0]["attempt_index"] == 1
    assert session.calls[0]["correlation"]["tick"] == 0
    assert first_row["taskgate__step_index"] == 0

    second = await processor.process(first, resources, tick=1)
    second_row = second.collect().to_pylist()[0]
    assert len(session.calls) == 2
    assert session.calls[1]["attempt_index"] == 2
    assert second_row["mission__succeeded"] is True

    await service.close_episode(mission_id)
    assert session.closed == 1


@pytest.mark.asyncio
async def test_coding_agent_processor_fans_out_independent_missions() -> None:
    entered = asyncio.Event()
    release = asyncio.Event()
    state = {"active": 0, "max_active": 0}

    class BlockingSession(_Session):
        async def run_attempt(self, **kwargs: Any) -> dict[str, Any]:
            state["active"] += 1
            state["max_active"] = max(state["max_active"], state["active"])
            if state["active"] == 2:
                entered.set()
            try:
                await release.wait()
                return await super().run_attempt(**kwargs)
            finally:
                state["active"] -= 1

    left_session = BlockingSession("sb-left", [_outcome(accepted=True)])
    right_session = BlockingSession("sb-right", [_outcome(accepted=True)])
    sandboxes = SandboxService([_Backend([left_session, right_session])])
    service = CodingAgentService(MissionService(), sandboxes)
    await service.start_episode("mission-left", "test", object())
    await service.start_episode("mission-right", "test", object())

    left = _mission_row()
    left.update(entity_id="left", codingagentepisode__mission_id="mission-left")
    right = _mission_row()
    right.update(entity_id="right", codingagentepisode__mission_id="mission-right")
    resources = Resources()
    resources.insert(service)

    processing = asyncio.create_task(
        CodingAgentProcessor().process(daft.from_pylist([left, right]), resources, tick=0)
    )
    await asyncio.wait_for(entered.wait(), timeout=1)
    assert state["max_active"] == 2
    release.set()
    result = await asyncio.wait_for(processing, timeout=1)
    assert all(row["mission__succeeded"] for row in result.to_pylist())

    await service.close_episode("mission-left")
    await service.close_episode("mission-right")
