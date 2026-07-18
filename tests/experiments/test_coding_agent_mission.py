# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Fast state-machine contracts for the real coding-agent mission example."""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from typing import Any

import daft
import pytest

from archetype.core.resources import Resources
from archetype.experiments.modal_coding_agent import ModalSandboxClient

_EXAMPLE = Path(__file__).resolve().parents[2] / "examples" / "11_coding_agent_mission.py"
_SPEC = importlib.util.spec_from_file_location("coding_agent_mission_example", _EXAMPLE)
assert _SPEC is not None and _SPEC.loader is not None
mission_example = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = mission_example
_SPEC.loader.exec_module(mission_example)


class _FakeSandbox:
    sandbox_id = "sandbox-test"

    def __init__(self, outcomes: list[dict[str, Any]]) -> None:
        self.outcomes = list(outcomes)
        self.calls: list[dict[str, Any]] = []

    async def run_attempt(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(kwargs)
        return self.outcomes.pop(0)


def _outcome(*, index: int, accepted: bool) -> dict[str, Any]:
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
    snapshot = f"modal-image://im-attempt-{index}"
    return {
        "attempt_id": f"attempt-{index}",
        "idempotency_key": f"gate-{index}",
        "status": status,
        "accepted": accepted,
        "sha": "verified-sha" if accepted else "",
        "message": "fix: issue",
        "pushed": False,
        "results": {"tests": accepted},
        "validator_details": details,
        "trace_ref": f"{snapshot}#/workspace/.archetype-agent/traces/attempt-{index}.jsonl",
        "traces_ref": f"{snapshot}#/workspace/.archetype-agent/traces",
        "live_status_ref": f"modal-sandbox://sb-test/workspace/live/session-{index}.json",
        "live_events_ref": f"modal-sandbox://sb-test/workspace/live/events-{index}.jsonl",
        "sandbox_state_ref": snapshot,
        "checkpoint_status": "ready",
        "checkpoint_provider": "modal",
        "checkpoint_restorable": True,
        "checkpoint_error": "",
        "checkpoint_created_at_ms": index,
        "checkpoint_expires_at_ms": index + 1000,
        "finalization_phase": "checkpointed",
        "finalization_error": "",
        "finalization_manifest_ref": f"{snapshot}#/workspace/manifest.json",
        "filesystem_start_ref": f"{snapshot}#/workspace/start.jsonl",
        "filesystem_end_ref": f"{snapshot}#/workspace/end.jsonl",
        "filesystem_diff_ref": f"{snapshot}#/workspace/diff.jsonl",
        "git_status_ref": f"{snapshot}#/workspace/status.txt",
        "git_patch_ref": f"{snapshot}#/workspace/attempt.patch",
        "git_bundle_ref": f"{snapshot}#/workspace/repository.bundle",
        "context_ref": f"{snapshot}#/workspace/.context",
        "harness": "codex",
        "agent_session_id": f"thread-{index}",
        "friction": [] if accepted else [{"finding": "tests failed"}],
        "pr_url": "",
    }


def _mission_row() -> dict[str, Any]:
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
        mission_example.Mission(plan_json=json.dumps(plan)),
        mission_example.TaskGate(max_attempts=3),
        mission_example.Attempt(),
        mission_example.Checkpoint(),
        mission_example.Finalization(),
        mission_example.Commit(),
        mission_example.Evidence(),
        mission_example.FrictionLog(),
    ):
        row.update(component.to_row_dict())
    return row


def test_checked_in_live_mission_keeps_prompt_naive_and_gate_authoritative() -> None:
    step = mission_example.PLAN[0]

    assert step["prompt"] == "Fix https://github.com/VangelisTech/archetype/issues/457."
    assert [validator["name"] for validator in step["validators"]] == [
        "same_world_lifecycle_contract",
        "material_app_diff",
        "app_api_regression_tests",
        "ruff",
        "git_diff_check",
        "tests",
    ]
    assert step["validators"][0]["command"][-1] == mission_example._ISSUE_457_CONCURRENCY_CONTRACT
    assert step["validators"][2]["command"][-2:] == ["tests/app", "tests/api"]
    assert step["validators"][-1]["command"] == ["make", "test"]


@pytest.mark.asyncio
async def test_one_tick_records_one_attempt_and_only_accepted_checkpoint_advances() -> None:
    sandbox = _FakeSandbox([_outcome(index=1, accepted=False), _outcome(index=2, accepted=True)])
    resources = Resources()
    resources.insert_as(sandbox, ModalSandboxClient)
    processor = mission_example.CodingAgentProcessor()

    first = await processor.process(daft.from_pylist([_mission_row()]), resources, tick=0)
    first_row = first.collect().to_pylist()[0]

    assert len(sandbox.calls) == 1
    assert sandbox.calls[0]["attempt_index"] == 1
    assert sandbox.calls[0]["correlation"]["world_id"] == "world-test"
    assert sandbox.calls[0]["correlation"]["tick"] == 0
    assert first_row["taskgate__attempts"] == 1
    assert first_row["taskgate__status"] == "rejected"
    assert first_row["taskgate__step_index"] == 0
    assert first_row["mission__finished"] is False
    assert first_row["checkpoint__restorable"] is True
    assert first_row["evidence__live_status_ref"].endswith("session-1.json")
    assert first_row["evidence__live_events_ref"].endswith("events-1.jsonl")

    second = await processor.process(first, resources, tick=1)
    second_row = second.collect().to_pylist()[0]

    assert len(sandbox.calls) == 2
    assert sandbox.calls[1]["attempt_index"] == 2
    assert sandbox.calls[1]["previous_session_id"] == "thread-1"
    assert sandbox.calls[1]["previous_validator_details"][0]["name"] == "tests"
    assert second_row["taskgate__attempts"] == 2
    assert second_row["taskgate__status"] == "passed"
    assert second_row["mission__finished"] is True
    assert second_row["mission__succeeded"] is True
    assert second_row["mission__pr_ready"] is True
    assert second_row["commit__sha"] == "verified-sha"


@pytest.mark.asyncio
async def test_accepted_attempt_with_failed_checkpoint_commits_tick_but_not_task() -> None:
    outcome = _outcome(index=1, accepted=True)
    outcome.update(
        {
            "sandbox_state_ref": "",
            "checkpoint_status": "failed",
            "checkpoint_restorable": False,
            "checkpoint_error": "RuntimeError: provider unavailable",
            "finalization_phase": "captured",
            "finalization_error": "RuntimeError: provider unavailable",
        }
    )
    sandbox = _FakeSandbox([outcome])
    resources = Resources()
    resources.insert_as(sandbox, ModalSandboxClient)

    frame = await mission_example.CodingAgentProcessor().process(
        daft.from_pylist([_mission_row()]), resources, tick=0
    )
    row = frame.collect().to_pylist()[0]

    assert row["attempt__status"] == "accepted"
    assert row["taskgate__attempts"] == 1
    assert row["taskgate__step_index"] == 0
    assert row["taskgate__passed"] is False
    assert row["checkpoint__status"] == "failed"
    assert row["checkpoint__restorable"] is False
    assert row["finalization__phase"] == "captured"
    assert "provider unavailable" in row["finalization__error"]
    assert row["mission__finished"] is False
    assert row["commit__sha"] == ""
