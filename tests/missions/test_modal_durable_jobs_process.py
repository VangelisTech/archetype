# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""OS-process crash contracts for provider-native durable Mission jobs."""

from __future__ import annotations

import json
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

pytestmark = pytest.mark.process

_CALL_IDENTITY_CRASH = 86
_RESULT_RECORD_CRASH = 87
_CALL_ID = "fc-process-1"
_OPERATION_ID = "mission:author:process-crash-1"
_WORLD_ID = "mission-process-world"
_RUN_ID = "mission-process-run"


@dataclass(frozen=True, slots=True)
class _ProcessHarness:
    root: Path

    @property
    def fixture(self) -> Path:
        return Path(__file__).resolve().parents[1] / "fixtures" / "modal_mission_job_process.py"

    def invoke(
        self,
        action: str,
        *,
        failpoint: str = "none",
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [
                sys.executable,
                str(self.fixture),
                str(self.root),
                action,
                "--failpoint",
                failpoint,
            ],
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )

    def run(self, action: str) -> dict[str, Any]:
        process = self.invoke(action)
        assert process.returncode == 0, (
            f"process fixture action {action!r} failed with {process.returncode}:\n"
            f"stdout={process.stdout[-2000:]}\nstderr={process.stderr[-4000:]}"
        )
        return json.loads(process.stdout)


def _assert_one_self_registered_call(state: dict[str, Any]) -> None:
    provider = state["provider"]
    assert provider["spawn_count"] == 1
    assert provider["call_identity_insertions"] == 1
    assert provider["job_record_count"] == 2
    assert [call["call_id"] for call in provider["calls"]] == [_CALL_ID]
    assert provider["calls"][0]["operation_id"] == _OPERATION_ID


def test_hard_killed_worker_replacement_polls_exact_self_registered_call(
    tmp_path: Path,
) -> None:
    harness = _ProcessHarness(tmp_path)

    crashed = harness.invoke("start", failpoint="after-call-identity")
    assert crashed.returncode == _CALL_IDENTITY_CRASH

    after_crash = harness.run("inspect")
    _assert_one_self_registered_call(after_crash)
    assert after_crash["provider"]["calls"][0]["status"] == "running"
    assert after_crash["provider"]["reattach_call_ids"] == []
    assert after_crash["activity"]["execution"] == {
        "operation_id": _OPERATION_ID,
        "provider": "modal-process-double",
    }
    assert after_crash["activity"]["result"] is None
    assert after_crash["activity"]["settlement"] is None

    replacement = harness.run("poll")
    _assert_one_self_registered_call(replacement)
    assert replacement["call_id"] == _CALL_ID
    assert replacement["poll"] == "running"
    assert replacement["provider"]["reattach_call_ids"] == [_CALL_ID]
    assert replacement["activity_inventory"]["attempt_count"] == 0


def test_published_result_survives_worker_death_before_exact_settlement(
    tmp_path: Path,
) -> None:
    harness = _ProcessHarness(tmp_path)

    crashed_start = harness.invoke("start", failpoint="after-call-identity")
    assert crashed_start.returncode == _CALL_IDENTITY_CRASH

    published = harness.run("publish")
    _assert_one_self_registered_call(published)
    result_digest = published["published_result_digest"]
    assert published["provider"]["result_publication_count"] == 1
    # The provider output is deliberately terminally unavailable. A later
    # poll can succeed only if it consults the durable first result first.
    assert published["provider"]["calls"][0]["status"] == "terminal-failed"
    assert published["provider"]["calls"][0]["result_digest"] == result_digest
    assert published["activity"]["result"] is None
    assert published["activity"]["settlement"] is None

    crashed_collector = harness.invoke("collect", failpoint="after-result-record")
    assert crashed_collector.returncode == _RESULT_RECORD_CRASH

    before_settlement = harness.run("inspect")
    _assert_one_self_registered_call(before_settlement)
    assert before_settlement["activity"]["result"]["digest"] == result_digest
    assert before_settlement["activity"]["result_pending_observation"] is True
    assert before_settlement["activity"]["result_attempt"] is None
    assert before_settlement["activity"]["result_fence"] is None
    assert before_settlement["activity"]["settlement"] is None
    assert before_settlement["activity_inventory"] == {
        "activity_count": 1,
        "attempt_count": 0,
        "provider_operation_count": 0,
    }

    replacement = harness.run("settle")
    _assert_one_self_registered_call(replacement)
    assert replacement["call_id"] == _CALL_ID
    assert replacement["poll"] == "ready"
    assert replacement["settled_result_digest"] == result_digest
    assert replacement["provider"]["reattach_call_ids"] == []
    assert replacement["activity"]["result_pending_observation"] is False
    assert replacement["activity"]["settlement"] == {
        "committed_tick": 2,
        "result_digest": result_digest,
        "run_id": _RUN_ID,
        "visibility_token": "mission-process-observation-2",
        "world_id": _WORLD_ID,
    }

    repeated = harness.run("settle")
    _assert_one_self_registered_call(repeated)
    assert repeated["activity"] == replacement["activity"]
    assert repeated["provider"]["result_publication_count"] == 1
    assert repeated["provider"]["reattach_call_ids"] == []
