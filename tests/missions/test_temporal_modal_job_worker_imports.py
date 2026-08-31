# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Import and queue isolation for the split Modal Mission Worker."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

from archetype.missions.temporal.contracts import (
    MISSION_MODAL_JOB_TASK_QUEUE,
    MISSION_TASK_QUEUE,
)


def test_split_worker_imports_neither_modal_sdk_nor_legacy_supervisor() -> None:
    source = """
import sys

class BlockedImport:
    def find_spec(self, fullname, path=None, target=None):
        if fullname == 'modal':
            raise RuntimeError(f'forbidden import: {fullname}')
        return None

sys.meta_path.insert(0, BlockedImport())
from archetype.missions.temporal.modal_job_worker import create_mission_modal_job_worker
assert callable(create_mission_modal_job_worker)
"""
    completed = subprocess.run(
        [sys.executable, "-c", source],
        cwd=Path(__file__).resolve().parents[2],
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stderr


def test_split_and_legacy_workers_have_disjoint_default_queues() -> None:
    assert MISSION_MODAL_JOB_TASK_QUEUE == "archetype-missions-modal-jobs-v1"
    assert MISSION_MODAL_JOB_TASK_QUEUE != MISSION_TASK_QUEUE
