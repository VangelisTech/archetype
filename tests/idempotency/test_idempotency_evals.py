# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""CI gates for the idempotency eval suite."""

from __future__ import annotations

import subprocess
import sys

import pytest

from archetype.core.aio.async_world import AsyncWorld
from evals.run import build_harness
from evals.suites import idempotency
from evals.suites.idempotency.process import (
    _wait_for_markers,
    run_process_evaluation_replay,
    task_process_writer_fence_race,
)

pytestmark = [pytest.mark.process, pytest.mark.slow]


def test_idempotency_eval_suite_is_registered_and_traceable() -> None:
    harness = build_harness(trials=1)
    registered = {
        task_id for task_id, suite, _, _ in harness.registered_tasks if suite == idempotency.SUITE
    }
    mapped = {case.task_id for case in idempotency.IDEMPOTENCY_CASES}

    assert registered == mapped | {"idempotency.manifest_traceability"}
    assert all(idempotency.traceability_checks().values())


def test_idempotency_contract_audit_cli() -> None:
    proc = subprocess.run(
        [sys.executable, "scripts/check_idempotency_contracts.py"],
        capture_output=True,
        text=True,
        check=False,
    )

    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert "Idempotency contract audit passed" in proc.stdout


def test_idempotency_contract_audit_detects_unmapped_spec_row(tmp_path, monkeypatch) -> None:
    drifted = tmp_path / "specification.md"
    text = idempotency.tasks.SPECIFICATION.read_text()
    text = text.replace(
        "\n## Required Hardening Work",
        "\n| Newly normative retry | Must have an independent eval |\n\n## Required Hardening Work",
    )
    drifted.write_text(text)
    monkeypatch.setattr(idempotency.tasks, "SPECIFICATION", drifted)

    checks = idempotency.traceability_checks()

    assert not checks["matrix_rows_match_eval_manifest"]


def test_staged_spawn_eval_detects_first_write_wins_regression(monkeypatch) -> None:
    original_spawn_frame = AsyncWorld._spawn_frame

    def first_write_wins(self, sig):
        rows = self.spawn_cache.get(sig)
        if rows:
            self.spawn_cache[sig] = list(reversed(rows))
        try:
            return original_spawn_frame(self, sig)
        finally:
            if rows is not None:
                self.spawn_cache[sig] = rows

    monkeypatch.setattr(AsyncWorld, "_spawn_frame", first_write_wins)

    results = idempotency.task_staged_spawn_last_write_wins()

    assert any(not result.passed for result in results)


def test_process_readiness_failure_reports_child_stderr(tmp_path) -> None:
    marker = tmp_path / "never-ready"
    process = subprocess.Popen(
        [
            sys.executable,
            "-c",
            "import sys; print('worker boom', file=sys.stderr); raise SystemExit(7)",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    with pytest.raises(AssertionError, match="worker exited before readiness") as exc_info:
        _wait_for_markers([marker], [process], timeout=5)

    assert "returncode=7" in str(exc_info.value)
    assert "worker boom" in str(exc_info.value)


def test_process_evaluation_replay_serializes_external_grader() -> None:
    results = run_process_evaluation_replay()

    assert all(result.passed for result in results), results


def test_process_writer_fence_race_classifies_reconciled_loser_as_stale() -> None:
    results = task_process_writer_fence_race()

    assert all(result.passed for result in results), results
