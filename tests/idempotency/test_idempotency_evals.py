# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""CI gates for the idempotency eval suite."""

from __future__ import annotations

import subprocess
import sys

from evals.run import build_harness
from evals.suites import idempotency


def test_idempotency_eval_suite_passes() -> None:
    harness = build_harness(trials=1)
    results = harness.run(suite_filter=idempotency.SUITE)

    assert results, "idempotency suite registered no tasks"

    failures: list[str] = []
    for result in results:
        for trial in result.trials:
            if trial.error:
                failures.append(f"{result.task_id}: {trial.error}")
            for grader in trial.grader_results:
                if not grader.passed:
                    failures.append(f"{result.task_id}/{grader.grader_name}: {grader.details}")

    assert not failures, "idempotency eval failures:\n  " + "\n  ".join(failures)


def test_idempotency_cli_suite_is_runnable() -> None:
    proc = subprocess.run(
        [sys.executable, "-m", "evals.run", "--suite", idempotency.SUITE, "--trials", "1"],
        capture_output=True,
        text=True,
        check=False,
    )

    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert "[IDEMPOTENCY]" in proc.stdout
