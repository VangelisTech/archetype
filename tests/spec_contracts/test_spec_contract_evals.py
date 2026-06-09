# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""CI gates for the independent spec-contract eval suite."""

from __future__ import annotations

import subprocess
import sys

from evals.run import build_harness
from evals.suites import spec_contracts


def test_spec_contract_eval_suite_passes() -> None:
    harness = build_harness(trials=1)
    results = harness.run(suite_filter=spec_contracts.SUITE)

    assert results, "spec-contract suite registered no tasks"

    failures: list[str] = []
    for result in results:
        for trial in result.trials:
            if trial.error:
                failures.append(f"{result.task_id}: {trial.error}")
            for grader in trial.grader_results:
                if not grader.passed:
                    failures.append(f"{result.task_id}/{grader.grader_name}: {grader.details}")

    assert not failures, "spec-contract eval failures:\n  " + "\n  ".join(failures)


def test_spec_contract_cli_suite_is_runnable() -> None:
    proc = subprocess.run(
        [sys.executable, "-m", "evals.run", "--suite", spec_contracts.SUITE, "--trials", "1"],
        capture_output=True,
        text=True,
        check=False,
    )

    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert "[SPEC]" in proc.stdout
