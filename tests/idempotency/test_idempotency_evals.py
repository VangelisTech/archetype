# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""CI gates for the idempotency eval suite."""

from __future__ import annotations

import subprocess
import sys

from evals.run import build_harness
from evals.suites import idempotency


def test_idempotency_eval_suite_is_registered_and_traceable() -> None:
    harness = build_harness(trials=1)
    registered = {task_id for task_id, suite, _, _ in harness._tasks if suite == idempotency.SUITE}
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
    text = idempotency.SPECIFICATION.read_text()
    text = text.replace(
        "\n## Required Hardening Work",
        "\n| Newly normative retry | Must have an independent eval |\n\n## Required Hardening Work",
    )
    drifted.write_text(text)
    monkeypatch.setattr(idempotency, "SPECIFICATION", drifted)

    checks = idempotency.traceability_checks()

    assert not checks["matrix_rows_match_eval_manifest"]
