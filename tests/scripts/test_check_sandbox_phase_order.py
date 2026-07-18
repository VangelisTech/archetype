# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path

CHECKER_PATH = Path(__file__).resolve().parents[2] / "scripts" / "check_sandbox_phase_order.py"
SPEC = importlib.util.spec_from_file_location("check_sandbox_phase_order", CHECKER_PATH)
assert SPEC is not None and SPEC.loader is not None
checker = importlib.util.module_from_spec(SPEC)
sys.modules["check_sandbox_phase_order"] = checker
SPEC.loader.exec_module(checker)


def _kernel_source() -> str:
    methods = "\n".join(
        f"    async def {name}(self):\n        pass" for name in checker.EXPECTED_PHASE_CALLS
    )
    return f"""
class CodingAgentSandboxClient:
    async def run_attempt(self):
        execution = await self._execution_phase()
        validation = await self._validation_phase()
        repository = await self._repository_finalization_phase()
        evidence = await self._evidence_phase()
        checkpoint = await self._checkpoint_phase()
        outcome = await self._artifact_handoff_phase()
        return execution, validation, repository, evidence, checkpoint, outcome

{methods}
"""


def test_phase_audit_detects_reordering_and_omission(tmp_path: Path) -> None:
    source = tmp_path / "common.py"
    reordered = _kernel_source().replace(
        "        validation = await self._validation_phase()\n"
        "        repository = await self._repository_finalization_phase()\n",
        "        repository = await self._repository_finalization_phase()\n"
        "        validation = await self._validation_phase()\n",
    )
    source.write_text(reordered, encoding="utf-8")
    assert any("exactly once in order" in error for error in checker.audit_source(source))

    omitted = _kernel_source().replace("        evidence = await self._evidence_phase()\n", "")
    source.write_text(omitted, encoding="utf-8")
    assert any("observed" in error for error in checker.audit_source(source))


def test_phase_audit_rejects_conditional_phase_calls(tmp_path: Path) -> None:
    source = tmp_path / "common.py"
    conditional = _kernel_source().replace(
        "        checkpoint = await self._checkpoint_phase()",
        "        if True:\n            checkpoint = await self._checkpoint_phase()",
    )
    source.write_text(conditional, encoding="utf-8")
    assert checker.audit_source(source) == [
        "phase calls must be unconditional top-level awaited statements in run_attempt"
    ]


def test_phase_audit_reports_missing_or_malformed_source(tmp_path: Path) -> None:
    missing = tmp_path / "missing.py"
    assert checker.audit_source(missing) == [f"sandbox kernel source does not exist: {missing}"]

    malformed = tmp_path / "malformed.py"
    malformed.write_text("not valid python :", encoding="utf-8")
    assert "not valid Python" in checker.audit_source(malformed)[0]

    no_client = tmp_path / "no-client.py"
    no_client.write_text("value = 1\n", encoding="utf-8")
    assert checker.audit_source(no_client) == [
        "expected exactly one CodingAgentSandboxClient class, found 0"
    ]


def test_repository_sandbox_phase_order_passes() -> None:
    completed = subprocess.run(
        [sys.executable, str(CHECKER_PATH)],
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr
    assert "Sandbox phase-order audit passed" in completed.stdout
