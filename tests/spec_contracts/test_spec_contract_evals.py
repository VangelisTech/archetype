# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""CI gates for the independent spec-contract eval suite."""

from __future__ import annotations

import subprocess
import sys

import pytest

from evals.run import build_harness
from evals.suites import spec_contracts


def _runtime_import_result(tmp_path, monkeypatch, source_text):
    runtime_dir = tmp_path / "src" / "archetype" / "runtime"
    runtime_dir.mkdir(parents=True)
    (runtime_dir / "probe.py").write_text(source_text, encoding="utf-8")
    monkeypatch.setattr(spec_contracts, "ROOT", tmp_path)
    monkeypatch.setattr(spec_contracts, "SRC", tmp_path / "src" / "archetype")

    return next(
        result
        for result in spec_contracts.task_runtime_gate_only_boundary()
        if result.grader_name == "runtime_app_imports"
    )


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


def test_runtime_gate_rejects_disallowed_plain_import(tmp_path, monkeypatch) -> None:
    result = _runtime_import_result(
        tmp_path,
        monkeypatch,
        "import archetype.app.world.service\n",
    )

    assert result.passed is False
    assert "archetype.app.world.service" in result.details


@pytest.mark.parametrize(
    "source_text",
    [
        "import archetype.application\n",
        "from archetype.application import Service\n",
    ],
)
def test_runtime_gate_uses_a_dotted_app_boundary(tmp_path, monkeypatch, source_text) -> None:
    result = _runtime_import_result(tmp_path, monkeypatch, source_text)

    assert result.passed is True


@pytest.mark.parametrize(
    "source_text",
    [
        "import archetype.app.evaluation.service\n",
        "from archetype.app.evaluation.service import EvaluationResult\n",
        "from typing import TYPE_CHECKING\n"
        "if TYPE_CHECKING:\n"
        "    import archetype.app.evaluation.service\n",
        "from typing import TYPE_CHECKING\n"
        "if TYPE_CHECKING:\n"
        "    from archetype.app.evaluation.service import EvaluationResult\n",
    ],
)
def test_runtime_gate_rejects_concrete_service_imports_even_when_type_only(
    tmp_path, monkeypatch, source_text
) -> None:
    result = _runtime_import_result(tmp_path, monkeypatch, source_text)

    assert result.passed is False
    assert "archetype.app.evaluation.service" in result.details


@pytest.mark.parametrize(
    "source_text",
    [
        "from typing import TYPE_CHECKING\n"
        "if TYPE_CHECKING:\n"
        "    import archetype.app.evaluation.interfaces\n",
        "from typing import TYPE_CHECKING\n"
        "if TYPE_CHECKING:\n"
        "    from archetype.app.evaluation.models import EvaluationResult\n",
    ],
)
def test_runtime_gate_allows_declared_contract_imports_under_type_checking(
    tmp_path, monkeypatch, source_text
) -> None:
    result = _runtime_import_result(tmp_path, monkeypatch, source_text)

    assert result.passed is True
