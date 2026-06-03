# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Regression coverage for the specification eval suite."""

from __future__ import annotations

from evals.harness import EvalHarness
from evals.run import build_harness
from evals.suites import specification


def _assert_task_passes(task):
    results = task()
    assert results
    failures = [result for result in results if not result.passed]
    assert not failures, [f"{result.grader_name}: {result.details}" for result in failures]


def test_specification_suite_is_registered_in_regression_harness() -> None:
    harness = build_harness()
    task_ids = {task_id for task_id, suite, _, _ in harness._tasks if suite == "regression"}

    assert {
        "spec_command_role_contract",
        "spec_runtime_gate_boundary",
        "spec_lifecycle_append_only",
        "spec_gate_info_downgrade",
        "spec_doc_references",
    }.issubset(task_ids)


def test_specification_suite_registers_regression_tasks() -> None:
    harness = EvalHarness()
    specification.register(harness)

    assert len(harness._tasks) == 5
    assert {suite for _, suite, _, _ in harness._tasks} == {"regression"}


def test_command_role_contract_eval_passes() -> None:
    _assert_task_passes(specification.task_command_role_contract)


def test_runtime_gate_boundary_eval_passes() -> None:
    _assert_task_passes(specification.task_runtime_gate_boundary_static)


def test_lifecycle_append_only_eval_passes() -> None:
    _assert_task_passes(specification.task_lifecycle_append_only_static)


def test_gate_info_downgrade_eval_passes() -> None:
    _assert_task_passes(specification.task_gate_info_downgrade_smoke)


def test_specification_doc_references_eval_passes() -> None:
    _assert_task_passes(specification.task_specification_doc_references_resolve)
