# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Executable policy for named eval profiles and retained result metadata."""

from __future__ import annotations

import json
import sys

from evals.graders import exact_match
from evals.harness import EvalHarness
from evals.run import build_harness, load_profiles
from evals.run import main as run_main


def test_profile_failure_semantics_are_explicit() -> None:
    profiles = load_profiles()

    assert profiles["conformance"]["suites"] == ["regression", "spec"]
    assert profiles["conformance"]["blocking"] is True
    assert profiles["reliability"]["suites"] == ["idempotency"]
    assert profiles["reliability"]["blocking"] is True
    assert profiles["capability"]["blocking"] is True


def test_every_blocking_eval_carries_contract_traceability() -> None:
    harness = build_harness()
    blocking_suites = {"regression", "spec", "idempotency", "capability"}
    task_ids = {
        task_id for task_id, suite, _fn, _desc in harness._tasks if suite in blocking_suites
    }

    assert task_ids
    assert all(harness._contract_map.get(task_id) for task_id in task_ids)


def test_eval_output_uses_common_result_envelope(tmp_path, monkeypatch) -> None:
    harness = EvalHarness(
        contract_map={
            "regression-task": ("core.ecs.data_model",),
            "spec-task": ("architecture.dependencies.enforced",),
        }
    )
    harness.add(
        "regression-task",
        suite="regression",
        fn=lambda: [exact_match(True, True)],
    )
    harness.add("spec-task", suite="spec", fn=lambda: [exact_match(True, True)])
    output = tmp_path / "eval.json"
    monkeypatch.setattr("evals.run.build_harness", lambda trials=1: harness)
    monkeypatch.setattr(
        sys,
        "argv",
        ["evals.run", "--profile", "conformance", "--out", str(output)],
    )

    assert run_main() == 0
    payload = json.loads(output.read_text())
    assert payload["schema_version"] == 1
    assert payload["kind"] == "eval"
    assert payload["profile"] == "conformance"
    assert payload["outcome"] == "passed"
    assert payload["revision"]["commit"]
    assert payload["environment"]["python_version"]
    assert payload["results"][0]["contract_ids"]
