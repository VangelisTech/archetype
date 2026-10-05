# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0
"""Absent, partial or failed current execution never counts as acceptance."""

import json

import pytest

from scripts.acceptance_receipts import require_child, run_with_receipt


def passing():
    return dict(
        schema="test.installed/v1",
        mode="installed actual",
        result="pass",
        validation_errors=[],
        failure=None,
        origins=1,
        tests_run=1,
        failures=0,
        errors=0,
        skipped=0,
    )


@pytest.mark.parametrize(
    "mutation",
    [{"result": v} for v in ("fail", "not_run", "skipped", None)]
    + [
        {"mode": "source"},
        {"schema": "old"},
        {"validation_errors": ["drift"]},
        {"skipped": 1},
        {"tests_run": True},
        {"failure": "failed"},
        {"origins": 0},
    ],
)
def test_partial_or_wrong_current_child_is_rejected(tmp_path, mutation):
    path = tmp_path / "child.json"
    path.write_text(json.dumps(passing() | mutation))
    with pytest.raises(RuntimeError):
        require_child(
            path,
            schema="test.installed/v1",
            mode="installed actual",
            counts={"tests_run": 1, "failures": 0, "errors": 0, "skipped": 0},
        )


def test_missing_child_is_rejected_and_complete_control_passes(tmp_path):
    path = tmp_path / "child.json"
    with pytest.raises(FileNotFoundError):
        require_child(path, schema="test.installed/v1", mode="installed actual")
    path.write_text(json.dumps(passing()))
    require_child(
        path,
        schema="test.installed/v1",
        mode="installed actual",
        counts={"tests_run": 1, "failures": 0, "errors": 0, "skipped": 0},
    )


@pytest.mark.parametrize("phase", ["candidate", "dependencies", "build", "child"])
def test_current_parent_failures_retain_stage_and_log_identity(tmp_path, phase):
    stage = tmp_path / phase

    def fail():
        stage.mkdir()
        (stage / (phase + ".log")).write_text("masked diagnosis")
        raise RuntimeError("synthetic-provider-secret")

    with pytest.raises(RuntimeError):
        run_with_receipt(stage, fail, mode="installed actual")
    receipt = json.loads((stage / "parent-result.json").read_text())
    assert receipt["result"] == "fail" and receipt["stage"] == str(stage)
    assert receipt["logs"] == [phase + ".log"]
    assert "synthetic-provider-secret" not in json.dumps(receipt)


def test_rejected_existing_stage_evidence_is_never_overwritten(tmp_path):
    stage = tmp_path / "prior"
    stage.mkdir()
    prior = stage / "parent-result.json"
    prior.write_text("preserved")

    def fail():
        raise ValueError("existing stage")

    with pytest.raises(ValueError):
        run_with_receipt(stage, fail, mode="installed actual")
    assert prior.read_text() == "preserved"
