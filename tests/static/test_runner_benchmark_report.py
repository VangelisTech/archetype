# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from typing import cast

import pytest

from scripts.report_runner_benchmark import SCHEMA, build_report, main


def job(
    name: str,
    *,
    runner_name: str,
    started: str = "2026-08-11T09:00:05Z",
    completed: str = "2026-08-11T09:01:05Z",
) -> dict[str, object]:
    return {
        "name": name,
        "conclusion": "success",
        "created_at": "2026-08-11T09:00:00Z",
        "started_at": started,
        "completed_at": completed,
        "runner_name": runner_name,
        "labels": ["ubuntu-latest"],
    }


def payload() -> dict[str, object]:
    return {
        "jobs": [
            job("Hosted Static", runner_name="GitHub Actions 1"),
            job(
                "Hosted Tests (3.12)",
                runner_name="GitHub Actions 2",
                completed="2026-08-11T09:05:05Z",
            ),
            job("Modal Static", runner_name="job-modal-static"),
            job(
                "Modal Tests (3.12)",
                runner_name="job-modal-tests",
                started="2026-08-11T09:00:10Z",
                completed="2026-08-11T09:04:10Z",
            ),
        ]
    }


def test_report_binds_revision_resources_and_paired_critical_paths() -> None:
    report = build_report(
        payload(),
        repository="VangelisTech/archetype",
        revision="a" * 40,
        run_id=42,
        modal_cpu=4.0,
        modal_memory_mib=16_384,
        runner_modal_revision="ead484b",
    )
    assert report["schema"] == SCHEMA
    assert report["revision"] == "a" * 40
    assert report["configuration"]["modal_cpu_physical_cores"] == 4.0
    assert report["configuration"]["runner_modal_revision"] == "ead484b"
    assert report["comparison"] == {
        "hosted_critical_path_s": 305.0,
        "modal_critical_path_s": 250.0,
        "modal_minus_hosted_s": -55.0,
        "hosted_over_modal_ratio": 1.22,
    }


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (lambda rows: rows.pop(), "exactly one"),
        (lambda rows: rows[0].update(conclusion="failure"), "did not succeed"),
        (
            lambda rows: rows[0].update(started_at="2026-08-11T08:59:59Z"),
            "non-monotonic",
        ),
    ],
)
def test_report_rejects_incomplete_or_incorrect_measurements(
    mutation: Callable[[list[dict[str, object]]], object],
    message: str,
) -> None:
    candidate = payload()
    rows = cast(list[dict[str, object]], candidate["jobs"])
    mutation(rows)
    with pytest.raises(ValueError, match=message):
        build_report(
            candidate,
            repository="VangelisTech/archetype",
            revision="a" * 40,
            run_id=42,
            modal_cpu=4.0,
            modal_memory_mib=16_384,
            runner_modal_revision="ead484b",
        )


def test_cli_writes_one_bounded_json_report(tmp_path: Path) -> None:
    jobs_path = tmp_path / "jobs.json"
    out_path = tmp_path / "report.json"
    summary_path = tmp_path / "summary.md"
    jobs_path.write_text(json.dumps(payload()))
    assert (
        main(
            [
                "--jobs-json",
                str(jobs_path),
                "--out",
                str(out_path),
                "--summary",
                str(summary_path),
                "--repository",
                "VangelisTech/archetype",
                "--revision",
                "b" * 40,
                "--run-id",
                "43",
                "--modal-cpu",
                "4",
                "--modal-memory-mib",
                "16384",
                "--runner-modal-revision",
                "ead484b",
            ]
        )
        == 0
    )
    assert json.loads(out_path.read_text())["schema"] == SCHEMA
    assert "Hosted/Modal ratio" in summary_path.read_text()


def test_workflow_runs_only_for_a_new_main_revision_or_manual_dispatch() -> None:
    workflow = Path(".github/workflows/nightly-runner-benchmark.yml").read_text()
    assert "workflow_dispatch:" in workflow
    assert "job-${{ github.run_id }}-modal-static" in workflow
    assert "job-${{ github.run_id }}-modal-tests" in workflow
    assert "runner-benchmark-${{ github.sha }}" in workflow
    trigger = Path(".github/workflows/nightly-runner-benchmark-trigger.yml").read_text()
    assert "schedule:" in trigger
    assert "ARCHETYPE_MODAL_RUNNER_ENABLED" in trigger
    assert 'workflow_id: "nightly-runner-benchmark.yml"' in trigger
    assert "previous.head_sha !== context.sha" in trigger
    assert "const shouldRun = enabled && moved" in trigger
    assert "createWorkflowDispatch" in trigger
    watchdog = Path(".github/workflows/nightly-runner-benchmark-watchdog.yml").read_text()
    assert 'const statuses = ["queued", "in_progress"]' in watchdog
