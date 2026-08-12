#!/usr/bin/env python3
# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Build one paired GitHub-hosted versus Modal runner benchmark report."""

from __future__ import annotations

import argparse
import json
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

SCHEMA = "archetype.ci-runner-benchmark/v1"
JOBS = {
    "Hosted Static": ("hosted_static", "github-hosted"),
    "Hosted Tests (3.12)": ("hosted_tests", "github-hosted"),
    "Modal Static": ("modal_static", "modal"),
    "Modal Tests (3.12)": ("modal_tests", "modal"),
}


def parse_time(value: object, *, field: str) -> datetime:
    if not isinstance(value, str) or not value:
        raise ValueError(f"job {field} must be a non-empty timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"job {field} is not an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None:
        raise ValueError(f"job {field} must include a timezone")
    return parsed


def measurement(row: Mapping[str, Any], *, runner: str) -> dict[str, Any]:
    if row.get("conclusion") != "success":
        raise ValueError(f"job {row.get('name')!r} did not succeed")
    created = parse_time(row.get("created_at"), field="created_at")
    started = parse_time(row.get("started_at"), field="started_at")
    completed = parse_time(row.get("completed_at"), field="completed_at")
    if not created <= started <= completed:
        raise ValueError(f"job {row.get('name')!r} has non-monotonic timestamps")
    labels = row.get("labels")
    if not isinstance(labels, list) or not all(isinstance(item, str) for item in labels):
        raise ValueError(f"job {row.get('name')!r} labels must be strings")
    runner_name = row.get("runner_name")
    if not isinstance(runner_name, str) or not runner_name:
        raise ValueError(f"job {row.get('name')!r} has no runner identity")
    return {
        "runner": runner,
        "runner_name": runner_name,
        "labels": labels,
        "queue_s": round((started - created).total_seconds(), 3),
        "execution_s": round((completed - started).total_seconds(), 3),
        "total_s": round((completed - created).total_seconds(), 3),
    }


def build_report(
    payload: Mapping[str, Any],
    *,
    repository: str,
    revision: str,
    run_id: int,
    modal_cpu: float,
    modal_memory_mib: int,
    runner_modal_revision: str,
) -> dict[str, Any]:
    rows = payload.get("jobs")
    if not isinstance(rows, list):
        raise ValueError("jobs payload must contain a jobs list")
    selected: dict[str, dict[str, Any]] = {}
    for display_name, (key, runner) in JOBS.items():
        matches = [row for row in rows if isinstance(row, dict) and row.get("name") == display_name]
        if len(matches) != 1:
            raise ValueError(f"expected exactly one {display_name!r} job")
        selected[key] = measurement(matches[0], runner=runner)

    hosted_critical = max(
        selected["hosted_static"]["total_s"],
        selected["hosted_tests"]["total_s"],
    )
    modal_critical = max(
        selected["modal_static"]["total_s"],
        selected["modal_tests"]["total_s"],
    )
    return {
        "schema": SCHEMA,
        "generated_at": datetime.now(UTC).isoformat(),
        "repository": repository,
        "revision": revision,
        "workflow_run_id": run_id,
        "configuration": {
            "hosted_label": "ubuntu-latest",
            "modal_cpu_physical_cores": modal_cpu,
            "modal_memory_mib": modal_memory_mib,
            "runner_modal_revision": runner_modal_revision,
            "commands": {
                "static": "make static",
                "tests": "make test",
            },
        },
        "jobs": selected,
        "comparison": {
            "hosted_critical_path_s": hosted_critical,
            "modal_critical_path_s": modal_critical,
            "modal_minus_hosted_s": round(modal_critical - hosted_critical, 3),
            "hosted_over_modal_ratio": round(hosted_critical / modal_critical, 4),
        },
    }


def summary(report: Mapping[str, Any]) -> str:
    jobs = report["jobs"]
    comparison = report["comparison"]
    return "\n".join(
        (
            "## Nightly runner benchmark",
            "",
            f"Revision: `{report['revision']}`",
            "",
            "| Lane | Queue | Execution | Total |",
            "|---|---:|---:|---:|",
            *(
                f"| {name} | {jobs[key]['queue_s']:.1f}s | "
                f"{jobs[key]['execution_s']:.1f}s | {jobs[key]['total_s']:.1f}s |"
                for name, key in (
                    ("Hosted static", "hosted_static"),
                    ("Hosted tests", "hosted_tests"),
                    ("Modal static", "modal_static"),
                    ("Modal tests", "modal_tests"),
                )
            ),
            "",
            f"Hosted critical path: {comparison['hosted_critical_path_s']:.1f}s  ",
            f"Modal critical path: {comparison['modal_critical_path_s']:.1f}s  ",
            f"Hosted/Modal ratio: {comparison['hosted_over_modal_ratio']:.3f}x",
            "",
        )
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--jobs-json", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--summary", type=Path)
    parser.add_argument("--repository", required=True)
    parser.add_argument("--revision", required=True)
    parser.add_argument("--run-id", type=int, required=True)
    parser.add_argument("--modal-cpu", type=float, required=True)
    parser.add_argument("--modal-memory-mib", type=int, required=True)
    parser.add_argument("--runner-modal-revision", required=True)
    args = parser.parse_args(argv)
    with args.jobs_json.open(encoding="utf-8") as stream:
        payload = json.load(stream)
    report = build_report(
        payload,
        repository=args.repository,
        revision=args.revision,
        run_id=args.run_id,
        modal_cpu=args.modal_cpu,
        modal_memory_mib=args.modal_memory_mib,
        runner_modal_revision=args.runner_modal_revision,
    )
    args.out.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    if args.summary is not None:
        args.summary.write_text(summary(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
