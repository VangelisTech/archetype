# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Contracts for lightweight, reproducible benchmark snapshots."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from bench.core.report import build_report, write_report

_ENVIRONMENT = {
    "runner_id": "stable-runner",
    "system": "TestOS",
    "release": "1",
    "machine": "test64",
    "processor": "test-cpu",
    "python_implementation": "CPython",
    "python_version": "3.12.10",
    "packages": {"archetype-ecs": "0.4.0"},
}
_REVISION = {"commit": "1" * 40, "dirty": False}


def _raw_result(*, elapsed_s: float = 2.0) -> dict:
    return {
        "name": "packed_iteration",
        "entities": 100,
        "steps": 1,
        "elapsed_s": elapsed_s,
        "extras": {"width": 5},
        "world_id": "world",
        "run_id": "run",
    }


def _report() -> dict:
    return build_report(
        [_raw_result()],
        suite="ecs",
        config={"steps": 1, "storage_backend": "lancedb", "cache": False},
        environment=_ENVIRONMENT,
        revision=_REVISION,
        created_at="2026-07-17T12:00:00Z",
    )


def test_report_is_a_plain_snapshot_with_reproduction_context() -> None:
    report = _report()

    assert set(report) == {
        "suite",
        "created_at",
        "revision",
        "environment",
        "config",
        "results",
    }
    assert report["suite"] == "ecs"
    assert report["revision"] == _REVISION
    assert report["environment"] == _ENVIRONMENT
    assert report["results"] == [_raw_result()]


@pytest.mark.parametrize(
    ("suite", "results", "message"),
    [
        ("", [_raw_result()], "suite"),
        ("ecs", [], "result"),
    ],
)
def test_report_rejects_degenerate_snapshots(suite: str, results: list[dict], message: str) -> None:
    with pytest.raises(ValueError, match=message):
        build_report(results, suite=suite, config={})


def test_report_rejects_non_finite_json() -> None:
    with pytest.raises(ValueError, match="finite JSON"):
        build_report([_raw_result(elapsed_s=float("nan"))], suite="ecs", config={})


def test_write_report_creates_a_readable_snapshot(tmp_path: Path) -> None:
    output = tmp_path / "nested" / "bench.json"

    written = write_report(_report(), output)

    assert written == output
    assert json.loads(output.read_text()) == _report()
