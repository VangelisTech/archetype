# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Behavioral contracts for the PR-triage example."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import daft
import pytest


def _load_example():
    path = Path("examples/pr_triage.py")
    spec = importlib.util.spec_from_file_location("pr_triage_contract", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.mark.asyncio
async def test_closed_pull_requests_are_terminal() -> None:
    triage = _load_example()
    frame = daft.from_pylist(
        [
            {
                "pullrequest__state": state,
                "triage__staleness": 0.1,
                "triage__risk": "low",
            }
            for state in ("CLOSED", "MERGED", "OPEN")
        ]
    )

    result = await triage.TriageProcessor().process(frame)

    assert [row["triage__action"] for row in result.collect().to_pylist()] == [
        "done",
        "done",
        "merge",
    ]
