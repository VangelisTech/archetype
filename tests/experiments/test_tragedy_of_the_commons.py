# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Contracts for the Tragedy of the Commons experiment."""

from __future__ import annotations

import importlib.util
import sys
import tomllib
from pathlib import Path

import daft
import pytest

from archetype.core.resources import Resources


def _load_experiment(name: str):
    path = Path("experiments/tragedy_of_the_commons.py")
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


@pytest.mark.asyncio
async def test_empty_pool_floors_agent_energy_at_zero() -> None:
    commons = _load_experiment("commons_empty_pool_contract")
    resources = Resources()
    resources.insert(commons.CommonPool(amount=0.0))
    resources.insert(commons.SimMetrics())
    frame = daft.from_pylist(
        [
            {
                "gatherer__name": "probe",
                "gatherer__strategy": "cooperative",
                "gatherer__harvest_rate": 0.02,
                "gatherer__energy": 1.0,
                "gatherer__total_harvested": 0.0,
            }
        ]
    )

    result = await commons.HarvestProcessor().process(frame, resources=resources)

    assert result.collect().to_pylist()[0]["gatherer__energy"] == 0.0


def test_pool_timeline_uses_recorded_tick_range(capsys) -> None:
    commons = _load_experiment("commons_timeline_contract")
    pool = commons.CommonPool(amount=500.0, max_capacity=1000.0, growth_rate=0.1)
    pool.regenerate()
    pool.record()

    commons.print_results(
        [
            {
                "scenario": scenario,
                "pool_history": pool.history,
                "final_pool": pool.amount,
                "agents": [],
            }
            for scenario in ("all_cooperative", "mixed", "all_greedy")
        ]
    )

    output = capsys.readouterr().out
    assert "Pool over time (1 recorded ticks)" in output
    assert "tick 1" in output
    assert "tick 0" not in output


def test_paper_requires_the_package_python_version() -> None:
    project = tomllib.loads(Path("pyproject.toml").read_text())
    paper = Path("experiments/paper.md").read_text()

    assert project["project"]["requires-python"].startswith(">=3.12")
    assert "Requires: Python 3.12+" in paper
