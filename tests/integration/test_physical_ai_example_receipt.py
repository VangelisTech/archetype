# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Semantic receipt for the credential-free physical-AI example."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType

import pytest

from scripts.run_example_receipt import captured_receipt_or_run

_EXAMPLE = Path(__file__).resolve().parents[2] / "examples" / "14_physical_ai.py"


def _load_example() -> ModuleType:
    module_name = "physical_ai_example_receipt"
    spec = importlib.util.spec_from_file_location(module_name, _EXAMPLE)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


physical_ai = _load_example()


@pytest.mark.asyncio
async def test_physical_ai_receipt_proves_eval_sweep_optimizer_and_telemetry(
    tmp_path: Path,
) -> None:
    result = await captured_receipt_or_run(
        physical_ai.run_demo,
        str(tmp_path / "physical-ai"),
    )

    assert result == {
        "task_evaluation": {
            "suite": "scripted-reach",
            "task_id": 0,
            "instruction": "reach",
            "trial_count": 2,
            "success_count": 1,
            "success_rate": 0.5,
            "mean_length": 3.5,
            "evidence_pair_verified": True,
            "trials": [
                {
                    "trial_idx": 0,
                    "env_key": 0,
                    "seed": 0,
                    "success": True,
                    "episode_length": 3,
                },
                {
                    "trial_idx": 1,
                    "env_key": 1,
                    "seed": 1,
                    "success": False,
                    "episode_length": 4,
                },
            ],
        },
        "telemetry": {
            "entity_count": 2,
            "row_count": 10,
            "coordinate_pair_count": 1,
            "ticks": [0, 1, 2, 3, 4],
            "all_frame_refs_present": True,
            "reset_refs": [
                {
                    "env_key": 0,
                    "agentview": "test-session/0/reset-agentview.png",
                    "wrist": "test-session/0/reset-wrist.png",
                },
                {
                    "env_key": 1,
                    "agentview": "test-session/1/reset-agentview.png",
                    "wrist": "test-session/1/reset-wrist.png",
                },
            ],
        },
        "instruction_sweep": {
            "suite": "scripted-reach",
            "task_id": 0,
            "seeds_per_variant": 2,
            "scores": {"": 0.0, "reach": 0.5, "reach red": 1.0},
            "best_instruction": "reach red",
            "outcomes": [
                {
                    "instruction": "",
                    "n_trials": 2,
                    "n_success": 0,
                    "success_rate": 0.0,
                    "mean_length": 4.0,
                },
                {
                    "instruction": "reach",
                    "n_trials": 2,
                    "n_success": 1,
                    "success_rate": 0.5,
                    "mean_length": 3.0,
                },
                {
                    "instruction": "reach red",
                    "n_trials": 2,
                    "n_success": 2,
                    "success_rate": 1.0,
                    "mean_length": 2.5,
                },
            ],
        },
        "optimization": {
            "initial_success_rate": 0.0,
            "best_instruction": "red reach",
            "best_success_rate": 1.0,
            "trace": [0.0, 0.5, 1.0],
            "improved": True,
        },
        "cleanup": {
            "runtime_context_completed": True,
            "unique_provider_count": 10,
            "environment_count": 5,
            "policy_count": 5,
            "closed_once_count": 10,
            "open_provider_count": 0,
        },
    }
