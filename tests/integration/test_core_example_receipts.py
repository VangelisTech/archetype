# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Semantic receipts for the credential-free core examples."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from uuid import UUID

import pytest

from scripts.run_example_receipt import captured_receipt_or_run

_EXAMPLES = Path(__file__).resolve().parents[2] / "examples"


def _load_example(filename: str):
    module_name = f"core_example_{Path(filename).stem}"
    spec = importlib.util.spec_from_file_location(module_name, _EXAMPLES / filename)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


quickstart = _load_example("00_quickstart.py")
world_mutations = _load_example("01_world_mutations.py")
fork_counterfactual = _load_example("02_fork_counterfactual.py")
time_travel = _load_example("03_time_travel.py")
messaging = _load_example("04_messaging.py")
hooks = _load_example("07_hooks.py")


@pytest.mark.asyncio
async def test_quickstart_receipt_proves_three_processor_ticks(tmp_path: Path) -> None:
    result = await captured_receipt_or_run(
        quickstart.run_demo,
        str(tmp_path / "quickstart"),
    )

    assert result == {"value": 3}


@pytest.mark.asyncio
async def test_world_mutation_receipt_proves_each_public_mutation(tmp_path: Path) -> None:
    result = await captured_receipt_or_run(
        world_mutations.run_demo,
        str(tmp_path / "mutations"),
    )

    assert result == {
        "spawned_entities": 2,
        "component_mutations": {
            "updated_position": [2.0, 1.0],
            "velocity": [1.5, 0.5],
            "health_added": 80,
            "health_removed": True,
            "dummy_despawned": True,
        },
        "processor_mutation": {
            "moved_position": [3.5, 1.5],
            "removal_stopped_movement": True,
        },
        "fork": {
            "distinct_worlds": True,
            "inherited_source_entity": True,
            "branch_entity_isolated": True,
        },
        "trusted_audit_rows": 0,
    }


@pytest.mark.asyncio
async def test_world_mutation_narration_does_not_log_audit_payload(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    sensitive_audit_payload = "audit-secret-must-not-be-logged"

    async def fake_run_demo(_storage_uri: str) -> dict[str, object]:
        return {
            "spawned_entities": 2,
            "component_mutations": {},
            "processor_mutation": {},
            "fork": {},
            "trusted_audit_rows": sensitive_audit_payload,
        }

    monkeypatch.setattr(world_mutations, "run_demo", fake_run_demo)

    await world_mutations.main()

    captured = capsys.readouterr()
    assert sensitive_audit_payload not in captured.out
    assert sensitive_audit_payload not in captured.err
    assert "projected audit history queried for trusted runtime calls" in captured.out


@pytest.mark.asyncio
async def test_counterfactual_receipt_proves_identity_lineage_and_divergence(
    tmp_path: Path,
) -> None:
    result = await captured_receipt_or_run(
        fork_counterfactual.run_demo,
        str(tmp_path / "counterfactual"),
    )

    world_ids = result["world_ids"]
    run_ids = result["run_ids"]
    assert world_ids["prime"] != world_ids["fork"]
    assert all(UUID(value).version == 7 for value in world_ids.values())
    assert run_ids["prime_seed"] == run_ids["prime"]
    assert run_ids["prime"] != run_ids["fork"]
    assert all(UUID(value).version == 7 for value in run_ids.values())
    assert result["fork_tick"] == 13
    assert result["inherited_rows"] == 39

    deltas = result["checkpoint_deltas"]
    assert set(deltas) == set(fork_counterfactual.REGIMES)
    for name, rate in fork_counterfactual.REGIMES.items():
        base = 0.5
        for _ in range(fork_counterfactual.PRE_TICKS):
            base = rate * base * (1.0 - base)

        expected = []
        prime = base
        fork = base + fork_counterfactual.NUDGE
        for tick in range(fork_counterfactual.POST_TICKS + 1):
            if tick in fork_counterfactual.CHECKPOINTS:
                expected.append(abs(prime - fork))
            prime = rate * prime * (1.0 - prime)
            fork = rate * fork * (1.0 - fork)

        assert deltas[name] == pytest.approx(expected)


@pytest.mark.asyncio
async def test_time_travel_receipt_proves_cold_resume_continuity(tmp_path: Path) -> None:
    result = await captured_receipt_or_run(
        time_travel.run_demo,
        str(tmp_path / "time-travel"),
    )

    assert result["world_ids"]["source"] != result["world_ids"]["fork"]
    assert result["history"] == {
        "0": [0.0, 0.0, 0.0],
        "2": [2.0, 4.0, 6.0],
        "4": [4.0, 8.0, 12.0],
    }
    assert result["comparison"] == {
        "tick": 7,
        "source_walker": 7.0,
        "fork_walker": 24.0,
        "difference": 17.0,
    }
    assert result["inherited_tick_zero"] == 0.0
    assert result["cold_resume"] == {
        "discovered": True,
        "resume_tick": 8,
        "continued_tick": 9,
        "continued_walker": 8.0,
    }


@pytest.mark.asyncio
async def test_messaging_receipt_proves_messages_and_public_hook_order(tmp_path: Path) -> None:
    result = await captured_receipt_or_run(
        messaging.run_demo,
        str(tmp_path / "messaging"),
    )

    assert result == {
        "ticks_completed": 3,
        "agents": [
            {"name": "Alice", "mood": "happy", "energy": 130.0, "messages": 2},
            {"name": "Bob", "mood": "happy", "energy": 130.0, "messages": 2},
            {"name": "Charlie", "mood": "happy", "energy": 130.0, "messages": 2},
        ],
        "messages_delivered": 6,
        "messages_pending": 6,
        "hook_order": ["pre:0", "post:0", "pre:1", "post:1", "pre:2", "post:2"],
    }


@pytest.mark.asyncio
async def test_hook_receipt_proves_order_isolation_removal_and_state(tmp_path: Path) -> None:
    result = await captured_receipt_or_run(
        hooks.run_demo,
        str(tmp_path / "hooks"),
    )

    assert result == {
        "lifecycle_order": [
            "spawn:[Position, Velocity, Battery]",
            "spawn:[Position, Velocity, Battery]",
            "add_components:[Payload]",
            "remove_components:[Payload]",
            "despawn",
        ],
        "tick_metrics": [
            {"tick": 0, "active": 2, "low_battery": 0},
            {"tick": 1, "active": 2, "low_battery": 1},
            {"tick": 2, "active": 2, "low_battery": 1},
            {"tick": 3, "active": 1, "low_battery": 0},
        ],
        "durations_recorded": 4,
        "advisory_failure_attempts": 1,
        "temporary_hook_ticks": [0],
        "final_rovers": [{"position": [2.0, 0.5], "battery": 82.0}],
    }
