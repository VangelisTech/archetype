# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Credential-free source/wheel proof for durable evaluation receipts."""

from __future__ import annotations

import pytest

from archetype import ArchetypeRuntime
from archetype.core.component import Component
from archetype.core.config import StorageBackend, StorageConfig
from archetype.evaluation.contracts import GraderContract, Outcome
from archetype.runtime_resources import RuntimeCloseState

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.contract("evaluation.result.snapshot_pinned"),
    pytest.mark.integration,
]


class OperationalMetric(Component):
    value: float = 0.0


async def test_durable_receipt_replays_once_across_a_cold_explicit_handle(
    tmp_path,
) -> None:
    storage = StorageConfig(
        uri=str(tmp_path / "evaluation-store"),
        namespace="evaluation_operational",
        backend=StorageBackend.ICEBERG,
    )
    contract = GraderContract(
        grader_id="operational-metric",
        implementation_version="v1",
        thresholds={"minimum": 0.5},
    )
    calls: list[float] = []
    first_resources = None

    async with ArchetypeRuntime() as runtime:
        first_resources = runtime._resources
        world = runtime.world("evaluation-operational", storage=storage)
        await world.spawn(OperationalMetric(value=0.75))
        await world.step()

        def grader(frame) -> Outcome:
            rows = frame.to_pylist()
            score = float(rows[-1]["operationalmetric__value"])
            calls.append(score)
            return Outcome(status="pass", score=score, evidence={"rows": len(rows)})

        first = await world.evaluate(
            OperationalMetric,
            contract=contract,
            grader=grader,
            evaluation_id="durable-operational-receipt",
        )
        replay = await world.evaluate(
            OperationalMetric,
            contract=contract,
            grader=grader,
            evaluation_id="durable-operational-receipt",
        )
        world_id = world.world_id

        assert replay == first
        assert calls == [0.75]
        assert first.subject_digest
        assert first.contract_digest == contract.digest()
        assert first.outcome == "pass"

    assert first_resources is not None
    assert first_resources.close_state is RuntimeCloseState.CLOSED

    cold_resources = None
    async with ArchetypeRuntime() as runtime:
        cold_resources = runtime._resources
        cold = runtime.attach(world_id, storage=storage)

        def must_not_regrade(_frame) -> Outcome:
            raise AssertionError("cold replay paid the grader twice")

        cold_replay = await cold.evaluate(
            OperationalMetric,
            contract=contract,
            grader=must_not_regrade,
            evaluation_id="durable-operational-receipt",
        )
        assert cold_replay == first

    assert cold_resources is not None
    assert cold_resources.close_state is RuntimeCloseState.CLOSED
