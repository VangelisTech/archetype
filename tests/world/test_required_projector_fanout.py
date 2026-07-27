# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Deterministic multi-family required projection."""

from __future__ import annotations

from dataclasses import dataclass

import pytest

from archetype.core.interfaces import CommittedTickReceipt
from archetype.world.projectors import RequiredProjectorFanout
from archetype.world.simulation import RequiredProjector


@dataclass
class _Binding:
    required_projector: RequiredProjector
    unsettled: bool = False

    async def has_unsettled_work(self, _world_id: str) -> bool:
        return self.unsettled


@pytest.mark.asyncio
async def test_mission_and_physical_projection_fan_out_in_stable_key_order() -> None:
    calls: list[str] = []

    def binding(name: str) -> _Binding:
        async def project(_receipt: CommittedTickReceipt) -> None:
            calls.append(name)

        return _Binding(RequiredProjector(consumer_name=name, project=project))

    fanout = RequiredProjectorFanout()
    mission = binding("missions.activities")
    physical = binding("physical-ai.hosted-episodes")
    await fanout.bind("world-1", physical)
    await fanout.bind("world-1", mission)

    projector = fanout.required_projector_for("world-1")
    assert projector is fanout.required_projector_for("world-1")
    await projector.project(
        CommittedTickReceipt(
            world_id="world-1",
            run_id="run-1",
            committed_tick=0,
            visibility_token="manifest-1",
            commands_applied=0,
        )
    )

    assert calls == ["missions.activities", "physical-ai.hosted-episodes"]
    physical.unsettled = True
    assert await fanout.has_unsettled("world-1")
    await fanout.unbind("world-1", physical)
    assert not await fanout.has_unsettled("world-1")
