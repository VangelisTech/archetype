# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Integration test: reserved spawn ID preservation through the command flow.

Test that submit_spawn reserves an ID, and drain_and_apply uses that exact ID.
Flow: submit_spawn -> broker queue -> drain_and_apply -> entity materialized with reserved ID
"""

import pytest
from uuid_utils import uuid7

from archetype.app.auth.guard import reset_daily_tokens, reset_tick_counters
from archetype.app.auth.models import ActorCtx
from archetype.app.container import ServiceContainer
from archetype.core.component import Component
from archetype.core.config import RunConfig, StorageConfig, WorldConfig


class Marker(Component):
    tag: str = ""


@pytest.fixture(autouse=True)
def _reset_quotas():
    reset_tick_counters()
    reset_daily_tokens()
    yield
    reset_tick_counters()
    reset_daily_tokens()


@pytest.mark.asyncio
async def test_submit_spawn_reserved_id_survives_drain(tmp_path):
    c = ServiceContainer()
    ctx = ActorCtx(id=uuid7(), roles={"admin"})
    try:
        world = await c.world_service.create_world(
            WorldConfig(name="flow"), StorageConfig(uri=str(tmp_path / "store"))
        )
        # Reserve an entity ID via submit_spawn
        reserved_id = await c.command_service.submit_spawn(
            ctx, world.world_id, [Marker(tag="reserved")], tick=0
        )
        # Drain and apply
        applied = await c.command_service.drain_and_apply(world.world_id, 0)
        assert len(applied) == 1
        # The entity should exist with the reserved ID
        assert reserved_id in world.entity2sig
        # Step to materialize
        await c.simulation_service.step(world.world_id, RunConfig())
    finally:
        await c.shutdown()
