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


@pytest.mark.asyncio
async def test_submit_to_unknown_world_rejected():
    """spec: docs/guide/specification.md 'Required Hardening Work' item 3.

    submit() and submit_batch() must reject commands targeted at a world_id
    that was never created. The previous behavior silently queued an orphan
    command, debited quota, and emitted an audit row, with no way for the
    caller to learn the command would never run.
    """
    from archetype.app.errors import WorldNotFoundError
    from archetype.app.models import Command, CommandType

    c = ServiceContainer()
    ctx = ActorCtx(id=uuid7(), roles={"admin"})
    try:
        phantom = uuid7()

        with pytest.raises(WorldNotFoundError):
            await c.command_service.submit(
                ctx, phantom, Command(type=CommandType.DESPAWN, payload={"entity_id": 1})
            )

        with pytest.raises(WorldNotFoundError):
            await c.command_service.submit_batch(
                ctx,
                phantom,
                [Command(type=CommandType.DESPAWN, payload={"entity_id": 1})],
            )

        # Reserved-id path already validates via get_world; tighten to the
        # same error type for consistency.
        with pytest.raises(WorldNotFoundError):
            await c.command_service.submit_spawn(ctx, phantom, [Marker(tag="x")])

        # Broker holds no orphan queue for the phantom world.
        assert await c.broker.get_pending_count(phantom) == 0
    finally:
        await c.shutdown()


@pytest.mark.asyncio
async def test_submit_to_destroyed_world_rejected(tmp_path):
    """A destroyed world_id is no longer a valid submit target."""
    from archetype.app.errors import WorldNotFoundError
    from archetype.app.models import Command, CommandType

    c = ServiceContainer()
    ctx = ActorCtx(id=uuid7(), roles={"admin"})
    try:
        world = await c.world_service.create_world(
            WorldConfig(name="ephemeral"),
            StorageConfig(uri=str(tmp_path / "store")),
        )
        wid = world.world_id
        await c.world_service.destroy_world(wid)

        with pytest.raises(WorldNotFoundError):
            await c.command_service.submit(
                ctx, wid, Command(type=CommandType.DESPAWN, payload={"entity_id": 1})
            )
    finally:
        await c.shutdown()
