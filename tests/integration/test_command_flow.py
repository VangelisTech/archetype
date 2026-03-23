# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Integration test: full command flow from submit to step to verify."""

import pytest
from uuid_utils import uuid7

from archetype.app.auth.guard import reset_daily_tokens, reset_tick_counters
from archetype.app.auth.models import ActorCtx
from archetype.app.container import ServiceContainer
from archetype.app.models import Command, CommandType
from archetype.core.config import StorageConfig, WorldConfig


@pytest.fixture(autouse=True)
def _reset_quotas():
    reset_tick_counters()
    reset_daily_tokens()
    yield
    reset_tick_counters()
    reset_daily_tokens()


@pytest.mark.asyncio
async def test_submit_spawn_step_verify(tmp_path):
    """Full e2e: create world → submit SPAWN → step → verify command was applied."""
    container = ServiceContainer()
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        world = await container.world_service.create_world(
            WorldConfig(name="e2e"), storage
        )

        ctx = ActorCtx(id=uuid7(), roles={"admin"})
        cmd = Command(
            type=CommandType.SPAWN,
            tick=0,
            payload={"components": []},
        )
        await container.command_service.submit(str(world.world_id), cmd, ctx)

        # Verify command is pending
        pending = await container.broker.get_pending_count(str(world.world_id))
        assert pending == 1

        # Step the simulation — should drain and apply the command
        cmds_applied = await container.simulation_service.step(world.world_id)
        assert cmds_applied == 1

        # Command should no longer be pending
        pending = await container.broker.get_pending_count(str(world.world_id))
        assert pending == 0

        # History should show the command
        history = await container.query_service.get_command_history(world.world_id)
        assert len(history) == 1
        assert history[0].type == CommandType.SPAWN
    finally:
        await container.shutdown()


@pytest.mark.asyncio
async def test_rbac_denies_viewer_spawn(tmp_path):
    """A viewer should not be able to submit a SPAWN command."""
    container = ServiceContainer()
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        world = await container.world_service.create_world(
            WorldConfig(name="rbac_test"), storage
        )

        viewer_ctx = ActorCtx(id=uuid7(), roles={"viewer"})
        cmd = Command(type=CommandType.SPAWN, payload={})

        with pytest.raises(PermissionError):
            await container.command_service.submit(str(world.world_id), cmd, viewer_ctx)
    finally:
        await container.shutdown()


@pytest.mark.asyncio
async def test_player_can_spawn_but_not_add_processor(tmp_path):
    """Player role can spawn entities but cannot add processors."""
    container = ServiceContainer()
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        world = await container.world_service.create_world(
            WorldConfig(name="player_test"), storage
        )

        player_ctx = ActorCtx(id=uuid7(), roles={"player"})

        # Player CAN spawn
        spawn_cmd = Command(type=CommandType.SPAWN, payload={"components": []})
        await container.command_service.submit(str(world.world_id), spawn_cmd, player_ctx)

        # Player CANNOT add processor
        proc_cmd = Command(type=CommandType.ADD_PROCESSOR, payload={})
        with pytest.raises(PermissionError):
            await container.command_service.submit(str(world.world_id), proc_cmd, player_ctx)
    finally:
        await container.shutdown()
