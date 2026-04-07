# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Integration test: full command flow from submit to step to verify."""

import pytest
from uuid_utils import uuid7

from archetype.app.auth.guard import reset_daily_tokens, reset_tick_counters
from archetype.app.auth.models import ActorCtx
from archetype.app.container import ServiceContainer
from archetype.app.models import Command, CommandType
from archetype.core.component import Component
from archetype.core.config import StorageConfig, WorldConfig


class _SpawnAgent(Component):
    name: str = ""
    score: int = 0


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
        world = await container.world_service.create_world(WorldConfig(name="e2e"), storage)

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
        world = await container.world_service.create_world(WorldConfig(name="rbac_test"), storage)

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
        world = await container.world_service.create_world(WorldConfig(name="player_test"), storage)

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


@pytest.mark.asyncio
async def test_spawn_typed_component_preserves_archetype(tmp_path):
    """SPAWN command submitted via model_dump() payload should create an entity
    with the correct typed archetype, not a generic Component signature.

    Regression test for: SPAWN command loses component type info through CommandService.
    """
    container = ServiceContainer()
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        world = await container.world_service.create_world(WorldConfig(name="typed"), storage)

        ctx = ActorCtx(id=uuid7(), roles={"admin"})

        # Use model_dump() — the idiomatic path that previously lost type info.
        agent = _SpawnAgent(name="Alice", score=42)
        cmd = Command(
            type=CommandType.SPAWN,
            payload={"components": [agent.model_dump()]},
        )
        await container.command_service.submit(str(world.world_id), cmd, ctx)
        await container.simulation_service.step(world.world_id)

        # The entity must be registered under (_SpawnAgent,), not (Component,).
        world_instance = container.world_service.get_world(world.world_id)
        sigs = list(world_instance._live.keys())
        assert len(sigs) == 1, f"Expected 1 archetype signature, got {sigs}"
        sig = sigs[0]
        assert _SpawnAgent in sig, (
            f"_SpawnAgent missing from archetype signature {sig}; "
            "component type info was lost during CommandService hydration"
        )
    finally:
        await container.shutdown()


@pytest.mark.asyncio
async def test_spawn_component_model_dump_includes_type():
    """Component.model_dump() must include a 'type' key equal to the class name
    so that round-trip serialization through command payloads works without any
    manual annotation.
    """
    agent = _SpawnAgent(name="Bob", score=7)
    data = agent.model_dump()
    assert "type" in data, "model_dump() must include 'type' key"
    assert data["type"] == "_SpawnAgent"
    assert data["name"] == "Bob"
    assert data["score"] == 7


def test_spawn_to_row_dict_excludes_type():
    """to_row_dict() must NOT include a 'type' column — it would pollute the
    storage schema with a field that is not part of the component definition.
    """
    agent = _SpawnAgent(name="Carol", score=3)
    row = agent.to_row_dict()
    assert "_spawnagent__type" not in row
    assert "_spawnagent__name" in row
    assert "_spawnagent__score" in row


def test_get_type_by_name_recursive():
    """get_type_by_name must find components nested beyond the direct subclass
    level (i.e., grandchildren of Component).
    """

    class _GrandparentComponent(Component):
        x: int = 0

    class _ChildComponent(_GrandparentComponent):
        y: int = 0

    found = Component.get_type_by_name("_ChildComponent")
    assert found is _ChildComponent
