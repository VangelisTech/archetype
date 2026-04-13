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
async def test_submit_spawn_returns_reserved_entity_id_and_materializes_it(tmp_path):
    """submit_spawn reserves an entity ID that survives broker drain/apply."""
    from archetype.core.component import Component

    class Pos(Component):
        x: int = 0

    container = ServiceContainer()
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        world = await container.world_service.create_world(
            WorldConfig(name="reserved-spawn"), storage
        )

        ctx = ActorCtx(id=uuid7(), roles={"admin"})
        entity_id = await container.command_service.submit_spawn(
            world.world_id,
            [Pos(x=7)],
            ctx,
        )

        assert entity_id == 1
        assert world._next_entity_id == 2

        await container.simulation_service.step(world.world_id)

        assert entity_id in world._entity2sig
        df = await world.get_components([Pos], entity_ids=[entity_id])
        rows = df.collect().to_pylist()
        assert len(rows) == 1
        assert rows[0]["entity_id"] == entity_id
        assert rows[0]["pos__x"] == 7
    finally:
        await container.shutdown()


@pytest.mark.asyncio
async def test_spawn_preserves_typed_components_through_command_service(tmp_path):
    """SPAWN with ``to_payload()`` dicts must land in the typed archetype, not ``(Component,)``.

    Regression for #90: ``CommandService._hydrate_components`` used to call
    ``Component.from_dict`` on dicts that lacked a ``"type"`` key, silently
    falling through to a base ``Component`` instance. Entities ended up in
    archetype ``(Component,)`` and processors declaring ``components = (Agent,)``
    never matched them.
    """
    from archetype.core.component import Component

    class Pose(Component):
        x: float = 0.0
        y: float = 0.0

    class Tag(Component):
        label: str = ""

    container = ServiceContainer()
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        world = await container.world_service.create_world(WorldConfig(name="typed-spawn"), storage)

        ctx = ActorCtx(id=uuid7(), roles={"admin"})
        cmd = Command(
            type=CommandType.SPAWN,
            tick=0,
            payload={
                "components": [
                    Pose(x=1.0, y=2.0).to_payload(),
                    Tag(label="hero").to_payload(),
                ],
            },
        )
        await container.command_service.submit(str(world.world_id), cmd, ctx)
        await container.simulation_service.step(world.world_id)

        # Entity should live in the (Pose, Tag) archetype — not (Component,).
        signatures = {frozenset(sig) for sig in world._live}
        assert frozenset({Pose, Tag}) in signatures, (
            f"SPAWN lost component type info; world archetypes = {signatures}"
        )
    finally:
        await container.shutdown()


@pytest.mark.asyncio
async def test_spawn_with_component_instances_passes_through(tmp_path):
    """Submitting raw Component instances in the payload should also work (pre-serialized)."""
    from archetype.core.component import Component

    class Foo(Component):
        value: int = 0

    container = ServiceContainer()
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        world = await container.world_service.create_world(WorldConfig(name="inst-spawn"), storage)

        ctx = ActorCtx(id=uuid7(), roles={"admin"})
        cmd = Command(
            type=CommandType.SPAWN,
            tick=0,
            payload={"components": [Foo(value=42)]},
        )
        await container.command_service.submit(str(world.world_id), cmd, ctx)
        await container.simulation_service.step(world.world_id)

        assert frozenset({Foo}) in {frozenset(sig) for sig in world._live}
    finally:
        await container.shutdown()


@pytest.mark.asyncio
async def test_spawn_with_bare_model_dump_raises(tmp_path):
    """Dicts without a 'type' key must raise loudly instead of silently losing type info.

    Regression guard for #90: the old behavior silently created base
    ``Component`` instances, which was the original footgun.
    """
    from archetype.core.component import Component

    class Bar(Component):
        count: int = 0

    container = ServiceContainer()
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        world = await container.world_service.create_world(WorldConfig(name="bare-spawn"), storage)

        ctx = ActorCtx(id=uuid7(), roles={"admin"})
        # Uses bare model_dump() — missing the "type" discriminator.
        cmd = Command(
            type=CommandType.SPAWN,
            tick=0,
            payload={"components": [Bar(count=1).model_dump()]},
        )
        await container.command_service.submit(str(world.world_id), cmd, ctx)
        # drain_and_apply catches per-command exceptions and logs them, so we
        # assert that the command was dequeued but no entity materialized in
        # any typed archetype.
        await container.simulation_service.step(world.world_id)

        signatures = {frozenset(sig) for sig in world._live}
        assert frozenset({Bar}) not in signatures
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
