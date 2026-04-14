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
async def test_update_command_mutates_existing_component_values(tmp_path):
    """UPDATE must change component values even when the entity already has
    that component type (same archetype signature).

    Regression for the ``update-command-silently-noops`` bug: the UPDATE and
    ADD_COMPONENT dispatch arms in ``CommandService.apply`` both routed to
    ``AsyncWorld.add_components``, which early-returns when the signature is
    unchanged. The user's "update Position to (99, 99)" looked like it
    succeeded end-to-end — the command acked, audit history recorded it — but
    the entity values never moved.
    """
    from archetype.core.component import Component
    from archetype.core.config import RunConfig

    class UpdateRegressionPos(Component):
        x: int = 0
        y: int = 0

    container = ServiceContainer()
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        world = await container.world_service.create_world(
            WorldConfig(name="update-regression"), storage
        )

        eid = await world.create_entity([UpdateRegressionPos(x=1, y=2)])
        await world.run(RunConfig(num_steps=1))

        ctx = ActorCtx(id=uuid7(), roles={"admin"})
        cmd = Command(
            type=CommandType.UPDATE,
            tick=0,
            payload={
                "entity_id": eid,
                "components": [UpdateRegressionPos(x=99, y=99).to_payload()],
            },
        )
        await container.command_service.submit(str(world.world_id), cmd, ctx)
        await container.simulation_service.step(world.world_id)

        df = await world.get_components([UpdateRegressionPos], entity_ids=[eid])
        rows = df.collect().to_pylist()
        assert rows, "UPDATE produced no rows for entity"
        latest = max(rows, key=lambda r: r["tick"])
        assert latest["updateregressionpos__x"] == 99 and latest["updateregressionpos__y"] == 99, (
            f"UPDATE silently dropped — latest row is "
            f"({latest['updateregressionpos__x']}, {latest['updateregressionpos__y']}), "
            f"expected (99, 99)."
        )
    finally:
        await container.shutdown()


@pytest.mark.asyncio
async def test_update_command_widens_archetype_with_new_component(tmp_path):
    """UPDATE that introduces a brand-new component type should behave like
    ADD_COMPONENT — the entity moves to a wider archetype and picks up the
    new values."""
    from archetype.core.component import Component
    from archetype.core.config import RunConfig

    class UpdateWidenPos(Component):
        x: int = 0

    class UpdateWidenVel(Component):
        vx: int = 0

    container = ServiceContainer()
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        world = await container.world_service.create_world(
            WorldConfig(name="update-widen"), storage
        )

        eid = await world.create_entity([UpdateWidenPos(x=1)])
        await world.run(RunConfig(num_steps=1))

        ctx = ActorCtx(id=uuid7(), roles={"admin"})
        cmd = Command(
            type=CommandType.UPDATE,
            tick=0,
            payload={
                "entity_id": eid,
                "components": [UpdateWidenVel(vx=7).to_payload()],
            },
        )
        await container.command_service.submit(str(world.world_id), cmd, ctx)
        await container.simulation_service.step(world.world_id)

        df = await world.get_components([UpdateWidenPos, UpdateWidenVel], entity_ids=[eid])
        rows = df.collect().to_pylist()
        assert rows, "UPDATE-widen produced no rows in (UpdateWidenPos, UpdateWidenVel) archetype"
        latest = max(rows, key=lambda r: r["tick"])
        assert latest["updatewidenpos__x"] == 1
        assert latest["updatewidenvel__vx"] == 7
    finally:
        await container.shutdown()


@pytest.mark.asyncio
async def test_remove_component_resolves_string_type_names(tmp_path):
    """REMOVE_COMPONENT must resolve string type names to Component classes.

    Regression for the ``remove-component-strings-noop`` bug:
    ``CommandService.apply`` used to forward ``payload["component_types"]``
    straight to ``world.remove_components`` without resolving wire-format
    strings to ``Component`` subclasses. ``Archetype.remove_components``
    then computed a set difference between class objects and strings,
    producing ``new_sig == old_sig`` so the move was skipped — the entity
    silently kept the component. Every REST/CLI/MCP caller hit this since
    JSON cannot carry Python class objects.
    """
    from archetype.core.component import Component

    class _RemovePosition(Component):
        x: int = 0
        y: int = 0

    class _RemoveVelocity(Component):
        vx: int = 0
        vy: int = 0

    container = ServiceContainer()
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        world = await container.world_service.create_world(
            WorldConfig(name="remove-strings"), storage
        )

        ctx = ActorCtx(id=uuid7(), roles={"admin"})
        spawn_cmd = Command(
            type=CommandType.SPAWN,
            tick=0,
            payload={
                "components": [
                    _RemovePosition(x=1, y=2).to_payload(),
                    _RemoveVelocity(vx=3, vy=4).to_payload(),
                ],
            },
        )
        await container.command_service.submit(str(world.world_id), spawn_cmd, ctx)
        await container.simulation_service.step(world.world_id)

        assert frozenset({_RemovePosition, _RemoveVelocity}) in {
            frozenset(sig) for sig in world._live
        }
        entity_id = next(
            eid
            for eid, sig in world._entity2sig.items()
            if frozenset(sig) == frozenset({_RemovePosition, _RemoveVelocity})
        )

        remove_cmd = Command(
            type=CommandType.REMOVE_COMPONENT,
            tick=1,
            payload={
                "entity_id": entity_id,
                # Wire format: list of strings — what JSON callers send.
                "component_types": ["_RemoveVelocity"],
            },
        )
        await container.command_service.submit(str(world.world_id), remove_cmd, ctx)
        await container.simulation_service.step(world.world_id)
        await container.simulation_service.step(world.world_id)

        # Entity must now live in the (_RemovePosition,) archetype.
        new_sig = world._entity2sig[entity_id]
        assert frozenset(new_sig) == frozenset({_RemovePosition}), (
            f"REMOVE_COMPONENT silently no-opped on string types: entity still at {new_sig}"
        )
    finally:
        await container.shutdown()


@pytest.mark.asyncio
async def test_remove_component_unknown_string_type_raises(tmp_path):
    """Unknown component type names must raise — not silently no-op."""
    from archetype.core.component import Component

    class Tag(Component):
        label: str = ""

    container = ServiceContainer()
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        world = await container.world_service.create_world(
            WorldConfig(name="remove-unknown"), storage
        )

        ctx = ActorCtx(id=uuid7(), roles={"admin"})
        spawn_cmd = Command(
            type=CommandType.SPAWN,
            tick=0,
            payload={"components": [Tag(label="x").to_payload()]},
        )
        await container.command_service.submit(str(world.world_id), spawn_cmd, ctx)
        await container.simulation_service.step(world.world_id)

        entity_id = next(iter(world._entity2sig))
        # Ask the service to apply directly so the ValueError surfaces (drain_and_apply
        # swallows per-command exceptions to keep the tick alive).
        bad_cmd = Command(
            type=CommandType.REMOVE_COMPONENT,
            tick=1,
            payload={
                "entity_id": entity_id,
                "component_types": ["NoSuchComponent"],
            },
        )
        async_world = container.world_service.get_world(world.world_id)
        with pytest.raises(ValueError, match="NoSuchComponent"):
            await container.command_service.apply(async_world, bad_cmd)
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
