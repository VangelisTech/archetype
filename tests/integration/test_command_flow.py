# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Integration test: reserved spawn ID preservation through the command flow.

Test that submit_spawn reserves an ID, and drain_and_apply uses that exact ID.
Flow: submit_spawn -> durable scheduler -> dispatcher -> entity materialized with reserved ID
"""

import pytest
from uuid_utils import uuid7

from archetype.app.container import ServiceContainer
from archetype.app.gateway.auth import guard as guard_state
from archetype.app.gateway.auth.errors import GuardrailError
from archetype.app.gateway.auth.guard import reset_daily_tokens, reset_tick_counters
from archetype.app.gateway.auth.models import ActorCtx
from archetype.app.models import Command, CommandType
from archetype.core.component import Component
from archetype.core.config import RunConfig, StorageConfig, WorldConfig

_DEFERRED_COMMAND_TYPES = frozenset(
    {
        CommandType.SPAWN,
        CommandType.UPDATE,
        CommandType.DESPAWN,
        CommandType.ADD_COMPONENT,
        CommandType.REMOVE_COMPONENT,
        CommandType.ADD_PROCESSOR,
        CommandType.REMOVE_PROCESSOR,
        CommandType.MESSAGE,
        CommandType.CUSTOM,
        CommandType.QUERY_WORLD,
    }
)
_DIRECT_COMMAND_TYPES = tuple(
    sorted(set(CommandType) - _DEFERRED_COMMAND_TYPES, key=lambda command_type: command_type.value)
)


class CommandFlowMarker(Component):
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
        reserved_id = await c.command_gateway.submit_spawn(
            ctx, world.world_id, [CommandFlowMarker(tag="reserved")], tick=0
        )
        # Tick-boundary dispatch and manifest settlement are one application path.
        applied = await c.simulation_service.step(world.world_id, RunConfig())
        assert applied == 1
        # The entity should exist with the reserved ID
        assert reserved_id in world.entity2sig
        (record,) = await c.command_scheduler.records(world.world_id)
        assert record.status == "APPLIED"
    finally:
        await c.shutdown()


@pytest.mark.asyncio
async def test_replayed_reserved_spawn_is_not_applied_twice(tmp_path):
    """The drain path enforces the same double-spawn guard as direct mutation calls."""
    c = ServiceContainer()
    ctx = ActorCtx(id=uuid7(), roles={"admin"})
    try:
        world = await c.world_service.create_world(
            WorldConfig(name="spawn-replay"),
            StorageConfig(uri=str(tmp_path / "store")),
        )
        (entity_id,) = world.reserve_entity_ids(1)
        first = Command(
            type=CommandType.SPAWN,
            payload={
                "entity_id": entity_id,
                "components": [CommandFlowMarker(tag="first")],
            },
        )
        replay = Command(
            type=CommandType.SPAWN,
            payload={
                "entity_id": entity_id,
                "components": [CommandFlowMarker(tag="replay")],
            },
        )
        await c.command_gateway.submit_batch(ctx, world.world_id, [first, replay])

        applied = await c.simulation_service.step(world.world_id, RunConfig())

        assert applied == 1
        records = await c.command_scheduler.records(world.world_id)
        assert [record.status for record in records] == ["APPLIED", "REJECTED"]
        rows = (await world.get_components([CommandFlowMarker])).to_pylist()
        assert len(rows) == 1
        assert rows[0][f"{CommandFlowMarker.get_prefix()}tag"] == "first"
    finally:
        await c.shutdown()


@pytest.mark.asyncio
async def test_queued_update_is_applied_during_drain(tmp_path):
    c = ServiceContainer()
    ctx = ActorCtx(id=uuid7(), roles={"admin"})
    storage = StorageConfig(uri=str(tmp_path / "store"), namespace="updates")
    try:
        world = await c.world_service.create_world(WorldConfig(name="updates"), storage)
        entity_id = await world.create_entity([CommandFlowMarker(tag="before")])
        await c.simulation_service.step(world.world_id, RunConfig())

        await c.command_gateway.submit(
            ctx,
            world.world_id,
            Command(
                type=CommandType.UPDATE,
                payload={
                    "entity_id": entity_id,
                    "components": [CommandFlowMarker(tag="after").to_payload()],
                },
            ),
        )
        applied = await c.simulation_service.step(world.world_id, RunConfig())
        assert applied == 1
        rows = (await world.get_components([CommandFlowMarker])).to_pylist()
        assert rows[0][f"{CommandFlowMarker.get_prefix()}tag"] == "after"
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
            await c.command_gateway.submit(
                ctx, phantom, Command(type=CommandType.DESPAWN, payload={"entity_id": 1})
            )

        with pytest.raises(WorldNotFoundError):
            await c.command_gateway.submit_batch(
                ctx,
                phantom,
                [Command(type=CommandType.DESPAWN, payload={"entity_id": 1})],
            )

        # Reserved-id path already validates via get_world; tighten to the
        # same error type for consistency.
        with pytest.raises(WorldNotFoundError):
            await c.command_gateway.submit_spawn(ctx, phantom, [CommandFlowMarker(tag="x")])

        # No world means no durable catalog can receive an orphan command.
    finally:
        await c.shutdown()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "command_type",
    _DIRECT_COMMAND_TYPES,
)
async def test_direct_only_commands_cannot_enter_tick_deferred_scheduler(tmp_path, command_type):
    """Direct operations cannot be acknowledged as tick-deferred queue work."""
    c = ServiceContainer()
    ctx = ActorCtx(id=uuid7(), roles={"admin"})
    try:
        world = await c.world_service.create_world(
            WorldConfig(name="lifecycle-submit"),
            StorageConfig(uri=str(tmp_path / "store")),
        )

        with pytest.raises(ValueError, match="no tick-deferred dispatcher"):
            await c.command_gateway.submit(ctx, world.world_id, Command(type=command_type))

        assert await c.command_scheduler.pending_count(world.world_id) == 0
        assert await c.command_scheduler.history(world.world_id) == []
    finally:
        await c.shutdown()


@pytest.mark.asyncio
async def test_lifecycle_command_rejects_entire_submit_batch(tmp_path):
    """Batch validation happens before any command is gated, audited, or enqueued."""
    c = ServiceContainer()
    ctx = ActorCtx(id=uuid7(), roles={"admin"})
    try:
        world = await c.world_service.create_world(
            WorldConfig(name="lifecycle-batch"),
            StorageConfig(uri=str(tmp_path / "store")),
        )
        commands = [
            Command(type=CommandType.CUSTOM),
            Command(type=CommandType.FORK_WORLD),
        ]

        with pytest.raises(ValueError, match="no tick-deferred dispatcher"):
            await c.command_gateway.submit_batch(ctx, world.world_id, commands)

        assert await c.command_scheduler.pending_count(world.world_id) == 0
        assert await c.command_scheduler.history(world.world_id) == []
    finally:
        await c.shutdown()


@pytest.mark.asyncio
async def test_rejected_submit_batch_does_not_debit_quota(tmp_path):
    """All batch members pass authorization before any quota is committed."""
    c = ServiceContainer()
    ctx = ActorCtx(id=uuid7(), roles={"player"})
    try:
        world = await c.world_service.create_world(
            WorldConfig(name="batch-quota"),
            StorageConfig(uri=str(tmp_path / "store")),
        )
        commands = [
            Command(type=CommandType.CUSTOM),
            Command(type=CommandType.ADD_PROCESSOR),
        ]

        with pytest.raises(GuardrailError):
            await c.command_gateway.submit_batch(ctx, world.world_id, commands)

        assert guard_state._tick_counters.get(ctx.id, 0) == 0
        assert guard_state._daily_tokens.get(ctx.id, 0) == 0
        assert await c.command_scheduler.pending_count(world.world_id) == 0
        assert await c.command_scheduler.history(world.world_id) == []
    finally:
        await c.shutdown()


@pytest.mark.asyncio
async def test_run_result_run_id_round_trips_to_query(tmp_path):
    """spec: docs/guide/specification.md Inv O3 — Query defaults SHOULD use
    world's active run_id.

    RunResult.run_id MUST match the run_id stamped on persisted rows so
    callers can round-trip the value back into a query and find the data
    they just wrote. Previously RunResult returned RunConfig.run_id while
    AsyncWorld stamped its construction-time uuid; the two diverged and the
    round-trip lost data.
    """
    from uuid_utils import UUID, uuid7

    c = ServiceContainer()
    ctx = ActorCtx(id=uuid7(), roles={"admin"})
    storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
    try:
        info = await c.command_gateway.create_world(ctx, WorldConfig(name="r"), storage)
        await c.command_gateway.create_entity(ctx, info.world_id, [CommandFlowMarker(tag="x")])

        rc = RunConfig(run_id=str(uuid7()), num_steps=1)
        result = await c.command_gateway.run(ctx, info.world_id, rc)

        world = c.world_service.get_world(UUID(str(info.world_id)))
        assert str(result.run_id) == str(world.run_id)

        df = await c.command_gateway.query_components(
            ctx,
            [CommandFlowMarker],
            str(info.world_id),
            str(result.run_id),
            storage_config=storage,
        )
        assert df.count_rows() >= 1, (
            "RunResult.run_id did not round-trip back into a query; data was "
            "stamped with a different run_id than RunResult reported."
        )
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
            await c.command_gateway.submit(
                ctx, wid, Command(type=CommandType.DESPAWN, payload={"entity_id": 1})
            )
    finally:
        await c.shutdown()


@pytest.mark.asyncio
async def test_consecutive_runs_share_world_run_id(tmp_path):
    """spec: docs/guide/execution-hierarchy.md section 2 — vanilla pattern is
    repeated run calls on a single world; state accumulates with run_id
    stable across steps. World run_id stays stable across consecutive run
    calls so cross-run reads/writes remain continuous in append-only storage.
    """
    from uuid_utils import UUID, uuid7

    c = ServiceContainer()
    ctx = ActorCtx(id=uuid7(), roles={"admin"})
    storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
    try:
        info = await c.command_gateway.create_world(ctx, WorldConfig(name="r2"), storage)
        await c.command_gateway.create_entity(ctx, info.world_id, [CommandFlowMarker(tag="x")])

        result_a = await c.command_gateway.run(
            ctx, info.world_id, RunConfig(run_id=str(uuid7()), num_steps=1)
        )
        result_b = await c.command_gateway.run(
            ctx, info.world_id, RunConfig(run_id=str(uuid7()), num_steps=1)
        )

        assert str(result_a.run_id) == str(result_b.run_id), (
            "Consecutive runs reported different run_ids; the world's active "
            "run_id must stay stable across runs for append-only state continuity."
        )

        world = c.world_service.get_world(UUID(str(info.world_id)))
        df = await c.command_gateway.query_components(
            ctx,
            [CommandFlowMarker],
            str(info.world_id),
            str(world.run_id),
            storage_config=storage,
        )
        assert df.count_rows() >= 1
    finally:
        await c.shutdown()
