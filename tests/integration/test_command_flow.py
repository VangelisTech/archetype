# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Integration test: reserved spawn ID preservation through the command flow.

Test that submit_spawn reserves an ID and scheduler materialization uses that exact ID.
Flow: submit_spawn -> durable scheduler -> dispatcher -> entity materialized with reserved ID
"""

import asyncio

import pytest
from uuid_utils import uuid7

from archetype.app.container import ServiceContainer
from archetype.app.gateway.auth.models import ActorCtx
from archetype.app.models import Command, CommandType
from archetype.core.component import Component
from archetype.core.config import RunConfig, StorageConfig, WorldConfig
from archetype.world.errors import WorldClosingError

_DEFERRED_COMMAND_TYPES = frozenset(
    {
        CommandType.SPAWN,
        CommandType.UPDATE,
        CommandType.DESPAWN,
        CommandType.ADD_COMPONENT,
        CommandType.REMOVE_COMPONENT,
    }
)
_DIRECT_COMMAND_TYPES = tuple(
    sorted(set(CommandType) - _DEFERRED_COMMAND_TYPES, key=lambda command_type: command_type.value)
)


class CommandFlowMarker(Component):
    tag: str = ""


@pytest.mark.asyncio
async def test_submit_spawn_reserved_id_survives_drain(tmp_path):
    c = ServiceContainer()
    ctx = ActorCtx(id=uuid7(), roles={"admin"})
    try:
        world = await c.world_lifecycle.create_world(
            WorldConfig(name="flow"), StorageConfig(uri=str(tmp_path / "store"))
        )
        # Reserve an entity ID via submit_spawn
        reserved_id = await c.command_gateway.submit_spawn(
            ctx, world.world_id, [CommandFlowMarker(tag="reserved")], tick=0
        )
        # Tick-boundary dispatch and manifest settlement are one application path.
        applied = await c.application.step(world.world_id, RunConfig())
        assert applied == 1
        # The due command is staged before the world snapshots active
        # signatures, so a brand-new signature is persisted in this same tick.
        assert reserved_id in world.entity2sig
        rows = (await world.get_components([CommandFlowMarker])).to_pylist()
        assert [(row["entity_id"], row["tick"]) for row in rows] == [(reserved_id, 0)]
        (record,) = await c.command_scheduler.records(world.world_id)
        assert record.status == "APPLIED"
    finally:
        await c.shutdown()


@pytest.mark.asyncio
async def test_command_materializer_infrastructure_failure_fails_tick_before_settlement(
    tmp_path, monkeypatch
):
    c = ServiceContainer()
    ctx = ActorCtx(id=uuid7(), roles={"admin"})
    try:
        world = await c.world_lifecycle.create_world(
            WorldConfig(name="materializer-failure"),
            StorageConfig(uri=str(tmp_path / "store")),
        )
        await c.command_gateway.submit_spawn(
            ctx,
            world.world_id,
            [CommandFlowMarker(tag="retry")],
            tick=0,
        )
        real_materialize = world._materialize_commands

        async def unavailable_materializer(world, tick):
            raise RuntimeError("command materializer unavailable")

        monkeypatch.setattr(world, "_materialize_commands", unavailable_materializer)
        with pytest.raises(RuntimeError, match="command materializer unavailable"):
            await c.application.step(world.world_id, RunConfig())

        assert world.tick == 0
        (pending,) = await c.command_scheduler.records(world.world_id)
        assert pending.status == "PENDING"
        assert world.entity2sig == {}

        monkeypatch.setattr(world, "_materialize_commands", real_materialize)
        assert await c.application.step(world.world_id, RunConfig()) == 1
        (applied,) = await c.command_scheduler.records(world.world_id)
        assert applied.status == "APPLIED"
        assert applied.applied_tick == 0
    finally:
        await c.shutdown()


@pytest.mark.asyncio
async def test_replayed_reserved_spawn_is_not_applied_twice(tmp_path):
    """The drain path enforces the same double-spawn guard as direct mutation calls."""
    c = ServiceContainer()
    try:
        world = await c.world_lifecycle.create_world(
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
        await c.application.submit_batch(world.world_id, [first, replay])

        applied = await c.application.step(world.world_id, RunConfig())

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
        world = await c.world_lifecycle.create_world(WorldConfig(name="updates"), storage)
        entity_id = await world.create_entity([CommandFlowMarker(tag="before")])
        await c.application.step(world.world_id, RunConfig())

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
        applied = await c.application.step(world.world_id, RunConfig())
        assert applied == 1
        rows = (await world.get_components([CommandFlowMarker])).to_pylist()
        assert rows[0][f"{CommandFlowMarker.get_prefix()}tag"] == "after"
    finally:
        await c.shutdown()


@pytest.mark.asyncio
async def test_submit_to_unknown_world_rejected():
    """spec: docs/guide/specification.md 'Required Hardening Work' item 3.

    submit() and submit_batch() must reject commands targeted at a world_id
    that was never created. Authorized dispatch may consume its instance-owned
    quota coordinate and emit bounded failed evidence, but it must not queue an
    orphan durable row and the caller must receive the canonical error.
    """
    from archetype.app.models import Command, CommandType
    from archetype.errors import WorldNotFoundError

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
        world = await c.world_lifecycle.create_world(
            WorldConfig(name="lifecycle-submit"),
            StorageConfig(uri=str(tmp_path / "store")),
        )

        with pytest.raises(
            ValueError,
            match="direct-only or unsupported|portable deferred admission",
        ):
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
        world = await c.world_lifecycle.create_world(
            WorldConfig(name="lifecycle-batch"),
            StorageConfig(uri=str(tmp_path / "store")),
        )
        commands = [
            Command(type=CommandType.CUSTOM),
            Command(type=CommandType.FORK_WORLD),
        ]

        with pytest.raises(
            ValueError,
            match="direct-only or unsupported|portable deferred admission",
        ):
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
        world = await c.world_lifecycle.create_world(
            WorldConfig(name="batch-quota"),
            StorageConfig(uri=str(tmp_path / "store")),
        )
        commands = [
            Command(type=CommandType.DESPAWN, payload={"entity_id": 1}),
            Command(
                type=CommandType.ADD_COMPONENT,
                payload={
                    "entity_id": 1,
                    "components": [CommandFlowMarker(tag="denied")],
                },
            ),
        ]

        with pytest.raises(PermissionError):
            await c.command_gateway.submit_batch(ctx, world.world_id, commands)

        assert c.policy._tick_debits == {}
        assert c.policy._daily_token_debits == {}
        assert await c.command_scheduler.pending_count(world.world_id) == 0
        assert await c.command_scheduler.history(world.world_id) == []
    finally:
        await c.shutdown()


@pytest.mark.asyncio
async def test_admission_racing_destroy_is_cancelled_without_orphaning(tmp_path, monkeypatch):
    """An admission that wins the world lock is visible to destroy cancellation."""
    c = ServiceContainer()
    ctx = ActorCtx(id=uuid7(), roles={"admin"})
    storage = StorageConfig(uri=str(tmp_path / "store"), namespace="admit-destroy-race")
    entered = asyncio.Event()
    release = asyncio.Event()
    try:
        world = await c.world_lifecycle.create_world(
            WorldConfig(name="admit-destroy-race"),
            storage,
        )
        catalog = c.storage_service.get_control_catalog(storage)
        admit_commands = catalog.admit_commands

        async def blocked_admit(world_id, admissions):
            entered.set()
            await release.wait()
            return await admit_commands(world_id, admissions)

        monkeypatch.setattr(catalog, "admit_commands", blocked_admit)
        submit = asyncio.create_task(
            c.command_gateway.submit(
                ctx,
                world.world_id,
                Command(
                    type=CommandType.DESPAWN,
                    tick=10_000,
                    payload={"entity_id": 9_999},
                ),
            )
        )
        await entered.wait()

        destroy = asyncio.create_task(c.application.destroy_world(world.world_id))
        await asyncio.sleep(0)
        assert not destroy.done()

        release.set()
        command_id = await submit
        await destroy

        (record,) = await c.command_scheduler.records(world.world_id)
        assert str(command_id) == record.command_id
        assert record.status == "REJECTED"
        assert not await c.world_registry.contains(world.world_id)

        with pytest.raises((WorldClosingError, LookupError)):
            await c.command_gateway.submit(
                ctx,
                world.world_id,
                Command(type=CommandType.DESPAWN, payload={"entity_id": 1}),
            )
    finally:
        release.set()
        await c.shutdown()


@pytest.mark.asyncio
async def test_run_result_run_id_round_trips_to_query(tmp_path):
    """spec: docs/guide/specification.md Inv O3 — Query defaults SHOULD use
    world's active run_id.

    RunResult.run_id MUST match the run_id stamped on persisted rows so
    callers can round-trip the value back into a query and find the data
    they just wrote. Run configuration cannot select identity; the immutable
    world UUID is the single value used for both persistence and the result.
    """
    from uuid_utils import uuid7

    c = ServiceContainer()
    ctx = ActorCtx(id=uuid7(), roles={"admin"})
    storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
    try:
        info = await c.command_gateway.create_world(ctx, WorldConfig(name="r"), storage)
        await c.command_gateway.create_entity(ctx, info.world_id, [CommandFlowMarker(tag="x")])

        rc = RunConfig(num_steps=1)
        result = await c.command_gateway.run(ctx, info.world_id, rc)

        world = await c.world_registry.live_world(str(info.world_id))
        assert world is not None
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
    from archetype.app.models import Command, CommandType
    from archetype.errors import WorldNotFoundError

    c = ServiceContainer()
    ctx = ActorCtx(id=uuid7(), roles={"admin"})
    try:
        world = await c.world_lifecycle.create_world(
            WorldConfig(name="ephemeral"),
            StorageConfig(uri=str(tmp_path / "store")),
        )
        wid = world.world_id
        await c.world_lifecycle.destroy_world(wid)

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
    from uuid_utils import uuid7

    c = ServiceContainer()
    ctx = ActorCtx(id=uuid7(), roles={"admin"})
    storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
    try:
        info = await c.command_gateway.create_world(ctx, WorldConfig(name="r2"), storage)
        await c.command_gateway.create_entity(ctx, info.world_id, [CommandFlowMarker(tag="x")])

        result_a = await c.command_gateway.run(ctx, info.world_id, RunConfig(num_steps=1))
        result_b = await c.command_gateway.run(ctx, info.world_id, RunConfig(num_steps=1))

        assert str(result_a.run_id) == str(result_b.run_id), (
            "Consecutive runs reported different run_ids; the world's active "
            "run_id must stay stable across runs for append-only state continuity."
        )

        world = await c.world_registry.live_world(str(info.world_id))
        assert world is not None
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
