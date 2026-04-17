# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Regression tests for drain_and_apply command failure handling.

Verifies that failed commands are excluded from the return value and
properly cleaned up from the broker's pending state, rather than
silently reported as applied.
"""

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
async def test_drain_and_apply_excludes_failed_commands(tmp_path):
    """drain_and_apply must return only commands that were successfully applied.

    A malformed SPAWN (missing type discriminator) should fail during apply
    but a valid SPAWN submitted alongside it should still succeed. The return
    value must contain only the successful command.
    """
    from archetype.core.component import Component

    class GoodComp(Component):
        val: int = 0

    container = ServiceContainer()
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        world = await container.world_service.create_world(WorldConfig(name="drain-fail"), storage)
        ctx = ActorCtx(id=uuid7(), roles={"admin"})

        good_cmd = Command(
            type=CommandType.SPAWN,
            tick=0,
            payload={"components": [GoodComp(val=42).to_payload()]},
        )
        bad_cmd = Command(
            type=CommandType.SPAWN,
            tick=0,
            payload={"components": [{"val": 1}]},
        )
        await container.command_service.submit(str(world.world_id), good_cmd, ctx)
        await container.command_service.submit(str(world.world_id), bad_cmd, ctx)

        applied = await container.command_service.drain_and_apply(str(world.world_id), tick=0)

        assert len(applied) == 1
        assert applied[0].id == good_cmd.id
    finally:
        await container.shutdown()


@pytest.mark.asyncio
async def test_step_count_excludes_failed_commands(tmp_path):
    """SimulationService.step() must report only successfully applied commands.

    When a batch contains one valid and one invalid command,
    the returned count must reflect only the successful application.
    """
    from archetype.core.component import Component

    class StepComp(Component):
        tag: str = ""

    container = ServiceContainer()
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        world = await container.world_service.create_world(WorldConfig(name="step-count"), storage)
        ctx = ActorCtx(id=uuid7(), roles={"admin"})

        good_cmd = Command(
            type=CommandType.SPAWN,
            tick=0,
            payload={"components": [StepComp(tag="ok").to_payload()]},
        )
        bad_cmd = Command(
            type=CommandType.SPAWN,
            tick=0,
            payload={"components": [{"tag": "bad"}]},
        )
        await container.command_service.submit(str(world.world_id), good_cmd, ctx)
        await container.command_service.submit(str(world.world_id), bad_cmd, ctx)

        cmds_applied = await container.simulation_service.step(world.world_id)

        assert cmds_applied == 1
    finally:
        await container.shutdown()


@pytest.mark.asyncio
async def test_drain_and_apply_clears_failed_commands_from_pending(tmp_path):
    """Failed commands must not leak in the broker's pending dict.

    After drain_and_apply, both successful and failed commands should be
    removed from _pending so they don't accumulate across ticks.
    """
    from archetype.core.component import Component

    class PendComp(Component):
        x: int = 0

    container = ServiceContainer()
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        world = await container.world_service.create_world(
            WorldConfig(name="pending-cleanup"), storage
        )
        ctx = ActorCtx(id=uuid7(), roles={"admin"})

        bad_cmd = Command(
            type=CommandType.SPAWN,
            tick=0,
            payload={"components": [{"x": 1}]},
        )
        await container.command_service.submit(str(world.world_id), bad_cmd, ctx)

        global_pending_before = await container.broker.get_pending_count()
        assert global_pending_before >= 1

        await container.command_service.drain_and_apply(str(world.world_id), tick=0)

        global_pending_after = await container.broker.get_pending_count()
        assert global_pending_after == 0
    finally:
        await container.shutdown()


@pytest.mark.asyncio
async def test_dequeue_due_preserves_pending_until_ack(tmp_path):
    """dequeue_due must leave commands in _pending until ack() is called.

    This is the broker-level invariant: a dequeued command is "in-flight"
    and visible in _pending until the caller explicitly acknowledges it.
    """
    from archetype.app.broker import CommandBroker
    from archetype.app.models import Command, CommandType

    broker = CommandBroker()
    world_id = "test-world"

    cmd = Command(type=CommandType.SPAWN, tick=0, payload={})
    await broker.enqueue(world_id, cmd, None)

    assert await broker.get_pending_count() == 1

    dequeued = await broker.dequeue_due(world_id, tick=0)
    assert len(dequeued) == 1

    pending_after_dequeue = await broker.get_pending_count()
    assert pending_after_dequeue == 1, (
        "dequeue_due must not remove from _pending; ack() handles that"
    )

    await broker.ack([cmd.id])
    pending_after_ack = await broker.get_pending_count()
    assert pending_after_ack == 0


@pytest.mark.asyncio
async def test_run_result_commands_applied_excludes_failures(tmp_path):
    """RunResult.commands_applied must reflect only successful commands.

    When commands fail during apply, the run result should not count them.
    """
    from archetype.core.component import Component
    from archetype.core.config import RunConfig

    class RunComp(Component):
        v: int = 0

    container = ServiceContainer()
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        world = await container.world_service.create_world(WorldConfig(name="run-result"), storage)
        ctx = ActorCtx(id=uuid7(), roles={"admin"})

        good_cmd = Command(
            type=CommandType.SPAWN,
            tick=0,
            payload={"components": [RunComp(v=1).to_payload()]},
        )
        bad_cmd = Command(
            type=CommandType.SPAWN,
            tick=0,
            payload={"components": [{"v": 99}]},
        )
        await container.command_service.submit(str(world.world_id), good_cmd, ctx)
        await container.command_service.submit(str(world.world_id), bad_cmd, ctx)

        result = await container.simulation_service.run(world.world_id, RunConfig(num_steps=1))

        assert result.commands_applied == 1
    finally:
        await container.shutdown()
