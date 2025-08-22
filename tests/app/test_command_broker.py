import asyncio
import pytest

from archetype.app.broker import CommandBroker
from archetype.app.models import Command
from archetype.app.auth.models import ActorCtx


@pytest.mark.asyncio
async def test_enqueue_dequeue_and_history_ordering():
    broker = CommandBroker()
    actor = ActorCtx(roles={"admin"})
    world_id = "w1"

    # Lower priority should come after higher priority if same tick
    c1 = Command(op="create_entity", payload={"a": 1}, tick=0, priority=0)
    c2 = Command(op="create_entity", payload={"a": 2}, tick=0, priority=-1)
    c3 = Command(op="add_component", payload={"b": 3}, tick=1, priority=0)

    await broker.enqueue(world_id, c1, actor)
    await broker.enqueue(world_id, c2, actor)
    await broker.enqueue(world_id, c3, actor)

    # Peek should not remove and should respect heap order
    peeked = await broker.peek(world_id, max_items=3)
    assert [c.payload.get("a") or c.payload.get("b") for c in peeked] == [2, 1, 3]

    # Dequeue should follow same order and drain pending
    got = await broker.dequeue(world_id, max_items=10)
    assert [c.payload.get("a") or c.payload.get("b") for c in got] == [2, 1, 3]
    assert await broker.get_pending_count(world_id) == 0

    # History should keep all commands in enqueue order (append order)
    history = await broker.get_history(world_id, limit=10)
    assert [c.payload.get("a") or c.payload.get("b") for c in history] == [1, 2, 3]


@pytest.mark.asyncio
async def test_clear_is_scoped_by_world_and_global():
    broker = CommandBroker()
    actor = ActorCtx(roles={"admin"})
    w1, w2 = "w1", "w2"

    await broker.enqueue(w1, Command(op="custom", payload={"w": 1}), actor)
    await broker.enqueue(w2, Command(op="custom", payload={"w": 2}), actor)
    assert await broker.get_pending_count(w1) == 1
    assert await broker.get_pending_count(w2) == 1

    await broker.clear(w1)
    assert await broker.get_pending_count(w1) == 0
    assert await broker.get_pending_count(w2) == 1

    await broker.clear()
    assert await broker.get_pending_count() == 0


