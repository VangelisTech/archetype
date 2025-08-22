import pytest

from archetype.app.auth.models import ActorCtx
from archetype.app.models import Command
from archetype.app.broker import CommandBroker
from archetype.app.auth.guard import reset_tick_counters


@pytest.mark.asyncio
async def test_rbac_denial_on_insufficient_role():
    broker = CommandBroker()
    # 'viewer' is allowed only read ops; create_entity should be denied
    actor = ActorCtx(roles={"viewer"})
    cmd = Command(op="create_entity", payload={})

    with pytest.raises(PermissionError):
        await broker.enqueue("w1", cmd, actor)


@pytest.mark.asyncio
async def test_per_tick_quota_limit_enforced():
    broker = CommandBroker()
    actor = ActorCtx(roles={"admin"})
    world = "wq"
    reset_tick_counters()  # ensure clean tick

    # Exceed PER_TICK_CMD_BUDGET (25) by one
    accepted = 0
    denied = 0
    for i in range(30):
        cmd = Command(op="custom", payload={"i": i})
        try:
            await broker.enqueue(world, cmd, actor)
            accepted += 1
        except PermissionError:
            denied += 1

    assert accepted >= 25  # at least budget
    assert denied >= 1     # some denied after budget


@pytest.mark.asyncio
async def test_daily_token_budget_enforced():
    broker = CommandBroker()
    # actor with minimal roles but include 'player' to allow 'custom'
    actor = ActorCtx(roles={"player"})

    # Create many heavy commands to exceed daily token budget
    large_payload = {"data": "x" * 10000}
    denied = 0
    for i in range(2000):
        cmd = Command(op="custom", payload=large_payload)
        try:
            await broker.enqueue("wd", cmd, actor)
        except PermissionError:
            denied += 1
            break

    assert denied >= 1


