import pytest, ray, asyncio
from archetype.infra.ray.broker import RayBroker
from archetype.core.aio.command import Command
from archetype.auth import ActorCtx  # your ctx helper

@pytest.mark.asyncio
async def test_ray_broker_spawn():
    ray.init()                 # local head
    broker = RayBroker()
    ctx = ActorCtx(id=uuid4(), roles={"maintainer"})
    await broker.enqueue(Command(op="spawn_entity", payload={}), ctx)
    batch = await broker.dequeue_due(tick=0)
    assert len(batch) == 1
    ray.shutdown()
