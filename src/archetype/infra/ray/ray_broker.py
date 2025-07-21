

import ray


from archetype.core.aio.async_broker import AsyncCommandQueue
from archetype.core.aio.async_interfaces import iBroker
from archetype.core.aio.command import Command

@ray.remote(max_restarts=-1)
class _RayBrokerActor(AsyncCommandQueue):
    """Ray actor that shares the same heap/log logic as AsyncCommandQueue."""
    pass  # inherit enqueue, dequeue_due, etc.

class RayBroker(iBroker):
    """Proxy object that satisfies BaseCommandQueue API and forwards to actor."""

    def __init__(self, actor_name: str = "arche-broker"):
        if ray.is_initialized():
            try:
                self._actor = ray.get_actor(actor_name)
            except ValueError:
                self._actor = _RayBrokerActor.options(
                    name=actor_name,
                    lifetime="detached",
                ).remote()
        else:
            raise RuntimeError("Ray is not initialized")

    async def enqueue(self, cmd: Command, ctx):
        await self._actor.enqueue.remote(cmd, ctx)

    async def dequeue_due(self, *, tick: int, limit: int = 1_000):
        return await self._actor.dequeue_due.remote(tick=tick, limit=limit)

    async def ack(self, cmd_ids):
        await self._actor.ack.remote(cmd_ids)
