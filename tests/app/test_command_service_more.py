import os
import pytest

from archetype.app.container import ServiceContainer
from archetype.app.auth.models import ActorCtx
from archetype.core.config import StorageConfig, CacheConfig, WorldConfig, RunConfig
from archetype.core.component import Component


class C1(Component):
    a: int


@pytest.mark.asyncio
async def test_remove_entity_command_and_pending_history(tmp_path):
    actor = ActorCtx(roles={"admin"})
    storage = StorageConfig(uri=os.fspath(tmp_path / "svc_more"))
    container = ServiceContainer(actor_ctx=actor, storage_config=storage, cache_config=CacheConfig())

    world = await container.orchestrator.create_world(
        config=WorldConfig(name="w"),
        system=__import__("archetype.core.aio", fromlist=["AsyncSystem"]).AsyncSystem(),
        storage_config=container.storage_config,
        cache_config=container.cache_config,
    )

    # Create + apply a spawn, then queue a remove
    cid1 = await container.command_service.create_entity_command(world.world_id, [C1(a=1)], actor)
    cmds = await container.command_service.dequeue_commands(world.world_id)
    await container.world_service.apply_commands(world.world_id, cmds)

    rid = await container.command_service.remove_entity_command(world.world_id, 1, actor)
    # Pending count reflects queued items
    assert await container.broker.get_pending_count(str(world.world_id)) >= 1

    # History exposes recent commands
    hist = await container.command_service.get_command_history(world.world_id, limit=10)
    assert any(c.id == rid for c in hist)

    # Apply remove and run one step
    cmds = await container.command_service.dequeue_commands(world.world_id)
    await container.world_service.apply_commands(world.world_id, cmds)
    await container.orchestrator.run_world(world.world_id, RunConfig(num_steps=1))

    await container.shutdown()


