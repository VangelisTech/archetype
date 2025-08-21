import pytest
import os

from archetype.app.container import ServiceContainer
from archetype.core.config import StorageConfig, CacheConfig, RunConfig, WorldConfig
from archetype.app.auth.models import ActorCtx
from archetype.core.aio import AsyncSystem
from archetype.core.component import Component


class Position(Component):
    x: int
    y: int


@pytest.mark.asyncio
async def test_world_and_command_services_integration_create_and_run(tmp_path):
    actor = ActorCtx(roles={"admin"})
    storage = StorageConfig(uri=os.fspath(tmp_path / "archetype_data_test"))
    container = ServiceContainer(actor_ctx=actor, storage_config=storage, cache_config=CacheConfig())

    # Create a world
    system = AsyncSystem()
    world = await container.orchestrator.create_world(
        config=WorldConfig(name="test_world"),
        system=system,
        storage_config=container.storage_config,
        cache_config=container.cache_config,
    )

    # Enqueue a create entity command via command service
    cmd_id = await container.command_service.create_entity_command(
        world.world_id, [Position(x=1, y=2)], actor
    )
    assert cmd_id is not None

    # Dequeue and apply
    cmds = await container.command_service.dequeue_commands(world.world_id)
    processed = await container.world_service.apply_commands(world.world_id, cmds)
    assert processed and processed[0] == cmd_id

    # Run a single step and verify tick increments
    await container.orchestrator.run_world(world.world_id, RunConfig(num_steps=1))
    assert getattr(world, "tick", 0) == 1

    await container.shutdown()


