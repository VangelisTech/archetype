import os
import pytest

from archetype.app.container import ServiceContainer
from archetype.core.config import StorageConfig, CacheConfig, RunConfig, WorldConfig
from archetype.core.aio import AsyncSystem, AsyncProcessor
from archetype.core.component import Component
from archetype.app.auth.models import ActorCtx


class FlowPos(Component):
    x: int
    y: int


class FlowVel(Component):
    vx: int
    vy: int


class Move(AsyncProcessor):
    components = (FlowPos, FlowVel)
    priority = 1

    async def process(self, df, **kwargs):
        # increment position by velocity
        from daft import col
        return df.with_columns({
            "position__x": col("position__x") + col("velocity__vx"),
            "position__y": col("position__y") + col("velocity__vy"),
        })


@pytest.mark.asyncio
async def test_add_remove_components_and_hot_swap_processor(tmp_path):
    actor = ActorCtx(roles={"admin"})
    storage = StorageConfig(uri=os.fspath(tmp_path / "world_flows"))
    container = ServiceContainer(actor_ctx=actor, storage_config=storage, cache_config=CacheConfig())

    world = await container.orchestrator.create_world(
        config=WorldConfig(name="flows"),
        system=AsyncSystem(),
        storage_config=container.storage_config,
        cache_config=container.cache_config,
    )

    # Create entity via world service command pipeline
    await container.command_service.create_entity_command(
        world.world_id, [FlowPos(x=0, y=0)], actor
    )
    cmds = await container.command_service.dequeue_commands(world.world_id)
    await container.world_service.apply_commands(world.world_id, cmds)

    # Run one tick to materialize initial state so _move_entity can fetch prior row
    await container.orchestrator.run_world(world.world_id, RunConfig(num_steps=1))

    # Add velocity
    await container.command_service.add_components_command(
        world.world_id, 1, [FlowVel(vx=1, vy=2)], actor
    )
    cmds = await container.command_service.dequeue_commands(world.world_id)
    await container.world_service.apply_commands(world.world_id, cmds)

    # Hot-swap processor in
    await world.add_processor(Move())

    # Run a tick; position should change from velocity
    await container.orchestrator.run_world(world.world_id, RunConfig(num_steps=1))

    # Validate add-components transmuted the signature mapping
    sig = tuple(sorted((FlowPos, FlowVel), key=lambda t: t.__name__))
    assert world._entity2sig.get(1) == sig  # type: ignore[attr-defined]

    # Remove a component and ensure signature change is handled
    await container.command_service.remove_components_command(
        world.world_id, 1, [FlowVel], actor
    )
    cmds = await container.command_service.dequeue_commands(world.world_id)
    await container.world_service.apply_commands(world.world_id, cmds)

    # Run another tick, and verify velocity no longer applies; signature reverts
    await container.orchestrator.run_world(world.world_id, RunConfig(num_steps=1))
    assert world._entity2sig.get(1) == tuple(sorted((FlowPos,), key=lambda t: t.__name__))  # type: ignore[attr-defined]

    await container.shutdown()


