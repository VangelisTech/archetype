import os
import pytest

from archetype.app.container import ServiceContainer
from archetype.core.config import StorageConfig, CacheConfig, RunConfig
from archetype.app.auth.models import ActorCtx
from archetype.core.aio import AsyncSystem


@pytest.mark.asyncio
async def test_run_parallel_worlds(tmp_path):
    actor = ActorCtx(roles={"admin"})
    storage = StorageConfig(uri=os.fspath(tmp_path / "sim_parallel"))
    container = ServiceContainer(actor_ctx=actor, storage_config=storage, cache_config=CacheConfig())

    world_ids = await container.simulation_service.run_parallel_worlds(
        num_worlds=3,
        run_config=RunConfig(num_steps=2),
        system_factory=lambda i: AsyncSystem(),
    )
    assert len(world_ids) == 3
    # Verify worlds advanced their ticks
    for wid in world_ids:
        world = container.orchestrator.get_world(wid)
        assert getattr(world, "tick", 0) == 2

    await container.shutdown()


@pytest.mark.asyncio
async def test_parameter_sweep(tmp_path):
    actor = ActorCtx(roles={"admin"})
    storage = StorageConfig(uri=os.fspath(tmp_path / "sim_sweep"))
    container = ServiceContainer(actor_ctx=actor, storage_config=storage, cache_config=CacheConfig())

    def factory(speed: int, friction: float):
        # In a real case we'd configure processors based on params
        return AsyncSystem()

    params = {"speed": [1, 2], "friction": [0.1, 0.2]}
    results = await container.simulation_service.run_parameter_sweep(
        base_system_factory=factory,
        parameters=params,
        run_config=RunConfig(num_steps=1),
    )

    # 2 x 2 combinations
    assert len(results) == 4
    # Ensure each mapped world ran 1 step
    for wid in results.values():
        world = container.orchestrator.get_world(wid)
        assert getattr(world, "tick", 0) == 1

    await container.shutdown()


@pytest.mark.asyncio
async def test_monte_carlo(tmp_path):
    actor = ActorCtx(roles={"admin"})
    storage = StorageConfig(uri=os.fspath(tmp_path / "sim_mc"))
    container = ServiceContainer(actor_ctx=actor, storage_config=storage, cache_config=CacheConfig())

    def sys_factory(seed: int):
        return AsyncSystem()

    summary = await container.simulation_service.run_monte_carlo(
        num_trials=5,
        system_factory=sys_factory,
        run_config=RunConfig(num_steps=1),
        seed=123,
    )
    assert summary["num_trials"] == 5
    assert len(summary["world_ids"]) == 5
    for wid in summary["world_ids"]:
        world = container.orchestrator.get_world(wid)
        assert getattr(world, "tick", 0) == 1

    await container.shutdown()


