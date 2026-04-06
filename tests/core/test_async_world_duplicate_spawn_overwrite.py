import pytest

from archetype.app.storage_service import StorageService
from archetype.app.world_service import WorldService
from archetype.core.aio import AsyncProcessor, AsyncSystem
from archetype.core.component import Component
from archetype.core.config import RunConfig, StorageConfig, WorldConfig


class A(Component):
    i: int


class Noop(AsyncProcessor):
    components = (A,)
    priority = 0

    async def process(self, df, **kwargs):
        return df


@pytest.mark.asyncio
async def test_duplicate_spawn_same_entity_overwrites(monkeypatch, tmp_path):
    """Simulate duplicate spawn commands for the same entity within the same tick; ensure last write wins via world internals by using public API to observe final state."""
    ws = WorldService(StorageService())
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        system = AsyncSystem()
        await system.add_processor(Noop())
        world = await ws.create_world(WorldConfig(name="w"), storage_config=storage, system=system)

        # Create one entity, then enqueue another spawn for the same entity id by patching next id backwards
        eid = await world.create_entity([A(i=1)])

        # Force the next created entity id to be the same as previous
        world._next_entity_id = eid  # type: ignore[attr-defined]
        await world.create_entity([A(i=99)])

        await world.run(RunConfig(num_steps=1))

        # Query live components; last write should deterministically win (value=99)
        df = await world.get_components([A])
        rows = df.select("a__i").to_pylist()
        assert len(rows) == 1
        assert rows[0]["a__i"] == 99
    finally:
        await ws.shutdown()
