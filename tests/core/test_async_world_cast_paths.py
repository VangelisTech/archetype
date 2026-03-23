import pytest

from archetype.app.storage_service import StorageService
from archetype.app.world_service import WorldService
from archetype.core.aio import AsyncProcessor, AsyncSystem
from archetype.core.archetype import Archetype
from archetype.core.component import Component
from archetype.core.config import RunConfig, StorageConfig, WorldConfig


class Pos(Component):
    x: int


class Noop(AsyncProcessor):
    components = (Pos,)
    priority = 0

    async def process(self, df, **kwargs):
        return df


@pytest.mark.asyncio
async def test_world_materialize_mutations_cast_and_join_paths(tmp_path):
    """Exercise cast and left-join branches in materialize_mutations by creating despawns and spawns and ensure no type errors."""
    ws = WorldService(StorageService())
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        system = AsyncSystem()
        await system.add_processor(Noop())
        world = await ws.create_world(
            WorldConfig(name="w"), storage_config=storage, system=system
        )

        # Spawn two entities
        e1 = await world.create_entity([Pos(x=1)])
        e2 = await world.create_entity([Pos(x=2)])

        # First step materializes spawns
        await world.run(RunConfig(num_steps=1))

        # Despawn e1 to hit left-join mask path; add component to e2 to move archetype
        await world.remove_entity(e1)
        await world.add_components(e2, [])  # no-op path safe

        # Second step hits despawn join and cast
        await world.run(RunConfig(num_steps=1))

        # Ensure execution completed; query the archetype via public query API to avoid schema concat requirements
        sig = Archetype.sig_from_components([Pos(x=0)])
        df = await world.query_archetype(sig, run_config=RunConfig())
        assert set(df.column_names) >= {"entity_id", "pos__x", "is_active"}
    finally:
        await ws.shutdown()
