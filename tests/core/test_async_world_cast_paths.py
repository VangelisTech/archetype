import pytest
import pytest_asyncio

import daft
from daft import col
import pyarrow as pa

from archetype.core.config import StorageConfig, WorldConfig, RunConfig
from archetype.core.aio import AsyncSystem, AsyncProcessor
from archetype.core.component import Component
from archetype.core.orchestrator import WorldOrchestrator
from archetype.core.archetype import Archetype


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
    orch = WorldOrchestrator()
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        system = AsyncSystem()
        await system.add_processor(Noop())
        world = await orch.create_world(WorldConfig(name="w"), system=system, storage_config=storage)

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
        await orch.shutdown()


