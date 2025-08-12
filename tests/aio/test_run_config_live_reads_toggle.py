import pytest
import pytest_asyncio
import daft

from archetype.core.runtime.storage import StorageSessionFactory
from archetype.core.config import StorageConfig, WorldConfig, RunConfig
from archetype.core.component import Component
from archetype.core.archetype import Archetype
from archetype.core.aio import AsyncStore, AsyncQueryManager, AsyncUpdateManager, AsyncSystem, AsyncWorld


class Position(Component):
    x: int
    y: int


@pytest_asyncio.fixture
async def make_world(tmp_path):
    async def _mk():
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="test")
        context = StorageSessionFactory.build(storage)
        store = AsyncStore(context)
        querier = AsyncQueryManager(store)
        updater = AsyncUpdateManager(store)
        system = AsyncSystem()
        wcfg = WorldConfig(name="w")
        return AsyncWorld(wcfg, querier, updater, system)
    return _mk


@pytest.mark.asyncio
async def test_prefer_live_reads_equivalence(make_world):
    world_live = await make_world()
    world_store = await make_world()

    # Spawn identical entities in both worlds
    for i in range(3):
        await world_live.create_entity([Position(x=i, y=i)])
        await world_store.create_entity([Position(x=i, y=i)])

    # Step twice under both modes
    rc_live = RunConfig(num_steps=1, prefer_live_reads=True)
    rc_store = RunConfig(num_steps=1, prefer_live_reads=False)

    await world_live.step(rc_live)
    await world_live.step(rc_live)

    await world_store.step(rc_store)
    await world_store.step(rc_store)

    # Compare live snapshots by signature counts
    sig = Archetype.sig_from_components([Position(x=0, y=0)])
    assert sig in world_live._live and sig in world_store._live
    cnt_live = world_live._live[sig].collect().count_rows()
    cnt_store = world_store._live[sig].collect().count_rows()
    assert cnt_live == cnt_store


