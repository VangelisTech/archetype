import os

import pytest

from archetype.app import ArchetypeApp


@pytest.mark.asyncio
async def test_archetype_app_smoke_create_run_shutdown(tmp_path):
    app = await ArchetypeApp.create(storage_uri=os.fspath(tmp_path / "archetype_data_app_test"))
    world = await app.create_world("smoke")
    await app.run_world(world.world_id, steps=1)
    assert getattr(world, "tick", 0) == 1
    await app.shutdown()
