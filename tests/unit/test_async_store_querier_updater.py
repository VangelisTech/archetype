
# Copyright 2025 Vangelis Technologies Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import pytest
import pytest_asyncio
from archetype.core.aio import (
    make_async_world,
    AsyncWorld,
    AsyncStore,
)
from archetype.core.interfaces import Component
import tempfile
import shutil
import daft


class Position(Component):
    x: float
    y: float


class Velocity(Component):
    dx: float
    dy: float


@pytest_asyncio.fixture
async def world():
    """Creates a temporary directory for the test and yields an AsyncWorld instance."""
    temp_dir = tempfile.mkdtemp()
    yield make_async_world(uri=temp_dir)
    shutil.rmtree(temp_dir)


@pytest.mark.asyncio
async def test_store_creation(world: AsyncWorld):
    """Tests that the async store is created correctly."""
    assert isinstance(world.querier._store, AsyncStore)


@pytest.mark.asyncio
async def test_store_materialize_spawns(world: AsyncWorld):
    """Tests that the async store can materialize spawns."""
    entity_id = world.spawn(Position(x=0, y=0), Velocity(dx=1, dy=1))
    await world.materialize_spawns()
    sig = world._entity2sig[entity_id]
    df = await world.querier.get_archetype(sig, world.tick, world.world_id, world.run_id)
    assert df.count_rows() == 1


@pytest.mark.asyncio
async def test_querier_get_archetype(world: AsyncWorld):
    """Tests that the async querier can get an archetype."""
    entity_id = world.spawn(Position(x=0, y=0), Velocity(dx=1, dy=1))
    await world.materialize_spawns()
    sig = world._entity2sig[entity_id]
    df = await world.querier.get_archetype(sig, world.tick, world.world_id, world.run_id)
    assert isinstance(df, daft.DataFrame)
    assert df.count_rows() == 1


@pytest.mark.asyncio
async def test_updater_update(world: AsyncWorld):
    """Tests that the async updater can update an archetype."""
    entity_id = world.spawn(Position(x=0, y=0), Velocity(dx=1, dy=1))
    await world.materialize_spawns()
    sig = world._entity2sig[entity_id]
    df = await world.querier.get_archetype(sig, world.tick, world.world_id, world.run_id)
    df = df.with_column("position__x", df["position__x"] + 1)
    await world.updater.update(df, sig, world.tick, world.world_id, world.run_id)
    
    df_updated = await world.querier.get_archetype(sig, world.tick, world.world_id, world.run_id)
    assert df_updated.count_rows() == 1
