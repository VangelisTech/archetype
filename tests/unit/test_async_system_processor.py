
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
    AsyncProcessor,
    AsyncWorld,
    AsyncSystem,
)
from archetype.core.processor import processor
from archetype.core.interfaces import Component
import daft
import asyncio


class Position(Component):
    x: float
    y: float


class Velocity(Component):
    dx: float
    dy: float


@processor(Position, Velocity)
class MovementProcessor(AsyncProcessor):
    async def process(self, df, semaphore: asyncio.Semaphore, dt: float = 1.0):
        df = df.with_column("position__x", df["position__x"] + df["velocity__dx"] * dt)
        df = df.with_column("position__y", df["position__y"] + df["velocity__dy"] * dt)
        return df


@pytest_asyncio.fixture
async def world():
    """Creates a temporary directory for the test and yields an AsyncWorld instance."""
    import tempfile
    import shutil
    temp_dir = tempfile.mkdtemp()
    yield make_async_world(uri=temp_dir)
    shutil.rmtree(temp_dir)


@pytest.mark.asyncio
async def test_system_add_processor(world: AsyncWorld):
    """Tests that a processor can be added to the async system."""
    system = AsyncSystem()
    system.add_processor(MovementProcessor())
    assert len(system.processors) == 1


@pytest.mark.asyncio
async def test_system_remove_processor(world: AsyncWorld):
    """Tests that a processor can be removed from the async system."""
    system = AsyncSystem()
    proc = MovementProcessor()
    system.add_processor(proc)
    system.remove_processor(proc)
    assert len(system.processors) == 0


@pytest.mark.asyncio
async def test_system_execute(world: AsyncWorld):
    """Tests that the async system can execute processors."""
    world.add_processor(MovementProcessor())
    entity_id = world.spawn(Position(x=0, y=0), Velocity(dx=1, dy=1))
    await world.materialize_spawns()
    sig = world._entity2sig[entity_id]
    df = await world.querier.get_archetype(sig, world.tick, world.world_id, world.run_id)
    
    processed_df = await world.async_system.execute(df, sig, world.semaphore, dt=1.0)
    
    assert isinstance(processed_df, daft.DataFrame)
    pydict = processed_df.to_pydict()
    assert pydict["position__x"][0] == 1
    assert pydict["position__y"][0] == 1
