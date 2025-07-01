
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
)
from archetype.core.interfaces import Component
from archetype.core.processor import processor
import tempfile
import shutil
import asyncio


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
async def test_world_creation(world: AsyncWorld):
    """Tests that the async world is created correctly."""
    assert world is not None
    assert world.tick == 0


@pytest.mark.asyncio
async def test_spawn_entity(world: AsyncWorld):
    """Tests that an entity can be spawned in the async world."""
    entity_id = world.spawn(Position(x=0, y=0), Velocity(dx=1, dy=1))
    assert entity_id is not None
    assert entity_id == 1
    assert len(world.get_active_signatures()) == 1


@pytest.mark.asyncio
async def test_step(world: AsyncWorld):
    """Tests that the async world can step."""
    world.spawn(Position(x=0, y=0), Velocity(dx=1, dy=1))
    await world.step()
    assert world.tick == 1


@pytest.mark.asyncio
async def test_despawn_entity(world: AsyncWorld):
    """Tests that an entity can be despawned in the async world."""
    entity_id = world.spawn(Position(x=0, y=0), Velocity(dx=1, dy=1))
    await world.despawn(entity_id)
    await world.materialize_spawns()
    await world.step()
    assert world.tick == 1


@processor(Position, Velocity)
class MovementProcessor(AsyncProcessor):
    async def process(self, df, semaphore: asyncio.Semaphore, dt: float = 1.0):
        df = df.with_column("position__x", df["position__x"] + df["velocity__dx"] * dt)
        df = df.with_column("position__y", df["position__y"] + df["velocity__dy"] * dt)
        return df


@pytest.mark.asyncio
async def test_processor(world: AsyncWorld):
    """Tests that a simple async processor works."""
    world.add_processor(MovementProcessor())
    world.spawn(Position(x=0, y=0), Velocity(dx=1, dy=1))
    await world.step(dt=1.0)
    assert world.tick == 1
