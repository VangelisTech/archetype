
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
from archetype.core import (
    make_simple_world,
    Component,
    SyncWorld,
    SyncStore,
    QueryManager,
    UpdateManager,
)
import tempfile
import shutil
import daft


class Position(Component):
    x: float
    y: float


class Velocity(Component):
    dx: float
    dy: float


@pytest.fixture
def world():
    """Creates a temporary directory for the test and yields a SyncWorld instance."""
    temp_dir = tempfile.mkdtemp()
    yield make_simple_world(uri=temp_dir)
    shutil.rmtree(temp_dir)


def test_store_creation(world: SyncWorld):
    """Tests that the store is created correctly."""
    assert isinstance(world.querier._store, SyncStore)


def test_store_materialize_spawns(world: SyncWorld):
    """Tests that the store can materialize spawns."""
    entity_id = world.spawn(Position(x=0, y=0), Velocity(dx=1, dy=1))
    world.materialize_spawns()
    sig = world._entity2sig[entity_id]
    df = world.querier.get_archetype(sig, world.tick, world.world_id, world.run_id)
    assert df.count_rows() == 1


def test_querier_get_archetype(world: SyncWorld):
    """Tests that the querier can get an archetype."""
    entity_id = world.spawn(Position(x=0, y=0), Velocity(dx=1, dy=1))
    world.materialize_spawns()
    sig = world._entity2sig[entity_id]
    df = world.querier.get_archetype(sig, world.tick, world.world_id, world.run_id)
    assert isinstance(df, daft.DataFrame)
    assert df.count_rows() == 1


def test_updater_update(world: SyncWorld):
    """Tests that the updater can update an archetype."""
    entity_id = world.spawn(Position(x=0, y=0), Velocity(dx=1, dy=1))
    world.materialize_spawns()
    sig = world._entity2sig[entity_id]
    df = world.querier.get_archetype(sig, world.tick, world.world_id, world.run_id)
    df = df.with_column("position__x", df["position__x"] + 1)
    world.updater.update(df, sig, world.tick, world.world_id, world.run_id)
    
    # To verify, we need to query again at the next step
    df_updated = world.querier.get_archetype(sig, world.tick, world.world_id, world.run_id)
    assert df_updated.count_rows() == 1

