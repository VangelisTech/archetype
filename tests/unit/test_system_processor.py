
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
    Processor,
    processor,
    SyncWorld,
    SyncSystem,
)
import daft


class Position(Component):
    x: float
    y: float


class Velocity(Component):
    dx: float
    dy: float


@processor(Position, Velocity)
class MovementProcessor(Processor):
    def process(self, df, dt: float = 1.0):
        df = df.with_column("position__x", df["position__x"] + df["velocity__dx"] * dt)
        df = df.with_column("position__y", df["position__y"] + df["velocity__dy"] * dt)
        return df


@pytest.fixture
def world():
    """Creates a temporary directory for the test and yields a SyncWorld instance."""
    import tempfile
    import shutil
    temp_dir = tempfile.mkdtemp()
    yield make_simple_world(uri=temp_dir)
    shutil.rmtree(temp_dir)


def test_system_add_processor(world: SyncWorld):
    """Tests that a processor can be added to the system."""
    system = SyncSystem()
    system.add_processor(MovementProcessor())
    assert len(system.processors) == 1


def test_system_remove_processor(world: SyncWorld):
    """Tests that a processor can be removed from the system."""
    system = SyncSystem()
    proc = MovementProcessor()
    system.add_processor(proc)
    system.remove_processor(proc)
    assert len(system.processors) == 0


def test_system_execute(world: SyncWorld):
    """Tests that the system can execute processors."""
    world.add_processor(MovementProcessor())
    entity_id = world.spawn(Position(x=0, y=0), Velocity(dx=1, dy=1))
    world.materialize_spawns()
    sig = world._entity2sig[entity_id]
    df = world.querier.get_archetype(sig, world.tick, world.world_id, world.run_id)
    
    processed_df = world.system.execute(df, sig, dt=1.0)
    
    assert isinstance(processed_df, daft.DataFrame)
    pydict = processed_df.to_pydict()
    assert pydict["position__x"][0] == 1
    assert pydict["position__y"][0] == 1

