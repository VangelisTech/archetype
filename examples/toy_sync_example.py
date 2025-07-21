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

import sys
import os

# Ensure the parent directory is in sys.path so 'archetype' can be imported
notebook_dir = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.getcwd()
project_root = os.path.abspath(os.path.join(notebook_dir, "..", "src"))
if project_root not in sys.path:
    sys.path.insert(0, project_root)


from archetype import Processor, Component # noqa: F401
import archetype

from daft import DataFrame, col # noqa: F401


# Define Components
class Position(Component):
    x: float
    y: float

class Velocity(Component):
    vx: float
    vy: float


class MovementProcessor(Processor):
    components = (Position, Velocity)
    priority = 1

    def process(self, df: DataFrame, dt: float) -> DataFrame: # Assuming df is passed by the system
        df = df.with_columns({ 
            "position__x": col("position__x") + col("velocity__vx") * dt,
            "position__y": col("position__y") + col("velocity__vy") * dt,
        })
        return df

def main(uri, debug):
    sim = archetype.init(uri, debug=debug)
    world_id = sim.spawn_world(uri, debug=debug,is_async=False)
    
    # Add Movement Processor to the World
    sim.add_processor_to_world(world_id, MovementProcessor())

    # Spawn 10 Entities in the world
    for i in range(10):
        sim.spawn_entity(world_id, Position(x=0, y=0), Velocity(vx=i, vy=i))

    for i in range(10):
        sim.step_world(world_id, dt=0.01)

    


if __name__ == "__main__":
    uri = ".archetype_examples/toy_sync_data"
    
    if not os.path.exists(uri): 
        os.makedirs(uri)


    world = main(uri)
