#!/usr/bin/env python3
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
import time

# Ensure the parent directory is in sys.path so 'archetype' can be imported
notebook_dir = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.getcwd()
project_root = os.path.abspath(os.path.join(notebook_dir, "..", "src"))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from daft import DataFrame, col

from archetype.core import processor, Component, make_simple_world


# Define Components
class Position(Component):
    x: float
    y: float

class Velocity(Component):
    vx: float
    vy: float


# Simple synchronous processor
@processor(Position, Velocity, priority=1)
class MovementProcessor:
    def process(self, df: DataFrame, dt: float) -> DataFrame:
        return df.with_columns({
            "position__x": col("position__x") + col("velocity__vx") * dt,
            "position__y": col("position__y") + col("velocity__vy") * dt,
        })


def main():
    """
    Synchronous ECS simulation demo for comparison.

    Shows the same physics simulation running synchronously
    for performance comparison with the async version.
    """

    uri = "/Users/everett-founder/git/vangelis/internal/work/libs/archetype/data"

    # Create sync world
    world = make_simple_world(uri, debug=False)
    world.add_processor(MovementProcessor())

    # Spawn entities for physics simulation
    print("🎯 Spawning 5 entities with Position + Velocity components...")
    world.spawn(Position(x=1, y=1), Velocity(vx=1, vy=1))
    world.spawn(Position(x=2, y=2), Velocity(vx=2, vy=2))
    world.spawn(Position(x=3, y=3), Velocity(vx=3, vy=3))
    world.spawn(Position(x=4, y=4), Velocity(vx=4, vy=4))
    world.spawn(Position(x=5, y=5), Velocity(vx=5, vy=5))
    world.materialize_spawns()

    print("⚡ Running 10-step physics simulation (dt=0.1)...")
    print("   Each step: Query → Process → Update → Persist")

    start = time.time()
    for i in range(10):
        world.step(dt=0.1)
    elapsed = time.time() - start



if __name__ == "__main__":
    main()
