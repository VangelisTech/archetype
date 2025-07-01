
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

import time
import os
import shutil
import sys
import random

# Add the project root to the python path
notebook_dir = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.getcwd()
project_root = os.path.abspath(os.path.join(notebook_dir, "..", "..", "src"))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from archetype.core import make_simple_world, Component, Processor, processor
from daft import DataFrame, col

# --- Configuration ---
NUM_ENTITIES = 100_000
NUM_ARCHETYPES = 50
COMPONENTS_PER_ARCHETYPE = 5
NUM_COMPONENT_TYPES = 20

# --- Setup ---

# Dynamically create component types
component_types = []
for i in range(NUM_COMPONENT_TYPES):
    comp_name = f"Component{i}"
    comp_type = type(comp_name, (Component,), {"__annotations__": {"value": int}, "value": 0})
    component_types.append(comp_type)

# Dynamically create processors
@processor(component_types[0], component_types[1])
class System1(Processor):
    def process(self, df: DataFrame) -> DataFrame:
        return df.with_column(f"{component_types[0].__name__.lower()}__value", col(f"{component_types[0].__name__.lower()}__value") * 2)

@processor(component_types[2], component_types[3], component_types[4])
class System2(Processor):
    def process(self, df: DataFrame) -> DataFrame:
        return df.with_column(f"{component_types[2].__name__.lower()}__value", col(f"{component_types[2].__name__.lower()}__value") * 3)

@processor(component_types[5], component_types[6])
class System3(Processor):
    def process(self, df: DataFrame) -> DataFrame:
        return df.with_column(f"{component_types[5].__name__.lower()}__value", col(f"{component_types[5].__name__.lower()}__value") + 1)


def run_benchmark(world):
    print(f"Setting up large-scale benchmark with {NUM_ENTITIES} entities across {NUM_ARCHETYPES} archetypes...")

    # Create a variety of archetypes
    archetypes_to_create = []
    for _ in range(NUM_ARCHETYPES):
        archetypes_to_create.append(random.sample(component_types, COMPONENTS_PER_ARCHETYPE))

    # Spawn entities
    for i in range(NUM_ENTITIES):
        archetype_comps = random.choice(archetypes_to_create)
        components_to_spawn = [comp(value=i) for comp in archetype_comps]
        world.spawn(*components_to_spawn)
        if (i + 1) % 10000 == 0:
            print(f"  ...spawned {i+1}/{NUM_ENTITIES} entities")


    world.add_processor(System1())
    world.add_processor(System2())
    world.add_processor(System3())

    print("\nRunning a single step on the large-scale, fragmented dataset...")
    start_time = time.time()
    world.step()
    end_time = time.time()

    return end_time - start_time

def main():
    temp_dir = ".archetype_benchmarks/large_scale"
    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)
    os.makedirs(temp_dir)

    world = make_simple_world(uri=temp_dir)
    
    duration = run_benchmark(world)
    print(f"\nLarge-Scale Benchmark duration: {duration:.3f}s")

    shutil.rmtree(temp_dir)

if __name__ == "__main__":
    main()
