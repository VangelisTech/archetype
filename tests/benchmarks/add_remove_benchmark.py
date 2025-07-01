
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

# Add the project root to the python path
notebook_dir = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.getcwd()
project_root = os.path.abspath(os.path.join(notebook_dir, "..", "..", "src"))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from archetype.core import make_simple_world, Component, Processor, processor
from daft import DataFrame

# Define Components
class A(Component): value: int
class B(Component): value: int

# Define Processors
@processor(A)
class SystemA(Processor):
    def process(self, df: DataFrame) -> DataFrame:
        # This would be where we add component B.
        # This is not currently supported.
        return df

@processor(A, B)
class SystemAB(Processor):
    def process(self, df: DataFrame) -> DataFrame:
        # This would be where we remove component B.
        # This is not currently supported.
        return df

def run_benchmark(world):
    # Add / Remove
    # Dataset: 1,000 entities with a single A component.
    for i in range(1000):
        world.spawn(A(value=i))

    world.add_processor(SystemA())
    world.add_processor(SystemAB())

    start_time = time.time()
    
    # Iterate through all entities, adding a B component.
    world.step()
    
    # Then iterate through all entities again, removing their B component.
    world.step()

    end_time = time.time()

    return end_time - start_time

def main():
    temp_dir = ".archetype_benchmarks/add_remove"
    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)
    os.makedirs(temp_dir)

    world = make_simple_world(uri=temp_dir)
    
    print("Running Add / Remove Benchmark...")
    print("NOTE: This benchmark is not fully implemented, as adding/removing components is not yet supported.")
    duration = run_benchmark(world)
    print(f"Add / Remove Benchmark duration: {duration:.3f}s")

    shutil.rmtree(temp_dir)

if __name__ == "__main__":
    main()
