
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
from daft import DataFrame, col

# Define Components
class A(Component): value: int
class B(Component): value: int
class C(Component): value: int
class D(Component): value: int
class E(Component): value: int

# Define Processors
@processor(A, B)
class SystemAB(Processor):
    def process(self, df: DataFrame) -> DataFrame:
        return df.with_columns({
            "a__value": col("b__value"),
            "b__value": col("a__value"),
        })

@processor(C, D)
class SystemCD(Processor):
    def process(self, df: DataFrame) -> DataFrame:
        return df.with_columns({
            "c__value": col("d__value"),
            "d__value": col("c__value"),
        })

@processor(C, E)
class SystemCE(Processor):
    def process(self, df: DataFrame) -> DataFrame:
        return df.with_columns({
            "c__value": col("e__value"),
            "e__value": col("c__value"),
        })

def run_benchmark(world):
    # Simple Iteration
    # Dataset:
    # 1,000 entities with (A, B) components
    # 1,000 entities with (A, B, C) components
    # 1,000 entities with (A, B, C, D) components
    # 1,000 entities with (A, B, C, E) components
    for i in range(1000):
        world.spawn(A(value=i), B(value=i))
        world.spawn(A(value=i), B(value=i), C(value=i))
        world.spawn(A(value=i), B(value=i), C(value=i), D(value=i))
        world.spawn(A(value=i), B(value=i), C(value=i), E(value=i))

    world.add_processor(SystemAB())
    world.add_processor(SystemCD())
    world.add_processor(SystemCE())

    start_time = time.time()
    world.step()
    end_time = time.time()

    return end_time - start_time

def main():
    temp_dir = ".archetype_benchmarks/simple_iteration"
    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)
    os.makedirs(temp_dir)

    world = make_simple_world(uri=temp_dir)
    
    print("Running Simple Iteration Benchmark...")
    duration = run_benchmark(world)
    print(f"Simple Iteration Benchmark duration: {duration:.3f}s")

    shutil.rmtree(temp_dir)

if __name__ == "__main__":
    main()
