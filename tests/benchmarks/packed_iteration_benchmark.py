
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
@processor(A)
class SystemA(Processor):
    def process(self, df: DataFrame) -> DataFrame:
        return df.with_column("a__value", col("a__value") * 2)

@processor(B)
class SystemB(Processor):
    def process(self, df: DataFrame) -> DataFrame:
        return df.with_column("b__value", col("b__value") * 2)

@processor(C)
class SystemC(Processor):
    def process(self, df: DataFrame) -> DataFrame:
        return df.with_column("c__value", col("c__value") * 2)

@processor(D)
class SystemD(Processor):
    def process(self, df: DataFrame) -> DataFrame:
        return df.with_column("d__value", col("d__value") * 2)

@processor(E)
class SystemE(Processor):
    def process(self, df: DataFrame) -> DataFrame:
        return df.with_column("e__value", col("e__value") * 2)

def run_benchmark(world):
    # Packed Iteration (5 queries)
    # Dataset: 1,000 entities, each with (A, B, C, D, E) components.
    for i in range(1000):
        world.spawn(A(value=i), B(value=i), C(value=i), D(value=i), E(value=i))

    world.add_processor(SystemA())
    world.add_processor(SystemB())
    world.add_processor(SystemC())
    world.add_processor(SystemD())
    world.add_processor(SystemE())

    start_time = time.time()
    world.step()
    end_time = time.time()

    return end_time - start_time

def main():
    temp_dir = ".archetype_benchmarks/packed_iteration"
    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)
    os.makedirs(temp_dir)

    world = make_simple_world(uri=temp_dir)
    
    print("Running Packed Iteration Benchmark...")
    duration = run_benchmark(world)
    print(f"Packed Iteration Benchmark duration: {duration:.3f}s")

    shutil.rmtree(temp_dir)

if __name__ == "__main__":
    main()
