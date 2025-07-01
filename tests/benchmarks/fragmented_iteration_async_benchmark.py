
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
import asyncio

# Add the project root to the python path
notebook_dir = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.getcwd()
project_root = os.path.abspath(os.path.join(notebook_dir, "..", "..", "src"))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from archetype.core.aio import make_async_world, AsyncProcessor
from archetype.core import Component, processor
from daft import DataFrame, col

# Define Components
class Data(Component): value: int
class A(Component): value: int
class B(Component): value: int
class C(Component): value: int
class D(Component): value: int
class E(Component): value: int
class F(Component): value: int
class G(Component): value: int
class H(Component): value: int
class I(Component): value: int
class J(Component): value: int
class K(Component): value: int
class L(Component): value: int
class M(Component): value: int
class N(Component): value: int
class O(Component): value: int
class P(Component): value: int
class Q(Component): value: int
class R(Component): value: int
class S(Component): value: int
class T(Component): value: int
class U(Component): value: int
class V(Component): value: int
class W(Component): value: int
class X(Component): value: int
class Y(Component): value: int
class Z(Component): value: int

# Define Processors
@processor(Data)
class SystemData(AsyncProcessor):
    async def process(self, df: DataFrame, semaphore: asyncio.Semaphore) -> DataFrame:
        return df.with_column("data__value", col("data__value") * 2)

@processor(Z)
class SystemZ(AsyncProcessor):
    async def process(self, df: DataFrame, semaphore: asyncio.Semaphore) -> DataFrame:
        return df.with_column("z__value", col("z__value") * 2)

async def run_benchmark(world):
    # Fragmented Iteration
    # Dataset: 26 component types (A through Z), each with 100 entities plus a Data component.
    components = [A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V, W, X, Y, Z]
    for comp in components:
        for i in range(100):
            world.spawn(comp(value=i), Data(value=i))

    world.add_processor(SystemData())
    world.add_processor(SystemZ())

    start_time = time.time()
    await world.step()
    end_time = time.time()

    return end_time - start_time

async def main():
    temp_dir = ".archetype_benchmarks/fragmented_iteration_async"
    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)
    os.makedirs(temp_dir)

    world = make_async_world(uri=temp_dir)
    
    print("Running Fragmented Iteration Benchmark (Async)...")
    duration = await run_benchmark(world)
    print(f"Fragmented Iteration Benchmark (Async) duration: {duration:.3f}s")

    shutil.rmtree(temp_dir)

if __name__ == "__main__":
    asyncio.run(main())
