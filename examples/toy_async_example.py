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
import asyncio

from archetype.core import Component, AsyncProcessor
from daft import DataFrame, col

# Define Components
class Position(Component):
    x: float
    y: float

class Velocity(Component):
    vx: float
    vy: float


class MovementProcessor(AsyncProcessor):
    components = [Position, Velocity]
    priority = 1
    
    async def process(self, df: DataFrame, dt: float) -> DataFrame:
        df = df.with_columns({
            "position__x": col("position__x") + col("velocity__vx") * dt,
            "position__y": col("position__y") + col("velocity__vy") * dt,
        })
        return df

async def main(uri, debug=False):
    world = await archetype.create_world(uri, debug=debug)
    await world.add_processor(MovementProcessor())

    for i in range(10):
        await world.create_entity(Position(x=0, y=0), Velocity(vx=i, vy=i))

    for i in range(10):
        await world.step(dt=0.01)

        # Demo Runtime Interatvitiy
        if i // 3 == 0: 
            # Dynamically add a new component and processor
            class NewComponent(Component):
                value: int

            @processor(Position, Velocity, NewComponent, priority=2)
            class NewProcessor(AsyncProcessor):
                async def process(self, df: DataFrame, dt: float):
                    df = df.with_columns({"new_component__value": col("position__x") + col("velocity__vx")})
                    return df
            
            # Command World 
            await world.create_entity(Position(x=0, y=0), Velocity(vx=1, vy=1), NewComponent(value=42))
            await world.add_component(1, NewComponent(value=100))
            await world.add_processor(NewProcessor())


    return world

if __name__ == "__main__":
    uri = ".archetype_examples/toy_async_data"
    if not os.path.exists(uri):
        os.makedirs(uri)

    world = asyncio.run(main(uri))
