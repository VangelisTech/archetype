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

import asyncio
import os
import sys
import time

import pyglet
from daft import col, DataFrame, lit

# Ensure the parent directory is in sys.path so 'archetype' can be imported
notebook_dir = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.getcwd()
project_root = os.path.abspath(os.path.join(notebook_dir, "..", "src"))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from archetype.core import Component, processor
from archetype.core.aio import (AsyncProcessor, AsyncWorld, AsyncSystem,
                                AsyncStore, AsyncQuerier, AsyncUpdater,
                                AsyncCommandQueue)

FPS = 10
RESOLUTION = 720, 480


##################################
#  Define some Components:
##################################
class Velocity(Component):
    vx: float = 0.0
    vy: float = 0.0

class Position(Component):
    x: float = 0.0
    y: float = 0.0

class Sprite(Component):
    image: str
    width: int
    height: int

class PlayerControlled(Component):
    """Marker component for player-controlled entities"""
    pass


################################
#  Define some Processors:
################################
@processor(Position, Velocity, Sprite)
class MovementProcessor(AsyncProcessor):
    def __init__(self, minx, maxx, miny, maxy):
        super().__init__()
        self.minx = minx
        self.miny = miny
        self.maxx = maxx
        self.maxy = maxy

    async def process(self, df: DataFrame, broker, semaphore, dt: float) -> DataFrame:
        # Update positions
        df = df.with_columns({
            "position__x": col("position__x") + col("velocity__vx") * dt,
            "position__y": col("position__y") + col("velocity__vy") * dt,
        })

        # Boundary checks
        df = df.with_columns({
            "position__x": col("position__x").clip(self.minx, self.maxx - col("sprite__width")),
            "position__y": col("position__y").clip(self.miny, self.maxy - col("sprite__height")),
        })
        return df

@processor(Velocity, PlayerControlled)
class PlayerInputProcessor(AsyncProcessor):
    def __init__(self, input_state):
        super().__init__()
        self.input_state = input_state
        self.speed = 180.0  # pixels per second

    async def process(self, df: DataFrame, broker, semaphore, dt: float) -> DataFrame:
        vx = 0.0
        vy = 0.0
        if self.input_state['right']:
            vx = self.speed
        elif self.input_state['left']:
            vx = -self.speed
        if self.input_state['up']:
            vy = self.speed
        elif self.input_state['down']:
            vy = -self.speed

        df = df.with_columns({
            "velocity__vx": lit(vx),
            "velocity__vy": lit(vy),
        })
        return df


################################
#  Rendering System
################################
class RenderingSystem:
    def __init__(self, window, batch):
        self.window = window
        self.batch = batch
        self.sprites = {}  # entity_id -> pyglet sprite

        pyglet.resource.path = [os.path.join(os.path.dirname(__file__), 'assets')]
        pyglet.resource.reindex()

        self.image_cache = {
            "red_ball.png": pyglet.resource.image("red_ball.png"),
            "blue_ball.png": pyglet.resource.image("blue_ball.png"),
        }

    async def update(self, world: AsyncWorld):
        from archetype.core.interfaces import Archetype
        sig = Archetype.sig_from_components([Position(x=0,y=0), Sprite(image="", width=0, height=0)])
        
        if sig:
            df = await world.get_archetype(sig, world.tick)
            if df and df.count_rows() > 0:
                data = df.to_pydict()
                for i in range(len(data["entity_id"])):
                    entity_id = data["entity_id"][i]
                    x = data["position__x"][i]
                    y = data["position__y"][i]
                    image_name = data["sprite__image"][i]

                    if entity_id not in self.sprites:
                        image = self.image_cache.get(image_name)
                        if image:
                            self.sprites[entity_id] = pyglet.sprite.Sprite(
                                img=image, x=x, y=y, batch=self.batch
                            )
                    else:
                        self.sprites[entity_id].position = (x, y)

async def main():
    # Initialize pyglet window and graphics batch
    window = pyglet.window.Window(width=RESOLUTION[0], height=RESOLUTION[1], caption="Archetype Pyglet Example")
    batch = pyglet.graphics.Batch()

    # Setup input handling
    input_state = {'left': False, 'right': False, 'up': False, 'down': False}

    @window.event
    def on_key_press(key, mod):
        if key == pyglet.window.key.RIGHT:
            input_state['right'] = True
        elif key == pyglet.window.key.LEFT:
            input_state['left'] = True
        elif key == pyglet.window.key.UP:
            input_state['up'] = True
        elif key == pyglet.window.key.DOWN:
            input_state['down'] = True

    @window.event
    def on_key_release(key, mod):
        if key == pyglet.window.key.RIGHT:
            input_state['right'] = False
        elif key == pyglet.window.key.LEFT:
            input_state['left'] = False
        elif key == pyglet.window.key.UP:
            input_state['up'] = False
        elif key == pyglet.window.key.DOWN:
            input_state['down'] = False

    # Create Archetype World
    uri = ".archetype_examples/pyglet_archetype_data"
    if not os.path.exists(uri):
        os.makedirs(uri)
        
    store = AsyncStore(uri)
    querier = AsyncQuerier(store)
    updater = AsyncUpdater(store)
    system = AsyncSystem()
    broker = AsyncCommandQueue()
    world = AsyncWorld(querier, updater, system, broker)

    # Add Processors
    world.system.add_processor(MovementProcessor(minx=0, maxx=RESOLUTION[0], miny=0, maxy=RESOLUTION[1]))
    world.system.add_processor(PlayerInputProcessor(input_state))

    # Create Rendering System
    renderer = RenderingSystem(window, batch)

    # Create entities
    player_image = renderer.image_cache["red_ball.png"]
    await world.create_entity(
        Position(x=100, y=100),
        Velocity(vx=0, vy=0),
        Sprite(image="red_ball.png", width=player_image.width, height=player_image.height),
        PlayerControlled()
    )

    enemy_image = renderer.image_cache["blue_ball.png"]
    await world.create_entity(
        Position(x=400, y=250),
        Velocity(vx=50, vy=-25),
        Sprite(image="blue_ball.png", width=enemy_image.width, height=enemy_image.height)
    )

    # Main loop
    last_time = time.time()
    while not window.has_exit:
        dt = time.time() - last_time
        last_time = time.time()

        window.dispatch_events()

        await world.step(dt=dt)
        await renderer.update(world)

        window.clear()
        batch.draw()
        window.flip()

        await asyncio.sleep(1 / FPS)

if __name__ == "__main__":
    asyncio.run(main())
