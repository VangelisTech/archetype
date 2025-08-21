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


import os
import asyncio
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))

import time
from typing import Dict, Any, List

import pyglet
from daft import col, DataFrame, lit

from archetype import (
    Component,
    AsyncProcessor,
    WorldOrchestrator,
    AsyncSystem,
    StorageConfig,
    WorldConfig,
    RunConfig,
    CacheConfig,
    Archetype,
)


# Components - Pure data, no rendering objects
class Position(Component):
    x: float = 0.0
    y: float = 0.0

class Velocity(Component):
    vx: float = 0.0
    vy: float = 0.0

class Wall(Component):
    min_x: float = 0.0
    max_x: float = 800.0
    min_y: float = 0.0
    max_y: float = 600.0

class Ball(Component):
    radius: float = 10.0
    color: str = "red"  # "red" or "blue"

class PlayerControlled(Component):
    """Marker component for player-controlled entities"""
    pass

# Processors (async style)
class MovementProcessor(AsyncProcessor):
    components = [Position, Velocity]
    priority = 10

    async def process(self, df: DataFrame, dt: float) -> DataFrame:
        df = df.with_columns({
            "position__x": col("position__x") + col("velocity__vx") * dt,
            "position__y": col("position__y") + col("velocity__vy") * dt,
        })
        return df

class WallCollisionProcessor(AsyncProcessor):
    components = [Position, Velocity, Ball]
    priority = 20
    def __init__(self, wall: Wall = None):
        super().__init__()
        self.wall = wall or Wall()

    async def process(self, df: DataFrame, dt: float) -> DataFrame:
        # Check wall collisions and reverse velocity if needed
        df = df.with_columns({
            # Check if we're past the walls (accounting for ball radius)
            "past_left":   col("position__x") - col("ball__radius") <= self.wall.min_x,
            "past_right":  col("position__x") + col("ball__radius") >= self.wall.max_x,
            "past_bottom": col("position__y") - col("ball__radius") <= self.wall.min_y,
            "past_top":    col("position__y") + col("ball__radius") >= self.wall.max_y,
        })

        # Reverse velocity and clamp position if past walls
        df = df.with_columns({
            "velocity__vx": col("past_left").if_else(
                col("velocity__vx").abs(),  # Make positive (move right)
                col("past_right").if_else(
                    col("velocity__vx").abs() * lit(-1),  # Make negative (move left)
                    col("velocity__vx")
                )
            ),
            "velocity__vy": col("past_bottom").if_else(
                col("velocity__vy").abs(),  # Make positive (move up)
                col("past_top").if_else(
                    col("velocity__vy").abs() * lit(-1),  # Make negative (move down)
                    col("velocity__vy")
                )
            ),
        })

        # Clamp positions to keep balls within bounds
        df = df.with_columns({
            "position__x": col("position__x").clip(
                self.wall.min_x + col("ball__radius"),
                self.wall.max_x - col("ball__radius")
            ),
            "position__y": col("position__y").clip(
                self.wall.min_y + col("ball__radius"),
                self.wall.max_y - col("ball__radius")
            ),
        })

        # Drop temporary columns
        df = df.select([c for c in df.column_names if not c.startswith("past_")])

        return df

class PlayerInputProcessor(AsyncProcessor):
    components = [Velocity, PlayerControlled]
    priority = 5
    def __init__(self, input_state):
        super().__init__()
        self.input_state = input_state
        self.speed = 150.0  # pixels per second

    async def process(self, df: DataFrame, dt: float) -> DataFrame:
        # Update velocity based on current input state
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

# Rendering System - Separate from ECS
class RenderingSystem:
    def __init__(self, window, batch):
        self.window = window
        self.batch = batch
        self.sprites = {}  # entity_id -> sprite

        # Set up resource path relative to this file
        assets_dir = os.path.join(os.path.dirname(__file__), 'assets')
        pyglet.resource.path = [assets_dir]
        pyglet.resource.reindex()

        # Load images
        self.red_ball_image = pyglet.resource.image("redsquare.png")
        self.blue_ball_image = pyglet.resource.image("bluesquare.png")

        # Center the anchor points
        self.red_ball_image.anchor_x = self.red_ball_image.width // 2
        self.red_ball_image.anchor_y = self.red_ball_image.height // 2
        self.blue_ball_image.anchor_x = self.blue_ball_image.width // 2
        self.blue_ball_image.anchor_y = self.blue_ball_image.height // 2

    def create_sprite(self, entity_id: int, color: str, x: float, y: float):
        """Create a sprite for an entity"""
        image = self.red_ball_image if color == "red" else self.blue_ball_image
        sprite = pyglet.sprite.Sprite(image, x=x, y=y, batch=self.batch)
        self.sprites[entity_id] = sprite
        return sprite

    def apply_rows(self, rows: List[Dict[str, Any]]):
        """Apply position updates computed off-thread."""
        for row in rows:
            entity_id = row['entity_id']
            x = row['position__x']
            y = row['position__y']
            color = row['ball__color']

            if entity_id not in self.sprites:
                self.create_sprite(entity_id, color, x, y)
            else:
                self.sprites[entity_id].x = x
                self.sprites[entity_id].y = y

async def main(storage_config: StorageConfig, world_config: WorldConfig, run_configs: List[RunConfig], cache_config: CacheConfig | None = None):
    # Create window
    window = pyglet.window.Window(
        width=800,
        height=600,
        caption="Archetype Wall Collision Simulation"
    )

    # Create batch for efficient rendering
    batch = pyglet.graphics.Batch()

    # Set up walls to match window size
    wall = Wall(min_x=0, max_x=window.width, min_y=0, max_y=window.height)

    # Input state tracking
    input_state = {
        'left': False,
        'right': False,
        'up': False,
        'down': False
    }

    # Create world directly (single event loop)
    orchestrator = WorldOrchestrator()
    world = await orchestrator.create_world(
        config=world_config,
        system=AsyncSystem(),
        storage_config=storage_config,
        cache_config=cache_config,
        instrumented=True,
    )
    await world.add_processor(PlayerInputProcessor(input_state))
    await world.add_processor(MovementProcessor())
    # Start simple: disable collisions until sprites render reliably
    # await world.add_processor(WallCollisionProcessor(wall))

    # Spawn minimal entities: one player (red, controlled) + one enemy (blue, drifting)
    await world.create_entity([
        Ball(radius=15, color="red"),
        Position(x=100, y=100),
        Velocity(vx=0, vy=0),
        PlayerControlled(),
    ])
    await world.create_entity([
        Ball(radius=20, color="blue"),
        Position(x=400, y=250),
        Velocity(vx=60, vy=-30),
    ])

    run_config = run_configs[0] if run_configs else RunConfig(num_steps=1_000_000)

    # Create rendering system
    renderer = RenderingSystem(window, batch)

    # Pyglet event handlers
    @window.event
    def on_key_press(symbol, modifiers):
        if symbol == pyglet.window.key.LEFT:
            input_state['left'] = True
        elif symbol == pyglet.window.key.RIGHT:
            input_state['right'] = True
        elif symbol == pyglet.window.key.UP:
            input_state['up'] = True
        elif symbol == pyglet.window.key.DOWN:
            input_state['down'] = True

    @window.event
    def on_key_release(symbol, modifiers):
        if symbol == pyglet.window.key.LEFT:
            input_state['left'] = False
        elif symbol == pyglet.window.key.RIGHT:
            input_state['right'] = False
        elif symbol == pyglet.window.key.UP:
            input_state['up'] = False
        elif symbol == pyglet.window.key.DOWN:
            input_state['down'] = False

    # Main loop: single asyncio loop drives simulation and rendering
    target_fps = 60.0
    frame_time = 1.0 / target_fps
    last_time = time.time()


    while not window.has_exit:
        # Compute dt
        now = time.time()
        dt = now - last_time
        last_time = now

        # Dispatch window events
        window.dispatch_events()
        world.run_id = run_config.run_id

        # Step one tick
        await world.step(dt=dt)

        # Query current positions for Position+Ball from live state (no tick lag)
        df = await world.query_components_live([Position(), Ball()])
        if df.count_rows() > 0:
            rows = df.collect().to_pylist()
            renderer.apply_rows(rows)

        # Draw
        window.clear()
        batch.draw()
        window.flip()

        # Frame pacing
        elapsed = time.time() - now
        sleep_s = max(0.0, frame_time - elapsed)
        await asyncio.sleep(sleep_s)


if __name__ == "__main__":
    # Define configs (mirrors toy example style)
    storage_config = StorageConfig(
        uri="./archetype_data",
        namespace="examples",
    )
    world_config = WorldConfig(name="wall_collision_world")
    cache_config = CacheConfig(flush_rows=100_000, flush_mb=128, idle_sec=30)
    run_configs = [
        RunConfig(num_steps=1_000_000, debug=False, show_rows=0, explain=False),
    ]

    asyncio.run(main(storage_config, world_config, run_configs, cache_config=cache_config))
