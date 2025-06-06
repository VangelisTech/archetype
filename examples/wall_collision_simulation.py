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

from archetype import Component, Processor, processor, make_simple_world
from daft import col, DataFrame, lit
import pyglet
import time
from typing import Dict, Any

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

# Processors
@processor(Position, Velocity, priority=10)
class MovementProcessor(Processor):
    def process(self, df: DataFrame, dt: float) -> DataFrame:
        df = df.with_columns({
            "position__x": col("position__x") + col("velocity__vx") * dt,
            "position__y": col("position__y") + col("velocity__vy") * dt,
        })
        return df

@processor(Position, Velocity, Ball, priority=5)
class WallCollisionProcessor(Processor):
    def __init__(self, wall: Wall = None):
        super().__init__()
        self.wall = wall or Wall()

    def process(self, df: DataFrame, dt: float) -> DataFrame:
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
            "velocity__vx": df["past_left"].if_else(
                col("velocity__vx").abs(),  # Make positive (move right)
                df["past_right"].if_else(
                    -col("velocity__vx").abs(),  # Make negative (move left)
                    col("velocity__vx")
                )
            ),
            "velocity__vy": df["past_bottom"].if_else(
                col("velocity__vy").abs(),  # Make positive (move up)
                df["past_top"].if_else(
                    -col("velocity__vy").abs(),  # Make negative (move down)
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

@processor(Velocity, PlayerControlled, priority=15)
class PlayerInputProcessor(Processor):
    def __init__(self, input_state):
        super().__init__()
        self.input_state = input_state
        self.speed = 150.0  # pixels per second

    def process(self, df: DataFrame, dt: float) -> DataFrame:
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

        # Set up resource path
        pyglet.resource.path = ['assets']
        pyglet.resource.reindex()

        # Load images
        self.red_ball_image = pyglet.resource.image("red_ball.png")
        self.blue_ball_image = pyglet.resource.image("blue_ball.png")

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

    def update_positions(self, world):
        """Update sprite positions from ECS data"""
        # Get all entities with Position and Ball components
        archetypes = world.store.get_archetypes(Position, Ball)

        for table_name, df in archetypes.items():
            # Get the latest step data
            latest_df = df.where(col("is_active") == True).collect()

            for row in latest_df.to_pylist():
                entity_id = row['entity_id']
                x = row['position__x']
                y = row['position__y']
                color = row['ball__color']

                # Create sprite if it doesn't exist
                if entity_id not in self.sprites:
                    self.create_sprite(entity_id, color, x, y)
                else:
                    # Update existing sprite position
                    self.sprites[entity_id].x = x
                    self.sprites[entity_id].y = y

def main(uri, debug=False):
    # Create the ECS world
    world = make_simple_world(uri, debug=debug)

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

    # Add processors
    world.add_processor(MovementProcessor())
    world.add_processor(WallCollisionProcessor(wall))

    # Input state tracking
    input_state = {
        'left': False,
        'right': False,
        'up': False,
        'down': False
    }

    # Add player input processor
    world.add_processor(PlayerInputProcessor(input_state))

    # Spawn entities
    red_enemy = world.spawn(
        Ball(radius=15, color="red"),
        Position(x=100, y=100),
        Velocity(vx=120, vy=80)
    )

    blue_player = world.spawn(
        Ball(radius=20, color="blue"),
        Position(x=400, y=300),
        Velocity(vx=0, vy=0),
        PlayerControlled()
    )

    # Materialize the initial spawn data
    world.store.materialize_spawns()

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

    @window.event
    def on_draw():
        window.clear()
        batch.draw()

    # Update function called by pyglet's scheduler
    def update(dt):
        # Step the ECS world
        world.step(dt=dt)

        # Update sprite positions from ECS data
        renderer.update_positions(world)

    # Schedule the update function to run at 60 FPS
    pyglet.clock.schedule_interval(update, 1/60.0)

    # Start pyglet's event loop
    pyglet.app.run()

if __name__ == "__main__":
    uri = "/Users/everett-founder/git/vangelis/internal/work/libs/archetype/data"
    main(uri, debug=True)
