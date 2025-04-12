# -*- coding: utf-8 -*-
"""
Toy example to test the Daft-Ray ECS implementation.
"""

import time
# from dataclasses import dataclass, field # No longer needed for components

# Import core ECS components from the 'core' directory
from core import Component, Processor, World, SequentialSystem

# Import Daft for DataFrame operations
import daft
from daft import col, lit

# Import Pyglet for visualization
import pyglet
from pyglet import shapes
from pyglet import clock
from pyglet import graphics
import random

# --- Define Components ---
# Components inherit from Component (lancedb.pydantic.LanceModel)
class Position(Component):
    x: float = 0.0
    y: float = 0.0
    z: float = 0.0

class Velocity(Component):
    vx: float = 0.0
    vy: float = 0.0
    vz: float = 0.0

class Acceleration(Component):
    ax: float = 0.0
    ay: float = 0.0
    az: float = 0.0

class Mass(Component):
    mass: float = 0.0 # kg

class Momentum(Component):
    px: float = 0.0
    py: float = 0.0
    pz: float = 0.0

class Force(Component):
    fx: float = 0.0
    fy: float = 0.0
    fz: float = 0.0

class KineticEnergy(Component):
    kex: float = 0.0 # Joules
    key: float = 0.0 # Joules
    kez: float = 0.0 # Joules


class Physics(Component):
    position: Position
    velocity: Velocity
    acceleration: Acceleration
    mass: Mass
    momentum: Momentum
    force: Force

# --- Define a simple Processor ---
class KinematicsProcessor(Processor): # Processors simply implement a "process" method
    """Updates Position based on Velocity."""
    def __init__(self, world: World):
        self.world = world

    def process(self, dt: float) -> None:
        
        components = [Position, Velocity, Acceleration]
        state_df = self.world.get_components(components) # Naive to step 
        
        # Calculate updates using Daft expressions
        update_df = state_df.with_columns(
            {
                # Update Position based on Velocity and Acceleration
                "x": col("x") + col("vx") * dt + 0.5 * col("ax") * dt * dt,
                "y": col("y") + col("vy") * dt + 0.5 * col("ay") * dt * dt,
                "z": col("z") + col("vz") * dt + 0.5 * col("az") * dt * dt,
                # Update Velocity based on Acceleration
                "vx": col("vx") + col("ax") * dt,
                "vy": col("vy") + col("ay") * dt,
                "vz": col("vz") + col("az") * dt,
                # Acceleration remains constant (in this simple model)
                "ax": col("ax"),
                "ay": col("ay"),
                "az": col("az"),
                "red_herring": lit("foo"),
            }
        )

        # Queue the update using the updater
        self.world.commit(update_df, components) # requires list of component types

class MomentumProcessor(Processor):
    pass

class EnergyProcessor(Processor):
    """Updates Kinetic Energy based on Velocity and Mass."""

    def process(self, dt: float, *args, **kwargs) -> None:
        components = [Velocity, Mass, KineticEnergy]
        self.state_df = self.world.get_components(components)

        

        # Calculate updates using Daft expressions
        update_df_plan = self.state_df.with_columns(
            {
                # Update Kinetic Energy based on Velocity and Mass
                "kex": 0.5 * col("mass") * (col("vx") + col("ax") * dt) * (col("vx") + col("ax") * dt),
                "key": 0.5 * col("mass") * (col("vy") + col("ay") * dt) * (col("vy") + col("ay") * dt),
                "kez": 0.5 * col("mass") * (col("vz") + col("az") * dt) * (col("vz") + col("az") * dt),
            }
        )

class CollisionProcessor(Processor):
    def __init__(self, querier: EcsQueryManager, updater: EcsUpdateManager):
        super().__init__(querier, updater)

    def process(self, dt: float, *args, **kwargs) -> None:
        components = [Position] 
        self.state_df = self.querier.get_components(components)


class RenderProcessor:
    def __init__(self, renderer, clear_color=(0, 0, 0)):
        super().__init__()
        self.renderer = renderer
        self.clear_color = clear_color

    def process(self):
        # Clear the window:
        self.renderer.clear(self.clear_color)
        # Create a destination Rect for the texture:
        destination = SDL_Rect(0, 0, 0, 0)
        # This will iterate over every Entity that has this Component, and blit it:
        for ent, rend in esper.get_component(Renderable):
            destination.x = int(rend.x)
            destination.y = int(rend.y)
            destination.w = rend.w
            destination.h = rend.h
            SDL_RenderCopy(self.renderer.renderer, rend.texture, None, destination)
        self.renderer.present()        

# --- Pyglet Visualization Setup ---
class WindowProcessor(Processor):
    def __init__(self, window_width: int, window_height: int):
        super().__init__(world)

        self.window_width = window_width
        self.window_height = window_height
        self.window = pyglet.window.Window(WINDOW_WIDTH, WINDOW_HEIGHT, caption='Daft ECS Simulation')

    def process(self, dt: float, *args, **kwargs) -> None:
        pass

WINDOW_WIDTH = 800
WINDOW_HEIGHT = 600
window = pyglet.window.Window(WINDOW_WIDTH, WINDOW_HEIGHT, caption='Daft ECS Simulation')
batch = graphics.Batch()
entity_shapes = {} # Maps entity_id -> pyglet shape

# --- Main Test Execution ---
if __name__ == "__main__":
    print("Starting ECS Test with Pyglet Visualization...")

    # 1. Define World Instance with the System
    world = EcsWorld(system=SequentialSystem())

    # 2. Add Processors to the World
    world.add_processor(PhysicsProcessor())

    # 3. Define Entities with Initial Component State
    print("\nCreating entities...")

    # Create 100 entities with random initial states within window bounds
    for _ in range(100):
        entity = world.create_entity()
        world.add_component(entity, Position(x=random.randint(0, WINDOW_WIDTH), y=random.randint(0, WINDOW_HEIGHT), z=0.0))
        world.add_component(entity, Velocity(vx=random.randint(-10, 10), vy=random.randint(-10, 10), vz=0.0))
        world.add_component(entity, Acceleration(ax=0.0, ay=-9.8, az=0.0)) # Simple gravity
        world.add_component(entity, Mass(mass=random.uniform(0.5, 5.0))) # Random mass
        # Add other required components needed by PhysicsProcessor
        world.add_component(entity, Momentum())
        world.add_component(entity, Force())
        world.add_component(entity, KineticEnergy())

    # 4. Run the initial process step to commit initial components
    print("\nRunning initial processing step (commit initial state)...")
    world.process(t=0.0) # dt=0 for initial commit

    # 5. Pyglet Update Function
    def update(dt):
        """Handles simulation step and shape updates."""
        # Run one ECS step
        world.process(dt)

        # Get the latest position data
        # Query only for Position, as that's needed for visualization
        pos_df_plan = world.get_components([Position])

        if pos_df_plan is None:
            print("Update: No Position components found.")
            return

        pos_df_collected = pos_df_plan.collect()
        pos_data = pos_df_collected.to_pydict() # Easier iteration

        if not pos_data or 'entity_id' not in pos_data:
             print("Update: Position data collected is empty or missing 'entity_id'.")
             return
             
        current_entity_ids = set(pos_data['entity_id'])

        # Update existing shapes and create new ones
        num_entities = len(pos_data['entity_id'])
        for i in range(num_entities):
            entity_id = pos_data['entity_id'][i]
            x = pos_data['x'][i]
            y = pos_data['y'][i]
            # z = pos_data['z'][i] # Not used in 2D visualization

            if entity_id not in entity_shapes:
                # Create new shape if entity is new
                # Draw circles in the batch
                new_shape = shapes.Circle(x, y, radius=5, batch=batch)
                entity_shapes[entity_id] = new_shape
                print(f"Created shape for entity {entity_id}")
            else:
                # Update existing shape's position
                shape = entity_shapes[entity_id]
                shape.x = x
                shape.y = y
                # Optionally update color, radius based on other components

        # Optional: Remove shapes for entities that no longer exist
        ids_to_remove = set(entity_shapes.keys()) - current_entity_ids
        for entity_id in ids_to_remove:
            print(f"Removing shape for entity {entity_id}")
            shape_to_remove = entity_shapes.pop(entity_id)
            shape_to_remove.delete() # Remove from batch and rendering

    # 6. Pyglet Draw Event
    @window.event
    def on_draw():
        """Clears the window and draws the batch."""
        window.clear()
        batch.draw()

    # 7. Schedule the update function and run the app
    print("\nStarting Pyglet event loop...")
    clock.schedule_interval(update, 1/60.0) # Aim for 60 updates per second
    pyglet.app.run()

    print("\nSimulation finished.")
    # (Optional) Final state verification can be added here if needed,
    # but the visualization serves as the primary output now.
