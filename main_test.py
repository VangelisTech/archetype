# -*- coding: utf-8 -*-
"""
Toy example to test the Daft-Ray ECS implementation.
"""

import time
# from dataclasses import dataclass, field # No longer needed for components

# Import core ECS components from the 'core' directory
from core.base import Component, Processor, System
from core.world import EcsWorld
from core.managers import EcsQueryInterface, EcsUpdateManager
from core.system import SequentialSystem # Import SequentialSystem
# Import Daft for DataFrame operations
import daft
import pyarrow as pa
import daft.expressions as F # Import F

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


# --- Define a simple Processor ---
class PhysicsProcessor(Processor): # Processors simply implement a "process" method
    """Updates Position based on Velocity."""
    priority = 10 # Give it a priority (Optional)

    def process(self, querier: EcsQueryInterface, updater: EcsUpdateManager, dt: float, *args, **kwargs) -> None:
        
        # Define required components for this processor
        components = [
            Position, 
            Velocity, 
            Acceleration, 
            Mass, 
            Momentum, 
            Force, 
            KineticEnergy
        ]
        # Get Entities with all required Components
        df_plan = querier.get_components(components) 
        
        # Avoid processing if the DataFrame plan is None or likely empty
        # (A more robust check might involve collecting a small sample or schema)
        if df_plan is None:
             print("PhysicsProcessor: No entities found with required components.")
             return

        # Need to collect before checking emptiness reliably, but this adds overhead.
        # A potential optimization is to let Daft handle empty DFs in `with_columns`.
        # For now, assume `get_components` returns a valid plan even if empty later.

        # Calculate updates using Daft expressions
        update_df_plan = df_plan.with_columns(
            {
                # Update Position based on Velocity and Acceleration
                "x": F.col("x") + F.col("vx") * dt + 0.5 * F.col("ax") * dt * dt,
                "y": F.col("y") + F.col("vy") * dt + 0.5 * F.col("ay") * dt * dt,
                "z": F.col("z") + F.col("vz") * dt + 0.5 * F.col("az") * dt * dt,
                # Update Velocity based on Acceleration
                "vx": F.col("vx") + F.col("ax") * dt,
                "vy": F.col("vy") + F.col("ay") * dt,
                "vz": F.col("vz") + F.col("az") * dt,
                # Acceleration remains constant (in this simple model)
                "ax": F.col("ax"),
                "ay": F.col("ay"),
                "az": F.col("az"),
                # Update Momentum based on new Velocity and Mass
                "px": (F.col("vx") + F.col("ax") * dt) * F.col("mass"),
                "py": (F.col("vy") + F.col("ay") * dt) * F.col("mass"),
                "pz": (F.col("vz") + F.col("az") * dt) * F.col("mass"),
                 # Update Force (F=ma, assuming mass is constant)
                "fx": F.col("ax") * F.col("mass"),
                "fy": F.col("ay") * F.col("mass"),
                "fz": F.col("az") * F.col("mass"),
                # Update Kinetic Energy based on new Velocity and Mass
                "kex": 0.5 * F.col("mass") * (F.col("vx") + F.col("ax") * dt) * (F.col("vx") + F.col("ax") * dt),
                "key": 0.5 * F.col("mass") * (F.col("vy") + F.col("ay") * dt) * (F.col("vy") + F.col("ay") * dt),
                "kez": 0.5 * F.col("mass") * (F.col("vz") + F.col("az") * dt) * (F.col("vz") + F.col("az") * dt),
            }
        )

        # Queue the update using the updater
        # The updater expects the list of component types being updated
        updater.add_update(components, update_df_plan)


# --- Pyglet Visualization Setup ---
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
    world.process(dt=0.0) # dt=0 for initial commit

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
