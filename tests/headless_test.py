# -*- coding: utf-8 -*-
"""
Toy example to test the Daft-Ray ECS implementation with interface segregation.
"""

import random
from typing import Optional, List, Type # Import List, Type
# Import core ECS components
from archetype import Component
# Import interfaces and container modules
from core.interfaces import ProcessorInterface # Use interface for type hint if needed
from core.container_interface import CoreContainerInterface
from core.container import CoreContainer # Import the implementation container
import archetype # Import the core module itself for wiring
from core.processors import Processor # Import the updated base Processor

# Import Daft for DataFrame operations
import daft
from daft import col, DataFrame # Import DataFrame

# --- Define Components ---
class Position(Component):
    x: float = 0.0 # m
    y: float = 0.0 # m

class Velocity(Component):
    vx: float = 0.0 # m/s
    vy: float = 0.0 # m/s

# Nested Component
class Kinematics(Component):
    position: Position
    velocity: Velocity

class Radius(Component):
    radius: float = 5.0 # m


# --- Define Processors ---

class KinematicsProcessor(Processor):
    """Processor that operates directly on the Kinematics component."""
    def __init__(self):
        super().__init__()
        # Declare component used
        self._components_used = [Kinematics]

    def process(self, dt: float, state_df: DataFrame) -> Optional[DataFrame]:
        """Applies movement calculation based on Kinematics data."""


        # Access nested fields using the prefixed component name
        kin_pos = col("kinematics.position")
        kin_vel = col("kinematics.velocity")

        pos_x = kin_pos.struct.get("x")
        pos_y = kin_pos.struct.get("y")
        vel_vx = kin_vel.struct.get("vx")
        vel_vy = kin_vel.struct.get("vy")

        # Calculate new position fields
        new_pos_x = (pos_x + vel_vx * dt).alias("x")
        new_pos_y = (pos_y + vel_vy * dt).alias("y")

        # Reconstruct the updated position struct
        updated_position_struct = daft.struct([new_pos_x, new_pos_y]).alias("position")

        # Reconstruct the *entire* Kinematics struct with the updated position part
        # We need to provide the full structure expected by the Kinematics component store.
        update_df = state_df.with_column(
            "kinematics", # The prefixed name for the updated component
            daft.struct([
                updated_position_struct,
                kin_vel # Keep original velocity struct
            ])
        )

        # Return only entity_id and the changed component (prefixed)
        return update_df.select(col("entity_id"), col("kinematics"))


class WallCollisionProcessor(Processor):
    """Processor for handling wall collisions."""
    def __init__(self, min_x: float, max_x: float, min_y: float, max_y: float):
        super().__init__()
        # Now depends on Kinematics (for pos/vel) and Radius
        self._components_used = [Kinematics, Radius]
        self.min_x = min_x
        self.max_x = max_x
        self.min_y = min_y
        self.max_y = max_y

    def process(self, dt: float, state_df: DataFrame) -> Optional[DataFrame]:
        """Checks for wall collisions and reverses velocity if needed."""

        # Access nested fields via Kinematics and direct field via Radius
        kin_pos = col("kinematics.position")
        kin_vel = col("kinematics.velocity")
        rad = col("radius.radius")

        pos_x = kin_pos.struct.get("x")
        pos_y = kin_pos.struct.get("y")
        vel_vx = kin_vel.struct.get("vx")
        vel_vy = kin_vel.struct.get("vy")

        # Wall Collision Detection
        collision_check_df = state_df.with_columns(
            {
                "hit_left_wall": (pos_x - rad) < self.min_x,
                "hit_right_wall": (pos_x + rad) > self.max_x,
                "hit_bottom_wall": (pos_y - rad) < self.min_y,
                "hit_top_wall": (pos_y + rad) > self.max_y,
                # Keep original values needed for update calculation
                "orig_vx": vel_vx,
                "orig_vy": vel_vy
            }
        )

        collision_check_df = collision_check_df.with_columns({
             "flip_vx": col("hit_left_wall") | col("hit_right_wall"),
             "flip_vy": col("hit_bottom_wall") | col("hit_top_wall")
        })

        # Filter to entities that actually hit a wall
        collided_df = collision_check_df.where(col("flip_vx") | col("flip_vy"))

        # Calculate new velocity fields for collided entities
        new_vel_vx = col("flip_vx").if_else(col("orig_vx") * -1, col("orig_vx")).alias("vx")
        new_vel_vy = col("flip_vy").if_else(col("orig_vy") * -1, col("orig_vy")).alias("vy")

        # Reconstruct the updated velocity struct
        updated_velocity_struct = daft.struct([new_vel_vx, new_vel_vy]).alias("velocity")

        # Reconstruct the *entire* Kinematics struct with the updated velocity part
        update_df = collided_df.with_column(
             "kinematics", # Prefixed name for the updated component
             daft.struct([
                 kin_pos, # Keep original position struct
                 updated_velocity_struct
             ])
        )

        # Return only entity_id and the changed component (prefixed)
        return update_df.select(col("entity_id"), col("kinematics"))


# --- Main Test Execution ---
if __name__ == "__main__":
    # --- Dependency Injection Setup ---
    # 1. Create the INTERFACE container instance
    container_interface = CoreContainerInterface()

    # 2. Ensure the IMPLEMENTATION container module is loaded
    _ = CoreContainer # Reference the implementation to prevent removal

    # 3. Wire the INTERFACE container to modules where @inject is used
    container_interface.wire(modules=[
        __name__,
        archetype.processors, # Wire core.processors where Processor base class is
        archetype.system,
        archetype.world,
        archetype.managers,
        archetype.store
    ])
    # --- End Dependency Injection Setup ---

    # Configuration
    WINDOW_WIDTH = 1000.0
    WINDOW_HEIGHT = 1000.0
    MAX_RADIUS = 5.0

    # 4. Get the World instance FROM THE INTERFACE CONTAINER
    # Ensure the world provider uses the config if needed (e.g., for verbosity)
    # world = container_interface.world(config={'verbose': True}) # Example if config passed to world
    world = container_interface.world() # Get managed World instance (interface)

    # 5. Add Processors
    world.add_processor(KinematicsProcessor())
    world.add_processor(WallCollisionProcessor(
        min_x=0.0,
        max_x=WINDOW_WIDTH,
        min_y=0.0,
        max_y=WINDOW_HEIGHT
    ))

    # Create entities
    for i in range(20):
        entity = world.add_entity(components=[
            Kinematics(
                position=Position(x=random.uniform(0.0 + MAX_RADIUS, WINDOW_WIDTH-MAX_RADIUS), y=random.uniform(0.0 + MAX_RADIUS, WINDOW_HEIGHT-MAX_RADIUS)),
                velocity=Velocity(vx=random.uniform(-10.0, 10.0), vy=random.uniform(-10.0, 10.0))
            ),
            Radius(radius=random.uniform(1.0, MAX_RADIUS))
        ])
 

    # Run the simulation
    for i in range(10):
        print(f"--- Running Step {i} --- ({world.current_step} before step)") # Check current step
        world.step(dt=0.01)

    print("\n--- Final State History --- Check via Querier ---")
    # Access querier via the world instance from the container
    final_df = world.querier.get_combined_state_history(Kinematics, Radius)
    if final_df is not None and len(final_df) > 0:
        print(f"History DataFrame columns: {final_df.column_names}")
        final_df.show()
        print(f"\nTotal Rows in History: {len(final_df)}")
    else:
        print("No final state history generated or returned by querier.")

    print("\n--- Latest Active State --- Check via World Facade ---")
    latest_df = world.get_components(Position, Velocity, Radius)
    if latest_df is not None and len(latest_df) > 0:
        print(f"Latest State DataFrame columns: {latest_df.column_names}")
        latest_df.show()
        print(f"\nTotal Active Entities: {len(latest_df)}")
    else:
        print("No active entities found.")

