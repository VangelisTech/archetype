# -*- coding: utf-8 -*-
"""
Toy example to test the Daft-Ray ECS implementation with interface segregation.
"""

import random
from typing import Optional # Import Optional
# Import core ECS components
from core import Component
# Import interfaces and container modules
from core.interfaces import ProcessorInterface # Use interface for type hint if needed
from core.container_interface import CoreContainerInterface
from core.container import CoreContainer # Import the implementation container
import core # Import the core module itself for wiring

# Import Daft for DataFrame operations
import daft
from daft import col

# --- Define Components ---
class Position(Component):
    x: float = 0.0 # m
    y: float = 0.0 # m

class Velocity(Component):
    vx: float = 0.0 # m/s
    vy: float = 0.0 # m/s

class Radius(Component):
    radius: float = 5.0 # m

# --- Define Processors ---
# Processors should now implicitly implement ProcessorInterface via core.Processor
# (assuming core.Processor was updated correctly)
class MovementProcessor(core.processors.Processor): # Inherit from concrete base which implements interface
    """Updates Position based on Velocity."""

    # process method remains largely the same, world is WorldInterface
    def process(self, dt: float) -> Optional[daft.DataFrame]: # Return the dataframe or None
        # Get the latest state of the components
        components = [Position, Velocity]
        # Need to get components relevant only to this processor
        # This might require filtering or assuming the system provides relevant entities
        state_df = self.world.get_components(*components) # Use injected world (interface)

        # Calculate 2D Frictionless Kinematics
        update_df = state_df.with_columns(
            {
                # Update Position based on Velocity and Acceleration
                "x": col("x") + col("vx") * dt,
                "y": col("y") + col("vy") * dt,
            }
        )


        return update_df.select(state_df.column_names)


class WallCollisionProcessor(core.processors.Processor): # Inherit from concrete base
    """Handles collision detection and resolution."""
    # Wall Bounds
    def __init__(self, wall_width: float, wall_height: float):
        super().__init__() # DI injects world interface here
        self.min_x = 0.0
        self.max_x = wall_width
        self.min_y = 0.0
        self.max_y = wall_height


    def process(self, dt: float) -> Optional[daft.DataFrame]: # Return the dataframe or None
        components = [Position, Velocity, Radius]
        state_df = self.world.get_components(*components) # Use injected world (interface)

        if state_df is None or len(state_df) == 0:
            return None

        # Wall Collision Detection (Find entities that are colliding with the walls)
        collision_check_df = state_df.with_columns(
            {
                "hit_left_wall": (col("x") - col("radius")) < self.min_x,
                "hit_right_wall": (col("x") + col("radius")) > self.max_x,
                "hit_bottom_wall": (col("y") - col("radius")) < self.min_y,
                "hit_top_wall": (col("y") + col("radius")) > self.max_y,
            }
        )

        collision_check_df = collision_check_df.with_columns({
             "flip_vx": col("hit_left_wall") | col("hit_right_wall"),
             "flip_vy": col("hit_bottom_wall") | col("hit_top_wall")
        })

        # Only select entities that actually hit a wall
        collided_df = collision_check_df.where(col("flip_vx") | col("flip_vy"))

        if collided_df is None or len(collided_df) == 0:
            return None

        # Calculate the updated velocities using conditional logic only for collided entities
        update_df = collided_df.with_columns({
                                  "vx": col("flip_vx").if_else(col("vx") * -1, col("vx")),
                                  "vy": col("flip_vy").if_else(col("vy") * -1, col("vy"))
                              })

        # PROCESSOR SHOULD RETURN THE UPDATE, NOT COMMIT IT
        # Return only relevant columns for update (entity_id, is_active?, vx, vy?)
        # Need to know what UpdateManager expects. Let's return updated vx, vy + keys
        return update_df.select("entity_id", "step", "is_active", "vx", "vy") # Adjust columns as needed


# --- Main Test Execution ---
if __name__ == "__main__":
    # --- Dependency Injection Setup ---
    # 1. Create the INTERFACE container instance
    container_interface = CoreContainerInterface()

    # 2. Ensure the IMPLEMENTATION container module is loaded 
    #    (just importing it is enough for @containers.override to work)
    _ = CoreContainer # Reference the implementation to prevent removal

    # 3. Wire the INTERFACE container to modules where @inject is used
    container_interface.wire(modules=[
        __name__,
        core.processors,
        core.systems,
        core.world,
        core.managers,
        core.store # Add store? Check if ComponentStore uses @inject (it doesn't currently)
    ])
    # --- End Dependency Injection Setup ---

    # Configuration (remains the same)
    WINDOW_WIDTH = 1000.0
    WINDOW_HEIGHT = 1000.0
    MAX_RADIUS = 5.0

    # 4. Get the World instance FROM THE INTERFACE CONTAINER
    world = container_interface.world() # Get managed World instance (interface)

    # 5. Add Processors 
    #    Instances are created manually. DI handles injecting world interface.
    world.add_processor(MovementProcessor())
    world.add_processor(WallCollisionProcessor(wall_width=WINDOW_WIDTH, wall_height=WINDOW_HEIGHT))

    # Create entities (remains the same, using the world instance from DI)
    for _ in range(20):
        # Use the world instance obtained from the container
        entity = world.add_entity(components=[
            Position(x=random.uniform(0.0 + MAX_RADIUS, WINDOW_WIDTH-MAX_RADIUS), y=random.uniform(0.0 + MAX_RADIUS, WINDOW_HEIGHT-MAX_RADIUS)),
            Velocity(vx=random.uniform(-10.0, 10.0), vy=random.uniform(-10.0, 10.0)),
            Radius(radius=random.uniform(1.0, MAX_RADIUS))
        ])

    # Run the simulation (remains the same)
    for i in range(10):
        print(f"--- Running Step {i} ---")
        world.step(dt=0.01)

    print("\n--- Final State History ---")
    # Access querier via the world instance from the container
    # Note: Accessing _querier directly assumes it's accessible; might need a public method on WorldInterface
    final_df = world._querier.get_combined_state_history(Position, Velocity, Radius)
    if final_df is not None and len(final_df) > 0:
        final_df.show()
        print(f"\nTotal Entities in History: {len(final_df)}")
    else:
        print("No final state history generated.")

    print("\n--- Latest Active State ---")
    latest_df = world.get_components(Position, Velocity, Radius)
    if latest_df is not None and len(latest_df) > 0:
        latest_df.show()
        print(f"\nTotal Active Entities: {len(latest_df)}")
    else:
        print("No active entities found.")

