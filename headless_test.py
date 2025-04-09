# -*- coding: utf-8 -*-
"""
Toy example to test the Daft-Ray ECS implementation with interface segregation.
"""

import random
from typing import Optional, List, Type # Import List, Type
# Import core ECS components
from core import Component
# Import interfaces and container modules
from core.interfaces import ProcessorInterface # Use interface for type hint if needed
from core.container_interface import CoreContainerInterface
from core.container import CoreContainer # Import the implementation container
import core # Import the core module itself for wiring
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

class Radius(Component):
    radius: float = 5.0 # m

# --- Define Processors ---

class MovementProcessor(Processor): # Inherit from new base Processor
    """Updates Position based on Velocity using the preprocess->process pattern."""

    def __init__(self):
        super().__init__() # Handles world injection
        # *** Declare components used ***
        self._components_used: List[Type[Component]] = [Position, Velocity]

    # No need to implement get_components_used (base class handles it)
    # No need to implement preprocess (base class handles it)

    def process(self, dt: float, state_df: DataFrame) -> Optional[DataFrame]:
        """Applies movement calculation based on Velocity."""
        # state_df is provided by the base preprocess method
        # No need for empty checks here

        # Calculate 2D Frictionless Kinematics
        update_df = state_df.with_columns(
            {
                "x": col("x") + col("vx") * dt,
                "y": col("y") + col("vy") * dt,
            }
        )

        # Return *only* the updated columns + necessary keys ('entity_id')
        # The base preprocess should provide entity_id, is_active, step ? check QueryManager
        # Assume QueryManager provides entity_id, step, is_active
        # We modified Position (x, y)
        return update_df.select("entity_id", "x", "y")


class WallCollisionProcessor(Processor): # Inherit from new base Processor
    """Handles collision detection and resolution with walls."""

    def __init__(self, wall_width: float, wall_height: float):
        super().__init__() # Handles world injection
        self.min_x = 0.0
        self.max_x = wall_width
        self.min_y = 0.0
        self.max_y = wall_height
        # *** Declare components used ***
        self._components_used: List[Type[Component]] = [Position, Velocity, Radius]

    # No need to implement get_components_used (base class handles it)
    # No need to implement preprocess (base class handles it)

    def process(self, dt: float, state_df: DataFrame) -> Optional[DataFrame]:
        """Checks for wall collisions and reverses velocity if needed."""
        # state_df is provided by the base preprocess method
        # No need for empty checks here

        # Wall Collision Detection
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

        # Filter to entities that actually hit a wall
        collided_df = collision_check_df.where(col("flip_vx") | col("flip_vy"))

        # If no entities collided, return None to signal no update
        if len(collided_df) == 0:
            return None

        # Calculate updated velocities for collided entities
        update_df = collided_df.with_columns({
                                  "vx": col("flip_vx").if_else(col("vx") * -1, col("vx")),
                                  "vy": col("flip_vy").if_else(col("vy") * -1, col("vy"))
                              })

        # Return *only* the updated columns (vx, vy) + necessary keys ('entity_id')
        return update_df.select("entity_id", "vx", "vy")


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
        core.processors, # Wire core.processors where Processor base class is
        core.systems,
        core.world,
        core.managers,
        core.store
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
    world.add_processor(MovementProcessor())
    world.add_processor(WallCollisionProcessor(wall_width=WINDOW_WIDTH, wall_height=WINDOW_HEIGHT))

    # Create entities
    for i in range(20):
        entity = world.add_entity(components=[
            Position(x=random.uniform(0.0 + MAX_RADIUS, WINDOW_WIDTH-MAX_RADIUS), y=random.uniform(0.0 + MAX_RADIUS, WINDOW_HEIGHT-MAX_RADIUS)),
            Velocity(vx=random.uniform(-10.0, 10.0), vy=random.uniform(-10.0, 10.0)),
            Radius(radius=random.uniform(1.0, MAX_RADIUS))
        ])
        print(f"Added Entity {entity}") # Added print

    # Run the simulation
    for i in range(10):
        print(f"--- Running Step {i} --- ({world.current_step} before step)") # Check current step
        world.step(dt=0.01)

    print("\n--- Final State History --- Check via Querier ---")
    # Access querier via the world instance from the container
    final_df = world.querier.get_combined_state_history(Position, Velocity, Radius)
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

