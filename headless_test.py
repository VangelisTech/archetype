# -*- coding: utf-8 -*-
"""
Toy example to test the Daft-Ray ECS implementation.
"""

import random
# Import core ECS components from the 'core' directory
from core import Component, World, TimedProcessor

# Import Daft for DataFrame operations
from daft import col
from daft import functions as F



# --- Define Components ---
# Components inherit from Component (lancedb.pydantic.LanceModel)
class Position(Component):
    x: float = 0.0 # m
    y: float = 0.0 # m

class Velocity(Component):
    vx: float = 0.0 # m/s
    vy: float = 0.0 # m/s


class Radius(Component):
    radius: float = 5.0 # m

# --- Define Processors ---
class MovementProcessor(TimedProcessor): # Processors simply implement a "process" method
    """Updates Position based on Velocity."""
    
    def __init__(self):
        super().__init__()

    def process(self, dt: float) -> None:
        # Get the latest state of the components
        components = [Position, Velocity,Radius]
        state_df = self.world.get_components(components) # Naive to step 
        
        # Calculate 2D Frictionless Kinematics
        update_df = state_df.with_columns(
            {
                # Update Position based on Velocity and Acceleration
                "x": col("x") + col("vx") * dt,
                "y": col("y") + col("vy") * dt,
            }
        )

        # Queue the update using the updater
        self.world.commit(update_df, components) # requires list of component types


class WallCollisionProcessor(TimedProcessor):
    """Handles collision detection and resolution."""
    # Wall Bounds
    def __init__(self, wall_width: float, wall_height: float):
        super().__init__()
        self.min_x = 0.0
        self.max_x = wall_width
        self.min_y = 0.0
        self.max_y = wall_height


    def process(self, dt: float) -> None:
        components = [Position, Velocity, Radius] 
        state_df = self.world.get_components(components)

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
        
        # Calculate the updated velocities using conditional logic
        # Only update entities that actually hit a wall
        update_df = collision_check_df.where(col("flip_vx") | col("flip_vy")) \
                                      .with_columns({
                                          "vx": F.if_else(col("flip_vx"), col("vx") * -1, col("vx")),
                                          "vy": F.if_else(col("flip_vy"), col("vy") * -1, col("vy"))
                                      })

        self.world.commit(update_df, components)
        
        


# --- Main Test Execution ---
if __name__ == "__main__":
    WINDOW_WIDTH = 1000.0
    WINDOW_HEIGHT = 1000.0
    MAX_RADIUS = 5.0

    # 1. Define World Instance with the System
    world = World()

    # 2. Add Processors to the World (world is injected automatically)
    world.add_processor(MovementProcessor())
    world.add_processor(WallCollisionProcessor(wall_width=WINDOW_WIDTH, wall_height=WINDOW_HEIGHT))

    # Create 100 entities with random initial states within window bounds
    for _ in range(20):
        entity = world.add_entity()
        world.add_component(entity, Position(x=random.uniform(0.0, WINDOW_WIDTH-MAX_RADIUS), y=random.uniform(0.0, WINDOW_HEIGHT-MAX_RADIUS)))
        world.add_component(entity, Velocity(vx=random.uniform(-10.0, 10.0), vy=random.uniform(-10.0, 10.0)))
        world.add_component(entity, Radius(radius=random.uniform(1.0, MAX_RADIUS)))

    # Demonstrate adding components through world.add_entity()
    entity = world.add_entity(Position(), Velocity(), Radius(radius=10.0))
    
    # Run the simulation
    for _ in range(10):
        world.step(dt=0.01)

    df = world._querier.get_combined_state_history().show()
    
