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

# --- Ray Setup (REMOVED) ---
# # Connect to existing Ray instance in Anyscale
# import ray
# if not ray.is_initialized():
# ... (removed Ray init block) ...
# else:
#     print("Ray already initialized.")

# --- Define Components ---
# Components inherit from Component (which is now a LanceModel)
class Position(Component):
    x: float = 0.0
    y: float = 0.0

# Components inherit from Component (which is now a LanceModel)
class Velocity(Component):
    vx: float = 0.0
    vy: float = 0.0

# --- Define a simple Processor ---
class MovementProcessor(Processor):
    """Updates Position based on Velocity."""
    priority = 10 # Give it a priority

    def process(self, querier: EcsQueryInterface, updater: EcsUpdateManager, dt: float, *args, **kwargs) -> None:
        # Get entities with Position and Velocity fields
        movable_df_plan = querier.get_components(Position, Velocity)

        # Calculate new x and y values directly
        # Assumes get_components returns a DataFrame with columns: entity_id, x, y, vx, vy
        update_plan = movable_df_plan.with_columns(
            {
                "x": F.col("x") + F.col("vx") * dt,
                "y": F.col("y") + F.col("vy") * dt,
            }
        ).select("entity_id", "x", "y") # Select required fields for Position update
        
        # Queue the update using the updater
        # UpdateManager will ensure only entity_id, x, y are kept if others were selected
        updater.add_update(Position, update_plan)


# --- Main Test Execution ---
if __name__ == "__main__":
    print("Starting ECS Test...")

    # 1. Initialize the System (Use SequentialSystem)
    try:
        ecs_system = SequentialSystem()
    except Exception as e:
        print(f"Error initializing SequentialSystem: {e}")
        exit()

    # 2. Initialize the World with the System
    world = EcsWorld(system=ecs_system)

    # 3. Register Component Types (optional, often handled implicitly)
    world.register_component(Position)
    world.register_component(Velocity)

    # 4. Create Entity Types
    movable_type = world.create_entity_type("Movable", allowed_components={Position, Velocity})

    # 5. Add Processors to the World (which delegates to the System)
    world.add_processor(MovementProcessor())

    # 6. Create Entities
    print("\nCreating entities...")
    entity1 = world.create_typed_entity(
        "Movable",
        Position(x=10.0, y=20.0),
        Velocity(vx=1.0, vy=-0.5)
    )
    entity2 = world.create_typed_entity(
        "Movable",
        Position(x=-5.0, y=0.0),
        Velocity(vx=0.0, vy=2.0)
    )
    # Entity that won't move (missing Velocity)
    entity3 = world.create_typed_entity(
         "Movable", # Still allowed type
         Position(x=100.0, y=100.0)
         # No Velocity component added initially
    )
    # Add Velocity later to test updates
    world.add_component(entity3, Velocity(vx=-1, vy=-1))

    print(f"Entities created: {entity1}, {entity2}, {entity3}")

    # 7. Run the initial process step to commit initial components
    print("\nRunning initial processing step (commit initial state)...")
    world.process(dt=0.0) # dt=0 for initial commit

    # 8. Verify Initial State (using get_component to get latest active state)
    print("\n--- Initial State Verification ---")
    # Get plans for the latest active state
    pos_df_plan = world.get_component(Position)
    vel_df_plan = world.get_component(Velocity)

    # Collect the DataFrames to show them
    pos_df = pos_df_plan.collect() if pos_df_plan is not None else None
    vel_df = vel_df_plan.collect() if vel_df_plan is not None else None

    if pos_df:
        print("Initial Positions:")
        pos_df.show()
    else:
        print("No initial Position data found.") # Adjusted message

    if vel_df:
        print("Initial Velocities:")
        vel_df.show()
    else:
        print("No initial Velocity data found.") # Adjusted message

    print("----------------------------------")

    # 9. Run the simulation loop a few times
    print("\nRunning simulation steps...")
    for i in range(5):
        step_num = i + 1
        print(f"\n===== Simulation Step {step_num} =====")
        dt = 0.1 # Example time delta
        world.process(dt)

        # Optional: Query and print state after each step for debugging
        pos_df_step_plan = world.get_component(Position) # Get latest active state plan
        if pos_df_step_plan:
            print(f"\n--- State after Step {step_num} ---")
            print("Positions:")
            # Collect the plan before showing
            pos_df_collected = pos_df_step_plan.collect()
            pos_df_collected.show()
            print("-----------------------------")
        else:
             print(f"\n--- No Position data after Step {step_num} ---")
        # Small delay to make output readable and allow Ray background tasks
        time.sleep(0.1)


    print("\nSimulation finished.")

    # 10. (Optional) Further Verification
    print("\n--- Final State Verification ---")
    final_pos_entity1 = world.component_for_entity(entity1, Position)
    print(f"Final Position for Entity {entity1}: {final_pos_entity1}")

    final_pos_entity2 = world.component_for_entity(entity2, Position)
    print(f"Final Position for Entity {entity2}: {final_pos_entity2}")

    final_pos_entity3 = world.component_for_entity(entity3, Position)
    print(f"Final Position for Entity {entity3}: {final_pos_entity3}")

    # Example of deleting an entity
    # print("\nTesting entity deletion...")
    # world.delete_entity(entity2) # Mark for deletion
    # world.process(dt=0.1) # Run one more step to process deletion
    # print(f"Entity 2 exists after delete step? {world.entity_exists(entity2)}")
    # final_pos_df = world.get_collected_component_data(Position)
    # if final_pos_df:
    #     print("Positions after deletion step:")
    #     final_pos_df.show()


    # Remember to shutdown Ray if you initialized it explicitly and want to clean up
    # ray.shutdown() # If RayDagSystem didn't handle it or you want explicit control
