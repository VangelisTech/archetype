# -*- coding: utf-8 -*-
"""
Toy example to test the Daft-Ray ECS implementation.
"""

import time
from dataclasses import dataclass, field

# Import core ECS components from the 'core' directory
from core.base import Component, Processor, System
from core.world import EcsWorld
from core.managers import EcsQueryInterface, EcsUpdateManager
from core.system import RayDagSystem # Import our Ray system

# --- Define Components ---
@dataclass
class Position(Component):
    x: float = 0.0
    y: float = 0.0

@dataclass
class Velocity(Component):
    vx: float = 0.0
    vy: float = 0.0

# --- Define a simple Processor ---
class MovementProcessor(Processor):
    """Updates Position based on Velocity."""
    priority = 10 # Give it a priority

    def process(self, querier: EcsQueryInterface, updater: EcsUpdateManager, dt: float, *args, **kwargs) -> None:
        # Get entities with both Position and Velocity using the querier
        # This returns a Daft DataFrame plan
        movable_df_plan = querier.get_components(Position, Velocity)

        # Check if the plan is potentially empty early to avoid unnecessary computation
        # Note: This collect() might have performance implications if the join is large,
        # but for processors it can prevent submitting empty updates.
        # Consider removing this check if performance profiling shows it's a bottleneck.
        if len(movable_df_plan.collect()) == 0:
            # print("MovementProcessor: No entities with Position and Velocity found.")
            return

        print(f"MovementProcessor: Processing {len(movable_df_plan.collect())} entities...") # Re-collecting here for count, less efficient

        # Define Daft expressions for the update logic
        pos_col = daft.col("position")
        vel_col = daft.col("velocity")

        # Calculate new position
        new_pos_df = movable_df_plan.with_columns(
            {
                "new_x": pos_col["x"] + vel_col["vx"] * dt,
                "new_y": pos_col["y"] + vel_col["vy"] * dt,
            }
        )

        # Create the updated Position struct
        # Important: Include all fields of the Position component in the struct
        updated_position_struct = daft.expressions.struct(
            {
                "x": daft.col("new_x"),
                "y": daft.col("new_y")
                # Add other Position fields here if they existed
            }
        ).alias("position") # Alias must match the component struct name

        # Select only the entity_id and the updated struct
        position_update_plan = new_pos_df.select(
            daft.col("entity_id"), updated_position_struct
        )

        # Queue the update using the updater
        # The UpdateManager handles casting and final commit
        updater.add_update(Position, position_update_plan)
        # print(f"MovementProcessor: Queued Position updates.") # Can be noisy

# --- Main Test Execution ---
if __name__ == "__main__":
    print("Starting ECS Test...")

    # 1. Initialize the System (RayDagSystem in this case)
    # Assumes Ray is running or can be started locally
    try:
        ecs_system = RayDagSystem()
    except Exception as e:
        print(f"Error initializing RayDagSystem: {e}")
        print("Please ensure Ray is installed and can be initialized.")
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

    # 8. Verify Initial State (using get_collected_component_data)
    print("\n--- Initial State Verification ---")
    pos_df = world.get_collected_component_data(Position)
    vel_df = world.get_collected_component_data(Velocity)

    if pos_df:
        print("Initial Positions:")
        pos_df.show()
    else:
        print("No initial Position data collected.")

    if vel_df:
        print("Initial Velocities:")
        vel_df.show()
    else:
        print("No initial Velocity data collected.")

    print("----------------------------------")

    # 9. Run the simulation loop a few times
    print("\nRunning simulation steps...")
    for i in range(5):
        step_num = i + 1
        print(f"\n===== Simulation Step {step_num} =====")
        dt = 0.1 # Example time delta
        world.process(dt)

        # Optional: Query and print state after each step for debugging
        pos_df_step = world.get_collected_component_data(Position)
        if pos_df_step:
            print(f"\n--- State after Step {step_num} ---")
            print("Positions:")
            pos_df_step.show()
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
