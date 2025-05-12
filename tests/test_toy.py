from archetype import make_world, Component, Processor, processor
from daft import col, DataFrame
import pytest 

# Define Components
class Position(Component):
    x: float
    y: float

class Velocity(Component):
    vx: float
    vy: float

@processor(Position, Velocity, priority=1) # Corrected: decorator takes a list
class MovementProcessor(Processor):
    def process(self, df: DataFrame, dt: float): # Assuming dt is passed by the system
        return df.with_columns({
            # Assuming column names from _fetch_state are namespaced
            "position__x": col("position__x") + col("velocity__vx") * dt,
            "position__y": col("position__y") + col("velocity__vy") * dt,
        })


@pytest.mark.asyncio 
async def test_toy(): # Make the test function async
    # Simulation setup
    sim_name = "toy_simulation_pytest"
    uri = "tests/data"
    # Ensure this directory exists or can be created by LanceDB, or use a temp path.
    
    world = await make_world(uri=uri, simulation=sim_name) # Await the async make_world

    # Add Processors
    # Pass an instance if your system expects an instance
    world.add_processor(MovementProcessor()) 

    # Spawn Entities 
    await world.spawn(Position(x=0.0, y=0.0), Velocity(vx=1.0, vy=1.0))
    await world.spawn(Position(x=10.0, y=5.0), Velocity(vx=2.0, vy=-1.0))
    await world.spawn(Position(x=-5.0, y=-10.0), Velocity(vx=-0.5, vy=0.5))

    # Run some steps
    for _ in range(10):
        await world.step(dt=0.1) # Pass dt to world.step

    # Query back results
    position_archetypes = await world.get_history(Position) 
    position_data_df = list(position_archetypes.values())
    position_data_df.show()
    # Basic assertion: Check if the query returned any data
    assert position_data_df is not None, "Query should return a DataFrame, not None."
    # Ensure that the DataFrame has a method to count rows, e.g. .count_rows() or len()
    # For Daft, it's len(df)
    assert len(position_data_df) > 0, "Query should return some position data."
    
    # For debugging, you can print the dataframe:
    # print("\nTest Query Result:")
    # print(position_data_df.to_pandas()) # if you want to see it as pandas
