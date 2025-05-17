from archetype import make_world, Component, Processor, processor
from daft import col, DataFrame
from daft.session import Session
from daft import Catalog
from pyiceberg.catalog.sql import SqlCatalog
import pytest 
import asyncio
import os
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
async def test_toy(sim_name, tmpdir): # Make the test function async
    
    # create a pyiceberg catalog backed by sqlite
    catalog = SqlCatalog(
        "default",
        **{
            "uri": f"sqlite:///{tmpdir}/catalog.db",
            "warehouse": f"file://{tmpdir}",
        },
    )

    world = await make_world(uri=tmpdir, simulation=sim_name, catalog=catalog) # Await the async make_world

    # Add Processors
    # Pass an instance if your system expects an instance
    world.add_processor(MovementProcessor())

    # Spawn Entities
    world.spawn(Position(x=0.0, y=0.0), Velocity(vx=1.0, vy=1.0))
    world.spawn(Position(x=10.0, y=5.0), Velocity(vx=2.0, vy=-1.0))
    world.spawn(Position(x=-5.0, y=-10.0), Velocity(vx=-0.5, vy=0.5))

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

if __name__ == "__main__":
    tmpdir = os.path.join(os.path.dirname(__file__), "tmp")
    sim_name = __file__.__name__

    # Run the test
    asyncio.run(test_toy(sim_name, tmpdir))
