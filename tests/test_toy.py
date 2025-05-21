from archetype import make_simple_world, Component, Processor, processor
from daft import col, DataFrame
import pytest

# Define Components
class Position(Component):
    x: float
    y: float

class Velocity(Component):
    vx: float
    vy: float

@processor(Position, Velocity, priority=1)
class MovementProcessor(Processor):
    def process(self, df: DataFrame, dt: float):
        return df.with_columns({
            "position__x": col("position__x") + col("velocity__vx") * dt,
            "position__y": col("position__y") + col("velocity__vy") * dt,
        })

@pytest.fixture
def sim_name() -> str:
    """Provides a default simulation name for tests."""
    return "test_toy_sim"

@pytest.fixture
def db_uri(tmp_path) -> str:
    """Provides a URI to a temporary directory for the SQLite database."""
    # tmp_path is a pathlib.Path object provided by pytest
    # ArchetypeStore will append its own filename (e.g., catalog.db) to this path
    return str(tmp_path) # Return the directory path


def test_toy(sim_name: str, db_uri: str): # Use the fixtures
    world = make_simple_world(uri=db_uri, simulation=sim_name)

    # Add Processors
    world.add_processor(MovementProcessor())

    # Spawn Entities
    world.spawn(Position(x=0.0, y=0.0), Velocity(vx=1.0, vy=1.0))
    world.spawn(Position(x=10.0, y=5.0), Velocity(vx=2.0, vy=-1.0))
    world.spawn(Position(x=-5.0, y=-10.0), Velocity(vx=-0.5, vy=0.5))

    # Run some steps
    for _ in range(10):
        world.step(dt=0.1)

    # Query back results
    session = world.get_session()
    tables = session.list_tables()
    assert len(tables) == 1, "Should be exactly one table."
    
    position_df = session.read_table(tables[0])
    assert position_df is not None, "Query should return a DataFrame, not None."
    assert len(position_df) == 3 # Assuming 3 entities were spawned

if __name__ == "__main__":
    test_toy()