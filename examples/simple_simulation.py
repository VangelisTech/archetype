



import sys
import os

# Ensure the parent directory is in sys.path so 'archetype' can be imported
notebook_dir = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.getcwd()
project_root = os.path.abspath(os.path.join(notebook_dir, "..", "src"))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

    
from archetype.core import processor, Processor, Component
from archetype import make_simple_world

from daft import DataFrame, col 




# Define Components (or import them if they are part of your public API)
class Position(Component):
    x: float
    y: float

class Velocity(Component):
    vx: float
    vy: float

@processor(Position, Velocity, priority=1)
class MovementProcessor(Processor):
    def process(self, df: DataFrame, dt: float) -> DataFrame: # Assuming df is passed by the system
        df = df.with_columns({
            "position__x": col("position__x") + col("velocity__vx") * dt,
            "position__y": col("position__y") + col("velocity__vy") * dt,
        })
        return df

def main(uri, debug=False):
    
    world = make_simple_world(uri, debug=debug)
    world.add_processor(MovementProcessor())

    world.spawn(Position(x=0, y=0), Velocity(vx=1, vy=1))
    world.spawn(Position(x=0, y=0), Velocity(vx=2, vy=2))
    world.spawn(Position(x=0, y=0), Velocity(vx=3, vy=3))
    world.spawn(Position(x=0, y=0), Velocity(vx=4, vy=4))
    world.spawn(Position(x=0, y=0), Velocity(vx=5, vy=5))
    world.spawn(Position(x=0, y=0), Velocity(vx=6, vy=6))
    world.spawn(Position(x=0, y=0), Velocity(vx=7, vy=7))
    world.spawn(Position(x=0, y=0), Velocity(vx=8, vy=8))
    world.spawn(Position(x=0, y=0), Velocity(vx=9, vy=9))
    world.spawn(Position(x=0, y=0), Velocity(vx=10, vy=10))
    

    for i in range(10):
        world.step(dt=0.1)

    return world


if __name__ == "__main__":
    uri = "/Users/everett-founder/git/vangelis/internal/work/libs/archetype/data"

    world = main(uri, debug=True)

    