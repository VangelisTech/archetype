from archetype import make_world, Component, Processor, processor
from daft import col, DataFrame

# Define Components

class Position(Component):
    x: float
    y: float

class Velocity(Component):
    vx: float
    vy: float

@processor(Position, Velocity, priority=1, )
class MovementProcessor(Processor):
    def process(self, df: DataFrame, dt: float):
        return df.with_columns({
            "position__x": col("position__x") + col("velocity__vx") * dt,
            "position__y": col("position__y") + col("velocity__vy") * dt,
        })

    def _fetch_state(self, world, step: int) -> daft.DataFrame:
        return world.get_components(*self.components_used, steps=step)

# Simulation setup
if __name__ == "__main__":
    sim_name = "toy_simulation"
    uri = "../data" 
    world = make_world(sim_name, uri)



    # Spawn some entities
    e1 = world.spawn([
        {'component': Position, 'data': {'x': 0.0, 'y': 0.0}},
        {'component': Velocity, 'data': {'vx': 1.0, 'vy': 1.0}},
        {'component': Acceleration, 'data': {'ax': 0.0, 'ay': 0.0}},
        {'component': Jerk, 'data': {'jx': 0.0, 'jy': 0.0}}
    ], step=0)
    e2 = world.spawn([
        {'component': Position, 'data': {'x': 0.0, 'y': 0.0}},
        {'component': Velocity, 'data': {'vx': 2.0, 'vy': 2.0}},
        {'component': Acceleration, 'data': {'ax': 0.0, 'ay': 0.0}},
        {'component': Jerk, 'data': {'jx': 0.0, 'jy': 0.0}}
    ], step=0)
    e3 = world.spawn([
        {'component': Position, 'data': {'x': 0.0, 'y': 0.0}},
        {'component': Velocity, 'data': {'vx': -3.0, 'vy': -3.0}},
        {'component': Acceleration, 'data': {'ax': 0.0, 'ay': 0.0}},
        {'component': Jerk, 'data': {'jx': 0.0, 'jy': 0.0}}
    ], step=0)

    for _ in range(10):
        world.step(dt=0.1)

    # Query back results
    df = world.get_components(Position, steps=1)
    if df:
        df.show()  # Shows each entity's up-to-date Position
