import tempfile 
from archetype import make_simple_world, Component, Processor, processor

# Define Components (or import them if they are part of your public API)
class Position(Component):
    x: float
    y: float

class Velocity(Component):
    vx: float
    vy: float

@processor(Position, Velocity, priority=1)
class MovementProcessor(Processor):
    def process(self, df, dt: float): # Assuming df is passed by the system
        # Simplified for example; actual Daft usage might differ
        df["position__x"] = df["position__x"] + df["velocity__vx"] * dt
        df["position__y"] = df["position__y"] + df["velocity__vy"] * dt
        return df

def main():
    sim_name = "example_sim"
    
    with tempfile.TemporaryDirectory() as temp_dir_path:
        db_uri = str(temp_dir_path)
        
        print(f"Running example simulation: {sim_name}")
        print(f"Database directory: {db_uri}")

        world = make_simple_world(uri=db_uri, simulation=sim_name)
        world.add_processor(MovementProcessor())

        world.spawn(Position(x=0.0, y=0.0), Velocity(vx=1.0, vy=1.0))
        world.spawn(Position(x=10.0, y=5.0), Velocity(vx=2.0, vy=-1.0))

        for i in range(5):
            print(f"Stepping world (step {i+1})...")
            world.step(dt=0.1)

        position_df = world.query(Position)
        if position_df is not None:
            # Assuming Daft DataFrame, might need .collect() or similar to print
            print("\nFinal Positions:")
            position_df.show() 
        else:
            print("\nNo position data returned.")
            
        print("\nExample simulation finished.")

if __name__ == "__main__":
    main()