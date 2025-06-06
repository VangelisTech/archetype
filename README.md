# Archetype

**A high-performance Entity Component System (ECS) for large scale simulation**

![Archetype Diagram](./assets/archetype_diagram.png)

Archetype is an ECS simulation engine designed for scalability from local development to distributed Ray clusters. It leverages incredible performance of daft dataframes and lancedb to provide big data scalability for Multi-modal AI processors.

The [archetype pattern](https://ajmmertens.medium.com/building-an-ecs-2-archetypes-and-vectorization-fe21690805f9), leverages entity creation definitions based on exact combinations of components for powerful data processing isolation and decoupling.


## Quick Start

### Installation

```bash
# Clone the repository
git clone <repository-url>
cd archetype

# Install with uv (recommended)
uv sync

# Or with pip
pip install -e .
```

### Basic Usage

```python
from archetype.core import Component, processor, Processor, make_simple_world
from daft import DataFrame, col

# Define components
class Position(Component):
    x: float
    y: float

class Velocity(Component):
    vx: float
    vy: float

# Define processors
@processor(Position, Velocity, priority=1)
class MovementProcessor(Processor):
    def process(self, df: DataFrame, dt: float) -> DataFrame:
        return df.with_columns({
            "position__x": col("position__x") + col("velocity__vx") * dt,
            "position__y": col("position__y") + col("velocity__vy") * dt,
        })

# Create and run simulation
world = make_simple_world("./data")
world.system.add_processor(MovementProcessor())

# Spawn entities
world.spawn(Position(x=0, y=0), Velocity(vx=1, vy=1))
world.spawn(Position(x=10, y=10), Velocity(vx=-1, vy=-1))

# Run simulation
for step in range(100):
    world.step(dt=0.1)
```
