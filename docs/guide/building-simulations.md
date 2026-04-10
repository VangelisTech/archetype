# Building Simulations

This is the full workflow: define components, write processors, wire them into a world, run it.

## The Pattern

Every simulation follows the same structure:

1. **Define components** -- the data your entities carry
2. **Write processors** -- the rules that transform that data each tick
3. **Create a world** and register processors
4. **Spawn entities** with initial component values
5. **Run** -- the engine drains commands, applies them, steps processors, persists state

## Complete Example

Agents gain experience each tick proportional to their skill. A second processor computes a rating. Copy this, run it.

```bash
uv run python examples/simulation_script.py
```

Source: [`examples/simulation_script.py`](https://github.com/VangelisTech/archetype/blob/main/examples/simulation_script.py)

```python
import asyncio
import daft
from daft import DataFrame, col
from archetype.core.aio.async_processor import AsyncProcessor
from archetype.core.aio.async_system import AsyncSystem
from archetype.core.aio.async_world import AsyncWorld
from archetype.core.component import Component
from archetype.core.config import RunConfig, WorldConfig


# ── Step 1: Define components ───────────────────────────────────────────

class Agent(Component):
    name: str = ""
    role: str = ""
    skill: float = 1.0
    experience: float = 0.0
    rating: float = 0.0


# ── Step 2: Write processors ───────────────────────────────────────────

class ExperienceProcessor(AsyncProcessor):
    """Each tick, agents gain experience proportional to their skill."""
    components = (Agent,)
    priority = 10

    async def process(self, df: DataFrame, **kwargs) -> DataFrame:
        return df.with_column(
            "agent__experience",
            col("agent__experience") + col("agent__skill") * 2.0,
        )


class RatingProcessor(AsyncProcessor):
    """Compute a rating from experience and skill."""
    components = (Agent,)
    priority = 50

    async def process(self, df: DataFrame, **kwargs) -> DataFrame:
        return df.with_column(
            "agent__rating",
            col("agent__experience") * col("agent__skill") / 10.0,
        )


# ── Step 3-5: Create world, spawn entities, run ────────────────────────

async def main():
    # (In-memory querier/updater omitted for brevity — see full source)
    ...
    await world.create_entity([Agent(name="Alice", role="engineer", skill=3.0)])
    await world.create_entity([Agent(name="Bob", role="designer", skill=2.0)])
    await world.create_entity([Agent(name="Charlie", role="manager", skill=1.5)])

    await world.run(RunConfig(num_steps=10))
```

Output:

```text
Alice:   skill=3.0, experience=60, rating=18.0
Bob:     skill=2.0, experience=40, rating=8.0
Charlie: skill=1.5, experience=30, rating=4.5
```

## Key Concepts

### Processors Declare Their Requirements

A processor's `components` tuple says which entities it operates on. The engine routes the right data to the right processor automatically.

```python
class MoveProcessor(AsyncProcessor):
    components = (Agent, Position)  # Only entities with BOTH
    priority = 20
```

If you spawn an `Agent` without `Position`, `MoveProcessor` won't touch it. Spawn one with both, and it will.

### Priority Controls Order

Lower priority runs first within each tick:

```python
class GatherInput(AsyncProcessor):
    priority = 1      # First: read sensors

class Think(AsyncProcessor):
    priority = 10     # Second: decide

class Act(AsyncProcessor):
    priority = 20     # Third: execute

class Record(AsyncProcessor):
    priority = 100    # Last: log
```

### Shared State via Resources

Processors can share configuration and services through the world's `Resources` container:

```python
from dataclasses import dataclass

@dataclass
class SimConfig:
    decay_rate: float = 2.0
    max_energy: float = 100.0

# Register
world.resources.insert(SimConfig(decay_rate=3.0))

# Access in processor
class DecayProcessor(AsyncProcessor):
    components = (Agent,)
    priority = 1

    async def process(self, df, resources=None, **kwargs):
        config = resources.require(SimConfig)
        return df.with_columns({
            "agent__energy": col("agent__energy") - config.decay_rate,
        })
```

### Mutations Are Deferred

Spawn, despawn, add/remove components -- all mutations queue during a tick and apply at the start of the **next** tick. This keeps each tick consistent.

```python
# These don't take effect until the next step()
await container.command_service.submit(wid, spawn_cmd, ctx)
await container.command_service.submit(wid, spawn_cmd, ctx)

# Now they materialize
await container.simulation_service.step(wid)
```

### Fork to Compare Strategies

Run the same starting state with different processors or parameters:

```python
# Base world with 100 ticks of history
await container.simulation_service.run(wid, RunConfig(num_steps=100))

# Fork and try a different strategy
fork = await container.world_service.fork_world(
    source_world_id=wid,
    name="aggressive",
    storage_config=StorageConfig(),
)
await fork.system.add_processor(AggressiveStrategy())
await container.simulation_service.run(fork.world_id, RunConfig(num_steps=50))

# Compare outcomes
base_state = await container.query_service.get_world_state(wid)
fork_state = await container.query_service.get_world_state(fork.world_id)
```

## What's Next

- [Components](./components.md) -- field types, column prefixing, archetype signatures
- [Processors](./processors.md) -- LLM-powered processors, structured outputs, testing
- [Examples](./examples.md) -- runnable examples for every pattern
