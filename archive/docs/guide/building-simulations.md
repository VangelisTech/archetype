# Building Simulations

This is the full workflow: define components, write processors, wire them into a
world, run it. For the diagram map of those boxes, see
[Core Architecture](core-architecture.md). For product families above the
engine, see [Application layer](app-overview.md).

## The Pattern

Every simulation follows the same structure:

1. **Define components** -- the data your entities carry
2. **Write processors** -- the rules that transform that data each tick
3. **Create a runtime world** and stage processors/resources
4. **Spawn entities** with initial component values
5. **Run** -- the gate delegates, the engine steps processors, and storage appends state

## Complete Example

Agents gain experience each tick proportional to their skill. A second processor computes a rating. Copy this, run it.

```bash
uv run python examples/simulation_script.py
```

Source: [`examples/simulation_script.py`](https://github.com/VangelisTech/archetype/blob/main/examples/simulation_script.py)

```python
import asyncio
from daft import DataFrame, col
from archetype import ArchetypeRuntime, AsyncProcessor, Component


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
    async with ArchetypeRuntime() as runtime:
        world = runtime.world(
            "agents",
            processors=[ExperienceProcessor(), RatingProcessor()],
        )
        await world.spawn(Agent(name="Alice", role="engineer", skill=3.0))
        await world.spawn(Agent(name="Bob", role="designer", skill=2.0))
        await world.spawn(Agent(name="Charlie", role="manager", skill=1.5))

        # Persist the initial values, then apply processors ten times.
        await world.step()
        result = await world.run(steps=10)

        history = await world.query(Agent)
        current = history.where(col("tick") == result.final_tick - 1).sort("entity_id")
        rows = current.select(
            "agent__name",
            "agent__skill",
            "agent__experience",
            "agent__rating",
        ).collect().to_pylist()
        for row in rows:
            print(
                f"{row['agent__name']}: skill={row['agent__skill']:.1f}, "
                f"experience={row['agent__experience']:.0f}, "
                f"rating={row['agent__rating']:.2f}"
            )
```

Output:

```text
Alice:   skill=3.0, experience=60, rating=18.00
Bob:     skill=2.0, experience=40, rating=8.00
Charlie: skill=1.5, experience=30, rating=4.50
```

The initial `step()` records the spawned values as tick 0. Processors first
apply on tick 1, so the ten-step run produces ticks 1 through 10. `query()`
returns the append-only history; filter to `result.final_tick - 1` when you
want the current persisted state.

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

Processors can share configuration and services through staged resources:

```python
from dataclasses import dataclass

@dataclass
class SimConfig:
    decay_rate: float = 2.0
    max_energy: float = 100.0

class DecayProcessor(AsyncProcessor):
    components = (Agent,)
    priority = 1

    async def process(self, df, resources=None, **kwargs):
        config = resources.require(SimConfig)
        return df.with_column(
            "agent__energy",
            col("agent__energy") - config.decay_rate,
        )

world = runtime.world(
    "decay",
    processors=[DecayProcessor()],
    resources=[SimConfig(decay_rate=3.0)],
)
```

### Mutations Materialize at Tick Boundaries

Spawn, despawn, add/remove components, and updates are accepted through the runtime/gate surface and materialize at tick boundaries. This keeps each tick consistent.

```python
entity_id = await world.spawn(Agent(name="Dana"))

# Now it materializes
await world.step()
```

### Fork to Compare Strategies

Run the same starting state with different processors or parameters:

```python
# Base world with 100 ticks of history
await world.run(steps=100)

# Fork and try a different strategy through the gate
fork = await world.fork(name="aggressive")
await fork.add_processor(AggressiveStrategy())
await fork.run(steps=50)

base_state = await world.query(Agent)
fork_state = await fork.query(Agent)
```

## What's Next

- [Components](./components.md) -- field types, column prefixing, archetype signatures
- [Processors](./processors.md) -- LLM-powered processors, structured outputs, testing
- [Examples](./examples.md) -- runnable examples for every pattern
