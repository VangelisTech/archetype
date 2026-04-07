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

This simulation models a room of agents who gain energy from greetings and lose it from thinking. Copy this, run it.

```bash
uv run python examples/simulation_script.py
```

```python
import asyncio
import json

from daft import DataFrame, col
from uuid_utils import uuid7

from archetype.app.auth.models import ActorCtx
from archetype.app.container import ServiceContainer
from archetype.app.models import Command, CommandType
from archetype.core.aio.async_processor import AsyncProcessor
from archetype.core.component import Component
from archetype.core.config import RunConfig, StorageConfig, WorldConfig


# ── Step 1: Define components ───────────────────────────────────────────

class Agent(Component):
    name: str = ""
    role: str = ""
    energy: float = 100.0
    mood: str = "neutral"
    log: str = "[]"   # JSON list of events


class Position(Component):
    x: float = 0.0
    y: float = 0.0


# ── Step 2: Write processors ───────────────────────────────────────────

class EnergyDecayProcessor(AsyncProcessor):
    """Every tick, agents lose energy from existing."""
    components = (Agent,)
    priority = 1

    async def process(self, df: DataFrame, **kwargs) -> DataFrame:
        return df.with_columns({
            "agent__energy": (col("agent__energy") - 2.0).if_else(
                col("agent__energy") - 2.0 > 0,
                col("agent__energy") - 2.0,
                daft.lit(0.0),
            ),
        })


class MoodProcessor(AsyncProcessor):
    """Update mood based on energy level."""
    components = (Agent,)
    priority = 50

    async def process(self, df: DataFrame, **kwargs) -> DataFrame:
        import daft as _daft
        mood = (
            col("agent__energy")
            .if_else(col("agent__energy") > 70, _daft.lit("happy"), _daft.lit("neutral"))
        )
        # Override: low energy = tired
        mood = col("agent__energy").if_else(
            col("agent__energy") > 30,
            mood,
            _daft.lit("tired"),
        )
        return df.with_columns({"agent__mood": mood})


# ── Step 3: Create world and register processors ───────────────────────

async def main():
    container = ServiceContainer()
    ctx = ActorCtx(id=uuid7(), roles={"admin"})

    world = await container.world_service.create_world(
        WorldConfig(name="social-sim"),
        StorageConfig(),
    )
    wid = world.world_id

    await world.system.add_processor(EnergyDecayProcessor())
    await world.system.add_processor(MoodProcessor())

    # ── Step 4: Spawn entities ─────────────────────────────────────────

    agents = [
        Agent(name="Alice", role="engineer", energy=100.0),
        Agent(name="Bob", role="designer", energy=80.0),
        Agent(name="Charlie", role="manager", energy=60.0),
    ]

    for agent in agents:
        cmd = Command(
            type=CommandType.SPAWN,
            payload={"components": [agent.model_dump()]},
        )
        await container.command_service.submit(wid, cmd, ctx)

    # ── Step 5: Run ────────────────────────────────────────────────────

    result = await container.simulation_service.run(
        wid, RunConfig(num_steps=20),
    )
    print(f"Ran {result.ticks_completed} ticks\n")

    # Print final state
    for sig, df in world._live.items():
        rows = df.collect().to_pylist()
        for row in rows:
            name = row.get("agent__name", "?")
            energy = row.get("agent__energy", 0)
            mood = row.get("agent__mood", "?")
            print(f"  {name}: energy={energy:.0f}, mood={mood}")

    await container.shutdown()


asyncio.run(main())
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
