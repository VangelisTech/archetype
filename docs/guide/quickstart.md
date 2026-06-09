# Quickstart

Get a simulation running in under 5 minutes.

## Install

```bash
git clone https://github.com/vangelis-tech/archetype.git
cd archetype
uv sync
```

Python 3.12+ required.

## Option A: Python API

Define components, write a processor, spawn entities, run:

```python
import asyncio

from daft import DataFrame, col

from archetype import ArchetypeRuntime, AsyncProcessor, Component


# 1. Define components — typed data models that become table columns
class Position(Component):
    x: float = 0.0
    y: float = 0.0

class Velocity(Component):
    dx: float = 0.0
    dy: float = 0.0


# 2. Write a processor — a DataFrame transform that runs each tick
class MovementProcessor(AsyncProcessor):
    components = (Position, Velocity)  # Only runs on entities with BOTH
    priority = 10

    async def process(self, df: DataFrame, **kwargs) -> DataFrame:
        return df.with_columns({
            "position__x": col("position__x") + col("velocity__dx"),
            "position__y": col("position__y") + col("velocity__dy"),
        })


async def main():
    async with ArchetypeRuntime() as runtime:
        # 3. Create a lazy world handle and stage the processor
        world = runtime.world("quickstart", processors=[MovementProcessor()])

        # 4. Spawn entities
        for dx, dy in [(1, 2), (-1, 0.5)]:
            await world.spawn(Position(x=0, y=0), Velocity(dx=dx, dy=dy))

        # 5. Run 5 ticks
        result = await world.run(steps=5)
        print(f"Done: {result.ticks_completed} ticks, {result.commands_applied} commands")

        # 6. Query state
        df = await world.query(Position)
        for row in df.collect().to_pylist():
            print(f"  entity {row['entity_id']}: x={row['position__x']}, y={row['position__y']}")

asyncio.run(main())
```

Key patterns in this example:

- Components are `LanceModel` subclasses — their fields become prefixed columns (`position__x`)
- Processors declare their component requirements via `components = (...)` — the engine routes matching entities automatically
- `ArchetypeRuntime` is the recommended script boundary; use `world.as_actor(...)` for explicit roles and `ServiceContainer` only for custom routing or lower-level orchestration

## Option B: HTTP API (curl)

```bash
# Start the API server
archetype serve &

# Create a world
curl -s -X POST localhost:8000/worlds \
  -H 'Content-Type: application/json' \
  -d '{"name": "quickstart"}' | python -m json.tool

# Use the returned world_id for subsequent commands
export WID=<world-id-from-above>

# Spawn an entity with components
curl -s -X POST localhost:8000/worlds/$WID/commands \
  -H 'Content-Type: application/json' \
  -d '{
    "type": "spawn",
    "payload": {
      "components": [
        {"type": "Position", "x": 0, "y": 0},
        {"type": "Velocity", "dx": 1, "dy": 2}
      ]
    }
  }'

# Run 5 ticks
curl -s -X POST localhost:8000/worlds/$WID/run \
  -H 'Content-Type: application/json' \
  -d '{"num_steps": 5}' | python -m json.tool

# Query state
curl -s localhost:8000/worlds/$WID/state | python -m json.tool
```

## Option C: CLI Commands

```bash
archetype world create quickstart
archetype run <world-id> --steps 5
archetype query <world-id>
archetype history <world-id>
```

## Next Steps

- [Running & Scripting](./running.md) — the fuller how-to: driving execution, reading state back, persistence, roles, and running the server
- [Architecture](./architecture.md) — how the layers and tick lifecycle fit together
- [Components](./components.md) — field types, Arrow serialization, column prefixing
- [Processors](./processors.md) — LLM-powered processors, structured outputs, testing
- [Building Simulations](./building-simulations.md) — the full workflow with forking and resources
- [Examples](./examples.md) — runnable examples for every pattern
