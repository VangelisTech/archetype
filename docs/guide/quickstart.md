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
from uuid_utils import uuid7

from archetype.app.auth.models import ActorCtx
from archetype.app.container import ServiceContainer
from archetype.app.models import Command, CommandType
from archetype.core.aio.async_processor import AsyncProcessor
from archetype.core.component import Component
from archetype.core.config import RunConfig, StorageConfig, WorldConfig


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
    container = ServiceContainer()

    # 3. Create a world
    world = await container.world_service.create_world(
        WorldConfig(name="quickstart"),
        StorageConfig(),
    )

    # 4. Register the processor
    await world.system.add_processor(MovementProcessor())

    # 5. Spawn entities with components via the command pipeline
    ctx = ActorCtx(id=uuid7(), roles={"admin"})
    for dx, dy in [(1, 2), (-1, 0.5)]:
        cmd = Command(
            type=CommandType.SPAWN,
            payload={
                "components": [
                    Position(x=0, y=0).to_payload(),
                    Velocity(dx=dx, dy=dy).to_payload(),
                ]
            },
        )
        await container.command_service.submit(world.world_id, cmd, ctx)

    # 6. Run 5 ticks — each tick: drain commands, materialize, process, persist
    result = await container.simulation_service.run(
        world.world_id,
        RunConfig(num_steps=5),
    )
    print(f"Done: {result.ticks_completed} ticks, {result.commands_applied} commands")

    # 7. Query state
    df = await world.get_components([Position])
    for row in df.collect().to_pylist():
        print(f"  entity {row['entity_id']}: x={row['position__x']}, y={row['position__y']}")

    await container.shutdown()

asyncio.run(main())
```

Key patterns in this example:

- Components are `LanceModel` subclasses — their fields become prefixed columns (`position__x`)
- `to_payload()` serializes components for the command pipeline
- Processors declare their component requirements via `components = (...)` — the engine routes matching entities automatically
- All external mutations go through `CommandService.submit()` with an `ActorCtx`

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

- [Architecture](./architecture.md) — how the layers and tick lifecycle fit together
- [Components](./components.md) — field types, Arrow serialization, column prefixing
- [Processors](./processors.md) — LLM-powered processors, structured outputs, testing
- [Building Simulations](./building-simulations.md) — the full workflow with forking and resources
- [Examples](./examples.md) — runnable examples for every pattern
