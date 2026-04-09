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

```python
import asyncio
from archetype.app.container import ServiceContainer
from archetype.app.models import Command, CommandType
from archetype.app.auth.models import ActorCtx
from archetype.core.config import WorldConfig, StorageConfig, RunConfig
from uuid_utils import uuid7

async def main():
    container = ServiceContainer()

    # 1. Create a world
    world = await container.world_service.create_world(
        WorldConfig(name="quickstart"),
        StorageConfig(),
    )

    # 2. Submit a spawn command
    ctx = ActorCtx(id=uuid7(), roles={"admin"})
    cmd = Command(
        type=CommandType.SPAWN,
        payload={"components": []},
    )
    await container.command_service.submit(world.world_id, cmd, ctx)

    # 3. Run 10 ticks
    result = await container.simulation_service.run(
        world.world_id,
        RunConfig(num_steps=10),
    )
    print(f"Done: {result.ticks_completed} ticks, {result.commands_applied} commands")

    # 4. Query state
    snapshot = await container.query_service.get_world_state(world.world_id)
    print(f"World at tick {snapshot.tick}")

    await container.shutdown()

asyncio.run(main())
```

## Option B: CLI

```bash
# Start the API server
archetype serve &

# Create a world
curl -s -X POST localhost:8000/worlds \
  -H 'Content-Type: application/json' \
  -d '{"name": "quickstart"}' | python -m json.tool

# Use the returned world_id for subsequent commands
export WID=<world-id-from-above>

# Submit a spawn command
curl -s -X POST localhost:8000/worlds/$WID/commands \
  -H 'Content-Type: application/json' \
  -d '{"type": "spawn", "payload": {"components": []}}'

# Run 10 ticks
curl -s -X POST localhost:8000/worlds/$WID/run \
  -H 'Content-Type: application/json' \
  -d '{"num_steps": 10}' | python -m json.tool

# Query state
curl -s localhost:8000/worlds/$WID/state | python -m json.tool
```

## Option C: CLI Commands

```bash
archetype world create quickstart
archetype run <world-id> --steps 10
archetype query <world-id>
archetype history <world-id>
```

## Next Steps

- [Architecture](./architecture.md) — how the pieces fit together
- [Writing Processors](./processors.md) — build custom simulation logic
- [REST API Reference](../reference/rest-api.md) — full REST endpoint docs
- [CLI Reference](../reference/cli.md) — all CLI commands
