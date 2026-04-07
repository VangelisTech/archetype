# Archetype

**Data-centric ECS simulation engine for multi-agent AI systems.**

Archetype is an Entity-Component-System runtime built on [Daft](https://www.getdaft.io/) DataFrames and [LanceDB](https://lancedb.github.io/lancedb/). World state is columnar tables. Every tick is an append-only write to storage. This gives you time-travel queries, world forking, and full audit trails out of the box.

## What You Get

1. **Simulation as data** — query any tick, replay any run, diff any two states
2. **World forking** — branch worlds for MCTS, counterfactual reasoning, or A/B experiments
3. **Trajectory analysis** — ingest, label, and score agent trajectories with fork-based comparison
4. **LLM-native processors** — parallel LLM calls across all entities in a single DataFrame operation

## Quick Start

```python
import asyncio
from archetype.app.container import ServiceContainer
from archetype.app.models import Command, CommandType
from archetype.app.auth.models import ActorCtx
from archetype.core.config import WorldConfig, StorageConfig, RunConfig
from uuid_utils import uuid7

async def main():
    container = ServiceContainer()

    world = await container.world_service.create_world(
        WorldConfig(name="my-sim"), StorageConfig(),
    )

    ctx = ActorCtx(id=uuid7(), roles={"admin"})
    cmd = Command(type=CommandType.SPAWN, payload={"components": []})
    await container.command_service.submit(world.world_id, cmd, ctx)

    result = await container.simulation_service.run(
        world.world_id, RunConfig(num_steps=10),
    )
    print(f"Completed {result.ticks_completed} ticks")
    await container.shutdown()

asyncio.run(main())
```

## World Forking

Fork worlds to explore alternatives:

```python
fork = await container.world_service.fork_world(
    source_world_id=world.world_id,
    name="branch-A",
    storage_config=StorageConfig(),
)
await container.simulation_service.run(fork.world_id, RunConfig(num_steps=100))
```

The fork gets a full snapshot of the source world's state. Source and fork diverge independently — use this for MCTS, counterfactual reasoning, or A/B testing strategies.

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                  archetype.api / cli                 │
│  FastAPI REST endpoints • Typer CLI (HTTP client)    │
└─────────────────────────────────────────────────────┘
                         │
┌─────────────────────────────────────────────────────┐
│                  archetype.app                       │
│  CommandBroker, WorldService, SimulationService      │
│  WorldRegistry, QueryService, RBAC                   │
└─────────────────────────────────────────────────────┘
                         │
┌─────────────────────────────────────────────────────┐
│                  archetype.core                      │
│  AsyncWorld, AsyncSystem, Resources, LanceDB Store   │
└─────────────────────────────────────────────────────┘
```

## Try It Live

Edit and run Python right here in the browser:

``` { .python .live }
import json

world = {
    "name": "cogito",
    "tick": 0,
    "entities": [
        {"id": 1, "name": "Descartes", "thought": "I think, therefore I am."},
        {"id": 2, "name": "Spinoza", "thought": "All things are in God."},
    ]
}

# Simulate a tick
world["tick"] += 1
for entity in world["entities"]:
    entity["thought"] = f"[Tick {world['tick']}] {entity['thought']}"

print(json.dumps(world, indent=2))
```

## Next Steps

- **[Quickstart](guide/quickstart.md)** — Get running in 5 minutes
- **[Architecture](guide/architecture.md)** — How the layers work together
- **[Processors](guide/processors.md)** — Build custom simulation logic
- **[Examples](guide/examples.md)** — Patterns and working demos
