<div align="center">

# Archetype

**Data-centric ECS simulation engine for multi-agent AI systems.**

[![CI](https://github.com/VangelisTech/archetype/actions/workflows/python-tests.yml/badge.svg)](https://github.com/VangelisTech/archetype/actions/workflows/python-tests.yml)
[![Python 3.12+](https://img.shields.io/badge/python-3.12%2B-blue)](https://python.org)
[![License: Apache 2.0](https://img.shields.io/badge/license-Apache%202.0-blue)](LICENSE)

</div>

---

Archetype is an Entity-Component-System runtime built on [Daft](https://www.getdaft.io/) DataFrames and [LanceDB](https://lancedb.github.io/lancedb/). World state is columnar tables. Every tick is an append-only write to storage. This gives you time-travel queries, world forking, and full audit trails out of the box.

## Key Capabilities

- **Simulation as data** -- query any tick, replay any run, diff any two states
- **World forking** -- branch worlds for MCTS, counterfactual reasoning, or A/B experiments
- **Trajectory analysis** -- ingest, label, and score agent trajectories with fork-based comparison
- **RBAC command pipeline** -- all mutations flow through a priority queue with role-based access control
- **LLM-native processors** -- `daft.functions.prompt` gives you parallel LLM calls across all entities in a single DataFrame operation

## Quick Start

```bash
git clone https://github.com/VangelisTech/archetype.git
cd archetype
uv sync

# Start the server
archetype serve

# Create and run a simulation
archetype world create my-sim
archetype run <world-id> --steps 100
archetype query <world-id>
```

**Python 3.12+** required. See the [Quickstart guide](https://archetype-docs.pages.dev/guide/quickstart/) for the full walkthrough.

## Minimal Example

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
        WorldConfig(name="my-sim"),
        StorageConfig(),
    )

    ctx = ActorCtx(id=uuid7(), roles={"admin"})
    cmd = Command(type=CommandType.SPAWN, payload={"components": []})
    await container.command_service.submit(world.world_id, cmd, ctx)

    result = await container.simulation_service.run(
        world.world_id,
        RunConfig(num_steps=10),
    )
    print(f"Completed {result.ticks_completed} ticks")

    await container.shutdown()

asyncio.run(main())
```

## Architecture

```
src/archetype/
├── core/           # ECS engine (Daft DataFrames + Arrow + LanceDB)
│   ├── aio/        #   Async runtime: AsyncWorld, AsyncProcessor, AsyncSystem
│   ├── sync/       #   Synchronous variants
│   ├── storage/    #   LanceDB/Iceberg storage layer
│   └── runtime/    #   Storage orchestration
├── app/            # Service layer
│   ├── auth/       #   RBAC (ActorCtx, roles, quotas)
│   ├── broker.py   #   CommandBroker (priority queue + RBAC + audit)
│   ├── registry.py #   WorldRegistry (JSON catalog)
│   └── *.py        #   WorldService, CommandService, SimulationService, QueryService
├── api/            # FastAPI REST endpoints
├── cli/            # Typer CLI (thin HTTP client to running server)
└── trajectories/   # Trajectory analysis pipeline
    ├── components.py   # Trajectory, Label, Turn components
    ├── pipeline.py     # TrajectoryPipeline high-level API
    └── processors.py   # Sampling, labeling, scoring processors
```

The system runs as a single `archetype serve` process. The CLI is a thin HTTP client -- it never instantiates `ServiceContainer` directly.

## REST API

Start with `archetype serve` (default: `http://localhost:8000`).

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/worlds` | Create a world |
| GET | `/worlds` | List worlds |
| POST | `/worlds/{id}/fork` | Fork a world |
| POST | `/worlds/{id}/commands` | Submit a command |
| POST | `/worlds/{id}/commands/batch` | Submit batch |
| POST | `/worlds/{id}/run` | Run N ticks |
| GET | `/worlds/{id}/state` | World snapshot |
| GET | `/worlds/{id}/history` | Command history |

See the full [API reference](https://archetype-docs.pages.dev/guide/api-reference/).

## Command Pipeline

All mutations flow through the CommandBroker with RBAC enforcement:

```
Client -> CommandService -> CommandBroker (RBAC + queue) -> World
                                  |
                          SimulationService (drain + step)
                                  |
                          QueryService (read path)
```

**Roles:** `viewer` (read-only) | `player` (spawn/despawn/message) | `coder` (components) | `operator` (trajectories) | `maintainer` (processors) | `admin` (all)

## Development

```bash
make ci          # Full gate: lint + lock-check + tests w/ 70% branch coverage
make test        # Fast tests, no coverage
make check       # Auto-format + lint (ruff)
make docs        # Build documentation (MkDocs)
make docs-serve  # Serve docs locally
```

## Documentation

Full documentation is available at [archetype-docs.pages.dev](https://archetype-docs.pages.dev) with interactive Python examples powered by Pyodide.

## License

Apache 2.0

---

<div align="center">

**[Vangelis Technologies Inc.](https://vangelis.tech)**

</div>
