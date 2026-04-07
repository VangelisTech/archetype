<div align="center">

# Archetype

**Data-centric ECS simulation engine for multi-agent AI systems.**

[![CI](https://github.com/VangelisTech/archetype/actions/workflows/python-tests.yml/badge.svg)](https://github.com/VangelisTech/archetype/actions/workflows/python-tests.yml)
[![Python 3.12+](https://img.shields.io/badge/python-3.12%2B-blue)](https://python.org)
[![License: Apache 2.0](https://img.shields.io/badge/license-Apache%202.0-blue)](LICENSE)

</div>

---

## Install

```bash
pip install archetype-ecs
# or
git clone https://github.com/VangelisTech/archetype.git && cd archetype && uv sync
```

Python 3.12+ required.

## Usage

```bash
# Start the server
archetype serve

# Create a world, run it, query it
archetype world create my-sim
archetype run <world-id> --steps 100
archetype query <world-id>

# Inspect and manage
archetype status                       # all worlds
archetype world inspect <world-id>     # single world
archetype history <world-id>           # command audit trail
```

The CLI talks to a running `archetype serve` process over HTTP. All state lives in the server.

## What It Does

Archetype is an Entity-Component-System runtime built on [Daft](https://www.getdaft.io/) DataFrames and [LanceDB](https://lancedb.github.io/lancedb/). World state is columnar tables. Every tick is an append-only write to storage.

- **Time-travel** -- query any tick, replay any run
- **World forking** -- branch worlds for MCTS, counterfactual reasoning, A/B experiments
- **Trajectory analysis** -- ingest, label, and score agent trajectories
- **RBAC command pipeline** -- all mutations flow through a priority queue with role-based access control
- **LLM-native processors** -- parallel LLM calls across all entities via `daft.functions.prompt`

## Python API

For programmatic access beyond the CLI:

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

## REST API

`archetype serve` exposes a FastAPI server at `http://localhost:8000`.

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/worlds` | Create a world |
| GET | `/worlds` | List worlds |
| POST | `/worlds/{id}/fork` | Fork a world |
| POST | `/worlds/{id}/commands` | Submit a command |
| POST | `/worlds/{id}/run` | Run N ticks |
| GET | `/worlds/{id}/state` | World snapshot |
| GET | `/worlds/{id}/history` | Command history |

## Architecture

```
src/archetype/
├── core/           # ECS engine (Daft DataFrames + Arrow + LanceDB)
├── app/            # Service layer (RBAC, CommandBroker, WorldRegistry)
├── api/            # FastAPI REST endpoints
├── cli/            # Typer CLI (thin HTTP client)
└── trajectories/   # Trajectory analysis pipeline
```

## Development

```bash
make ci          # Full gate: lint + lock-check + tests w/ 70% branch coverage
make test        # Fast tests
make check       # Auto-format + lint (ruff)
make docs        # Build documentation (MkDocs)
```

## Documentation

[archetype-docs.pages.dev](https://archetype-docs.pages.dev)

## License

Apache 2.0 -- [Vangelis Technologies Inc.](https://vangelis.tech)
