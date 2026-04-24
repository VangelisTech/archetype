# Archetype

## A dataframe-first, append-only ECS runtime for simulations and AI agents

Archetype stores world state as columnar archetype tables, executes behavior as DataFrame transforms, and persists every tick as a new snapshot instead of overwriting rows. Consequences of that storage model:

- entities are grouped by exact component sets
- processors run over whole archetype DataFrames
- writes are append-only
- time-travel and world forking fall out of the storage model

## What It Is

Archetype is split into layers:

| Layer | Purpose |
|---|---|
| `src/archetype/sugar.py` | `ArchetypeRuntime` — recommended top-level API for scripts and simulations |
| `src/archetype/core` | ECS primitives: `Component`, `Archetype`, `AsyncWorld`, `AsyncProcessor`, storage/query/update contracts |
| `src/archetype/app` | Service layer (lower-level): `CommandBroker`, `CommandService`, `WorldService`, `SimulationService`, `QueryService` |
| `src/archetype/api` + `src/archetype/cli` | FastAPI server and Typer CLI |

The runtime model is:

1. commands are enqueued through the broker
2. each tick drains due commands
3. worlds materialize structural mutations
4. processors transform matching archetype DataFrames
5. updated rows are appended to storage

## Use Cases

Simulations where tick-by-tick history is part of the model:

- multi-agent worlds
- counterfactual branches and forks
- rollout-heavy evaluation
- LLM-powered processors running over many entities in parallel

## Quickstart

Install the package:

```bash
pip install archetype-ecs
```

For development:

```bash
git clone https://github.com/VangelisTech/archetype.git
cd archetype
uv sync --group dev
```

The recommended entry point for scripts is `ArchetypeRuntime`:

```python
import asyncio

from daft import DataFrame, col

from archetype import ArchetypeRuntime, AsyncProcessor, Component


class Position(Component):
    x: float = 0.0
    y: float = 0.0


class Velocity(Component):
    dx: float = 0.0
    dy: float = 0.0


class MovementProcessor(AsyncProcessor):
    components = (Position, Velocity)
    priority = 10

    async def process(self, df: DataFrame, **kwargs) -> DataFrame:
        return df.with_columns(
            {
                "position__x": col("position__x") + col("velocity__dx"),
                "position__y": col("position__y") + col("velocity__dy"),
            }
        )


async def main():
    async with ArchetypeRuntime() as runtime:
        world = runtime.world("demo", processors=[MovementProcessor()])

        await world.spawn(Position(x=0, y=0), Velocity(dx=1, dy=2))
        await world.run(steps=3)

        df = await world.query(Position)
        print(df.collect().to_pylist())


asyncio.run(main())
```

Two important details:

- processor columns are always prefixed as `componentname__field`
- `ArchetypeRuntime` is the script boundary; use `world.as_actor(...)` for explicit roles and drop to `ServiceContainer` only for custom command routing or lower-level lifecycle control

See [Quickstart](guide/quickstart.md) for more.

## CLI

The CLI is a thin HTTP client. Except for `serve`, every command talks to a running FastAPI server.

```bash
archetype serve
archetype world create demo
archetype world list
archetype run <world-id> --steps 10
archetype world fork <world-id> --name branch-a
archetype history <world-id>
```

Useful environment variables:

- `ARCHETYPE_URL` for the CLI base URL
- `ARCHETYPE_REGISTRY_PATH` for the persisted world registry

See [CLI Reference](reference/cli.md) for the generated command docs.

## REST API

`archetype serve` exposes a FastAPI app with these routes:

| Method | Endpoint | Purpose |
|---|---|---|
| `POST` | `/worlds` | Create a world |
| `GET` | `/worlds` | List worlds |
| `GET` | `/worlds/{world_id}` | Inspect one world |
| `DELETE` | `/worlds/{world_id}` | Remove a world |
| `POST` | `/worlds/{world_id}/fork` | Fork a world |
| `POST` | `/worlds/{world_id}/commands` | Submit one command |
| `POST` | `/worlds/{world_id}/commands/batch` | Submit multiple commands |
| `GET` | `/worlds/{world_id}/commands` | Command history |
| `GET` | `/worlds/{world_id}/commands/pending` | Pending command count |
| `POST` | `/worlds/{world_id}/step` | Run one tick |
| `POST` | `/worlds/{world_id}/run` | Run multiple ticks |
| `GET` | `/worlds/{world_id}/processors` | List processors |
| `GET` | `/worlds/{world_id}/state` | Query world snapshot |
| `GET` | `/worlds/{world_id}/entities/{entity_id}` | Query one entity |
| `GET` | `/worlds/{world_id}/components` | Query component projections |
| `GET` | `/worlds/{world_id}/history` | Query command history |

See [REST API Reference](reference/rest-api.md) for the generated API docs.

## Core Concepts

### Components

Components are typed `LanceModel` subclasses. Their fields define the archetype schema fragments that get flattened into storage columns.

See [Components](guide/components.md).

### Archetypes

An archetype is the exact set of component types attached to an entity. If you add or remove a component, the entity migrates to a different archetype table.

See [Archetype](guide/archetype.md).

### Processors

Processors are DataFrame transforms selected by subset match on component signatures.

See [Processors](guide/processors.md) and [System Execution](guide/system-execution.md).

### Worlds

`AsyncWorld` owns:

- entity-to-archetype bookkeeping
- pending spawn/despawn caches
- the live in-memory snapshot for the latest tick
- lifecycle hooks
- query / execute / update orchestration

Different archetypes are processed concurrently. Processors within one archetype run in ascending `priority`.

See [Worlds](guide/worlds.md).

### Commands and RBAC

All external mutations are designed to flow through:

```text
API / CLI / caller
  → CommandService
  → CommandBroker
  → AsyncWorld
```

The broker enforces:

- role permissions
- per-tick command quotas
- daily token budgets

See [Services](guide/services.md) and [Data Flow](guide/data-flow.md).

### Storage

Archetype supports two async storage backends behind the same contracts:

- `AsyncLancedbStore` for LanceDB-backed archetype tables
- `AsyncStore` for the Daft catalog-backed path

See [Stores](guide/stores.md).

### Resources

Resources are world-scoped dependencies injected into processors outside the entity/component storage path.

See [Resources](guide/resources.md).

### Run Configuration

`RunConfig` controls run-level execution behavior such as step count and debug options.

See [Run Config](guide/run-config.md).

## World Forking

Forking is a first-class operation in `WorldService`.

A fork:

- gets a new `world_id`
- copies the source world's current live snapshot
- preserves tick position
- shares processor instances
- persists the copied snapshot under the new world identity

Source and fork diverge independently after that point.

## Status

Current state worth knowing before using it:

- the core runtime and append-only write path are the most mature parts
- the Python service layer is richer than the REST read models
- the FastAPI layer currently uses a default admin `ActorCtx` — not multi-tenant auth yet

## Where to Start

- **New to Archetype?** Start with the [Quickstart](guide/quickstart.md), which leads with `ArchetypeRuntime`, then read the [Architecture](guide/architecture.md) overview
- **Building a simulation?** See [Building Simulations](guide/building-simulations.md) for the full workflow
- **Integrating with the API?** See [App Overview](guide/app-overview.md) for how core connects through services to the HTTP layer
