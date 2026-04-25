<div align="center">

# Archetype

**A dataframe-first, append-only ECS runtime for simulations and AI agents.**

[![CI](https://github.com/VangelisTech/archetype/actions/workflows/python-tests.yml/badge.svg)](https://github.com/VangelisTech/archetype/actions/workflows/python-tests.yml)
[![Python 3.12+](https://img.shields.io/badge/python-3.12%2B-blue)](https://python.org)
[![License: Apache 2.0](https://img.shields.io/badge/license-Apache%202.0-blue)](LICENSE)

</div>

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

1. commands are enqueued through the broker,
2. each tick drains due commands,
3. worlds materialize structural mutations,
4. processors transform matching archetype DataFrames,
5. updated rows are appended to storage.

## Use Cases

Simulations where tick-by-tick history is part of the model:

- multi-agent worlds
- counterfactual branches and forks
- rollout-heavy evaluation
- LLM-powered processors running over many entities in parallel

## Installation

### Package

```bash
pip install archetype-ecs
```

### Development

```bash
git clone https://github.com/VangelisTech/archetype.git
cd archetype
uv sync --group dev
```

## Quickstart

`ArchetypeRuntime` is the recommended entry point. It owns the shared container, activates a world lazily on first use, and returns a real `entity_id` from `spawn()`.

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

For sync scripts, use `with ArchetypeRuntime.sync() as runtime:` and drop the `await`s.

Two things to know:

- processor columns are prefixed `componentname__field` (e.g., `position__x`)
- `ArchetypeRuntime` is the script boundary — process lifetime and world lifetime are separate concerns. See `docs/guide/specification.md` § "Contracts Before Sugar" for the full contract set (single-flight activation, honest `spawn()`, fork isolation, world-local shutdown). Drop to `ServiceContainer` only when you need explicit RBAC, custom command routing, or a non-script host.

## CLI

The CLI is a thin HTTP client. Except for `serve`, every command talks to a running FastAPI server.

```bash
# Start the server
archetype serve

# Create a world
archetype world create demo

# List worlds
archetype world list

# Run 10 ticks
archetype run <world-id> --steps 10

# Fork the current world state
archetype world fork <world-id> --name branch-a

# Show command history
archetype history <world-id>
```

Useful environment variables:

- `ARCHETYPE_URL`: base URL for the CLI, default `http://localhost:8000`
- `ARCHETYPE_REGISTRY_PATH`: file path for the persisted world registry

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

## Core Concepts

### Components

Components are typed `LanceModel` subclasses. Their fields define the archetype schema fragments that get flattened into storage columns.

```python
class Health(Component):
    hp: int = 100
    max_hp: int = 100
```

`Health` becomes columns like `health__hp` and `health__max_hp`.

### Archetypes

An archetype is the exact set of component types attached to an entity. Archetype signatures are canonicalized by sorted component type name, so component order is not meaningful.

If you add or remove a component, the entity migrates to a different archetype table.

### Processors

Processors are pure-ish DataFrame transforms selected by subset match on component signatures:

```python
class ThinkProcessor(AsyncProcessor):
    components = (Agent, Memory)
    priority = 20
```

If an archetype contains at least `Agent` and `Memory`, that processor runs on its DataFrame.

### Worlds

`AsyncWorld` owns:

- entity-to-archetype bookkeeping
- pending spawn/despawn caches
- the live in-memory snapshot for the latest tick
- lifecycle hooks
- query / execute / update orchestration

Different archetypes are processed concurrently; processors within one archetype run in ascending `priority`.

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

Default roles in code include `viewer`, `player`, `coder`, `operator`, `maintainer`, and `admin`.

### Storage

Archetype supports two async storage backends behind the same contracts:

- `AsyncLancedbStore` for LanceDB-backed archetype tables
- `AsyncStore` for the Daft catalog-backed path

`StorageService` shares backend instances across worlds with the same `(uri, namespace)`.

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

Start with `src/archetype/sugar.py` (`ArchetypeRuntime`) to use the system. Read `src/archetype/core` and `src/archetype/app` to understand how it works underneath.

## Repository Map

```text
archetype/
├── src/archetype/sugar.py   # ArchetypeRuntime — recommended top-level API
├── src/archetype/core/      # ECS runtime and storage contracts
├── src/archetype/app/       # Brokered service layer (lower-level)
├── src/archetype/api/       # FastAPI server
├── src/archetype/cli/       # Typer CLI
├── examples/                # Runnable examples
├── tests/                   # Test suite
├── docs/                    # MkDocs site
├── AGENTS.md                # Repo-specific collaborator guidance
└── LEARNINGS.md             # Architecture notes
```

## Examples

Run the examples directly:

```bash
uv run python examples/01_world_mutations.py
uv run python examples/02_fork_counterfactual.py
uv run python examples/03_time_travel.py
uv run python examples/04_messaging.py
uv run python examples/05_llm_agents.py
uv run python examples/06_trajectory_analysis.py
```

`examples/05_llm_agents.py` and parts of `examples/06_trajectory_analysis.py` require `OPENAI_API_KEY`.

## Development

```bash
make test        # fast test suite
make test-cov    # coverage run
make check       # format + lint
make ci          # CI gate
make docs        # build docs
```

## Documentation

- Docs site: `https://archetype-docs.pages.dev`
- Examples index: `examples/README.md`
- Architecture notes: `LEARNINGS.md`

## License

Apache 2.0 — `LICENSE`
