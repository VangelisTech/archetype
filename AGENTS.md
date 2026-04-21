# AGENTS.md

Repo-specific guidance for AI collaborators. For normative behavior, read `docs/guide/specification.md` — that is the contract document and takes precedence over anything here.

## Layout

```text
archetype/
├── src/archetype/
│   ├── core/           # ECS engine (Daft + Arrow + LanceDB)
│   ├── app/            # Service layer
│   │   ├── auth/       #   RBAC guard (ActorCtx, roles, quotas)
│   │   ├── broker.py   #   CommandBroker (priority queue + RBAC)
│   │   ├── command_service.py    # Enqueue / drain / apply
│   │   ├── world_service.py      # World lifecycle
│   │   ├── simulation_service.py # Tick stepping and runs
│   │   ├── query_service.py      # Read path
│   │   ├── storage_service.py    # Backend pooling
│   │   └── container.py          # Composition root
│   ├── api/            # FastAPI REST layer
│   ├── cli/            # Typer CLI (thin HTTP client)
│   └── sugar.py        # Top-level ergonomic wrapper over the service layer
├── examples/
├── tests/
└── LEARNINGS.md        # Daft patterns and architectural notes
```

### Layers

| Layer | Access |
|-------|--------|
| `core/` | Modify only after discussion. It holds the hard invariants; breakage there cascades everywhere. |
| `app/` | Extend carefully. Service contracts are in the specification. |
| `api/`, `cli/`, `sugar.py` | Write freely, subject to the contracts they wrap. |

## Using the service layer

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
        WorldConfig(name="experiment"),
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

## LLM-powered processors

`daft.functions.prompt` inside `AsyncProcessor.process()` gives every entity an LLM call per tick, executed in parallel by Daft.

```python
from daft import DataFrame, col
from daft.functions import prompt
from archetype.core.aio.async_processor import AsyncProcessor
from archetype.core.component import Component

class Agent(Component):
    name: str = ""
    memory: str = "[]"
    last_thought: str = ""

class ThinkProcessor(AsyncProcessor):
    components = (Agent,)
    priority = 10

    async def process(self, df: DataFrame, tick: int = 0, **kwargs) -> DataFrame:
        return df.with_column(
            "agent__last_thought",
            prompt(
                "You are " + col("agent__name")
                + ". Tick: " + str(tick)
                + ". What is your next action? Be brief.",
                model="gpt-5-mini",
            ),
        )
```

See `LEARNINGS.md` for the data-centric principle and the UDF decision tree. Both are mandatory reading before writing a processor.

## CLI

```bash
archetype serve                    # Start API server
archetype world create my-sim      # Create world
archetype run <world-id> --steps 10
archetype query <world-id>
archetype history <world-id>
```

The CLI (except `serve`) is an HTTP client against the running server. See `README.md` for full REST routes.

## Tests and CI

```bash
make ci          # lint + lock-check + tests with coverage (gate before push)
make test        # fast tests, no coverage
make check       # format + lint
make test-cov    # coverage report
```

## Command flow

```text
API / CLI / caller
    → CommandService
    → CommandBroker    (RBAC + priority queue + audit)
    → WorldService / SimulationService
    → AsyncWorld       (query → mutate → execute → persist)
    → QueryService     (read path)
```

Roles (flat, not hierarchical):

| Role | Permissions |
|------|-------------|
| `viewer` | Read-only |
| `player` | spawn, despawn, update, message, custom |
| `coder` | add/remove components, update |
| `maintainer` | spawn, despawn, components, processors, update |
| `admin` | All commands |

## Conventions

**Components**

- Use `_json` suffix for non-primitive types that must serialize through Arrow: `history_json: str = "[]"`
- Keep components small and single-purpose.

**Processors**

- One processor, one concern.
- Lower `priority` runs first.
- Return a new DataFrame; never mutate in place.
- `daft.functions.prompt` for LLM calls.

**Testing**

- Integration tests in `tests/integration/`.
- Use the `tmp_path` fixture for storage isolation.

**Commits**

- Conventional prefixes: `feat:`, `fix:`, `docs:`, `refactor:`.
- Atomic commits; reference issues when applicable.

## Key files

| File | Purpose |
|------|---------|
| `docs/guide/specification.md` | Normative contracts (engine, app, sugar/runtime) |
| `LEARNINGS.md` | Daft patterns, UDF rules, data-centric principle |
| `src/archetype/app/container.py` | Service wiring |
| `src/archetype/app/command_service.py` | Mutation dispatch |
| `src/archetype/app/broker.py` | RBAC + priority queue |
| `src/archetype/core/aio/async_world.py` | World runtime |
