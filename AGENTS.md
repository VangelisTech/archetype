# AGENTS.md

Repo-specific guidance for AI collaborators. For normative behavior, read the specification group under `docs/guide/`, starting with `docs/guide/specification.md`.

## Layout

```text
archetype/
├── src/archetype/
│   ├── core/           # ECS engine (Daft + Arrow + LanceDB)
│   ├── app/            # Service layer
│   │   ├── auth/       #   RBAC guard (ActorCtx, roles, quotas)
│   │   ├── broker.py   #   CommandBroker (priority queue)
│   │   ├── command_service.py    # Gate: authorize / delegate / audit
│   │   ├── world_service.py      # World lifecycle
│   │   ├── simulation_service.py # Tick stepping and runs
│   │   ├── query_service.py      # Read path
│   │   ├── storage_service.py    # Backend pooling
│   │   └── container.py          # Composition root
│   ├── api/            # FastAPI REST layer
│   ├── cli/            # Typer CLI (thin HTTP client)
│   └── runtime/        # Top-level runtime over the service layer
├── examples/
├── tests/
└── LEARNINGS.md        # Daft patterns and architectural notes
```

### Layers

| Layer | Access |
|-------|--------|
| `core/` | Modify only after discussion. It holds the hard invariants; breakage there cascades everywhere. |
| `app/` | Extend carefully. Service contracts are in the specification. Lower-level interface. |
| `runtime/` | Recommended top-level API (`ArchetypeRuntime`). Additive only; top-level exports stay stable. |
| `api/`, `cli/` | Write freely, subject to the contracts they wrap. |

## Top-level runtime (recommended)

`ArchetypeRuntime` is the recommended entry point for scripts and beginner docs. Process lifetime and world lifetime are separate concerns: the runtime owns the shared container; `world()` handles are lazy and world-local. See `docs/guide/runtime.md` for the full runtime contract.

```python
import asyncio
from archetype import ArchetypeRuntime

async def main():
    async with ArchetypeRuntime() as runtime:
        world = runtime.world("experiment")
        entity_id = await world.spawn()  # real entity_id, reserved through the chain
        result = await world.run(steps=10)
        print(f"Completed {result.ticks_completed} ticks")

asyncio.run(main())
```

Sync scripts use `with ArchetypeRuntime.sync() as runtime:` instead.

## Using the service layer (lower-level)

`ServiceContainer`, `CommandService`, and broker semantics are lower-level interfaces. Reach for them when you need explicit `ActorCtx` / RBAC, custom command routing, or to wire a non-script host. Beginner docs and quickstarts should default to `ArchetypeRuntime`.

```python
import asyncio
from archetype.app.container import ServiceContainer
from archetype.app.models import Command, CommandType
from archetype.app.auth.models import ActorCtx
from archetype.core.config import WorldConfig, StorageConfig, RunConfig
from uuid_utils import uuid7

async def main():
    container = ServiceContainer()
    ctx = ActorCtx(id=uuid7(), roles={"admin"})
    info = await container.command_service.create_world(
        ctx,
        WorldConfig(name="experiment"),
        StorageConfig(),
    )

    cmd = Command(type=CommandType.SPAWN, payload={"components": []})
    await container.command_service.submit(ctx, info.world_id, cmd)

    result = await container.command_service.run(
        ctx,
        info.world_id,
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
    → direct delegate or CommandBroker (tick-deferred queue)
    → WorldService / SimulationService
    → AsyncWorld       (query → mutate → execute → persist)
    → QueryService / AuditLog
```

Roles (flat, not hierarchical):

| Role | Permissions |
|------|-------------|
| `viewer` | Read-only |
| `player` | spawn, despawn, update, message, custom |
| `operator` | schema, processors, hooks, resources, simulation, fork, destroy |
| `admin` | All commands |

## Conventions

### Components

- Use `_json` suffix for non-primitive types that must serialize through Arrow: `history_json: str = "[]"`
- Keep components small and single-purpose.

### Processors

- One processor, one concern.
- Lower `priority` runs first.
- Return a new DataFrame; never mutate in place.
- `daft.functions.prompt` for LLM calls.

### Testing

- Integration tests in `tests/integration/`.
- Use the `tmp_path` fixture for storage isolation.
- Prefer contract tests over happy-path coverage: concurrent first-use activation, shutdown/fork races, multi-world lifetime isolation, spawn materialization timing, and example-script smoke execution. If a test feels "too specific," it is usually testing the real semantic boundary.
- Examples are part of the contract. Run them in CI; gate LLM-backed ones on credentials or degrade gracefully.
- Mutation testing via `mutmut` (`make mutmut`) is on-demand, not in `make ci`. Scope is narrow by design — see `docs/guide/mutation-testing.md`.

### Commits

- Conventional prefixes: `feat:`, `fix:`, `docs:`, `refactor:`.
- Atomic commits; reference issues when applicable.

## Key files

| File | Purpose |
|------|---------|
| `docs/guide/specification.md` | Specification overview |
| `docs/guide/runtime.md` | Runtime contract |
| `docs/guide/service-protocols.md` | App service contracts |
| `docs/guide/command-gate.md` | Roles, permissions, and audit gate |
| `LEARNINGS.md` | Daft patterns, UDF rules, data-centric principle |
| `src/archetype/runtime/` | `ArchetypeRuntime` — recommended top-level API |
| `src/archetype/app/container.py` | Service wiring |
| `src/archetype/app/command_service.py` | Mutation dispatch |
| `src/archetype/app/broker.py` | Priority queue |
| `src/archetype/core/aio/async_world.py` | World runtime |
| `tests/app/test_runtime_contracts.py` | Executable runtime contracts |
| `tests/sync/test_sync_stack_contracts.py` | Executable sync engine contracts |
