# AGENTS.md

Repo-specific guidance for AI collaborators. For normative behavior, read the specification group under `docs/guide/`, starting with `docs/guide/specification.md`.

## Package ownership

Choose the owning package before adding a type or behavior:

| Kind | Canonical location |
|---|---|
| Components, processors, pure DataFrame transforms, transition graphs, and reusable projections | `archetype.<family>` |
| Supported family value contracts | `archetype.<family>.contracts` or another specifically named family module |
| Capability-scoped resources and provider adapters implementing a family-owned protocol | A named subpackage of `archetype.<family>` |
| Durable authority, cross-family orchestration, internal service ports, and concrete application services | `archetype.app.<family>` |
| Transport, authentication, application facade, and composition | `archetype.api`, `archetype.app.gateway`, `archetype.app.application`, and `archetype.app.container` |

Top-level families may import `archetype.core`, themselves, third-party
libraries, and only lower top-level family contracts declared in
`quality/architecture.toml` and the family fragments under
`quality/architecture.d/`. They never import `app`, `runtime`, `api`, or `cli`;
application families may consume their contracts in the other direction. Use
`components.py`, `processors.py`, `contracts.py`,
`transitions.py`, `interfaces.py`, and `service.py` according to those semantic
roles. Every first-party top-level package or module must be classified as
reserved infrastructure or a registered family, and the family graph must
remain acyclic. Root-facade imports receive the disposition of their owning module.
Package placement never makes a symbol public by itself.

A reviewed family may own a capability-scoped resource adapter without gaining
application authority. Agent Missions is the concrete example: coding-agent
state, processors, relations, and sandbox resources live under
`archetype.missions`; `archetype.app.missions` composes them into a workflow.

## Layout

```text
archetype/
├── src/archetype/
│   ├── core/           # ECS engine (Daft + Arrow + LanceDB)
│   ├── <family>/       # Reusable ECS/domain state and pure behavior
│   ├── app/            # Internal application families
│   │   ├── application/ #   Actor-free RuntimeApplication facade
│   │   ├── gateway/     #   CommandGateway + RBAC/auth
│   │   ├── commands/    #   Durable scheduler/dispatcher
│   │   ├── world/       #   Lifecycle, mutation, simulation
│   │   ├── storage/     #   Daft execution, Catalog, stores + control authority
│   │   ├── query/       #   Persisted read path
│   │   ├── ingestion/   #   World/run envelope + append selection
│   │   ├── artifacts/   #   File source policy + typed index publication
│   │   ├── evaluation/  #   Grading + receipts
│   │   ├── research/    #   Autoresearch workflows
│   │   ├── audit/       #   Append-only projection
│   │   └── container.py #   Sole concrete composition root
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
| `<family>/` | Reusable domain contracts and pure behavior. Follow the declared top-level family DAG. |
| `app/` | Extend carefully. Internal authority, orchestration, service ports, and concrete implementations. |
| `runtime/` | Recommended top-level API (`ArchetypeRuntime`). Contract changes require focused specs/tests. |
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

## Inspecting the service layer (internal)

`ServiceContainer` and concrete services are internal implementation
machinery, not supported application APIs. Focused implementation tests and
repository wiring code may use them. Application code and examples use
`ArchetypeRuntime`; untrusted hosts use the REST/API gateway boundary.

The container exposes actor-free `application` and authorized
`command_gateway` ports. The runtime consumes only `application`; FastAPI
consumes only `iCommandGateway`. `ActorCtx` is an ingress/gateway concept and
never belongs on a runtime handle.

```python
import asyncio
from archetype.app.container import ServiceContainer
from archetype.app.models import Command, CommandType
from archetype.app.gateway.auth.models import ActorCtx
from archetype.core.config import WorldConfig, StorageConfig, RunConfig
from uuid_utils import uuid7

async def main():
    container = ServiceContainer()
    ctx = ActorCtx(id=uuid7(), roles={"admin"})
    info = await container.command_gateway.create_world(
        ctx,
        WorldConfig(name="experiment"),
        StorageConfig(),
    )

    cmd = Command(type=CommandType.SPAWN, payload={"components": []})
    await container.command_gateway.submit(ctx, info.world_id, cmd)

    result = await container.command_gateway.run(
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
archetype query <world-id> [ComponentTypes] [--where EXPR] [--show N | --count]
archetype history <world-id>
```

The CLI (except `serve`) is an HTTP client against the running server. See `README.md` for full REST routes.

## Tests and CI

```bash
make ci          # complete PR verification profile
make verify-full # PR profile + process/reliability evidence
make verify-release # installed-artifact release profile
make test        # fast tests, no coverage
make static      # format/lint/type/lock/registry checks
make test-cov    # coverage report
```

PR flow: open the PR and stop — never run `gh pr merge --auto`. The
automerge workflow arms after the review gate passes your current head
(premature arms are auto-reverted; arming early only skips the review, it
never merges sooner). Reply to footgun review threads with what you
changed before resolving them.

## Application flow

```text
Trusted script → ArchetypeRuntime → RuntimeApplication

CLI → API authentication → CommandGateway authorization
                             → RuntimeApplication

RuntimeApplication → family workflow ports → AsyncWorld / durable storage

Deferred admission → CommandScheduler → durable control catalog
Simulation tick    → CommandScheduler drain → tick commit + command settlement

CLI → API over HTTP (except server startup)
```

Roles (flat, not hierarchical):

| Role | Permissions |
|------|-------------|
| `viewer` | Read-only |
| `player` | spawn, despawn, update, message, custom |
| `operator` | schema, processors, hooks, resources, simulation, fork, destroy |
| `admin` | All commands |

## Change-safety quick reference

- Keep dependencies pointing downward: runtime → actor-free app application
  port; API → gateway port; gateway → application port; app → core. CLI is an
  HTTP client of API. Do not leak `AsyncWorld`, the container, backend clients,
  or concrete services across either boundary.
- Treat `src/archetype/core/` as invariant-owned. Prefer an app or runtime
  extension when it can meet the requirement; discuss any core behavior change
  before implementing it.
- Preserve the lazy Daft DAG. Prefer expressions and DataFrame transforms;
  `.collect()` or `.to_pylist()` in `src/` needs a documented
  `lazy_audit.toml` exception at a real execution boundary. Application-owned
  terminal Daft work flows through `StorageService`; keep Catalog table
  registration/read/write, schema comparison, and Iceberg retry there.
- Keep storage planes distinct. SQLite or the remote Durable Object is the
  transactional control authority for world records, fences, commands, and
  manifests. Iceberg is the data authority for atomic table snapshots and
  optimistic multi-writer commits. `IngestionService` adds the world/run
  envelope and selects plain versus key-conditional append; it does not
  duplicate storage execution.
- A tick is a commit boundary: compute all archetypes before persistence, and
  do not consume staged mutations or advance the tick until durable visibility
  is published. Failed ticks must remain retryable.
- Keep runtime and world lifetimes distinct. Handles are lazy and actor-free;
  world shutdown is local, while runtime teardown owns shared services.
- When changing behavior, update the focused contract test (and the
  specification if the contract itself changes), not only a happy-path test.

### Contract-first issue loop

For non-trivial work, capture the behavior, owning layer, normative source,
executable oracle, invariants at risk, validation, and affected docs in the
issue. Use focused normative specs first, then executable contracts, then the
umbrella specification, with guides and examples as teaching material. If
those sources disagree, surface the mismatch instead of choosing silently.
Split adjacent drift into separate issues, implement the smallest layer-correct
change, and report the exact validation that ran. See
`docs/guide/contributing.md` for the full workflow and documentation register.

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
- A reported bug gets one deterministic pytest regression first. Add a repository scenario only when a matrix of backends, entry points, lifecycle states, or concurrency schedules proves a broader invariant that the focused test cannot own alone.
- The self-harness is repository-level: `tests/`, `evals/`, `bench/`, static audits, and mutation probes consume the shipped library. Do not move them into `src/archetype/core/` or import them from production code. Product-facing evaluation remains under `src/archetype/` (`EvalService`, dataset identity, graders, and receipts).
- Examples are part of the contract. Run them in CI; gate LLM-backed ones on credentials or degrade gracefully.
- Mutation testing via `mutmut` (`make mutmut`) is on-demand, not in `make ci`. Scope is narrow by design — see `docs/guide/mutation-testing.md`.

### Commits

- Conventional prefixes: `feat:`, `fix:`, `docs:`, `refactor:`.
- Atomic commits; reference issues when applicable.

## Key files

| File | Purpose |
|------|---------|
| `docs/guide/specification.md` | Specification overview |
| `docs/guide/application-architecture.md` | Normative dependency and encapsulation policy |
| `docs/guide/runtime.md` | Runtime contract |
| `docs/guide/service-protocols.md` | App service contracts |
| `docs/guide/command-gate.md` | Roles, permissions, and audit gate |
| `LEARNINGS.md` | Daft patterns, UDF rules, data-centric principle |
| `src/archetype/runtime/` | `ArchetypeRuntime` — recommended top-level API |
| `src/archetype/app/container.py` | Service wiring |
| `src/archetype/app/gateway/service.py` | Authorized ingress gateway |
| `src/archetype/app/commands/service.py` | Durable scheduler and dispatcher |
| `src/archetype/app/storage/service.py` | Daft execution and durable storage authority |
| `src/archetype/ingestion/pipeline.py` | Cohesive reusable file-ingestion graph |
| `src/archetype/core/aio/async_world.py` | World runtime |
| `tests/app/test_runtime_contracts.py` | Executable runtime contracts |
| `tests/sync/test_sync_stack_contracts.py` | Executable sync engine contracts |
