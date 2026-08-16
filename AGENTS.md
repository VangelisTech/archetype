# AGENTS.md

Repo-specific guidance for AI collaborators. For normative behavior, read the specification group under `docs/guide/`, starting with `docs/guide/specification.md`.

## Package ownership

Choose the owning package before adding a type or behavior:

| Kind | Canonical location |
|---|---|
| Components, processors, pure DataFrame transforms, transition graphs, reusable projections, and family-owned free handlers/workflows over declared lower-family ports | `archetype.<family>` |
| Supported family value contracts | `archetype.<family>.contracts` or another specifically named family module |
| Capability-scoped resources and provider adapters implementing a family-owned protocol | A named subpackage of `archetype.<family>` |
| Generic Activity identity, claims, attempts, fences, result references, and settlement | `archetype.activities` |
| Physical storage, control catalogs, commit coordination, and generic durable world/run envelopes | `archetype.storage` |
| Offline whole-storage migration planning, transfer, verification, and receipts | `archetype.migration` |
| Transport and authentication | `archetype.api` |
| Concrete composition and process lifetime | `archetype.wiring` and `archetype.runtime_resources` |
| Missions, Physical AI, or Research domain behavior | The owning distribution under `packages/archetype-<library>/src/archetype/<family>/` |
| One library's trusted framework composition adapter | Its private `archetype.<family>._extension` module only |

Top-level families may import `archetype.core`, themselves, third-party
libraries, and only lower top-level family contracts declared in
`quality/architecture.toml` and the family fragments under
`quality/architecture.d/`. They never import `app`, `runtime`,
`runtime_resources`, `wiring`, `api`, or `cli`; application families may
consume their contracts in the other direction. Use
`components.py`, `processors.py`, `contracts.py`,
`transitions.py`, `interfaces.py`, and `service.py` according to those semantic
roles. Every first-party top-level package or module must be classified as
reserved infrastructure or a registered family, and the family graph must
remain acyclic. Root-facade imports receive the disposition of their owning module.
Package placement never makes a symbol public by itself. First-party world
libraries depend on `archetype-ecs`, never on one another. Their ordinary
domain modules follow the family DAG above; only the private `_extension.py`
adapter may import framework composition contracts to register the exact
operations declared by its manifest.

`archetype.storage` is the reviewed physical-substrate family: it owns storage
execution, control-catalog implementations and records, physical visibility,
commit coordination, and the generic durable world/run envelope. Workflow
families consume that substrate through the narrow `iStorageService` port and
retain the meaning and orchestration of their workflows.

`archetype.migration` owns the offline whole-storage administrative workflow
over the declared `artifacts` and `storage` families. Concrete local endpoint
composition remains in `archetype.wiring`; migration does not gain process
composition authority.

The separately installed `archetype.research` library is the concrete
free-workflow example: it owns
AutoResearch values, ledger state, views, experiment-scoped admission, and the
directly awaited handler over the declared storage and world-family ports. It
does not require an application facade or service protocol.

A reviewed family may own a capability-scoped resource adapter and workflows
over declared lower-family ports without gaining framework composition
authority. Agent Missions owns coding-agent state, processors, relations,
sandbox resources, transcript/trajectory evidence, and Activity choreography
under the separately installed `archetype.missions` namespace.

The accepted Activity migration distinguishes tick-time capability from
between-tick durable work. A Resource is available while executing a tick;
correctness must not depend on its process-local lifetime. An Activity is
durably coordinated work admitted from one committed tick and observed by a
later committed tick. `archetype.activities` owns generic delivery mechanics
only and consumes the lower `archetype.storage.activity_catalog`; recovery
meaning stays with the owning family/provider adapter. Application choreography
belongs to the owning top-level family over declared lower-family ports.
Hosted-episode choreography belongs to the separately installed
`archetype.physical_ai` library; no application mirror is recreated. The
`AsyncResources`/WorldHost spike is frozen and must not be merged into this
path. See `docs/guide/activities.md`.

## Layout

```text
archetype/
├── packages/
│   ├── archetype-ecs/src/archetype/
│   │   ├── core/           # ECS engine (Daft + Arrow + LanceDB)
│   │   ├── storage/        # Physical rows, catalogs, commits + control authority
│   │   ├── world/          # Managed lifecycle, behavior, reads + operations
│   │   ├── commands/       # Registry, policy, dispatch, scheduling + audit
│   │   ├── activities/     # Generic between-tick delivery mechanics
│   │   ├── artifacts/      # File ingestion + typed/common indexes
│   │   ├── evaluation/     # Grading, leases + durable receipts
│   │   ├── migration/      # Offline whole-storage migration workflow
│   │   ├── world_libraries/ # Manifest contracts + deterministic discovery
│   │   ├── redaction/      # Canonical pre-durability redaction
│   │   ├── api/            # Domain-free FastAPI host
│   │   ├── cli/            # Domain-free HTTP client and server startup
│   │   ├── runtime/        # Supported trusted scripting handles
│   │   ├── runtime_resources.py
│   │   └── wiring.py       # Framework composition + extension installation
│   ├── archetype-missions/src/archetype/missions/
│   │   ├── _extension.py   # Private manifest/installation adapter
│   │   ├── runtime.py      # Missions + MissionWorld typed adapters
│   │   └── ...             # Coding agents, sandboxes, transcripts, trajectories
│   ├── archetype-physical-ai/src/archetype/physical_ai/
│   │   ├── _extension.py   # Private manifest/installation adapter
│   │   └── ...             # Physical state, policies + hosted episodes
│   └── archetype-research/src/archetype/research/
│       ├── _extension.py   # Private manifest/installation adapter
│       └── ...             # AutoResearch values, ledger + workflow
├── examples/
├── tests/
└── LEARNINGS.md        # Daft patterns and architectural notes
```

### Layers

| Layer | Access |
|-------|--------|
| `core/` | Modify only after discussion. It holds the hard invariants; breakage there cascades everywhere. |
| Framework families | Reusable contracts and generic behavior. Follow the declared top-level family DAG. |
| World-library family | Domain state, behavior, adapters, providers, tests, and docs owned by one independently installable distribution. |
| `_extension.py` | Trusted installation boundary. Keep it private, deterministic, and limited to declared manifest contributions. |
| Framework `runtime/` | Recommended generic API (`ArchetypeRuntime`). Contract changes require focused specs/tests. |
| Framework `api/`, `cli/` | Domain-free hosts; library routers arrive only through installed manifests. |

## Top-level runtime (recommended)

`ArchetypeRuntime` is the recommended entry point for scripts and beginner
docs. Process lifetime and world lifetime are separate concerns: the runtime
owns one `RuntimeResources`; `world()` handles are lazy and world-local. See
`docs/guide/runtime.md` for the full runtime contract.

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

Install domain behavior separately and use its typed adapter:

```python
from archetype.research import Research

research = Research(world)
result = await research.autoresearch(config, evaluator)
```

See `docs/guide/world-libraries.md` for base, selective, and full-stack
installation commands. Installing a world library is authorization to run its
trusted Python extension code in the process.

## Inspecting process wiring (maintainers only)

Concrete services and `archetype.wiring` are internal implementation
machinery, not supported application APIs. Application code and examples use
`ArchetypeRuntime`; untrusted hosts use REST/API authentication followed by
the same `CommandDispatcher`.

`build_runtime_resources()` is the single concrete composition transaction.
It returns the process owner; it does not expose parallel trusted or
actor-aware adapters. Maintainer diagnostics may inspect that owner narrowly:

```python
import asyncio
from archetype.wiring import RuntimeBootstrapConfig, build_runtime_resources

async def main():
    resources = build_runtime_resources(RuntimeBootstrapConfig.from_env())
    try:
        dispatcher = resources.dispatcher
        print(type(dispatcher).__name__)
    finally:
        await resources.aclose()

asyncio.run(main())
```

Trusted runtime handles call `dispatcher.apply()` or `defer()`. FastAPI
authenticates an `ActorCtx` from `archetype.commands.models`, then calls
`apply_as()` or `defer_as()`. Family handlers never receive the actor.

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
make ci          # required PR profile: static checks + fast tests
make verify-full # coverage, eval, package, example, operational + docs evidence
make verify-release # exact-wheel release profile
make test        # fast tests, no coverage
make static      # format/lint/type/lock/registry checks
make test-cov    # coverage report
```

PR flow: open the PR, wait for required `Static` and `Tests (3.12)`, address
concrete Codex and Cursor Bugbot findings, then squash merge manually. The
AI review gate, automatic merge workflow, and merge queue are disabled.
Heavy coverage, eval, external infrastructure, example, package, and
compatibility evidence runs at release cadence instead of blocking every PR.

### Rerun policy

- A required-check failure gets **one** authorized rerun, and only after a
  concrete classification (read the log or receipt first — never
  rerun-and-hope). A second failure of the same kind is a harness defect:
  stop, file it, do not keep rerunning.
- Treat Codex and Cursor Bugbot review as advisory evidence. Fix concrete
  findings; do not invent a second merge gate around them.

## Application flow

```text
Trusted script → ArchetypeRuntime / RuntimeWorld
                 → CommandDispatcher.apply / defer

Typed world-library adapter → exact library operation
Installed entry point → manifest validation → private installer

CLI → API authentication → CommandDispatcher.apply_as / defer_as

CommandDispatcher → exact OperationRegistry handler or CommandScheduler

Deferred admission → CommandScheduler → durable control catalog
Simulation tick    → CommandScheduler drain → tick commit + command settlement

CLI → API over HTTP (except server startup)
```

Role labels are flat inputs. Their built-in grant sets explicitly include the
preceding row; no unknown permission is inferred from a role name.

| Role | Permissions |
|------|-------------|
| `viewer` | Registered read-only operations |
| `player` | Viewer grants plus spawn, batch create, despawn, and update |
| `operator` | Player grants plus schema, processors, hooks, resources, simulation, fork, and destroy |
| `admin` | Operator grants plus world creation and mutable resume |

## Change-safety quick reference

- Keep dependencies pointing downward: runtime/API → commands and framework
  operation models; commands → world/storage; world → storage. A world library
  depends only on `archetype-ecs`, never another world library. Framework
  `wiring.py` owns the installation transaction; each library's private
  `_extension.py` composes only that library through `WorldLibraryContext`.
  CLI is an HTTP client of API. Do not leak `AsyncWorld`, `RuntimeResources`,
  backend clients, or concrete services across a supported boundary.
- Treat `packages/archetype-ecs/src/archetype/core/` as invariant-owned. Prefer
  a family, world-library, or runtime extension when it can meet the
  requirement; discuss any core behavior change before implementing it.
- Preserve the lazy Daft DAG. Prefer expressions and DataFrame transforms;
  `.collect()` or `.to_pylist()` in any package `src/` needs a documented
  `lazy_audit.toml` exception at a real execution boundary. Application-owned
  terminal Daft work flows through `StorageService`; keep Catalog table
  registration/read/write, schema comparison, and Iceberg retry there.
- Keep storage planes distinct. SQLite or the remote Durable Object is the
  transactional control authority for world records, fences, commands, and
  manifests. Iceberg is the data authority for atomic table snapshots and
  optimistic multi-writer commits. `StorageService` resolves and stamps the
  durable world/run envelope and selects plain versus key-conditional append.
  Artifact handlers require explicit durable coordinates and delegate typed
  publication directly to that substrate.
- A tick is a commit boundary: compute all archetypes before persistence, and
  do not consume staged mutations or advance the tick until durable visibility
  is published. Failed ticks must remain retryable.
- Required projectors persist deterministic intent only. Provider work derived
  from committed world intent runs as an Activity outside the world lock and
  returns bounded factual evidence to a later tick. Lease expiry or confirmed
  absence alone never authorizes replay without a provider-side retry guard,
  and settlement requires family completeness evidence bound to the exact
  recorded result digest.
- Keep runtime and world lifetimes distinct. Handles are lazy and actor-free;
  world shutdown is local, while `RuntimeResources` owns phased shared teardown.
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
- The self-harness is repository-level: `tests/`, `evals/`, `bench/`, static audits, and mutation probes consume the shipped library. Do not move them into `packages/archetype-ecs/src/archetype/core/` or import them from production code. Product-facing evaluation remains under `packages/archetype-ecs/src/archetype/evaluation/` (free handlers, dataset identity, graders, and receipts).
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
| `docs/guide/world-libraries.md` | Distribution, manifest, adapter, and compatibility contract |
| `docs/guide/runtime.md` | Runtime contract |
| `docs/guide/service-protocols.md` | App service contracts |
| `docs/guide/command-gate.md` | Roles, permissions, and audit gate |
| `docs/guide/activities.md` | Resource/Activity boundary and crash-recovery contract |
| `LEARNINGS.md` | Daft patterns, UDF rules, data-centric principle |
| `packages/archetype-ecs/src/archetype/runtime/` | `ArchetypeRuntime` — recommended top-level API |
| `packages/archetype-ecs/src/archetype/wiring.py` | Sole concrete composition transaction |
| `packages/archetype-ecs/src/archetype/runtime_resources.py` | Process lifetime, admission drain, and phased cleanup |
| `packages/archetype-ecs/src/archetype/commands/dispatch.py` | Governed direct and deferred command entry |
| `packages/archetype-ecs/src/archetype/commands/scheduler.py` | Durable scheduler and materializer |
| `packages/archetype-ecs/src/archetype/storage/service.py` | Daft execution and durable storage authority |
| `packages/archetype-ecs/src/archetype/artifacts/pipeline.py` | Cohesive reusable file-ingestion graph |
| `packages/archetype-ecs/src/archetype/core/aio/async_world.py` | World runtime |
| `packages/archetype-ecs/src/archetype/world_libraries/` | Trusted extension contracts and discovery |
| `packages/archetype-missions/src/archetype/missions/_extension.py` | Missions manifest and installation |
| `packages/archetype-physical-ai/src/archetype/physical_ai/_extension.py` | Physical-AI manifest and installation |
| `packages/archetype-research/src/archetype/research/_extension.py` | Research manifest and installation |
| `tests/app/test_runtime_contracts.py` | Executable runtime contracts |
| `tests/sync/test_sync_stack_contracts.py` | Executable sync engine contracts |
