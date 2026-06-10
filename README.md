<div align="center">

# Archetype

**A forkable, append-only world ledger for simulations and AI agents.**

[![CI](https://github.com/VangelisTech/archetype/actions/workflows/python-tests.yml/badge.svg)](https://github.com/VangelisTech/archetype/actions/workflows/python-tests.yml)
[![Python 3.12+](https://img.shields.io/badge/python-3.12%2B-blue)](https://python.org)
[![License: Apache 2.0](https://img.shields.io/badge/license-Apache%202.0-blue)](LICENSE)

</div>

![Archetype Architecture Diagram](assets/archetype_diagram2.png)

Every tick of a running world persists as queryable Arrow rows keyed
`(world_id, run_id, tick)`. Nothing is ever overwritten — there is no update
path and no delete path anywhere in the storage layer. Everything distinctive
about Archetype falls out of that one decision:

- **Time travel is a query.** `df.where(col("tick") == t)` is the state of the
  world at tick `t`. Forever.
- **Forking is branching the timeline.** Fork any moment of any run, vary one
  condition, and diff the branches with a dataframe query. Forks read pre-fork
  history through lineage — O(metadata), no row copying.
- **Every run leaves a dataset behind.** Trajectories, rollout results, and
  audit history land in the same store as world state, ready for analysis
  without an export step.
- **A tick either commits or it didn't happen.** Failed persistence raises; a
  failed processor fails its tick. The ledger has no silent holes.

Mechanically, Archetype is an ECS on the [Daft](https://daft.ai) dataframe
engine: entities are rows grouped into columnar archetype tables by exact
component set, behavior is DataFrame transforms over whole archetypes, and a
deterministic tick loop is the ledger's commit protocol.

## What it's for

Workloads where history is part of the model, not exhaust:

- **Counterfactual evaluation of agent populations** — run many LLM agents in
  a shared world, fork mid-history, replay a branch under a different
  condition, and compare outcomes as tables.
- **Rollout-heavy simulation** — episodes and rollouts are first-class; every
  rollout's full tick history is queryable after the fact.
- **Trajectory datasets** — agent runs recorded as rows you can filter, join,
  grade, and train on (`archetype.experiments`).
- **Multi-agent worlds with replay** — anything you'd want to rewind, audit,
  or branch.

Orchestration frameworks checkpoint a conversation thread. Game ECS engines
snapshot in-process memory. RL environments discard state on `reset()`.
Archetype persists the whole world, every tick, as data — so replay, forks,
and audit are storage facts rather than features.

## The tick

One pass of the loop, for every archetype concurrently:

1. external calls enter through the command gate, which authorizes, audits,
   and defers mutations to the next tick boundary
2. queued commands drain in deterministic `(tick, priority, sequence)` order,
   with entity ids reserved at submit time
3. the world reads tick `N-1`, materializes spawns/despawns
4. processors transform the archetype's DataFrame in priority order
5. the result is appended at tick `N` — or the step raises

The tick boundary is the frame of the system: the deterministic answer to
"when does an agent's action land." Same world state + same command queue +
same processor outputs → same ledger.

## Installation

```bash
pip install archetype-ecs
```

Development:

```bash
git clone https://github.com/VangelisTech/archetype.git
cd archetype
uv sync --group dev
```

## Quickstart

`ArchetypeRuntime` is the recommended entry point. It owns the shared
container, activates a world lazily on first use, and returns a real
`entity_id` from `spawn()`.

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

        df = await world.query(Position)  # full append-only history
        print(df.collect().to_pylist())


asyncio.run(main())
```

Fork-and-diff — the move the storage model exists for:

```python
fork = await world.fork("counterfactual")  # inherits the source's store
await fork.step()                          # continues from the source's last tick

source_df = await world.query(Position)
fork_df = await fork.query(Position)       # pre-fork history + its own branch
```

For sync scripts, use `with ArchetypeRuntime.sync() as runtime:` and drop the
`await`s.

Two things to know:

- component columns are prefixed `componentname__field` (e.g., `position__x`)
- `ArchetypeRuntime` is the script boundary. Process lifetime and world
  lifetime are separate concerns. See `docs/guide/runtime.md` and the
  Specifications group for the full contract set. Drop to `ServiceContainer`
  only when you need explicit RBAC, custom command routing, or a non-script
  host.

## How it's organized

| Layer | What it is |
|---|---|
| `src/archetype/core` | The ledger and the tick loop. Hard invariants: append-only stores, canonical archetype identity, lineage-aware reads, loud persistence failures. |
| `src/archetype/app` | The gate. Every operation is authorized, audited, and — for mutations — deferred to the tick boundary through a deterministic broker. |
| `src/archetype/runtime` | `ArchetypeRuntime` — the recommended script boundary. World handles that route everything through the gate. |
| `src/archetype/api` + `src/archetype/cli` | A reference deployment of the gate over HTTP, plus a thin CLI client. Inspection and ops — worlds get their behavior (processors, hooks) in-process. |
| `src/archetype/experiments` | Experiment tracking as components: runs, results, trajectories, branch heads. The ledger's first first-party consumer. |

## Core Concepts

### Components

Components are typed `LanceModel` subclasses. Their fields define the
archetype schema fragments that get flattened into storage columns.

```python
class Health(Component):
    hp: int = 100
    max_hp: int = 100
```

`Health` becomes columns like `health__hp` and `health__max_hp`.

### Archetypes

An archetype is the exact set of component types attached to an entity.
Signatures are canonicalized by sorted component type name, so component
order is not meaningful. Adding or removing a component migrates the entity
to a different archetype table.

### Processors

Processors are pure DataFrame transforms selected by subset match on
component signatures:

```python
class ThinkProcessor(AsyncProcessor):
    components = (Agent, Memory)
    priority = 20
```

If an archetype contains at least `Agent` and `Memory`, that processor runs
on its DataFrame. Because a processor sees the whole population at once, an
LLM-backed processor batches inference across every matching agent in one
pass instead of looping agent by agent.

### Forking and lineage

A fork gets a new `world_id` and `run_id`, preserves the tick position, and
carries a *lineage* — pointers to the ancestor segments of its timeline.
Pre-fork ticks resolve to the ancestor's immutable rows; post-fork ticks are
the fork's own. The parent can keep running or be destroyed without affecting
the fork's view. Lineage is persisted append-only at fork time, so ancestry
survives process restarts and dead worlds. See
`docs/guide/world-lifecycle.md`.

### Commands and governance

All external mutations flow through one gate:

```text
caller → CommandService → direct delegate or tick-deferred CommandBroker → world → store
```

The gate enforces role permissions (`viewer`, `player`, `operator`, `admin`),
per-tick command quotas, token budgets, and emits one audit row per gated
call — to an append-only audit table you query like any other DataFrame.
Auth today is developer-mode (role-as-bearer-token); treat the RBAC surface
as single-trusted-user until v2 auth lands.

### Storage

Two async backends behind the same contracts: `AsyncLancedbStore` (LanceDB,
default) and `AsyncStore` (Daft catalog / Iceberg). `StorageService` pools
instances by `(uri, namespace, backend, cache config)`, and the gate resolves
each world's recorded store so readers find rows wherever the world wrote
them.

## CLI and REST (reference deployment)

`archetype serve` exposes the gate over HTTP; the CLI is a thin client for
it. This surface is for inspection and operations — listing worlds, stepping,
forking, reading audit history. Worlds created over the wire have no
processors; behavior is attached in-process through `ArchetypeRuntime`.

```bash
archetype serve                       # start the FastAPI server
archetype world create demo           # create a world
archetype run <world-id> --steps 10   # run ticks
archetype world fork <world-id> --name branch-a
archetype history <world-id>          # audit history
```

Full route table and flags: `docs/guide/api-layer.md`.

## Status

Honest state of the system:

- the ledger — append-only write path, tick loop, time travel, fork lineage —
  is the most mature part, and the most heavily contract-tested
- `archetype.experiments` (runs, results, trajectories) is young but real;
  the AutoResearch loop controller is early
- the FastAPI layer runs a default admin `ActorCtx` — not multi-tenant auth
  yet; the four-role model is enforced at the gate but identities are
  developer-mode
- a Rust core implementing the same engine semantics (arrow-rs, append-only
  Parquet, Arrow C Data Interface) is in progress on a separate branch

Start with `src/archetype/runtime` (`ArchetypeRuntime`) to use the system.
Read `src/archetype/core` and `src/archetype/app` to understand how it works
underneath.

## Repository Map

```text
archetype/
├── src/archetype/runtime/   # ArchetypeRuntime — recommended top-level API
├── src/archetype/core/      # The ledger: ECS runtime and storage contracts
├── src/archetype/app/       # The gate: command service, broker, audit
├── src/archetype/api/       # FastAPI server (reference deployment)
├── src/archetype/cli/       # Typer CLI (thin client)
├── src/archetype/experiments/ # Runs, results, trajectories as components
├── examples/                # Runnable examples
├── tests/                   # Test suite (contract tests pin the spec)
├── docs/                    # MkDocs site
├── AGENTS.md                # Repo-specific collaborator guidance
└── LEARNINGS.md             # Architecture notes
```

## Examples

```bash
uv run python examples/01_world_mutations.py
uv run python examples/02_fork_counterfactual.py
uv run python examples/03_time_travel.py      # historical reads + fork-and-diff
uv run python examples/04_messaging.py
uv run python examples/05_llm_agents.py
uv run python examples/06_trajectory_analysis.py
uv run python examples/07_hooks.py
```

`examples/05_llm_agents.py` and parts of `examples/06_trajectory_analysis.py`
require `OPENAI_API_KEY`.

## Observability

Archetype ships with [Logfire](https://pydantic.dev/logfire) integration at
three levels: gate spans on every `CommandService` method, step-phase spans
inside each tick (query / materialize / execute / update), and opt-in
per-tick/per-entity hooks:

```python
from archetype.contrib.logfire_observer import logfire_hooks

world = runtime.world("demo", processors=[...], hooks=logfire_hooks())
```

The runtime calls `logfire.configure()` automatically, and stdlib logging is
bridged in, so `logger.*` calls appear as Logfire events.

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
- Specifications: `docs/guide/specification.md`, `docs/guide/runtime.md`,
  `docs/guide/service-protocols.md`, `docs/guide/command-gate.md`,
  `docs/guide/execution-hierarchy.md`, `docs/guide/world-lifecycle.md`,
  `docs/guide/audit-log.md`

## License

Apache 2.0 — `LICENSE`
