# Archetype

## A forkable, append-only world ledger for simulations and AI agents

Every tick of a running world persists as queryable Arrow rows keyed
`(world_id, run_id, tick)`. Nothing is ever overwritten — there is no update
path and no delete path anywhere in the storage layer. Everything distinctive
about Archetype falls out of that one decision:

- **Time travel is a query.** `df.where(col("tick") == t)` is the state of the
  world at tick `t`. Forever.
- **Forking is branching the timeline.** Fork any moment of any run, vary one
  condition, and diff the branches with a dataframe query. Forks read
  pre-fork history through lineage — O(metadata), no row copying.
- **Every run leaves a dataset behind.** Trajectories, rollout results, and
  audit history land in the same store as world state.
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
  condition, and compare outcomes as tables
- **Rollout-heavy simulation** — episodes and rollouts are first-class; every
  rollout's full tick history is queryable after the fact
- **Trajectory datasets** — agent runs recorded as rows you can filter, join,
  grade, and train on (see [Trajectories](guide/trajectories.md) and
  [AutoResearch](guide/autoresearch.md))
- **Multi-agent worlds with replay** — anything you'd want to rewind, audit,
  or branch

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

## How it's organized

| Layer | What it is |
|---|---|
| `src/archetype/core` | The ledger and the tick loop. Hard invariants: append-only stores, canonical archetype identity, lineage-aware reads, loud persistence failures. |
| `src/archetype/app` | The gate. Every operation is authorized, audited, and — for mutations — deferred to the tick boundary through a deterministic broker. |
| `src/archetype/runtime` | `ArchetypeRuntime` — the recommended script boundary. World handles that route everything through the gate. |
| `src/archetype/api` + `src/archetype/cli` | A reference deployment of the gate over HTTP, plus a thin CLI client. Inspection and ops — worlds get their behavior in-process. |
| `src/archetype/experiments` | Experiment tracking as components: runs, results, trajectories, branch heads. |

## Quickstart

Install the package:

```bash
pip install archetype-ecs
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

Two important details:

- component columns are always prefixed `componentname__field`
- `ArchetypeRuntime` is the script boundary; use `world.as_actor(...)` for
  explicit roles and drop to `ServiceContainer` only for custom command
  routing or lower-level lifecycle control

See [Quickstart](guide/quickstart.md) for more.

## Core Concepts

### Components

Components are typed `LanceModel` subclasses. Their fields define the
archetype schema fragments that get flattened into storage columns.
See [Components](guide/components.md).

### Archetypes

An archetype is the exact set of component types attached to an entity.
Adding or removing a component migrates the entity to a different archetype
table. See [Archetype](guide/archetype.md).

### Processors

Processors are DataFrame transforms selected by subset match on component
signatures. Because a processor sees the whole population at once, an
LLM-backed processor batches inference across every matching agent in one
pass. See [Processors](guide/processors.md) and
[System Execution](guide/system-execution.md).

### Worlds

`AsyncWorld` owns entity-to-archetype bookkeeping, pending mutation caches,
lifecycle hooks, and the query / execute / update orchestration of the tick.
Different archetypes are processed concurrently; processors within one
archetype run in ascending `priority`. See [Worlds](guide/worlds.md).

### Forking and lineage

A fork gets a new `world_id` and `run_id`, preserves the tick position, and
carries a *lineage* — pointers to the ancestor segments of its timeline.
Pre-fork ticks resolve to the ancestor's immutable rows; post-fork ticks are
the fork's own. Lineage is persisted append-only at fork time, so ancestry
survives process restarts and destroyed worlds.
See [World Lifecycle](guide/world-lifecycle.md).

### Commands and governance

All external mutations flow through one gate:

```text
caller → CommandService → direct delegate or tick-deferred CommandBroker → world → store
```

The gate enforces role permissions, per-tick command quotas, token budgets,
and emits one audit row per gated call — to an append-only audit table you
query like any other DataFrame. See [Command Gate](guide/command-gate.md),
[Services](guide/services.md), and [Data Flow](guide/data-flow.md).

### Storage

Two async storage backends behind the same contracts: `AsyncLancedbStore`
(LanceDB, default) and `AsyncStore` (Daft catalog / Iceberg).
`StorageService` pools instances by `(uri, namespace, backend, cache config)`.
See [Stores](guide/stores.md).

### Resources

Resources are world-scoped dependencies injected into processors outside the
entity/component storage path. See [Resources](guide/resources.md).

## CLI and REST (reference deployment)

`archetype serve` exposes the gate over HTTP; the CLI is a thin client for
it. This surface is for inspection and operations — listing worlds, stepping,
forking, reading audit history. Worlds created over the wire have no
processors; behavior is attached in-process through `ArchetypeRuntime`.

```bash
archetype serve
archetype world create demo
archetype run <world-id> --steps 10
archetype world fork <world-id> --name branch-a
archetype history <world-id>
```

See [API Layer](guide/api-layer.md), [REST API Reference](reference/rest-api.md),
and [CLI Reference](reference/cli.md).

## Specifications

Normative behavior lives in the Specifications group:

- [Overview](guide/specification.md)
- [Runtime](guide/runtime.md)
- [Service Protocols](guide/service-protocols.md)
- [Command Gate](guide/command-gate.md)
- [Execution Hierarchy](guide/execution-hierarchy.md)
- [World Lifecycle](guide/world-lifecycle.md)
- [Audit Log](guide/audit-log.md)

## Status

Honest state of the system:

- the ledger — append-only write path, tick loop, time travel, fork lineage —
  is the most mature part, and the most heavily contract-tested
- `archetype.experiments` (runs, results, trajectories) is young but real
- the FastAPI layer currently uses a default admin `ActorCtx` — not
  multi-tenant auth yet

## Where to Start

- **New to Archetype?** Start with the [Quickstart](guide/quickstart.md),
  which leads with `ArchetypeRuntime`, then read the
  [Architecture](guide/architecture.md) overview
- **Building a simulation?** See
  [Building Simulations](guide/building-simulations.md) for the full workflow
- **Evaluating agents?** See [Trajectories](guide/trajectories.md) and
  [AutoResearch](guide/autoresearch.md)
- **Integrating with the API?** See [App Overview](guide/app-overview.md) for
  how core connects through services to the HTTP layer
- **Changing contracts?** Start with
  [Service Protocols](guide/service-protocols.md) and the focused
  specification pages
