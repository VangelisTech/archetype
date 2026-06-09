# Running & Scripting Archetype

This page is the practical counterpart to the architecture docs: how to actually
**run** Archetype and **drive a world from your own code**. If you want to know
*how it works* underneath, read [Architecture](architecture.md); if you want it
*running in five minutes*, the [Quickstart](quickstart.md) is the condensed
version of this page.

## Three ways to run

Archetype exposes the same world through three entry points. Pick the one that
matches how you want to drive it.

| Entry point | You write | Use when |
|---|---|---|
| **Python script** (`ArchetypeRuntime`) | Python, in-process | Simulations, prototypes, notebooks, tests — **start here** |
| **CLI** (`archetype ...`) | Shell commands against a running server | Operating a world from the terminal |
| **HTTP / REST** (`archetype serve`) | Requests to a FastAPI server | Integrating Archetype into another service |

`ArchetypeRuntime` is the **script boundary** — the recommended top-level API.
The CLI is a thin HTTP client; except for `serve`, every CLI command talks to a
running server. The HTTP layer and the CLI share the same gated service layer the
runtime uses, so behavior is identical across all three.

## Install

```bash
git clone https://github.com/VangelisTech/archetype.git
cd archetype
uv sync --group dev          # dev extras: tests, linters, examples
```

Or, to use the published package in your own project:

```bash
pip install archetype-ecs
```

Python 3.12+ is required.

## Run a Python script

This is the canonical way to use Archetype. The whole lifecycle lives inside one
`async with ArchetypeRuntime()` block — process lifetime and world lifetime are
separate concerns, and the runtime owns the shared container, activates a world
lazily on first use, and shuts everything down cleanly on exit.

```python
import asyncio

from daft import DataFrame, col

from archetype import ArchetypeRuntime, AsyncProcessor, Component


# 1. Define components — typed models whose fields become prefixed columns
class Position(Component):
    x: float = 0.0
    y: float = 0.0


class Velocity(Component):
    dx: float = 0.0
    dy: float = 0.0


# 2. Write a processor — a DataFrame transform that runs each tick on every
#    entity whose archetype contains AT LEAST these components
class Movement(AsyncProcessor):
    components = (Position, Velocity)
    priority = 10

    async def process(self, df: DataFrame, **_) -> DataFrame:
        return df.with_columns(
            {
                "position__x": col("position__x") + col("velocity__dx"),
                "position__y": col("position__y") + col("velocity__dy"),
            }
        )


async def main():
    async with ArchetypeRuntime() as runtime:
        # 3. Create a lazy world handle and stage processors
        world = runtime.world("demo", processors=[Movement()])

        # 4. Spawn entities — spawn() returns a real entity_id
        await world.spawn(Position(x=0, y=0), Velocity(dx=1, dy=2))

        # 5. Run ticks
        result = await world.run(steps=5)
        print(f"{result.ticks_completed} ticks, final tick {result.final_tick}")

        # 6. Read state back
        info = await world.info()
        df = await world.query(Position)
        current = df.where(col("tick") == info.tick - 1)
        print(current.collect().to_pylist())


asyncio.run(main())
```

Run it:

```bash
uv run python my_sim.py
```

### Sync scripts (no `await`)

If you don't want to write `async`/`await`, use the sync facade. It owns its own
event loop and exposes the identical surface:

```python
from archetype import ArchetypeRuntime

with ArchetypeRuntime.sync() as runtime:
    world = runtime.world("demo", processors=[Movement()])
    world.spawn(Position(), Velocity(dx=1, dy=2))
    world.run(steps=5)
    print(world.query(Position).collect().to_pylist())
```

Component and processor definitions are identical; only the runtime boundary and
the dropped `await`s differ.

## The scripting pattern

Every simulation, no matter how complex, is the same five steps:

1. **Define components** — the data your entities carry (`class X(Component)`).
2. **Write processors** — the per-tick rules (`class Y(AsyncProcessor)`), selected
   by subset match on their `components` tuple and ordered by ascending `priority`.
3. **Create a world** and stage processors, resources, and hooks at construction.
4. **Spawn entities** with initial component values.
5. **Run** — `step`, `run`, `run_episode`, or `run_rollout`.

The full worked example (agents that accumulate experience and a scorer
processor) lives in [`examples/simulation_script.py`](https://github.com/VangelisTech/archetype/blob/main/examples/simulation_script.py)
and is walked through in [Building Simulations](building-simulations.md).

### Configuring the world at construction

The handle is declarative — pass everything it needs up front:

```python
from archetype.core.config import StorageConfig, CacheConfig
from archetype.core.hooks import PreTick

world = runtime.world(
    "demo",
    storage="./data",                       # str | Path | StorageConfig
    cache=CacheConfig(flush_rows=500_000),
    processors=[Movement(), Scorer()],
    resources=[shared_state],
    hooks=[(PreTick, lambda e: print(f"tick {e.tick}"))],
)
```

Processors can be added or removed later (`world.add_processor(...)`,
`world.remove_processor(...)`). Hooks can be added with `world.add_hook(...)`
only **after** the world has activated (its first operation) — before that,
pass them via `runtime.world(..., hooks=[...])` or the call raises. Resources
can only be staged at construction. See the full surface in
[Runtime §3](runtime.md#3-ergonomic-surface).

## Driving execution

Four levels of execution, from one tick to many forked episodes:

```python
await world.step()                       # advance exactly one tick
await world.run(steps=10)                # run 10 ticks, returns RunResult
await world.run_episode(episode_config)  # step until a termination condition
await world.run_rollout(rollout_config)  # fork N episodes and aggregate
```

`run()` returns a `RunResult` with `ticks_completed`, `commands_applied`, and
`final_tick`. For fine-grained control, pass a `RunConfig`:

```python
from archetype.core.config import RunConfig

await world.run(config=RunConfig.dev(steps=5))     # debug output, live reads
await world.run(config=RunConfig(num_steps=100))   # explicit
```

`RunConfig` has named constructors for common modes (`RunConfig.dev()`,
`.benchmark()`, `.validate()`). See [Configuration](run-config.md) for every field
and [Execution Hierarchy](execution-hierarchy.md) for step/run/episode/rollout
semantics.

## Reading state back

!!! warning "`query()` returns full history, not just the current tick"
    Archetype is append-only: every tick is preserved as a new snapshot. So
    `world.query(...)` returns **all ticks** for matching entities. To get the
    *current* state, filter to the latest tick:

    ```python
    info = await world.info()
    df = await world.query(Position, Velocity)
    current = df.where(col("tick") == info.tick - 1)
    ```

    (Use `info.tick - 1` because `info.tick` is the *next* tick to run — the most
    recently persisted state is one less.)

This is a feature, not a quirk: keeping every tick is what makes time-travel
queries and trajectory analysis fall out for free. To inspect a past tick, filter
to any `tick` value instead of the latest. From there it's a normal Daft
DataFrame:

```python
rows = current.collect().to_pylist()   # list of dicts
current.show()                          # pretty-print
current.where(col("entity_id") == eid).show()
```

Columns are prefixed `componentname__field` — `Position` becomes `position__x`,
`position__y`.

## Persistence

By default a world keeps state in memory for the life of the runtime. To persist
to disk (or cloud storage), pass `storage`:

```python
from archetype.core.config import StorageConfig

storage = StorageConfig(uri="./archetype_data", namespace="my_experiment")
world = runtime.world("skill-sim", storage=storage, processors=[...])
```

`storage` accepts a plain `str`/`Path` (coerced to `StorageConfig(uri=...)`) or a
full `StorageConfig`. Storage and audit rows are **never** deleted — even
`world.destroy()` only drops the in-memory world. See [Stores](stores.md).

## Mutations, roles, and forking

The full mutation surface is available on the handle:

```python
eid = await world.spawn(Position(), Velocity(dx=1))
await world.update(eid, Position(x=10))            # overlay values (same archetype)
await world.add_components(eid, Health(hp=100))    # extend the archetype
await world.remove_components(eid, Velocity)       # shrink the archetype
await world.despawn(eid)
```

The runtime's default actor is `admin`. To test under a constrained role, rebind
the handle — operations that the role can't perform raise `PermissionError`:

```python
from archetype.app.auth.models import ActorCtx
from uuid_utils import uuid7

player = world.as_actor(ActorCtx(id=uuid7(), roles={"player"}))
await player.spawn(Position())          # allowed
# await player.add_processor(...)       # raises PermissionError
```

Fork a world to branch its state; the fork gets its own `world_id` and diverges
independently. Audit history is queryable per world:

```python
branch = await world.fork("branch-a")
history = await world.history(limit=100)   # DataFrame of audit rows
```

See [Worlds](worlds.md), [World Lifecycle](world-lifecycle.md), and the
[Command Gate](command-gate.md) for the full RBAC model.

## Run the server (CLI & HTTP)

To drive a world from the terminal or another service, start the FastAPI server:

```bash
archetype serve                  # defaults to http://localhost:8000
```

Then use the CLI (a thin HTTP client):

```bash
archetype world create demo
archetype world list
archetype entity spawn <world-id> --components '[{"type":"Position","x":0,"y":0}]'
archetype run <world-id> --steps 10
archetype world fork <world-id> --name branch-a
archetype history <world-id>
```

Useful knobs:

- `ARCHETYPE_URL` — base URL for the CLI (default `http://localhost:8000`);
  `--url` overrides it per command.
- `--role` / `-r` — developer auth shortcut (`admin`, `operator`, `player`, `viewer`).
- `--json` — emit raw JSON from read commands.

Or talk to the REST API directly:

```bash
# Create a world
curl -s -X POST localhost:8000/worlds \
  -H 'Content-Type: application/json' \
  -d '{"name": "demo"}'

# Spawn, run, and read state (use the returned world_id)
curl -s -X POST localhost:8000/worlds/$WID/entities \
  -H 'Content-Type: application/json' \
  -d '{"components": [{"type": "Position", "x": 0, "y": 0}]}'
curl -s -X POST localhost:8000/worlds/$WID/run \
  -H 'Content-Type: application/json' -d '{"num_steps": 5}'
curl -s localhost:8000/worlds/$WID/state
```

The complete route table is in the [REST API Reference](../reference/rest-api.md);
the generated command docs are in the [CLI Reference](../reference/cli.md).

## Runnable examples

Every core feature has a self-contained, runnable example. They're numbered in
onboarding order:

```bash
uv run python examples/01_world_mutations.py
```

| # | Example | Shows |
|---|---------|-------|
| 1 | `01_world_mutations.py` | Every mutation: spawn, despawn, update, add/remove components & processors, RBAC, fork, audit history |
| 2 | `02_fork_counterfactual.py` | Fork a world, run branches, compare |
| 3 | `03_time_travel.py` | Query any point in history — every tick is preserved |
| 4 | `04_messaging.py` | Agent-to-agent messaging, resources, lifecycle hooks |
| 5 | `05_llm_agents.py` | LLM-powered agents (one parallel LLM call per entity per tick) — needs `OPENAI_API_KEY` |
| 6 | `06_trajectory_analysis.py` | Trajectory analysis via forking |
| 7 | `07_hooks.py` | Lifecycle hooks for audit, metrics, debug traces |

See [`examples/README.md`](https://github.com/VangelisTech/archetype/blob/main/examples/README.md).

## Where to go next

- [Building Simulations](building-simulations.md) — the full design workflow.
- [Runtime](runtime.md) — the normative contract for the script boundary.
- [Processors](processors.md) — LLM processors, structured outputs, testing.
- [Configuration](run-config.md) — `RunConfig`, `StorageConfig`, `CacheConfig`.
- [Architecture](architecture.md) — how the layers and tick lifecycle fit together.
