# Build simulations that keep their history

Archetype is a Python runtime for simulations and agent workflows. Components
describe state. Processors transform matching entities as Daft DataFrames.
Each tick is stored, so inspecting an earlier state or branching a run does
not require a separate replay system.

```bash
pip install archetype-ecs
```

[Start the quickstart](guide/quickstart.md) ·
[Browse examples](guide/examples.md)

## The model

```text
components  +  processors  +  world
    state       behavior       history
```

- A **component** is typed entity data, such as `Position` or `Task`.
- A **processor** is a DataFrame transform that runs on entities with the
  components it declares.
- A **world** owns entities, runs ticks, and persists the resulting rows.

The Python runtime is the usual entry point. It owns the process-level
services and gives you world handles that are lazy until first use.

## Pick a path

| If you want to… | Start here |
|---|---|
| Run your first world | [Quickstart](guide/quickstart.md) |
| Build a simulation | [Building simulations](guide/building-simulations.md) |
| Model state | [Components](guide/components.md) |
| Write behavior | [Processors](guide/processors.md) |
| Spawn, query, and fork | [Working with worlds](guide/working-with-worlds.md) |
| Inspect past state | [History and forks](guide/history-and-forks.md) |
| Run a coding-agent software factory | [Agent Missions](guide/agent-missions.md) |
| Run a service over HTTP | [Service hosting](guide/app-overview.md) |
| Find an exact method or endpoint | [Reference](reference/python-api.md) |

## A complete tick

```python
await world.spawn(Position(x=0), Velocity(dx=1))
await world.run(steps=10)

history = await world.query(Position)
fork = await world.fork("faster-model")
```

`query()` returns the append-only history for the requested components. A fork
inherits the source history through lineage and writes its later ticks to its
own branch.

## Use it from a script or a service

For scripts, use `ArchetypeRuntime`:

```python
async with ArchetypeRuntime() as runtime:
    world = runtime.world("experiment", processors=[MyProcessor()])
    await world.run(steps=10)
```

For a long-running service, start `archetype serve` and use the REST API or
CLI. Both enter through the command gate, which can authorize and audit a
multi-user host.

## Project status

Archetype is alpha software. The core world, append-only history, and fork
paths are the best-tested parts. Protected HTTP routes fail closed without an
injected authenticator. The built-in role shortcut is available only through
explicit loopback development auth.

## Development and design notes

The [Development](guide/contributing.md) section contains the contribution
workflow, architecture, and normative contracts. Those pages describe how the
engine is built; the User Guide is the place to start when you are using it.
