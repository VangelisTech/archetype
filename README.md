# Archetype

Archetype is an ECS framework for simulations with typed state, world history,
forks, and world-context artifacts. It remains an active project, with limited
current investment.

The execution architecture is moving to Rust and DDlog Runtime. Authored DDlog
programs compose the simulation; explicitly persistent relations become typed
ECS components. Archetype manages worlds, completed ticks, durable state, and
artifacts. Daft is intended for queries and analysis outside live execution.

| Surface | Current status |
| --- | --- |
| Python 0.6 runtime, generic REST/CLI, history and artifacts | Implemented; still uses the existing Daft execution loop |
| Rust DDlog adapter and typed Iceberg cuts | Preview in this branch; native and local storage contract tests |
| Opt-in local [DDlog Python preview](docs/guide/ddlog-python-preview.md) | Implemented over the hosted Rust owner; not a replacement for the 0.6 runtime |
| Generic DDlog simulation API/MCP | Planned |
| Missions and Physical AI evaluation products | Removed from the intended product surface |

See the [DDlog migration contract](docs/guide/ddlog-runtime.md) for exact schema
mapping, visibility, recovery limits, and remaining work. Agent memory, prompts,
tools, and budgets belong to X0. Independent projects and stored histories are
not migrated or removed by this change.

## Try the Rust preview

Install Rust 1.95 and the native DDlog build prerequisites described by
[DDlog Runtime](https://github.com/everettVT/ddlog-runtime). Then run:

```sh
cargo +1.95.0 test -p archetype-ddlog --locked
cargo +1.95.0 run -p archetype-ddlog --locked --example persistent_simulation \
  -- /new/data/directory /absolute/path/to/native-driver
```

The example composes two native programs, publishes two complete ticks through
Iceberg, retracts an entity, and verifies current and historical state. It needs
no model credentials or paid services. Native acceptance is an explicit test;
the ordinary Rust suite does not silently claim native coverage.

## Use the existing Python runtime

```sh
pip install archetype-ecs
```

```python
import asyncio
from archetype import ArchetypeRuntime

async def main():
    async with ArchetypeRuntime() as runtime:
        world = runtime.world("experiment")
        entity_id = await world.spawn()
        result = await world.run(steps=10)
        print(entity_id, result.ticks_completed)

asyncio.run(main())
```

This is the existing Python API, not the new DDlog bridge. Generic inference,
artifact ingestion, and the separately installed Research library remain.
[Smol](packages/archetype-smol/README.md) is an independent teaching engine.

Read the [runtime guide](docs/guide/runtime.md),
[artifact contract](docs/guide/artifacts.md), and
[example inventory](examples/README.md). Run `make ci` for required Python PR
checks and the Rust commands above for the new adapter. Heavy release and
external-service evidence remains separate.

Apache 2.0. Historical releases and design records remain in Git.
