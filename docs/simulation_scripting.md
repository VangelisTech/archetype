This doc describes **current** scripting patterns for the `archetype` package.

If you want a shorter version, see `docs/guide/quickstart.md`.

## Pattern A: quick sync scripting (`archetype.init`)

Use this for small sims, simple debugging, and local iteration.

```python
import archetype
from archetype import Component, Processor
from daft import DataFrame, col


class Position(Component):
    x: float
    y: float


class Velocity(Component):
    vx: float
    vy: float


class Movement(Processor):
    components = (Position, Velocity)
    priority = 1

    def process(self, df: DataFrame, dt: float = 0.1) -> DataFrame:
        return df.with_columns(
            {
                "position__x": col("position__x") + col("velocity__vx") * dt,
                "position__y": col("position__y") + col("velocity__vy") * dt,
            }
        )


sim = archetype.init("./archetype_data")
world_id = sim.spawn_world("physics")
sim.add_processor_to_world(world_id, Movement())
sim.spawn_entity(world_id, Position(x=0, y=0), Velocity(vx=1, vy=1))
sim.step_world(world_id, dt=0.1)
```

## Pattern B: async orchestration (`ArchetypeApp`)

Use this for parallel rollouts, multi-world execution, and anything you might later run on a cluster.

```python
import asyncio
from archetype.app import ArchetypeApp


async def main() -> None:
    app = await ArchetypeApp.create(storage_uri="./archetype_data")
    world = await app.create_world("rollouts")
    await app.run_world(world.world_id, steps=100)
    await app.shutdown()


asyncio.run(main())
```

## Notes on querying

Archetype persists per-tick state keyed by `(world_id, run_id, tick, entity_id)`.

- For **async** worlds, `AsyncWorld.get_components(...)` is a convenient way to union component projections across matching archetypes (see `src/archetype/core/aio/async_world.py`).
- For **sync** worlds, the lower-level API is `SyncWorld.query_archetype(...)` which takes an archetype signature and a `RunConfig`.
