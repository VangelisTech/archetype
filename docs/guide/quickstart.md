# Quickstart

Archetype is a data-centric ECS simulation runtime built on Daft. You write **processors** as DataFrame transforms and Archetype handles storage, ticks, and orchestration.

## Install

Archetype targets **Python 3.12**.

```bash
cd archetype

# Recommended (matches repo tooling)
uv sync

# Or: editable install
python -m pip install -e .
```

## Your first world (sync)

This uses `archetype.init()` which builds a simple, synchronous simulation wrapper.

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

## Async worlds (parallel rollouts)

For high-throughput rollouts, use the application layer and `AsyncWorld`.

```python
import asyncio
from archetype.app import ArchetypeApp


async def main() -> None:
    app = await ArchetypeApp.create(storage_uri="./archetype_data")
    world = await app.create_world("rollouts")
    await app.run_world(world.world_id, steps=10)
    await app.shutdown()


asyncio.run(main())
```

## Run a known-good example

Text-only GRPO end-to-end smoke test:

```bash
cd archetype
PYTHONPATH=src uv run python examples/grpo_text_end_to_end.py
```

