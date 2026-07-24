# Quickstart

Use `ArchetypeRuntime` for a Python script. It owns one process resource graph,
then gives you a lazy handle for each world.

For a copy-and-run version of this page, see
[`examples/00_quickstart.py`](https://github.com/VangelisTech/archetype/blob/main/examples/00_quickstart.py).

## Install

```bash
pip install archetype-ecs
```

From a checkout, run `make sync-dev` instead.

## Define state and behavior

Components hold typed state. A processor receives a Daft DataFrame for every
archetype that has its required components, then returns the next DataFrame.

```python
import asyncio

from daft import DataFrame, col

from archetype import ArchetypeRuntime, AsyncProcessor, Component


class Position(Component):
    x: float = 0.0


class Velocity(Component):
    dx: float = 0.0


class Move(AsyncProcessor):
    components = (Position, Velocity)

    async def process(self, df: DataFrame, **_) -> DataFrame:
        return df.with_columns(
            {"position__x": col("position__x") + col("velocity__dx")}
        )
```

## Create a world and run it

```python
async def main() -> None:
    async with ArchetypeRuntime() as runtime:
        world = runtime.world("demo", processors=[Move()])

        entity_id = await world.spawn(Position(), Velocity(dx=2))
        await world.step()  # Persist the initial component values.
        await world.run(steps=3)

        history = await world.query(Position)
        print(entity_id)
        print(history.collect().to_pylist())


asyncio.run(main())
```

`spawn()` reserves a real entity ID immediately. The first `step()` persists
the initial component values; processors apply on the three subsequent ticks.
`query()` returns a lazy Daft DataFrame containing the full append-only history
for the requested components.

## Read the current tick

Filter history by its `tick` column when you need the most recent rows:

```python
from daft import col

info = await world.info()
current = (await world.query(Position)).where(col("tick") == info.tick - 1)
current.show()
```

## Fork a world

Forks retain their source history and receive their own future writes:

```python
branch = await world.fork("alternative")
await branch.update(entity_id, Velocity(dx=10))
await branch.run(steps=3)
```

See [History and forks](history-and-forks.md) for the details.

## Synchronous scripts

The sync facade has the same operations without `await`:

```python
with ArchetypeRuntime.sync() as runtime:
    world = runtime.world("demo", processors=[Move()])
    world.spawn(Position(), Velocity(dx=2))
    world.step()  # Persist the initial component values.
    world.run(steps=3)
```

## Next steps

- [Build a simulation](building-simulations.md)
- [Define components](components.md)
- [Write processors](processors.md)
- [Work with worlds](working-with-worlds.md)
- [Choose a runnable example](examples.md)
- [Run a coding-agent mission](agent-missions.md)
- [Host a service](app-overview.md)
