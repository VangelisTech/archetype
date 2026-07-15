# Working with worlds

A world is an isolated simulation and its history. Create a handle with
`runtime.world()`, then activate it by calling an operation such as `spawn()`,
`step()`, or `query()`.

```python
async with ArchetypeRuntime() as runtime:
    world = runtime.world("traffic", processors=[MoveCars()])
```

World handles are cheap and lazy. The runtime creates the underlying world on
first use, including any processors, resources, and hooks passed to
`runtime.world()`.

## Create entities

Pass component instances to `spawn()`. It returns an entity ID straight away;
the initial values are written when the next tick materializes the mutation.

```python
car_id = await world.spawn(
    Position(x=0, y=0),
    Velocity(dx=1, dy=0),
)

await world.step()
```

For many identical entities, use `spawn_batch()`. Use `spawn_many()` when each
entity has different initial values.

```python
await world.spawn_batch(Position(), Velocity(dx=1), 10_000)
await world.spawn_many([[Position(x=float(i))] for i in range(100)])
```

## Change entities

`update()` changes values on components the entity already has.
`add_components()` and `remove_components()` change its shape, so they move it
to a different archetype.

```python
await world.update(car_id, Position(x=10, y=4))
await world.add_components(car_id, Fuel(litres=20))
await world.remove_components(car_id, Velocity)
await world.despawn(car_id)
```

All of these calls go through the command gate. In a normal script the runtime
uses a local admin actor; a service can bind a handle to a restricted actor
with `world.as_actor(actor_ctx)`.

## Run ticks

Use `step()` for one tick and `run()` for several:

```python
await world.step()
result = await world.run(steps=100)
print(result.ticks_completed)
```

Processors run in ascending `priority` order. Each receives the rows for
matching component sets and returns a new DataFrame.

## Query state and history

Ask for component types, not instances. The result is a lazy Daft DataFrame.

```python
from daft import col

history = await world.query(Position, Velocity)
one_car = history.where(col("entity_id") == car_id)
one_car.show()
```

`query()` is historical by design. Filter `tick` for a snapshot; see
[History and forks](history-and-forks.md) for a complete example.

## Fork or attach

Fork before trying a different policy or input. The returned handle runs
independently and can read its source's pre-fork rows.

```python
alternative = await world.fork("no-drag")
await alternative.run(steps=100)
```

Use `runtime.attach(world_id)` when a world is already registered with the
same runtime, for example an episode world returned by a rollout.

## Lifecycle

The runtime shuts down all its handles when its context exits. Call
`world.destroy()` only when you want to remove the live world from the current
process. Destroying a world does not delete persisted state or audit history.

For every available method, see the [Python API reference](../reference/python-api.md).
