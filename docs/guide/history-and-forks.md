# History and forks

Archetype appends rows for every tick. A query therefore gives you history,
not just the latest value. Use ordinary Daft filters to inspect a past tick or
compare branches.

## Read a snapshot

```python
from daft import col

info = await world.info()
history = await world.query(Position)
latest = history.where(col("tick") == info.tick - 1)
latest.show()
```

The initial spawn values are written before processors update that entity. A
processor's first result appears on the following tick.

## Inspect an older tick

```python
tick_two = history.where(col("tick") == 2)
tick_two.select("entity_id", "position__x", "position__y").show()
```

Rows include `world_id`, `run_id`, `entity_id`, `tick`, and `is_active`, plus
the prefixed fields for every requested component. A component named
`Position` with field `x` becomes `position__x`.

## Branch a run

`fork()` creates another world from the source's current state. The branch
inherits earlier rows through lineage and writes later rows under its own
world identity.

```python
branch = await world.fork("higher-speed")
await branch.update(car_id, Velocity(dx=10))
await branch.run(steps=10)
```

The source is unchanged. You can keep running both worlds and query each at
the same tick:

```python
comparison_tick = (await world.info()).tick - 1
source = (await world.query(Position)).where(col("tick") == comparison_tick)
candidate = (await branch.query(Position)).where(col("tick") == comparison_tick)
```

## Audit commands separately

World state and command history answer different questions. Use `query()` for
entity state and `history()` for the gated operations that acted on a world.

```python
audit = await world.history(limit=100)
audit.select("command_type", "actor_id", "created_at").show()
```

## Runnable example

[`examples/03_time_travel.py`](https://github.com/VangelisTech/archetype/blob/main/examples/03_time_travel.py)
shows historical reads, a counterfactual branch, and a same-tick comparison.
