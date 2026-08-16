# archetype-smol

`archetype-smol` is a deliberately small, synchronous, in-memory DataFrame ECS
for learning the core Archetype model. It is a separate engine, not a
compatibility layer for `archetype-ecs`.

```bash
uv add archetype-smol
```

```python
from daft import col

from archetype.smol import Component, Processor, World


class Position(Component):
    x: float = 0.0


class Move(Processor):
    components = (Position,)

    def process(self, df, *, tick):
        return df.with_column("position__x", col("position__x") + 1)


world = World(processors=[Move()])
entity_id = world.spawn(Position(x=2))
world.step()

assert world.query(Position).to_pylist() == [
    {"entity_id": entity_id, "tick": 1, "is_active": True, "position__x": 3.0}
]
```

Smol keeps only the teaching loop: typed Components, priority-ordered
DataFrame Processors, immediate in-memory entity mutation, atomic steps, and
queryable snapshots. It intentionally has no storage providers, commands,
Activities, hooks, runtime host, API, CLI, or world-library extension system.

Component fields must resolve to `None`, `bool`, `int`, `float`, `str`,
`bytes`, or nested lists of those scalar values. Smol rejects mappings,
tuples, enums, nested models, and other values whose Python identity is erased
by DataFrame materialization. Encode structured teaching state as a JSON string
in a `_json` field; use `archetype-ecs` when typed structured storage is part of
the exercise.
