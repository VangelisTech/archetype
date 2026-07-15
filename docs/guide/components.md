# Components

Components are typed entity state. Keep one component focused on one part of
the model; put behavior in a processor.

```python
from archetype import Component


class Position(Component):
    x: float = 0.0
    y: float = 0.0


class Velocity(Component):
    dx: float = 0.0
    dy: float = 0.0


class Health(Component):
    current: int = 100
```

Pass component instances when creating or updating an entity:

```python
entity_id = await world.spawn(Position(x=1, y=2), Velocity(dx=3))
await world.update(entity_id, Position(x=4, y=2))
```

## Components define an archetype

An entity's exact set of component types is its archetype. Entities with the
same set share a columnar table and can be processed together. Adding or
removing a component moves an entity to another archetype.

```python
await world.add_components(entity_id, Health(current=100))
await world.remove_components(entity_id, Velocity)
```

The order of components does not matter. `Position, Velocity` and `Velocity,
Position` produce the same archetype.

## Component fields become columns

Archetype prefixes every field with the lowercase component name. This keeps
fields from different components distinct in a DataFrame.

```text
entity_id | tick | position__x | position__y | velocity__dx | velocity__dy
```

Use those names inside DataFrame processors and queries:

```python
from daft import col

current = (await world.query(Position)).where(col("position__x") > 0)
```

## Serialization

Component types must remain importable and have stable names when they cross a
command or HTTP boundary. Use primitive fields where possible. For a list,
dictionary, or other structured value that must pass through Arrow, store a
serialized string with a `_json` suffix:

```python
class Conversation(Component):
    messages_json: str = "[]"
```

## Design guidelines

- Use a small component for one coherent piece of state.
- Make defaults safe so an entity can be created with little boilerplate.
- Treat a class name and field names as persistent schema.
- Add a component for optional capability instead of filling another component
  with nullable, unrelated fields.

Next: [write a processor](processors.md) or [work with worlds](working-with-worlds.md).
