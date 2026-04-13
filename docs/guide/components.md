# Components

A `Component` is a Pydantic model backed by Arrow serialization. Components hold entity state as typed fields. Processors transform that state each tick.

```python
class Component(LanceModel):
    @classmethod
    def get_type_by_name(cls, name: str) -> type["Component"]:
        stack: list[type[Component]] = [cls]
        seen: set[type[Component]] = set()
        while stack:
            current = stack.pop()
            for subclass in current.__subclasses__():
                if subclass in seen:
                    continue
                seen.add(subclass)
                if subclass.__name__ == name:
                    return subclass
                stack.append(subclass)
        raise ValueError(f"Component type '{name}' not found.")

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Component":
        component_type_name = data.pop("type", None)
        if component_type_name:
            return cls.get_type_by_name(component_type_name)(**data)
        if cls is Component:
            raise ValueError("Component.from_dict requires a 'type' key in the payload.")
        return cls(**data)

    def to_payload(self) -> dict[str, Any]:
        return {"type": type(self).__name__, **self.model_dump()}

    @classmethod
    def get_prefix(cls) -> str:
        return cls.__name__.lower() + "__"

    @classmethod
    def to_pyarrow_schema(cls) -> pa.Schema:
        return cls.to_arrow_schema()

    @classmethod
    def get_prefixed_schema(cls) -> pa.Schema:
        component_schema = cls.to_pyarrow_schema()
        prefix = cls.get_prefix()
        for i, field_name in enumerate(component_schema.names):
            field = component_schema.field(field_name)
            component_schema = component_schema.set(i, field.with_name(prefix + field_name))
        return component_schema

    def to_row_dict(self):
        prefix = self.get_prefix()
        return {prefix + key: value for key, value in self.model_dump().items()}
```

## Defining a Component

```python
from archetype.core.component import Component

class Position(Component):
    x: float = 0.0
    y: float = 0.0

class Velocity(Component):
    vx: float = 0.0
    vy: float = 0.0

class Health(Component):
    hp: int = 100
    max_hp: int = 100
```

Every field requires a default value so the Arrow schema can be inferred at class definition time.

## Column Prefixing

When components become DataFrame columns, each field is prefixed with the lowercase class name:

```python
pos = Position(x=5, y=10)
pos.to_row_dict()
# {"position__x": 5, "position__y": 10}
```

In a processor, you access these columns by their prefixed names:

```python
df.with_column(
    "position__x",
    col("position__x") + col("velocity__vx"),
)
```

## Supported Field Types

| Python Type | Arrow-Compatible | Notes |
|-------------|:---:|-------|
| `str`, `int`, `float`, `bool` | Yes | Direct |
| `list[str]`, `list[int]` | Yes | Direct |
| `bytes` | Yes | Direct |
| `dict` | No | JSON-encode to `str` |
| `list[dict]` | No | JSON-encode to `str` |
| Custom objects | No | JSON-encode to `str` |

**Pattern for complex types** -- store as JSON strings:

```python
class Agent(Component):
    name: str = ""
    role: str = ""
    journal: str = "[]"          # JSON list of thoughts
    metadata: str = "{}"         # JSON dict
    tags: str = "[]"             # JSON list of strings
```

Reading back:

```python
import json
journal = json.loads(row["agent__journal"])
```

## Archetype Signatures

Entities sharing the same set of component types belong to one archetype — a single DataFrame table whose rows are entities and whose columns are the base metadata fields plus each component's prefixed fields.

```text
Archetype (Position, Velocity):
  entity_id | tick | position__x | position__y | velocity__vx | velocity__vy
  1         | 0    | 5.0         | 10.0        | 0.5          | 1.0
  2         | 0    | 15.0        | 20.0        | 2.0          | 3.0

Archetype (Position):
  entity_id | tick | position__x | position__y
  3         | 0    | 25.0        | 30.0
```

Order doesn't matter -- `[Position, Velocity]` and `[Velocity, Position]` produce the same signature.

## Adding and Removing Components

When you add or remove a component from an entity, its archetype signature changes and it **moves to a different table**. The old row is soft-deleted (`is_active=False`), a new row is inserted in the target archetype. All existing state is preserved.

```python
# Entity starts with (Position,)
entity_id = await world.create_entity([Position(x=5, y=6)])
await world.step(run_config)

# Add Velocity — entity moves from (Position,) to (Position, Velocity)
await world.add_components(entity_id, [Velocity(vx=1, vy=1)])
await world.step(run_config)

# Remove Velocity — entity moves back to (Position,)
await world.remove_components(entity_id, [Velocity])
await world.step(run_config)
```

The full history is preserved for time-travel queries.

## Composition Patterns

**Agent with capabilities:**

```python
class Identity(Component):
    name: str = ""
    role: str = ""

class Memory(Component):
    journal: str = "[]"

class Spatial(Component):
    x: float = 0.0
    y: float = 0.0
    zone: str = "spawn"

# Basic agent
await world.create_entity([Identity(name="Scout")])

# Agent with memory
await world.create_entity([Identity(name="Historian"), Memory()])

# Agent with memory and position
await world.create_entity([Identity(name="Explorer"), Memory(), Spatial()])
```

Each combination creates a different archetype. Processors declare which components they need and only run on matching archetypes:

```python
class ThinkProcessor(AsyncProcessor):
    components = (Identity, Memory)    # Only entities with both
    priority = 10

class MoveProcessor(AsyncProcessor):
    components = (Identity, Spatial)   # Only entities with both
    priority = 20
```

The Scout gets neither processor. The Historian gets ThinkProcessor only. The Explorer gets both.

## Real-World Example

From the trajectory analysis pipeline:

```python
class Trajectory(Component):
    trajectory_id: str = ""
    source: str = ""
    turns_json: str = "[]"       # JSON list of Turn dicts
    total_turns: int = 0
    total_tokens: int = 0
    duration_seconds: float = 0.0
    outcome: str = ""
    tags_json: str = "[]"
    metadata_json: str = "{}"

class Label(Component):
    technique: str = ""
    description: str = ""
    value: str = ""
    score: float = 0.0
    rationale: str = ""
    sampled: bool = True
```

An entity spawned with `[Trajectory(...), Label(...)]` lives in the `(Label, Trajectory)` archetype and has columns like `trajectory__trajectory_id`, `label__score`, etc.
