# Components

`Component` is the typed state unit of the ECS. Every entity is stored as the union of its component fields, and every archetype table schema is derived from component types.

The implementation lives in `src/archetype/core/component.py`.

## Contract

`Component` extends `lancedb.pydantic.LanceModel` and adds the small set of methods the rest of the runtime depends on:

```python
class Component(LanceModel):
    @classmethod
    def get_type_by_name(cls, name: str) -> type["Component"]: ...
    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Component": ...
    def to_payload(self) -> dict[str, Any]: ...
    @classmethod
    def get_prefix(cls) -> str: ...
    @classmethod
    def to_pyarrow_schema(cls) -> pa.Schema: ...
    @classmethod
    def get_prefixed_schema(cls) -> pa.Schema: ...
    def to_row_dict(self) -> dict[str, Any]: ...
```

At runtime, components serve three roles:

1. define typed entity fields,
2. generate Arrow-compatible schema fragments,
3. serialize themselves into command payloads and archetype rows.

## Defining a Component

Define components as subclasses with typed model fields:

```python
from archetype.core.component import Component

class Position(Component):
    x: float = 0.0
    y: float = 0.0

class Velocity(Component):
    dx: float = 0.0
    dy: float = 0.0
```

The runtime does not attach behavior to components. Behavior lives in processors; components are schema-bearing data models.

## Column Model

Component fields are flattened into archetype table columns using a stable prefix:

```python
Position.get_prefix()  # "position__"
Position(x=1.0, y=2.0).to_row_dict()
# {"position__x": 1.0, "position__y": 2.0}
```

The prefix is always `lowercase_class_name + "__"`.

This naming convention is part of the storage contract:

- `Component.get_prefixed_schema()` renames every field in the component schema,
- `Archetype.get_archetype_schema()` merges prefixed component schemas with base metadata columns,
- processors read and write component state through those prefixed column names.

Example archetype row shape:

```text
world_id | run_id | entity_id | tick | is_active | position__x | position__y
```

## Serialization Paths

### Command payloads

`to_payload()` includes the concrete class name:

```python
Position(x=1, y=2).to_payload()
# {"type": "Position", "x": 1.0, "y": 2.0}
```

`from_dict()` reverses that process:

```python
Component.from_dict({"type": "Position", "x": 1, "y": 2})
# Position(x=1.0, y=2.0)
```

Two details matter:

- `Component.from_dict(...)` requires a `"type"` key when called on the base class,
- subclass resolution walks the full subclass tree, not only direct subclasses.

That is the mechanism the command layer relies on when components cross API or broker boundaries.

### Storage rows

`to_row_dict()` emits only prefixed component fields. Archetype-level metadata is added by `Archetype.to_row_dict()` and later stamped by the updater/world pipeline.

## Relationship to Archetypes

An archetype is defined by the exact set of component types attached to an entity.

The connection points are explicit in code:

- `Archetype.sig_from_components()` converts component instances into a canonical signature,
- `Archetype.get_archetype_schema()` combines `Component.get_prefixed_schema()` for every type in that signature,
- `Archetype.to_row_dict()` merges base columns with each component's `model_dump()`.

Because signatures are sorted by component type name, component order is not semantically meaningful:

```python
[Position(), Velocity()]
[Velocity(), Position()]
```

Both produce the same archetype signature.

## Relationship to Worlds

`AsyncWorld` and `SyncWorld` treat components as the unit of structural change:

- `create_entity([...])` assigns an entity to the signature implied by its components,
- `add_components(...)` migrates the entity to a new archetype,
- `remove_components(...)` migrates it again by removing schema columns associated with those types.

Component add/remove is not an in-place schema edit. It is an archetype transition.

## Practical Implications

- Keep components focused on data shape, not behavior.
- Treat the class name as part of the serialization contract because payload round-trips use it.
- Treat prefixed field names as part of the storage and processor contract.
- Expect component composition to determine which processors can run and which archetype table stores the entity.

## Further Reading

- [Archetypes](archetype.md) -- how component sets define signatures, schemas, and table names
- [Processors](processors.md) -- how processors declare component requirements and transform DataFrames
- [Worlds](worlds.md) -- entity creation, component add/remove, and archetype migration
- [Data Flow](data-flow.md) -- the command pipeline for submitting component mutations externally

## Source References

- `src/archetype/core/component.py`
- `src/archetype/core/archetype.py`
- `src/archetype/core/interfaces.py`
- `src/archetype/core/aio/async_world.py`
