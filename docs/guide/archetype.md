# Archetype

An archetype is the fundamental grouping mechanism in the ECS.

- Entities that share the same set of components share an archetype
- Archetypes map directly to a table schema definitions.

```python
class Archetype:
    BASE_SCHEMA = pa.schema([
        pa.field("world_id", pa.string(), nullable=False),
        pa.field("run_id", pa.string(), nullable=False),
        pa.field("entity_id", pa.int32(), nullable=False),
        pa.field("tick", pa.int32(), nullable=False),
        pa.field("is_active", pa.bool_(), nullable=False),
    ])
    PARTITION_KEYS = ["world_id", "run_id", "tick"]

    def __init__(self, components: list["Component"]):
        self.components = components
        self.sig: ArchetypeSignature = self.sig_from_components(components)
        self.name = self.get_name(self.sig)
        self.schema = self.get_archetype_schema(self.sig)

    @staticmethod
    def sig_from_components(components: list["Component"]) -> ArchetypeSignature:
        component_types = [type(c) for c in components]
        return tuple(sorted(component_types, key=lambda t: t.__name__))

    @staticmethod
    def get_name(sig: ArchetypeSignature) -> str:
        combined_schema = Archetype.get_archetype_schema(sig)
        schema_hash = hashlib.sha256(str(combined_schema).encode()).hexdigest()[:16]
        return f"a_{len(sig)}c_s{schema_hash}"

    @staticmethod
    def get_archetype_schema(sig: ArchetypeSignature) -> pa.Schema:
        archetype_schema = Archetype.BASE_SCHEMA
        for component_type in sig:
            component_schema = component_type.get_prefixed_schema()
            archetype_schema = pa.unify_schemas([archetype_schema, component_schema])
        return archetype_schema

    @staticmethod
    def to_row_dict(
        entity_id: int, tick: int, components: list[Component], world_id: str, run_id: str
    ) -> dict[str, Any]:
        row_dict = {
            "world_id": str(world_id), "run_id": str(run_id),
            "entity_id": entity_id, "tick": tick, "is_active": True,
        }
        for c in components:
            row_dict.update({c.get_prefix() + k: v for k, v in c.model_dump().items()})
        return row_dict
```

## Signatures

An `ArchetypeSignature` is a tuple of component types, sorted alphabetically by class name:

```python
from archetype.core.archetype import Archetype
from archetype.core.interfaces import ArchetypeSignature

# ArchetypeSignature = tuple[type[Component], ...]

sig = Archetype.sig_from_components([Position(x=0, y=0), Velocity(vx=1, vy=0)])
# sig == (Position, Velocity)  -- sorted by __name__
```

Sorting ensures signatures are deterministic regardless of the order components are passed in.

## Naming

Each archetype gets a compact, filesystem-safe table name:

```text
a_2c_s9f3a1b2c4d5e6f7
|  |   |
|  |   +-- SHA-256 hash of the PyArrow schema (first 16 chars)
|  +------ number of component types
+--------- "a" prefix (archetype)
```

Names are stable -- the same set of component types always produces the same name, regardless of component order. This allows multiple simulations and runs to share the same catalog.

```python
name = Archetype.get_name(sig)  # "a_2c_s9f3a1b2c4d5e6f7"
```

## Schema

An archetype schema combines a base set of housekeeping columns with prefixed component fields:

```python
schema = Archetype.get_archetype_schema(sig)
```

**Base columns** (present in every archetype):

| Column | Type | Description |
|--------|------|-------------|
| `world_id` | `string` | Which world this entity belongs to |
| `run_id` | `string` | Which run produced this row |
| `entity_id` | `int32` | Unique entity identifier |
| `tick` | `int32` | Simulation tick when this row was written |
| `is_active` | `bool` | Whether the entity is alive |

**Component columns** are prefixed with the lowercase class name. A `Position(x=5, y=10)` component adds columns `position__x` and `position__y`.

The full schema for an archetype with `(Health, Position)` would be:

```text
world_id    | run_id | entity_id | tick | is_active | health__current | health__max_hp | position__x | position__y
```

## Partition Keys

Archetypes are partitioned by `["world_id", "run_id", "tick"]` for efficient storage filtering. This lets the querier skip irrelevant partitions when reading a specific world at a specific tick.

## Composing Signatures

Add or remove component types from an existing signature:

```python
# Add a component type
new_sig = Archetype.add_components(sig, [Health])
# (Health, Position, Velocity)

# Remove a component type
new_sig = Archetype.remove_components(sig, [Velocity])
# (Position,)
```

Both return a new sorted tuple -- signatures are immutable.

## Row Serialization

Convert an entity's components to a flat dictionary for storage:

```python
row = Archetype.to_row_dict(
    entity_id=1,
    tick=0,
    components=[Position(x=5, y=10), Velocity(vx=1, vy=0)],
    world_id="abc-123",
    run_id="run-001",
)
# {
#     "world_id": "abc-123",
#     "run_id": "run-001",
#     "entity_id": 1,
#     "tick": 0,
#     "is_active": True,
#     "position__x": 5.0,
#     "position__y": 10.0,
#     "velocity__vx": 1.0,
#     "velocity__vy": 0.0,
# }
```

## Entities

An entity is an integer ID (`entity_id`). It carries no logic — its state is the union of its component fields. The world tracks each entity's current archetype signature via an internal `_entity2sig` mapping.

When you add or remove components from an entity, it migrates to a different archetype: the old row is marked inactive, and a new row is spawned in the target archetype's table with the updated component set. See [Worlds -- Entity Migration](worlds.md#entity-migration) for the full algorithm.

## Further Reading

- [Components](components.md) -- field types, Arrow serialization, the column prefixing contract
- [System Execution](system-execution.md) -- how signatures drive the subset rule for processor matching
- [Worlds](worlds.md) -- tick lifecycle, spawn/despawn caches, entity migration
- [Stores](stores.md) -- how archetype tables are persisted

## Source Reference

The archetype system is defined in `src/archetype/core/archetype.py`.
