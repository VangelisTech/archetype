# Architecture

Archetype is a data-centric Entity-Component-System (ECS) simulation engine. World state is columnar DataFrames. Every tick is an append-only write to storage. This gives you time-travel, forking, and replay for free.

## System Diagram

```
                  ┌──────────────────────────────────────────────┐
                  │              External Actors                  │
                  │     (REST API, CLI, Python scripts, agents)   │
                  └──────────────┬───────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────┐
│                        Service Layer                             │
│                                                                  │
│  CommandService ──→ CommandBroker ──→ WorldService ──→ AsyncWorld│
│       │                  │                                │      │
│       │            RBAC + queue              ┌────────────┤      │
│       │                  │                   │            │      │
│  SimulationService       │            Resources      Processors  │
│    (drain + step)        │           (type-safe DI)   (DataFrame │
│       │                  │                            transforms)│
│       ▼                  ▼                                       │
│  QueryService      Command History                               │
│  (read path)        (audit trail)                                │
└─────────────────────────────────────────────────────────────────┘
                                 │
                                 ▼
                  ┌──────────────────────────────────────┐
                  │           Storage Layer                │
                  │    LanceDB (default) or Iceberg        │
                  │    Append-only, partitioned by tick     │
                  └──────────────────────────────────────┘
```

## Core ECS Concepts

### Components

Data-only value objects. A Component is a Pydantic model that defines the schema for one aspect of an entity.

```python
from archetype.core.component import Component

class Position(Component):
    x: float = 0.0
    y: float = 0.0

class Health(Component):
    current: int = 100
    max_hp: int = 100
```

Components are stored as prefixed columns in Arrow tables: `position__x`, `position__y`, `health__current`, etc.

### Entities

An entity is just an integer ID (`entity_id`). It has no behavior — it's a bag of components. Entities with the same set of component types are grouped into **archetypes**.

### Archetypes

An archetype is a group of entities sharing the same component types. Each archetype is a single DataFrame where:
- Rows are entities
- Columns are prefixed component fields + metadata (`entity_id`, `tick`, `world_id`, `run_id`, `is_active`)

This columnar layout means bulk operations across thousands of entities are a single DataFrame transform.

### Processors

Processors are pure DataFrame transforms that run each tick. They define which components they need, and the system routes the right archetypes to them.

```python
from daft import DataFrame, col
from archetype.core.aio.async_processor import AsyncProcessor

class MovementProcessor(AsyncProcessor):
    components = (Position, Velocity)
    priority = 10

    async def process(self, df: DataFrame, **kwargs) -> DataFrame:
        return df.with_columns({
            "position__x": col("position__x") + col("velocity__vx"),
            "position__y": col("position__y") + col("velocity__vy"),
        })
```

Processors run in priority order (lower = earlier) and can access shared state via `Resources`.

### Resources

A type-safe dependency injection container scoped to each world. Processors use it to access shared configuration, brokers, or any object.

```python
world.resources.insert(SimConfig(gravity=9.8))

# In a processor:
config = resources.require(SimConfig)
```

## Tick Lifecycle

Each tick executes these phases:

```
1. pre_tick hooks fire
2. For each archetype (in parallel):
   a. Query previous state (DataFrame)
   b. Materialize deferred mutations (spawns/despawns)
   c. Execute matching processors in priority order
   d. Persist updated DataFrame to storage
3. Update in-memory live snapshots
4. Increment tick counter
5. post_tick hooks fire
```

Mutations (spawn, despawn, add/remove components) are **deferred** — they queue during a tick and apply at the start of the next tick. This ensures consistency within a single tick.

## Service Layer

The service layer mediates all access to worlds.

### ServiceContainer

Wires everything together:

```python
from archetype.app.container import ServiceContainer

container = ServiceContainer()
# container.world_service     — world lifecycle
# container.command_service   — command submission
# container.simulation_service — tick stepping
# container.query_service     — read path
# container.broker            — command queue
# container.storage_service   — storage backends
```

### Command Flow

All mutations from external actors flow through the command pipeline:

1. **CommandService.submit()** — accepts a `Command` with type, payload, tick, priority
2. **CommandBroker.enqueue()** — validates RBAC via `ActorCtx`, enforces quotas, queues by priority
3. **SimulationService.step()** — drains due commands, applies them to the world, steps processors
4. **QueryService** — reads world state (current or historical)

### RBAC

Every command submission requires an `ActorCtx` specifying the actor's roles:

| Role | Permissions |
|------|-------------|
| `viewer` | Read-only (query, history) |
| `player` | spawn, despawn, update, message |
| `coder` | + add/remove processors |
| `maintainer` | + create/destroy/fork worlds |
| `admin` | All commands |

Quotas: 500 commands per tick, 200k token budget per day.

## Storage

World state is persisted as Arrow tables to LanceDB (default) or Iceberg. Each tick is an append — nothing is overwritten. This gives you:

- **Time-travel**: Query any tick's state
- **Replay**: Re-run from any checkpoint
- **Forking**: Branch a world to explore alternatives
- **Audit**: Full command history

Storage is configured via `StorageConfig`:

```python
from archetype.core.config import StorageConfig, StorageBackend

config = StorageConfig(
    uri="./my_data",
    namespace="experiment_1",
    backend=StorageBackend.LANCEDB,  # default
)
```

## World Forking

Create a new world from a snapshot of an existing one:

```python
from archetype.core.config import WorldConfig

new_world = await container.world_service.fork_world(
    source_world_id=original.world_id,
    config=WorldConfig(name="branch-A"),
)
```

The forked world gets a new ID and independent state. Use this for MCTS, counterfactual reasoning, or A/B testing simulation strategies.
