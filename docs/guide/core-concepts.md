# Core Concepts

Archetype implements the Entity-Component-System (ECS) pattern with DataFrames as the underlying data structure.

## Components

A **Component** is a typed data record that defines a schema:

```python
from archetype import Component

class Position(Component):
    x: float = 0.0
    y: float = 0.0

class Velocity(Component):
    vx: float = 0.0
    vy: float = 0.0

class Health(Component):
    current: int = 100
    max: int = 100
```

Components are stored with **prefixed columns**: `position__x`, `velocity__vx`, etc. This avoids collisions when multiple components share a table.

## Entities

An **Entity** is a unique identifier (`entity_id: UUID`) that groups components together. Entities don't store data directly—they're just IDs that reference rows in component tables.

```python
# Spawn creates an entity with the given components
await world.spawn(
    Position(x=10, y=20),
    Velocity(vx=1, vy=0),
    Health(current=100, max=100)
)
```

## Archetypes

An **Archetype** is the unique *set of component types* attached to an entity. Entities with the same archetype share a physical table:

```
Archetype: (Position, Velocity)
┌──────────┬────────┬────────────────┬──────┬────────────┬────────────┬─────────────┬─────────────┐
│ world_id │ run_id │ entity_id      │ tick │ is_active  │ position_x │ position_y  │ velocity_vx │ ...
├──────────┼────────┼────────────────┼──────┼────────────┼────────────┼─────────────┼─────────────┤
│ world_1  │ run_1  │ uuid-1         │ 0    │ true       │ 0.0        │ 0.0         │ 1.0         │
│ world_1  │ run_1  │ uuid-1         │ 1    │ true       │ 1.0        │ 0.5         │ 1.0         │
│ world_1  │ run_1  │ uuid-2         │ 0    │ true       │ 10.0       │ 20.0        │ -1.0        │
└──────────┴────────┴────────────────┴──────┴────────────┴────────────┴─────────────┴─────────────┘
```

When you add/remove components from an entity, it moves to a different archetype table.

## Behaviors (DSL)

A **Behavior** is a function that transforms agent state. The `@behavior` decorator compiles it to a processor:

```python
from archetype.dsl import behavior

@behavior(Position, Velocity)
async def move(agent, ctx):
    """Move entity based on velocity."""
    agent.position.x += agent.velocity.vx * ctx.dt
    agent.position.y += agent.velocity.vy * ctx.dt
```

The `agent` parameter is an `AgentProxy` providing natural attribute access. The `ctx` parameter provides:
- `ctx.tick` - Current tick number
- `ctx.dt` - Time delta
- `ctx.world` - Reference to the world (for spawning, queries)

## Processors (Core)

At the core level, a **Processor** is a pure DataFrame transform:

```python
from archetype.core import AsyncProcessor
from daft import DataFrame, col

class MovementProcessor(AsyncProcessor):
    components = (Position, Velocity)
    priority = 10
    
    async def process(self, df: DataFrame, **kwargs) -> DataFrame:
        dt = kwargs.get("dt", 1.0)
        return df.with_columns({
            "position__x": col("position__x") + col("velocity__vx") * dt,
            "position__y": col("position__y") + col("velocity__vy") * dt,
        })
```

Processors declare:
- `components` - Required component types
- `priority` - Execution order (lower runs first)

## System

A **System** is an ordered collection of processors. Each tick:
1. Query processors matching the current archetype
2. Execute in priority order
3. Each processor receives the DataFrame and returns a transformed version

## World

A **World** owns:
- Entity ID namespace
- Entity → archetype mapping
- Tick counter and run_id
- Spawn/despawn caches
- Live snapshot of latest state per archetype

## Time Travel

Because each tick is persisted, you can query historical state:

```python
# What was entity's position at tick 50?
historical = await store.query(
    archetype_sig,
    world_id=world.id,
    tick=50
)
```

This enables:
- Debugging ("what happened at tick N?")
- Deterministic replay
- Branching simulations (fork from any tick)

## Resources

The **Resources** container provides world-level dependency injection:

```python
from archetype.core import Resources

resources = Resources()
resources.register(CommandBroker, broker_instance)
resources.register(Config, config_instance)

# Later...
broker = resources.get(CommandBroker)
```

## Hooks

**Hooks** allow lifecycle callbacks:

```python
world.hooks.add_pre_tick(lambda world, tick: log(f"Starting {tick}"))
world.hooks.add_post_tick(lambda world, tick: checkpoint(world))
```

Available hooks:
- `pre_tick` - Before processor execution
- `post_tick` - After tick completion
