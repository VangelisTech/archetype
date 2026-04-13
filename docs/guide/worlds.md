# Worlds

`AsyncWorld` is the central simulation coordinator. It orchestrates entity-archetype mappings, mutation caches, the parallel tick cycle, and lifecycle hooks. Each world is an independent simulation with its own entity space, tick counter, and resources.

## Creating a World

Worlds are typically created through the service layer:

```python
from archetype.app.container import ServiceContainer

container = ServiceContainer()
world = await container.world_service.create_world(name="my-sim")
```

Directly:

```python
from archetype.core.aio.async_world import AsyncWorld
from archetype.core.config import WorldConfig

world = AsyncWorld(
    world_config=WorldConfig(name="my-sim"),
    querier=querier,
    updater=updater,
    system=system,
)
```

## World Properties

| Property | Type | Description |
|----------|------|-------------|
| `world_id` | `UUID` | Unique identifier, set at creation |
| `name` | `str` | Human-readable name |
| `tick` | `int` | Current simulation tick (starts at 0) |
| `resources` | `Resources` | Type-safe dependency injection container |
| `run_id` | `str` | Current run identifier (set by `run()`) |

## Entity Management

### Creating Entities

```python
entity_id = await world.create_entity([
    Position(x=0, y=0),
    Velocity(vx=1, vy=0),
])
```

Entities are not persisted immediately. They enter a **spawn cache** and are written to the archetype table at the start of the next `step()`. Deferring mutations to tick boundaries ensures that all processors within a single tick observe the same entity set.

### Removing Entities

```python
await world.remove_entity(entity_id)
```

Like spawns, removals are deferred. The entity is marked `is_active=False` during materialization.

### Adding and Removing Components

```python
# Add a component -- entity migrates to a new archetype
await world.add_components(entity_id, [Health(current=100, max_hp=100)])

# Remove a component type -- entity migrates back
await world.remove_components(entity_id, [Health])
```

Component mutations trigger **archetype migration**: the entity's row is marked inactive in the old archetype table and a new row (with carried-over field values) is spawned in the target archetype table.

## Tick Lifecycle

Each call to `step()` executes one simulation tick:

```text
1. pre_tick hooks fire
2. For each archetype (in parallel):
   a. Query previous state (from _live cache or store)
   b. Materialize deferred mutations (spawns/despawns)
   c. Execute matching processors in priority order
   d. Persist updated DataFrame to store
3. Update _live snapshots
4. Increment tick counter
5. post_tick hooks fire
```

The `_live` cache holds the most recent processed DataFrame per archetype. Subsequent ticks read from this cache rather than the store, avoiding redundant disk reads and ensuring processors observe the most recent output across consecutive steps.

### Running Multiple Ticks

```python
from archetype.core.config import RunConfig

await world.run(RunConfig(num_steps=10))
```

This calls `step()` in a loop. Each run gets a unique `run_id` for storage isolation.

## Lifecycle Hooks

Register callbacks for observability or side effects:

```python
async def log_tick(world, tick, **kwargs):
    print(f"Tick {tick} complete")

world.add_hook("post_tick", log_tick)
```

| Event | Arguments | When |
|-------|-----------|------|
| `pre_tick` | `world`, `tick` | Before any processing |
| `post_tick` | `world`, `tick`, `results` | After all archetypes processed |
| `on_spawn` | `world`, `entity_id`, `components` | When entity is created |
| `on_despawn` | `world`, `entity_id` | When entity is removed |

Hook errors are logged but do not halt the tick. Hooks execute asynchronously within the tick lifecycle and do not block processor execution.

## Querying State

```python
# Query a specific archetype
df = await world.query_archetype(sig, ticks=[5], entity_ids=[1, 2])

# Query by component types across all matching archetypes
df = await world.get_components([Position, Health], entity_ids=[1, 2])
```

`get_components` unions rows from every archetype whose signature is a superset of the requested types.

## Processors

Add or remove processors at runtime:

```python
await world.add_processor(MovementProcessor())
await world.remove_processor(MovementProcessor)
```

See [Processors](processors.md) and [Systems](system-execution.md) for how processors are matched to archetypes and executed.

## World Forking

Create a new world from a snapshot of an existing one:

```python
new_world = await container.world_service.fork_world(
    source_world_id=world.world_id,
    name="branch-A",
)
```

**What's cloned:** tick, entity-to-signature mapping, entity counter, live archetype snapshots (re-stamped with the new `world_id`), processors, and non-broker resources.

**What's not cloned:** pending spawn/despawn caches (step first to materialize), lifecycle hooks, and the `CommandBroker` (re-injected by the service).

Use forking for MCTS, counterfactual reasoning, or A/B testing simulation strategies.

## Source Reference

The world implementation is in `src/archetype/core/aio/async_world.py`.
