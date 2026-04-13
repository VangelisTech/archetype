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

### Running Multiple Ticks

```python
from archetype.core.config import RunConfig

await world.run(RunConfig(num_steps=10))
```

This calls `step()` in a loop. Each run gets a unique `run_id` for storage isolation.

## The _live Cache

`_live` is a `dict[ArchetypeSignature, DataFrame]` that holds the most recent processed DataFrame per archetype. It is the authoritative in-memory state of the world between ticks.

### Why It Exists

The store is the durability layer, but reading from it between consecutive ticks is fragile. Each `SimulationService.step()` emits a fresh `run_id`, so store reads filtered by the current `run_id` miss rows written by earlier ticks. World forks exhibit the same issue: the cloned snapshot is persisted under a placeholder run_id and the next step queries under a different one.

`_live` fixes this (archetype#72). After all archetypes finish processing, `step()` updates `_live` with the output DataFrames filtered to active rows:

```python
self._live = {
    sig: df.where(col("is_active")) for sig, df in zip(sigs, results)
}
```

On subsequent ticks, `_run_archetype` checks `_live` first:

```python
if self.tick > 0 and sig in self._live:
    df = self._live[sig]
else:
    df = await self.query_archetype(sig, ...)
```

The store read is only used for tick 0 (when there is no prior output) or for archetypes not yet in `_live`.

## Mutation Internals

### Spawn/Despawn Caches

`_spawn_cache` and `_despawn_cache` are `dict[ArchetypeSignature, list]`. Mutations accumulate during the interval between ticks and are materialized at the start of each archetype's processing in `materialize_mutations()`.

**Despawns** are applied first. The method deduplicates entity IDs, then sets `is_active=False` on matching rows using `when().otherwise()`:

```python
df = df.with_column(
    "is_active",
    when(col("entity_id").is_in(entities_to_despawn), then=False)
    .otherwise(col("is_active")),
)
```

**Spawns** are applied second. Duplicate spawns for the same entity are deduplicated with last-write-wins semantics -- a forward dict comprehension keeps the latest row per `entity_id`:

```python
rows = list({row["entity_id"]: row for row in self._spawn_cache[sig]}.values())
```

The deduplicated rows are converted to a PyArrow table using the archetype's schema, then concatenated to the existing DataFrame.

Both caches are cleared after materialization.

### Entity Migration

When `add_components()` or `remove_components()` changes an entity's component set, the entity migrates between archetype tables. The algorithm in `_move_entity()`:

1. **Fetch** -- Read the entity's current row from `_live` (or an empty DataFrame if `_live` has no data for the old archetype). Filter to the target entity, materialize, take the latest tick row.

2. **Overlay** -- Apply mutated component fields. For `add_components`, the new component's `to_row_dict()` overwrites matching keys. For `remove_components`, no overlay is needed -- the row simply drops the removed component's columns when it enters the narrower archetype schema.

3. **Stamp** -- Set housekeeping columns (`entity_id`, `tick`, `world_id`, `is_active=True`). The `run_id` is set to a placeholder (`""`) and the updater stamps the real value during `update()`.

After `_move_entity` returns the new row:

- The old entity is marked for despawn in the old archetype
- The new row is added to the spawn cache for the new archetype
- `_entity2sig` is updated atomically

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
| `post_tick` | `world`, `tick`, `results` | After all archetypes processed and `_live` updated |
| `on_spawn` | `world`, `entity_id`, `components` | Defined but not currently fired |
| `on_despawn` | `world`, `entity_id` | Defined but not currently fired |

**Notes:**

- Hook errors are logged but do not halt the tick.
- `post_tick` fires after `_live` is updated and the tick counter is incremented. The `tick` argument is the new (incremented) value.
- `on_spawn` and `on_despawn` hooks are registered in the hook infrastructure but are not fired by `create_entity()` or `remove_entity()`. Spawns and despawns are deferred to materialization, which operates on batch DataFrames rather than individual entities.
- The `WorldService` attaches a `post_tick` hook for registry sync when a `WorldRegistry` is configured. This hook writes the updated tick to the registry after each step.

## Querying State

```python
# Query a specific archetype
df = await world.query_archetype(sig, ticks=[5], entity_ids=[1, 2])

# Query by component types across all matching archetypes
df = await world.get_components([Position, Health], entity_ids=[1, 2])
```

`get_components` reads from `_live`, unions rows from every archetype whose signature is a superset of the requested types, and projects to the requested component schema.

## Processors

Add or remove processors at runtime:

```python
await world.add_processor(MovementProcessor())
await world.remove_processor(MovementProcessor)
```

See [Processors](processors.md) and [Systems](system-execution.md) for how processors are matched to archetypes and executed.

## Forking Internals

`WorldService.fork_world()` creates a new world from a snapshot of an existing one.

### Guard Clause

Forking rejects worlds with pending mutations (un-materialized spawn/despawn caches). Call `step()` first so `_live` reflects the intended snapshot:

```python
if has_pending_spawns or has_pending_despawns:
    raise ValueError("Cannot fork a world with pending mutations. ...")
```

### What's Cloned

The fork receives a fresh `world_id` (system-generated, not caller-controlled). State copying:

| State | Copied | Notes |
|-------|:------:|-------|
| `tick`, `run_id` | Yes | Fork continues from the same tick |
| `_entity2sig` | Yes | Deep copy of entity-to-signature mapping |
| `_next_entity_id` | Yes | Entity ID counter |
| `_live` snapshots | Yes | Re-stamped with new `world_id` |
| Processors | Yes | Shared instances (stateless transforms) |
| Non-broker resources | Yes | Selective copy, skipping `CommandBroker` |
| `CommandBroker` | No | Re-injected by `WorldService.create_world()` |
| Spawn/despawn caches | No | Guarded -- must be empty |
| Lifecycle hooks | No | Fork-specific; source hooks are not inherited |

### Persistence

The live snapshots are persisted to the store under the new `world_id` at tick `source.tick - 1`. This ensures store-backed reads (which query the previous tick) find the forked state on the fork's first step:

```python
if source.tick > 0 and new_live:
    persist_tick = source.tick - 1
    for sig, df in new_live.items():
        await new_world.updater.update(df, sig, persist_tick, ...)
```

### Usage

```python
fork = await container.world_service.fork_world(
    source_world_id=world.world_id,
    name="branch-A",
    storage_config=storage_config,
)
fork.resources.insert(PhysicsConfig(gravity=0.0))  # override per fork
```

Use forking for MCTS, counterfactual reasoning, or A/B testing simulation strategies.

## Source Reference

- World: `src/archetype/core/aio/async_world.py`
- World service: `src/archetype/app/world_service.py`
