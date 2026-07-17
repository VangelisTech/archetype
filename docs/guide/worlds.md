# Worlds

`AsyncWorld` is the central simulation coordinator in Archetype's core layer,
but beginner-facing scripts should usually interact with a `RuntimeWorld`
handle from `ArchetypeRuntime`. `RuntimeWorld` is the governed script API;
`AsyncWorld` is the underlying engine object that owns entity-archetype
mappings, mutation caches, the parallel tick cycle, and lifecycle hooks.

## Creating a World

Recommended for scripts:

```python
from archetype import ArchetypeRuntime

async with ArchetypeRuntime() as runtime:
    world = runtime.world("my-sim")
```

## RuntimeWorld vs AsyncWorld

`ArchetypeRuntime.world(...)` returns `RuntimeWorld`, not a raw `AsyncWorld`.
That distinction is intentional:

- `RuntimeWorld` is the public script surface. Its entity and processor
  mutations (`spawn`, `despawn`, `update`, `add_components`,
  `remove_components`, `add_processor`, `remove_processor`) route through
  `iCommandService`, so they honor RBAC and emit audit rows. `RuntimeWorld.history()`
  reads that audit trail back through the gate.
- `RuntimeWorld.as_actor(ctx)` returns another handle to the same logical
  world, bound to a different `ActorCtx`, without creating a new world or
  storage backend.
- `AsyncWorld` remains the direct engine API. Calling it directly may bypass
  command-gate semantics, which is appropriate for engine and service-layer code.

The runtime keeps handle construction and actor rebinding declarative:

- `runtime.world(...)` / `world.as_actor(...)`

World activation, resource attachment, hook registration, mutations, reads,
simulation, fork, and destroy all go through gated service methods.

The rest of this page describes the engine-level `AsyncWorld` behavior that
those runtime calls ultimately drive.

Lower-level via the service layer:

```python
from archetype.core.config import WorldConfig
from archetype.app.container import ServiceContainer

container = ServiceContainer()
world = await container.world_service.create_world(WorldConfig(name="my-sim"))
```

Direct construction is core-internal / advanced:

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
| `run_id` | `str` | Active durable timeline identifier, retained across repeated runs |

## Entity Management

On `RuntimeWorld`, the corresponding public verbs are `spawn`, `despawn`,
`update`, `add_components`, and `remove_components`. Those calls still
materialize at tick boundaries; the sections below describe the underlying
`AsyncWorld` mechanics.

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
1. `PreTick` hooks fire
2. For each archetype (in parallel):
   a. Query previous state (from _live cache or store)
   b. Materialize deferred mutations (spawns/despawns)
   c. Execute matching processors in priority order
   d. Persist updated DataFrame to store
3. Update _live snapshots
4. Increment tick counter
5. `PostTick` hooks fire
```

### Running Multiple Ticks

```python
from archetype.core.config import RunConfig

await world.run(RunConfig(num_steps=10))
```

This calls `step()` in a loop. The world keeps one active `run_id` across
repeated steps and `run()` calls so every row belongs to one continuous
timeline. A fork, by contrast, mints its own `run_id` and carries explicit
lineage back to its source.

## The _live Cache

`_live` is a `dict[ArchetypeSignature, DataFrame]` that holds the most recent processed DataFrame per archetype. It is the authoritative in-memory state of the world between ticks.

### Why It Exists

The store is an append-only historical ledger, while the next engine tick
needs exactly the latest active frame. Reconstructing that frame from durable
history on every step would add query work to the hot execution path.

`_live` retains the already-computed frame. After all archetypes finish
processing, `step()` updates it with the output DataFrames filtered to active
rows:

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

The store read is only used for tick 0 (when there is no prior output) or for
archetypes not yet in `_live`. This cache is an engine implementation detail,
not a user-facing read preference: `RuntimeWorld.query()` goes through the
durable query path for both live and cold worlds.

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

Worlds expose typed lifecycle hooks for observability and integration glue. The
canonical hook API and event catalogue are documented in
[Lifecycle Hooks](hooks.md).

Hooks are registered against dataclass event types from `archetype.core.hooks`.
`add_hook` returns an opaque `HookHandle` for removal, and handlers take a
single `event` argument:

```python
from archetype.core.hooks import PostTick

async def log_tick(event: PostTick) -> None:
    print(f"Tick {event.tick} complete")

handle = world.add_hook(PostTick, log_tick)
# ...later...
world.remove_hook(handle)
```

| Event | Payload fields | When |
|-------|---------------|------|
| `PreTick` | `world_id`, `tick` | Before any archetype runs in `step()` |
| `PostTick` | `world_id`, `tick`, `results` | After all archetypes processed, `_live` refreshed, tick incremented |
| `OnSpawn` | `world_id`, `entity_id`, `components` | After `create_entity` / `spawn_reserved` registers the entity |
| `OnDespawn` | `world_id`, `entity_id` | After `remove_entity` cancels a pending spawn or queues a despawn row |
| `OnComponentAdded` | `world_id`, `entity_id`, `components` | After `add_components` moves the entity to a new archetype |
| `OnComponentRemoved` | `world_id`, `entity_id`, `component_types` | After `remove_components` moves the entity to a new archetype |

Payloads carry `world_id: UUID`, not the `AsyncWorld` instance itself. Handler
exceptions are logged at warning level and do not halt the tick.

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

The runtime surface is `await world.fork(name="branch-A")`, which calls the
gated `iCommandService.fork_world(...)` and returns a new handle bound to the
same `ActorCtx`.

### What's Cloned

The fork receives fresh identity and an independent snapshot of world-local
bookkeeping:

| State | Copied | Notes |
|-------|:------:|-------|
| `world_id` | Fresh | New `uuid7()` |
| `run_id` | Fresh | Fork starts a new run lineage |
| `tick` | Yes | Fork continues from the same tick |
| `_entity2sig` | Yes | Deep copy of entity-to-signature mapping |
| `_next_entity_id` | Yes | Entity ID counter |
| Spawn/despawn caches | Yes | Pending mutations transfer to the fork |
| Lifecycle hooks | Yes | Registrations at fork time copy; later registrations do not propagate |
| Processors | Shared | Same processor instances |
| Resources | Shared | Same `Resources` instance |

Pending mutation transfer is intentional. If a user spawns an entity and forks before the next tick, both source and fork materialize the entity on their next tick and diverge from there.

### Persistence

The fork writes to the same physical store by default, partitioned by its new `world_id`. A fork may be created with a different storage config through the gated service/runtime call.

Destroying a fork later removes only the live world object. Storage and audit rows remain queryable.

### Usage

```python
fork = await world.fork(name="branch-A")
await fork.run(steps=10)
```

Use forking for MCTS, counterfactual reasoning, or A/B testing simulation strategies.

For normative lifecycle semantics, see [World Lifecycle](world-lifecycle.md).

## Source Reference

- World: `src/archetype/core/aio/async_world.py`
- World service: `src/archetype/app/world_service.py`
