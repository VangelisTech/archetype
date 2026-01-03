# Storage

Archetype persists simulation state to columnar storage (LanceDB) enabling time-travel queries and deterministic replay.

## Storage Model

Each **archetype** (unique set of component types) maps to a physical table:

```
Table: archetype_position_velocity
┌──────────┬────────┬────────────────┬──────┬───────────┬────────────┬────────────┬─────────────┐
│ world_id │ run_id │ entity_id      │ tick │ is_active │ position_x │ position_y │ velocity_vx │
├──────────┼────────┼────────────────┼──────┼───────────┼────────────┼────────────┼─────────────┤
│ physics  │ run_1  │ uuid-abc       │ 0    │ true      │ 0.0        │ 0.0        │ 1.0         │
│ physics  │ run_1  │ uuid-abc       │ 1    │ true      │ 1.0        │ 0.5        │ 1.0         │
│ physics  │ run_1  │ uuid-abc       │ 2    │ true      │ 2.0        │ 1.0        │ 1.0         │
└──────────┴────────┴────────────────┴──────┴───────────┴────────────┴────────────┴─────────────┘
```

Key columns:
- `world_id` - Which world owns this entity
- `run_id` - Groups ticks into logical runs
- `entity_id` - Unique entity identifier
- `tick` - Simulation tick number
- `is_active` - Soft delete flag (false = despawned)

## Configuration

### Local Storage

```python
from archetype.dsl import World

async with World("sim", storage_uri="./archetype_data") as world:
    ...
```

### Remote Storage (S3/GCS)

```python
async with World(
    "sim",
    storage_uri="s3://my-bucket/archetype",
) as world:
    ...
```

## Time Travel Queries

Query historical state at any tick:

```python
from archetype.core import AsyncLancedbStore

store = AsyncLancedbStore(path="./archetype_data")

# Get state at specific tick
df = await store.query(
    archetype_signature,
    world_id="physics",
    run_id="run_1",
    tick=50
)

# Get entity history
df = await store.query(
    archetype_signature,
    world_id="physics",
    entity_id="uuid-abc"
)
```

## Forking from History

Use `spawn_world()` to fork from a historical tick:

```python
from archetype.dsl import spawn_world

# Fork from tick 50
async with spawn_world(world, fork_state=True, from_tick=50) as branch:
    # This branch starts with state as of tick 50
    for _ in range(10):
        await branch.step()
```

## Storage Backends

### LanceDB (Default)

Optimized for:
- Vector similarity search
- Append-heavy workloads
- Local + cloud storage

```python
from archetype.core import AsyncLancedbStore

store = AsyncLancedbStore(
    path="./data",
    read_consistency_interval=timedelta(seconds=0),  # Strong consistency
)
```

### Iceberg (Coming Soon)

For data lake integration:

```python
from archetype.app import StorageBackendManager

manager = StorageBackendManager()
await manager.register_backend(
    "iceberg",
    catalog_uri="s3://bucket/catalog",
    warehouse="s3://bucket/warehouse"
)
```

## Caching

The `AsyncCachedStore` provides a caching layer:

```python
from archetype.core import AsyncCachedStore, AsyncLancedbStore

base_store = AsyncLancedbStore(path="./data")
cached_store = AsyncCachedStore(base_store, max_cached_ticks=10)
```

This keeps recent ticks in memory for fast reads during simulation.

## Best Practices

1. **Use run_id for experiments** - Group related ticks together
2. **Checkpoint periodically** - Not every tick needs persistence in fast simulations
3. **Index frequently queried columns** - LanceDB supports ANN indexes for vector fields
4. **Clean up old runs** - Implement retention policies for disk space
