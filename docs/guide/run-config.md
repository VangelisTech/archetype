# Configuration

Archetype uses Pydantic models for all configuration. There are four config types, each scoped to a different concern: what the world is, where it stores data, how it caches, and how a run behaves.

## RunConfig

`RunConfig` describes one bounded sequence of ticks. A normal world already
owns an active `run_id` when it is constructed; execution keeps that identity
across ticks and repeated calls so its append-only history stays continuous.
The config contains execution policy only and cannot select or rename durable
world identity.

```python
from archetype.core.config import RunConfig

config = RunConfig(num_steps=10, debug=True)
await world.run(config=config)
```

### Contract

- A new world mints its active `run_id`; mutable resume restores it, and a fork
  mints a fresh identity for its new lineage.
- `world.run_id` is immutable construction state. `RunResult.run_id` reports
  that same identity, which is the value stamped on durable rows.
- The world family's managed `step()` and the lower-level `AsyncWorld.step()`
  require an explicit `RunConfig`. `RuntimeWorld.step()` creates the ordinary
  one-step config when the public caller omits it.
- The world family's `run()` function threads the caller's `RunConfig` into
  every internal step.
- `EpisodeConfig` wraps `RunConfig` with termination semantics.
- `RolloutConfig` wraps `EpisodeConfig` with fork-and-aggregate semantics.

### Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `num_steps` | `int` | `1` | Number of ticks to execute |
| `debug` | `bool` | `False` | Emit per-tick diagnostic panels |
| `show_rows` | `int` | `8` | Maximum rows in each debug snapshot; `0` disables snapshots |
| `metadata` | `dict?` | `None` | Optional metadata recorded with the run |

`RunConfig` is frozen (immutable after construction).

### Named Constructors

`RunConfig` provides named constructors for common scenarios:

#### RunConfig.dev()

Interactive development with debug output and a five-row display limit by
default.

```python
config = RunConfig.dev(steps=5)
# debug=True, show_rows=5
```

#### RunConfig.benchmark()

Performance measurement with debug output and row snapshots disabled. Put
experiment labels in `metadata` when needed.

```python
config = RunConfig.benchmark(
    steps=100,
    metadata={"suite": "latency", "trial": 0},
)
# debug=False, show_rows=0
```

## WorldConfig

Identifies a world instance. Runtime/API callers create worlds through
`iCommandGateway.create_world(...)`; internal composition calls the
family-owned `iWorldLifecycle.create_world(...)` port.

```python
from archetype.core.config import WorldConfig

config = WorldConfig(name="my-sim")
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `world_id` | `UUID?` | auto (uuid7) | Unique identifier. Auto-generated if omitted |
| `name` | `str?` | `None` | Human-readable alias for lookup via `get_world_by_name()` |

## StorageConfig

Configures the persistence backend. Runtime/API callers pass it through the
gate; internal lifecycle code passes it to `StorageService` through the
family-owned `iStorageService` port.

```python
from archetype.core.config import StorageConfig, StorageBackend

config = StorageConfig(
    uri="./my_data",
    namespace="experiment_1",
    backend=StorageBackend.LANCEDB,
)
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `uri` | `str \| Path` | `"./archetype_data"` | Storage location (local path or cloud URI) |
| `namespace` | `str` | `"archetypes"` | Catalog namespace for table isolation |
| `backend` | `StorageBackend` | `LANCEDB` | Backend engine: `LANCEDB` or `ICEBERG` |
| `io_config` | `IOConfig?` | `None` | Native Daft I/O config passed to Iceberg storage read/write operations |

`Path` values are coerced to `str` via a field validator. See [Stores](stores.md) for backend-specific behavior.

## CacheConfig

Configures write-behind caching when wrapping a store with `AsyncCachedStore`. Optional -- omit for direct writes.

```python
from archetype.core.config import CacheConfig

cache = CacheConfig(flush_rows=500_000, idle_sec=15.0)
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `flush_rows` | `int` | `1,000,000` | Row count threshold to trigger flush |
| `flush_mb` | `int` | `512` | Size threshold (MB) to trigger flush |
| `global_mb` | `int` | `1 GB` | Total memory budget for the cache |
| `idle_sec` | `float` | `30.0` | Seconds of inactivity before auto-flush |

The cache flushes when any threshold is exceeded. See [Stores -- Write-Behind Cache](stores.md#write-behind-cache) for details.

## EpisodeConfig

`EpisodeConfig` describes step-until-termination execution on the world supplied by the caller. It does not fork.

Key fields:

| Field | Description |
|---|---|
| `episode_id` | Episode identity |
| `run_config` | `RunConfig` used for each step |
| `max_steps` | Defensive cap |
| `terminal_component` | Terminate when any active entity has this component |
| `termination` | Optional callable predicate over world state |

## RolloutConfig

`RolloutConfig` describes N forked episodes from a base world.

Key fields:

| Field | Description |
|---|---|
| `rollout_id` | Rollout identity |
| `episode_config` | Episode template per fork |
| `num_episodes` | Number of forked episodes |
| `parallel` | Whether to run forked episodes concurrently |
| `name_prefix` | Prefix for fork names |
| `destroy_forks_on_complete` | Remove live fork worlds after completion; persisted rows remain |

See [Execution Hierarchy](execution-hierarchy.md).

## Source Reference

- RunConfig, WorldConfig, StorageConfig, CacheConfig: `src/archetype/core/config.py`
- EpisodeConfig, RolloutConfig: `src/archetype/world/models.py`
