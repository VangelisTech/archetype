# Configuration

Archetype uses Pydantic models for all configuration. There are four config types, each scoped to a different concern: what the world is, where it stores data, how it caches, and how a run behaves.

## RunConfig

`RunConfig` controls the behavior of `world.run()` and `world.step()`. Each run gets a unique `run_id` (UUID7) for storage isolation.

```python
from archetype.core.config import RunConfig

config = RunConfig(num_steps=10, debug=True)
await world.run(config)
```

### Contract

- A `RunConfig` identifies a run — one `run_id` shared by every tick in the run.
- `SimulationService.step()` and `world.step()` **require** an explicit `RunConfig`. They MUST NOT mint one per call. Callers driving a multi-tick run reuse the same `RunConfig` across every step so persisted rows stay addressable by `run_id`.
- `SimulationService.run()` and `world.run()` thread the caller's `RunConfig` into every internal `step()` call.

### Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `run_id` | `UUID` | auto (uuid7) | Unique identifier for this run sequence |
| `num_steps` | `int` | `1` | Number of ticks to execute |
| `debug` | `bool` | `False` | Emit structured debug events (tick start/end, entity counts) |
| `enable_validation` | `bool` | `False` | Enable schema validation checks during processing |
| `show_rows` | `int` | `3` | Max rows in debug DataFrame snapshots (0 disables) |
| `explain` | `bool` | `False` | Render DataFrame logical plans in debug panels |
| `prefer_live_reads` | `bool` | `False` | Read from in-memory `_live` cache instead of store |
| `suite` | `str?` | `None` | Optional label for grouping runs (benchmarks, ensembles) |
| `trial` | `int?` | `None` | Optional trial index for ensemble/grid runs |
| `metadata` | `dict?` | `None` | Arbitrary metadata for experiment tracking |

`RunConfig` is frozen (immutable after construction).

### prefer_live_reads

Controls how `_run_archetype` fetches previous state at the start of each tick:

- **`False` (default):** Reads from the store via the querier. Suitable for single-step runs, validation, and benchmarks where you want to verify that persisted state round-trips correctly.
- **`True`:** Reads from the in-memory `_live` cache. Avoids redundant store reads between consecutive ticks. Required for multi-step runs where each step's `run_id` differs from the persisted rows.

In practice, `_live` is always used after tick 0 regardless of this flag -- the core tick loop falls back to `_live` when it is populated for a given signature. This flag controls the intent at the `RunConfig` level; the actual behavior is governed by the `_live` cache population logic in `AsyncWorld._run_archetype`.

### Named Constructors

`RunConfig` provides named constructors for common scenarios:

#### RunConfig.dev()

Interactive development. Debug output on, live reads enabled, higher row display limit.

```python
config = RunConfig.dev(steps=5)
# debug=True, prefer_live_reads=True, show_rows=5
```

#### RunConfig.benchmark()

Performance measurement. Debug off, no row display, optional suite/trial labels for organizing results.

```python
config = RunConfig.benchmark(steps=100, suite="latency", trial=0)
# debug=False, show_rows=0, suite="benchmark"
```

#### RunConfig.validate()

Schema and invariant checking. Validation enabled, debug on, reads from store to verify persistence.

```python
config = RunConfig.validate(steps=3)
# enable_validation=True, debug=True, prefer_live_reads=False, suite="validate"
```

## WorldConfig

Identifies a world instance. Passed to `WorldService.create_world()`.

```python
from archetype.core.config import WorldConfig

config = WorldConfig(name="my-sim")
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `world_id` | `UUID?` | auto (uuid7) | Unique identifier. Auto-generated if omitted |
| `name` | `str?` | `None` | Human-readable alias for lookup via `get_world_by_name()` |

## StorageConfig

Configures the persistence backend. Passed to `WorldService.create_world()` and `StorageService.get_backend()`.

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
| `io_config` | `IOConfig?` | `None` | Default Daft I/O config applied through `daft.set_planning_config(default_io_config=...)` for catalog-backed storage |

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

## Source Reference

- RunConfig, WorldConfig, StorageConfig, CacheConfig: `src/archetype/core/config.py`
