# Configuration

Archetype uses Pydantic models for all configuration. There are four config types, each scoped to a different concern: what the world is, where it stores data, how it caches, and how a run behaves.

## RunConfig

`RunConfig` controls the behavior of `step` and `run`. Each run gets a unique `run_id` (UUID7) for storage isolation.

```python
from archetype.core.config import RunConfig

config = RunConfig(num_steps=10, debug=True)
await world.run(config=config)
```

### Contract

- A `RunConfig` identifies a run — one `run_id` shared by every tick in the run.
- `SimulationService.step()` and `world.step()` **require** an explicit `RunConfig`. They MUST NOT mint one per call. Callers driving a multi-tick run reuse the same `RunConfig` across every step so persisted rows stay addressable by `run_id`.
- `SimulationService.run()` and `world.run()` thread the caller's `RunConfig` into every internal `step()` call.
- `EpisodeConfig` wraps `RunConfig` with termination semantics.
- `RolloutConfig` wraps `EpisodeConfig` with fork-and-aggregate semantics.

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

Identifies a world instance. Runtime/API callers create worlds through `iCommandService.create_world(...)`; lower-level internal callers use `WorldService.create_world()`.

```python
from archetype.core.config import WorldConfig

config = WorldConfig(name="my-sim")
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `world_id` | `UUID?` | auto (uuid7) | Unique identifier. Auto-generated if omitted |
| `name` | `str?` | `None` | Human-readable alias for lookup via `get_world_by_name()` |

## StorageConfig

Configures the persistence backend. Runtime/API callers pass it through the gate; lower-level services pass it to `WorldService.create_world()` and `StorageService.get_or_create_store()`.

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
- EpisodeConfig, RolloutConfig: `src/archetype/app/models.py`
