# Stores

`AsyncStore` is the persistence layer for archetype tables. It manages table creation, lazy reads/writes via Daft catalogs, and storage namespacing for multi-world/multi-run isolation.

## How It Works

The store delegates persistence to Daft's catalog and session system. All reads and writes go through lazy DataFrame references:

- **Reads** return a lazy `DataFrame` -- no data is materialized until you collect
- **Writes** append rows to the backing table, passing `StorageConfig.io_config` explicitly for Iceberg-backed stores when configured
- **Tables** are created on demand when an archetype is first accessed

Each archetype signature maps to a single table, named by the archetype's deterministic hash (see [Archetype](archetype.md)).

## Storage Construction

`StorageService` owns the conversion from user-facing `StorageConfig` into
backend-native core store inputs. The core stores do not interpret
`StorageConfig` themselves.

```python
from archetype.core.config import StorageConfig, StorageBackend
from archetype.app.storage_service import StorageService

config = StorageConfig(
    uri="./my_data",
    namespace="experiment_1",
    backend=StorageBackend.ICEBERG,
)
session = StorageService.build_session(config)
```

`StorageService.build_session()` is the default convenience path for catalog-backed
storage. It initializes:

1. An **Iceberg SqlCatalog** backed by SQLite for metadata
2. A **Daft Session** attached to the catalog
3. The **namespace** (created if it doesn't exist)

For LanceDB, `StorageService` passes the resolved storage URI and namespace
directly to `AsyncLancedbStore`. It does not build a Daft session/catalog for
the LanceDB backend.

### Local vs Remote Storage

| URI scheme | Warehouse | Metadata |
|------------|-----------|----------|
| `./path` or `file://` | Local filesystem | SQLite in `path/catalog.db` |
| `s3://bucket` or `gs://bucket` | Remote object store | SQLite in `.archetype_meta/catalog.db` |

Remote warehouses store data in the cloud but keep catalog metadata locally in a `.archetype_meta/` directory.

### Store Inputs

| Store | Input |
|-------|-------|
| `AsyncStore` | Daft `Session`, optional Daft `IOConfig` |
| `AsyncLancedbStore` | resolved `uri`, `namespace` |

Legacy imports from `archetype.core.runtime.storage` remain available as
compatibility shims for the old `StorageContext` name. New code should use the
Daft-native session and app-level factories.

## Store API

### Reading

```python
df = await store.get_archetype_df(sig, world_id="abc", run_id="run-1")
```

Returns a lazy DataFrame filtered by `world_id` and `run_id`. The table is created if it doesn't exist yet.

### Writing

```python
await store.append(sig, df)
```

Appends rows to the archetype table. Zero-row and empty-schema DataFrames are silently skipped. The table is created if it doesn't exist.

### Shutdown

```python
await store.shutdown()
```

No-op in the base implementation -- Daft handles cleanup automatically. `AsyncCachedStore` overrides this to flush pending data.

## Append-Only Model

Storage is strictly append-only. Nothing is overwritten or deleted. Each tick appends new rows with the current tick number. This gives you:

- **Time-travel** -- query any tick's state by filtering on `tick`
- **Replay** -- re-run from any checkpoint
- **Forking** -- branch a world and append independently
- **Audit** -- full history of every entity at every tick

## Storage Backends

`StorageService` selects the store implementation based on `StorageConfig.backend`:

| Backend | Store class | Format | Best for |
|---------|------------|--------|----------|
| `StorageBackend.LANCEDB` (default) | `AsyncLancedbStore` | Lance columnar | Local development, single-process |
| `StorageBackend.ICEBERG` | `AsyncStore` | Iceberg (Parquet via Daft catalog) | Distributed, cloud-native |

Both implement the `iAsyncStore` interface -- the querier and updater are backend-agnostic.

### LanceDB (Default)

LanceDB stores data in Lance format on the local filesystem. It is the default because it requires no external infrastructure and provides fast columnar reads for single-process simulations.

### Iceberg

The Iceberg backend uses Daft's native Iceberg integration with a SQLite-backed PyIceberg SQL catalog. It writes Parquet files and supports:

- Cloud object stores (S3, GCS) via `StorageConfig.io_config`, passed explicitly to Daft Iceberg reads/writes
- Catalog-level namespace isolation
- Compatibility with the broader Iceberg ecosystem

### Backend Selection

`StorageService._create_backend()` checks `storage_config.use_lancedb` (derived from the `backend` enum) to pick the store class. Both are wrapped identically by `AsyncQueryManager` and `AsyncUpdateManager`:

```text
StorageService._create_backend(config, cache_config)
    |
    +-- config.use_lancedb? --> StorageService.resolve_location(config)
    |                         --> AsyncLancedbStore(uri, namespace)
    +-- else                --> StorageService.build_session(config)
                              --> AsyncStore(session)
    |
    +-- cache_config?     --> AsyncCachedStore(store, cache_config)
    |
    +-- AsyncQueryManager(store)
    +-- AsyncUpdateManager(store)
```

## Write-Behind Cache

`AsyncCachedStore` wraps any `iAsyncStore` with an in-memory write buffer. Appends accumulate in per-archetype `MemTable` structures (lists of PyArrow `RecordBatch`) and flush to the inner store when thresholds are exceeded.

### Flush Triggers

A flush fires when any of these conditions is met:

| Threshold | Config field | Default |
|-----------|-------------|---------|
| Row count per archetype | `flush_rows` | 1,000,000 |
| Bytes per archetype | `flush_mb` | 512 MB |
| Total cached bytes (global) | `global_mb` | 1 GB |
| Idle time (background loop) | `idle_sec` | 30 seconds |

The first three are checked synchronously after each `append()`. The idle timer runs as a background `asyncio.Task` that scans all memtables and flushes any that have been untouched for `idle_sec`.

### Read Path

`AsyncCachedStore.get_archetype_df()` checks the memtable first. If the archetype has cached rows, it builds a DataFrame directly from the in-memory Arrow batches. Otherwise it falls through to the inner store.

### Shutdown

`AsyncCachedStore.shutdown()` cancels the background task, flushes all remaining memtables, and delegates to the inner store's shutdown.

### Configuration

```python
from archetype.core.config import CacheConfig

cache = CacheConfig(flush_rows=500_000, idle_sec=15.0)
```

Pass `CacheConfig` to `StorageService.get_backend()` or `WorldService.create_world()` to enable caching. See [Configuration](run-config.md#cacheconfig) for all fields.

## Source Reference

- Store (Iceberg): `src/archetype/core/aio/async_store.py`
- Store (LanceDB): `src/archetype/core/storage/lancedb.py`
- Storage service/builders: `src/archetype/app/storage_service.py`
- Cached store: `src/archetype/core/aio/async_cached_store.py`
- Storage service: `src/archetype/app/storage_service.py`
