# Stores

`AsyncStore` is the persistence layer for archetype tables. It manages Daft session-based table creation, lazy reads/writes via Daft catalogs, and storage namespacing for multi-world/multi-run isolation.

## How It Works

The store never holds data in memory. It uses Daft's catalog and session system to reference tables lazily:

- **Reads** return a lazy `DataFrame` -- no data is materialized until you collect
- **Writes** append rows to the backing table via `Table.append()`
- **Tables** are created on demand when an archetype is first accessed

Each archetype signature maps to a single table, named by the archetype's deterministic hash (see [Archetype](archetype.md)).

## StorageContext

Before creating a store, you need a `StorageContext` -- the initialized runtime resources:

```python
from archetype.core.config import StorageConfig, StorageBackend
from archetype.core.runtime.storage import StorageContextFactory

config = StorageConfig(
    uri="./my_data",
    namespace="experiment_1",
)
context = StorageContextFactory.build(config)
```

`StorageContextFactory.build()` initializes:

1. An **Iceberg SqlCatalog** backed by SQLite for metadata
2. A **Daft Session** attached to the catalog
3. The **namespace** (created if it doesn't exist)

### Local vs Remote Storage

| URI scheme | Warehouse | Metadata |
|------------|-----------|----------|
| `./path` or `file://` | Local filesystem | SQLite in `path/catalog.db` |
| `s3://bucket` or `gs://bucket` | Remote object store | SQLite in `.archetype_meta/catalog.db` |

Remote warehouses store data in the cloud but keep catalog metadata locally in a `.archetype_meta/` directory.

### StorageContext Fields

| Field | Type | Description |
|-------|------|-------------|
| `uri` | `str` | Resolved storage URI |
| `namespace` | `str` | Daft namespace for table isolation |
| `session` | `Session` | Daft session with catalog attached |
| `catalog` | `Catalog` | Iceberg catalog (via Daft) |
| `io_config` | `IOConfig` | Daft I/O configuration |

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

No-op in the current implementation -- Daft handles cleanup automatically.

## Append-Only Model

Storage is strictly append-only. Nothing is overwritten or deleted. Each tick appends new rows with the current tick number. This gives you:

- **Time-travel** -- query any tick's state by filtering on `tick`
- **Replay** -- re-run from any checkpoint
- **Forking** -- branch a world and append independently
- **Audit** -- full history of every entity at every tick

## Source Reference

- Store: `src/archetype/core/aio/async_store.py`
- Storage context: `src/archetype/core/runtime/storage.py`
