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
`StorageConfig` themselves. Archetype supplies one concrete data-plane catalog
factory: a local Iceberg warehouse with SQLite-backed PyIceberg metadata.

```python
from archetype.core.config import StorageConfig, StorageBackend
from archetype.storage import StorageService

storage = StorageConfig(
    uri="./my_data",
    namespace="experiment_1",
    backend=StorageBackend.ICEBERG,
)
storage_service = StorageService()
```

On first use, the local path initializes:

1. An **Iceberg SqlCatalog** backed by SQLite for table metadata
2. A **Daft Session** attached to the catalog
3. The **namespace** (created if it doesn't exist)

For LanceDB, `StorageService` passes the resolved storage URI and namespace
directly to `AsyncLancedbStore`. It does not build a Daft session/catalog for
the LanceDB backend.

### Control plane and data plane

Local SQLite can appear in two deliberately separate roles:

| Plane | Local implementation | Remote implementation | Authority |
|---|---|---|---|
| Control | `SqliteControlCatalog` | Durable Object control catalog | World identity, writer fences, visibility manifests, deferred commands, and narrow workflow leases |
| Data | PyIceberg `SqlCatalog` plus local files | Caller-configured Iceberg catalog plus object storage | Table metadata, atomic snapshots, manifests, and data files |

The control catalog answers whether a writer or workflow action is admitted
and which committed state is visible. Iceberg answers whether one table update
committed atomically and which files belong to its snapshot. Iceberg's
serializable table history does not replace cross-table workflow coordination,
and the control catalog does not store artifact bytes or analytical rows.

`StorageService` composes both planes for one storage identity. The built-in
local Iceberg path therefore has a control-catalog SQLite database and a
separate PyIceberg metadata database. A managed deployment can independently
replace the first with the Durable Object control catalog and the second with a
caller-configured catalog attached to Daft.

The local implementation can place world discovery, per-world control state,
commands, manifests, evaluations, and outbox rows in one SQLite database. That
layout is not a distributed transaction promise. In the remote topology,
directory discovery may use a directory Durable Object, each world's manifest,
command settlement, and control-outbox append share that world's control
authority, and Iceberg commits data separately. The atomic control transaction
runs only after the data flush has completed.

### Control-catalog bootstrap

`ControlCatalogConfig` is an immutable configuration snapshot. Ordinary
storage operations never reread environment variables. The application
composition root captures `ARCHETYPE_CATALOG_DIR`,
`ARCHETYPE_CONTROL_CATALOG_URL`, and
`ARCHETYPE_CONTROL_CATALOG_TOKEN` once when it constructs its owned
`StorageService`:

```python
from archetype.storage import ControlCatalogConfig, StorageService

catalog_config = ControlCatalogConfig.from_env()
storage_service = StorageService(control_catalog_config=catalog_config)
```

A remote URL without a token fails during bootstrap. Passing an explicit
`ControlCatalogConfig` is the deterministic choice for tests and embedded
hosts; later environment changes do not alter the already-constructed service.

### Managed and remote Iceberg

Archetype does not infer a remote catalog from environment variables and does
not pair remote object data with hidden local metadata. Configure the catalog,
namespace, and catalog credentials directly in a Daft `Session`, then inject
that session at the composition root:

```python
from daft.session import Session
from archetype.app.container import ServiceContainer
from archetype.storage import StorageService

session = Session()
session.attach_catalog(configured_catalog)
session.set_namespace("experiment_1")

services = ServiceContainer(storage_service=StorageService(session=session))
```

`configured_catalog` may wrap a managed PyIceberg catalog or another catalog
already supported by Daft. That attached catalog and namespace are
authoritative. `StorageConfig.io_config` remains the single explicit entry
point for object-data credentials passed to Daft reads and writes; Archetype
does not translate it into catalog properties.

An injected `StorageService` remains caller-owned. Container shutdown leaves
it open so another container or host service can continue using it; the caller
closes it after its final consumer stops.

An injected session is bound to one configured storage URI and namespace.
Create a separate `Session` and `StorageService` for another namespace;
Archetype rejects a mismatch instead of mutating shared session state.

## Cloud Provider Banners

The provider snippets below show `IOConfig` data-plane configuration. They are
used with a caller-configured catalog session as shown above; a remote
`StorageConfig` by itself intentionally fails closed.

```python
from archetype.core.config import StorageBackend, StorageConfig

storage = StorageConfig(
    uri="s3://your-bucket/archetype/warehouse",
    namespace="product_demo",
    backend=StorageBackend.ICEBERG,
    io_config=io_config,
)

# Pass this storage config through the ServiceContainer backed by the
# preconfigured session above.
```

The full runnable catalog is in
[`examples/09_cloud_storage.py`](https://github.com/VangelisTech/archetype/blob/main/examples/09_cloud_storage.py).
It prints each provider banner without opening network connections, and
`--smoke-local` runs a local world through the same runtime storage API.

### AWS S3

```python
from daft.io import IOConfig, S3Config
from archetype.core.config import StorageBackend, StorageConfig

storage = StorageConfig(
    uri="s3://your-bucket/archetype/warehouse",
    namespace="product_demo",
    backend=StorageBackend.ICEBERG,
    io_config=IOConfig(
        s3=S3Config(region_name="us-east-1", profile_name="default")
    ),
)
```

### Google Cloud Storage

```python
from daft.io import GCSConfig, IOConfig
from archetype.core.config import StorageBackend, StorageConfig

storage = StorageConfig(
    uri="gs://your-bucket/archetype/warehouse",
    namespace="product_demo",
    backend=StorageBackend.ICEBERG,
    io_config=IOConfig(
        gcs=GCSConfig(project_id="your-project")
    ),
)
```

### Azure Blob or ADLS

```python
from daft.io import AzureConfig, IOConfig
from archetype.core.config import StorageBackend, StorageConfig

storage = StorageConfig(
    uri="az://container/archetype/warehouse",
    namespace="product_demo",
    backend=StorageBackend.ICEBERG,
    io_config=IOConfig(
        azure=AzureConfig(storage_account="account-name")
    ),
)
```

### Cloudflare R2

```python
from daft.io import IOConfig, S3Config
from archetype.core.config import StorageBackend, StorageConfig

storage = StorageConfig(
    uri="s3://your-r2-bucket/archetype/warehouse",
    namespace="product_demo",
    backend=StorageBackend.ICEBERG,
    io_config=IOConfig(
        s3=S3Config(
            endpoint_url="https://<account-id>.r2.cloudflarestorage.com",
            region_name="auto",
        )
    ),
)
```

### MinIO

```python
from daft.io import IOConfig, S3Config
from archetype.core.config import StorageBackend, StorageConfig

storage = StorageConfig(
    uri="s3://your-minio-bucket/archetype/warehouse",
    namespace="product_demo",
    backend=StorageBackend.ICEBERG,
    io_config=IOConfig(
        s3=S3Config(
            endpoint_url="http://localhost:9000",
            region_name="us-east-1",
        )
    ),
)
```

### Tencent COS

```python
from daft.io import CosConfig, IOConfig
from archetype.core.config import StorageBackend, StorageConfig

storage = StorageConfig(
    uri="cos://your-bucket/archetype/warehouse",
    namespace="product_demo",
    backend=StorageBackend.ICEBERG,
    io_config=IOConfig(
        cos=CosConfig(region="ap-guangzhou")
    ),
)
```

### Volcengine TOS

```python
from daft.io import IOConfig, TosConfig
from archetype.core.config import StorageBackend, StorageConfig

storage = StorageConfig(
    uri="tos://your-bucket/archetype/warehouse",
    namespace="product_demo",
    backend=StorageBackend.ICEBERG,
    io_config=IOConfig(
        tos=TosConfig(region="cn-beijing")
    ),
)
```

### Store Inputs

| Store | Input |
|-------|-------|
| `AsyncStore` | Daft `Session`, optional Daft `IOConfig` |
| `AsyncLancedbStore` | resolved `uri`, `namespace` |

## Application Daft execution authority

`StorageService` is the sole terminal Daft execution authority for application
families. Its narrow substrate operations include:

| Operation | Contract |
|---|---|
| `materialize(frame)` | Admit and execute one Archetype-owned lazy plan, returning the completed frame |
| `read_table(config, name)` | Resolve an existing registered app table and return a lazy Iceberg read |
| `append_table(config, name, rows)` | Register or schema-check the table, materialize the producer once, and append all rows |
| `append_missing(config, name, rows, key_columns=...)` | Register or schema-check the table, anti-join visible keys, and append only missing rows |
| `pin_visibility(config, world_id, ...)` | Capture an immutable manifest-token allowlist for one world/run segment |
| `scan_visible_world_rows(config, record, visibility)` | Return raw physical signature-table frames admitted by that pin |
| `append_world_rows(config, world_id, name, rows, ...)` | Resolve and stamp the catalog-owned world/run envelope before append |
| `read_world_rows(config, world_id, name)` | Return a lazy application-table read scoped to the durable world/run |
| `bind_commit_coordinator(config, world_id=..., run_id=..., writer_epoch=...)` | Construct a coordinator bound to one exact durable writer identity |

The physical scan deliberately does not decide entity liveness, resolve
same-tick active/inactive ties, load component classes, interpret lineage,
choose a resumed tick, or allocate the next entity ID. Those are world-family
semantics layered over the raw visible frames.

The service uses one reentrant execution gate for terminal plans and for
appends made by its pooled ECS stores. Reentrancy lets a cached-store append
flush into its inner store in the same task; a background flush or another
application job waits its turn. Daft still parallelizes the admitted plan over
rows and partitions. The gate coordinates local submissions so application
services do not independently saturate or reorder one process's Daft runtime.

This gate is not an Iceberg lock. Iceberg commits remain optimistic and atomic.
A plain append freezes its producer result once and can reuse it after a
metadata refresh. A conditional append refreshes after a conflict and
recomputes its anti-join against the new snapshot before retrying. That
distinction prevents a stale retry from publishing a logical key that another
writer committed first.

Application families may construct lazy DataFrame transforms. They request
materialization or table persistence from `iStorageService`; they do not call
Daft collection, Iceberg read/write, or catalog table-creation primitives
directly. Public query callers still own execution of the lazy DataFrames
returned across the runtime boundary.

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

The Iceberg backend uses Daft's native Iceberg integration and writes Parquet
files. The built-in path uses a local SQLite-backed PyIceberg SQL catalog;
managed deployments inject their configured Daft session. It supports:

- Cloud object stores via the injected catalog plus `StorageConfig.io_config`
- Catalog-level namespace isolation
- Compatibility with the broader Iceberg ecosystem

### Backend Selection

`create_async_store()` selects the backend enum and then applies the optional
write-behind cache:

```text
create_async_store(config, session, cache_config)
    |
    +-- LANCEDB --> AsyncLancedbStore(uri, namespace)
    +-- ICEBERG + supplied session --> AsyncStore(session)
    +-- ICEBERG + no session --> configure_session(config)
                                 --> local SQLite catalog
                                 --> AsyncStore(session)
    |
    +-- cache_config? --> AsyncCachedStore(store, cache_config)
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

Pass `CacheConfig` through runtime/world creation or `StorageService.get_or_create_store()` to enable caching. See [Configuration](run-config.md#cacheconfig) for all fields.

## Source Reference

- Store (Iceberg): `src/archetype/core/aio/async_store.py`
- Store (LanceDB): `src/archetype/core/storage/lancedb.py`
- Storage service/builders: `src/archetype/storage/service.py`
- Durable catalog contract and implementations: `src/archetype/storage/catalog/`
- Storage bootstrap configuration: `src/archetype/storage/config.py`
- Cached store: `src/archetype/core/aio/async_cached_store.py`
