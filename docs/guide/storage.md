# Storage model

Archetype treats simulation state as an append-only time series.

## Primary keys

Every stored row includes:

- `world_id`: which world produced the row
- `run_id`: which run (a sequence of ticks) the row belongs to
- `tick`: simulation step
- `entity_id`: entity identity within the world
- `is_active`: soft-delete flag (despawn marks entities inactive)

Archetype partitions by `world_id`, `run_id`, and `tick` and stores per-archetype tables keyed by the archetype schema signature.

## Why LanceDB

LanceDB gives you:

- columnar layout (good for component projections)
- fast local reads
- indices on frequently filtered columns (`entity_id`, `tick`, `world_id`, `run_id`)

In `AsyncLancedbStore`, table creation is lazy: tables are created on first use, with optional index creation controlled by environment flags.

## StorageContext and remote object stores

Archetype uses a `StorageContext` to initialize runtime storage resources from a declarative `StorageConfig`.

The `StorageContextFactory` supports:

- local file URIs (default): local warehouse + local SQLite Iceberg catalog
- remote object stores (e.g. `s3://`, `gs://`): remote warehouse with a local SQLite catalog for metadata

The important contract is that **I/O configuration** (credentials, object store config) is centralized in `StorageConfig` and propagated into Daft planning via `IOConfig`.

