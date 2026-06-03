# Committed Read Model

This page specifies the durable external read model that `QueryService` MUST
eventually satisfy.

Current status:

- This is a normative target contract for the engine and service layer.
- The current implementation is only partially aligned.
- In particular, process-local world caches such as `_live` and `_entity2sig`
  are not a sufficient public read model.

## Why This Exists

Archetype already has two important pieces:

1. an execution model: `AsyncWorld`, staged mutation caches, `_live`, and
   processor execution
2. an append-only storage model: one Lance `AsyncTable` per archetype
   signature, shaped and aggregated with Daft

What is still missing is a durable snapshot model for external queries.

Without that third piece, `QueryService` cannot guarantee:

- restart-safe reads
- exact historical reads by tick
- correct reads after archetype migration or despawn
- read consistency while a world is stepping

## Design Principles

### Store-first public reads

External reads MUST resolve from durable storage, not from process-local world
memory.

`AsyncWorld` is the execution engine. The store is the public read model.

### Broker-governed access, store-served data

The broker or guard layer MUST govern authorization, quotas, and read
consistency mode selection.

The broker SHOULD NOT become the storage access layer. `QueryService` remains
responsible for reading and shaping data from Lance `AsyncTable` objects.

### Committed snapshots by default

The default consistency mode for external reads MUST be `committed`.

Committed reads only see ticks that have been fully persisted and marked
readable by durable metadata.

### Archetype ECS means unions, not N-way joins

Archetype storage is already denormalized by signature. For most ECS queries,
the efficient plan is:

1. identify the archetype tables relevant to the snapshot
2. project the requested columns from each matching table
3. union the projected results with Daft

Large N-way joins are not the default access pattern.

### `_live` remains an optimization, not the contract

`_live` MAY remain an internal execution cache or an explicit
`consistency="live"` debugging path.

It MUST NOT define the default semantics of external `QueryService` APIs.

## Core Terms

| Term | Meaning |
|---|---|
| `world.tick` | The next tick to process in `AsyncWorld` |
| `Committed tick` | A tick whose full snapshot is durably readable |
| `Readable head` | The latest committed tick for a world |
| `Snapshot manifest` | Durable metadata describing which archetypes participate in one committed world snapshot |
| `Locator` | Durable metadata resolving an entity to its archetype signature at a committed tick |
| `Consistency mode` | The read source and freshness policy for a query |

## Public Query Contracts

### QueryService authority

- `QueryService` is the only external read facade for world state.
- External adapters such as REST and CLI MUST read through `QueryService`.
- `QueryService` MUST validate world existence consistently before returning
  snapshot data.

### Authorization contract

- Read authorization and quota accounting MUST occur at the service-layer
  boundary.
- The current `guardrail_allow()` function is command-only. A conforming
  implementation SHOULD add an equivalent read admission path rather than
  bypassing governance entirely.
- The broker MAY issue a lightweight read ticket containing:
  `world_id`, resolved `tick`, requested consistency mode, and actor metadata.

### Consistency modes

| Mode | Source | Restart-safe | Historical | Default |
|---|---|---:|---:|---:|
| `committed` | Durable Lance metadata + archetype tables | Yes | Yes | Yes |
| `live` | `AsyncWorld` process-local caches | No | No | No |

Rules:

- `committed` MUST be the default for all external API reads.
- `live` MUST be explicit and MUST be documented as best-effort only.
- `live` mode MUST NOT claim historical or restart-safe semantics.

### Tick semantics

- `world.tick` MUST continue to mean "the next tick to process."
- `tick=None` on public reads MUST resolve to the readable head, not to
  `world.tick`.
- A request for a tick that is not durably committed MUST fail explicitly.
- Public query identity is `(world_id, tick)`, with `run_id` treated as
  provenance rather than the primary external lookup key.

### Error semantics

- Unknown world MUST fail explicitly.
- Unknown or uncommitted tick MUST fail explicitly.
- Partial snapshot reads MUST fail explicitly; they may not silently degrade to
  partial results.
- An unknown entity at a readable tick MAY return an empty component map for
  compatibility, but it MUST reflect the requested tick truthfully and MUST NOT
  fall back to current state.

## Durable Tables

The committed read model requires three metadata tables in addition to the
existing per-signature archetype tables.

### Existing archetype tables

These remain the primary fact tables.

- one Lance `AsyncTable` per `ArchetypeSignature`
- append-only rows
- existing base columns:
  `world_id`, `run_id`, `entity_id`, `tick`, `is_active`

### `world_commits`

One row per committed world snapshot.

Required columns:

| Column | Purpose |
|---|---|
| `world_id` | World identity |
| `tick` | Committed tick |
| `run_id` | Provenance for this committed snapshot |
| `status` | At minimum `committed`; may later include `tombstoned` |
| `committed_at` | Commit timestamp |
| `entity_count` | Total active entities at this tick |
| `archetype_counts_json` | Optional pre-aggregated counts keyed by signature or descriptor |

Recommended indexes:

- `world_id`
- `tick`
- `status`

Contract:

- A `(world_id, tick)` pair MUST have at most one readable committed row.
- Readers MUST treat `world_commits` as the visibility barrier for snapshots.

### `world_signatures`

The snapshot manifest: which archetypes participate in one committed world
snapshot.

Required columns:

| Column | Purpose |
|---|---|
| `world_id` | World identity |
| `tick` | Committed tick |
| `run_id` | Provenance |
| `signature_name` | Durable archetype table name |
| `component_types_json` | Ordered component type names for subset checks |
| `entity_count` | Active entities in this signature at this tick |

Recommended indexes:

- `world_id`
- `tick`
- `signature_name`

Contract:

- `world_signatures` MUST be sufficient to decide which archetype tables are
  eligible for one committed snapshot without consulting current world memory.
- Because `signature_name` is a compact hash-derived identifier, component type
  names MUST also be persisted durably.

### `entity_locator`

Durable entity-to-signature history.

Required columns:

| Column | Purpose |
|---|---|
| `world_id` | World identity |
| `entity_id` | Entity identity |
| `tick` | Committed tick |
| `run_id` | Provenance |
| `signature_name` | Archetype table containing this entity at this tick |
| `component_types_json` | Component names for this signature |
| `is_active` | Whether the entity exists at this tick |

Recommended indexes:

- `world_id`
- `entity_id`
- `tick`
- `signature_name`

Contract:

- `entity_locator` MUST let `get_entity(world_id, entity_id, tick)` resolve one
  archetype table without scanning all signatures.
- Archetype migration and despawn MUST append new locator rows; prior history
  remains immutable.

## Commit Protocol

Lance `AsyncTable` objects do not provide cross-table transactions. The engine
therefore MUST use commit ordering to establish a readable snapshot boundary.

For a world step producing tick `T`, the write order MUST be:

1. append archetype rows for tick `T`
2. append `entity_locator` rows for tick `T`
3. append `world_signatures` rows for tick `T`
4. append `world_commits` row for tick `T` last

Rules:

- The snapshot is not externally readable until step 4 succeeds.
- If a failure occurs before the `world_commits` row is written, readers MUST
  ignore the partial tick.
- Writers MUST NOT advance the readable head by mutating in-memory state alone.

## Query Planning

The store is Lance. The shaping layer is Daft.

The intended execution model is:

1. use Lance `AsyncTable.query().where(...)` for predicate pushdown
2. convert each matching result to Arrow
3. wrap Arrow in Daft DataFrames
4. use Daft for projection, union, and final aggregation
5. materialize only at the `QueryService` response boundary

Pushdown filters SHOULD include:

- `world_id`
- `tick`
- `entity_id` when available
- `run_id` when resolved from `world_commits`
- `is_active`

## API Semantics

### `get_world_state(world_id, tick=None, consistency="committed")`

Algorithm for committed reads:

1. authorize the read
2. resolve the target tick from `world_commits`
3. load `world_signatures` for `(world_id, tick)`
4. scan only those archetype tables
5. use Daft to union the active `entity_id` rows and build the entity map
6. return `WorldSnapshot`

Contract:

- Counts MAY be served directly from `world_commits` or `world_signatures`
  without rescanning all entities.
- The entity map MUST be derived from the committed snapshot, not from current
  `_live`.

### `get_entity(world_id, entity_id, tick=None, consistency="committed")`

Algorithm for committed reads:

1. authorize the read
2. resolve the target tick from `world_commits`
3. read exactly one locator row from `entity_locator`
4. if no active locator row exists, return the documented absent-entity shape
5. open the resolved archetype table and fetch the matching row
6. project component fields by prefix into the response shape

Contract:

- `get_entity()` MUST NOT inspect current `_entity2sig` for historical reads.
- An entity that migrated archetypes MUST remain queryable at older ticks.

### `get_components(world_id, component_types, entity_ids=None, tick=None, consistency="committed")`

Algorithm for committed reads:

1. authorize the read
2. resolve the target tick from `world_commits`
3. read `world_signatures` for the snapshot
4. choose only signatures whose `component_types_json` is a superset of the
   requested component set
5. query those archetype tables with pushdown filters
6. project the requested columns and union the result with Daft

Contract:

- This is primarily a union over matching archetype tables, not an N-way join.
- Joins are only required for future normalized component-storage designs, not
  for the current archetype-table layout.

### `get_command_history(world_id, limit=100)`

`get_command_history()` remains broker-backed rather than snapshot-backed.

Contract:

- command history MAY remain separate from the committed world snapshot model
- command history queries MUST still validate world-lifecycle expectations
  consistently with the rest of `QueryService`

## Lifecycle Contracts

### Restart and discovery

- Query correctness MUST survive `WorldService.discover_worlds()` and process
  restart.
- Rehydration MAY restore `tick` and other execution metadata, but public query
  correctness MUST NOT depend on reconstructing `_live` or `_entity2sig`.

### Concurrent step vs read

- While a world is building tick `T`, committed readers MUST continue to see
  the latest committed tick `T-1`.
- No external caller may observe a partially written tick.
- `live` mode MAY expose in-flight runtime state, but that behavior MUST be
  explicitly opt-in.

### World removal

- The engine MUST define whether world removal is a tombstone or a hard delete.
- If tombstoned, historical committed reads MAY remain available.
- If hard deleted, all `QueryService` methods MUST fail consistently.

## Required Tests

Any implementation claiming conformance to this spec MUST cover at least:

- historical `get_entity()` across archetype migration
- historical `get_entity()` after despawn
- historical `get_world_state()` after signature disappearance in the current
  world
- restart-safe reads after `discover_worlds()`
- concurrent step/read behavior for committed mode
- absence of visibility for partially written ticks
- `get_components()` unions across multiple archetypes for one snapshot
- explicit failure on unknown or uncommitted ticks

## Current Gaps

The current stack is not yet aligned with this spec:

- `QueryService` still behaves like a provisional service-layer facade rather
  than a durable snapshot reader.
- `AsyncQueryManager` is an internal archetype-table reader, not a complete
  public snapshot model.
- Existing store reads are still framed heavily around `run_id`, while public
  read identity should be `world_id + committed tick`.
- No durable snapshot manifest or entity locator exists yet.

That gap is expected. This document defines the intended engine contract for
closing it.
