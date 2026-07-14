# Durable Discovery

**Document type:** Normative.
**Scope:** The control catalog, `discover_worlds`, `open_world_readonly`, cold subset queries. Issue #272 (v0.3.0 slice A1-read).

## 1. The problem this solves

Every store keeps a process-local registry of the worlds and archetype
signatures it has seen. That registry dies with the process. The data does
not: archetype tables are append-only and survive on disk or in object
storage. Before durable discovery, a fresh process pointed at existing
storage could read tables only if it already knew — from Python code — which
worlds and component classes had been written there.

Durable discovery closes that gap. A world written by one process is
discoverable and queryable from any later process pointed at the same
storage identity, with no shared memory and no live world object.

## 2. The control catalog

Each storage identity gets one SQLite control catalog. The catalog is
**authoritative for world identity** and **advisory for progress**:

| Recorded | Authority |
|---|---|
| World identity (`world_id`, `name`, `run_id`, `parent_world_id`) | Authoritative. Registration failure fails `create_world`/`fork_world`. |
| World status (`active`, `destroyed`) | Authoritative for catalog state; destroyed worlds stay discoverable (their rows are still queryable; append-only). |
| Tick head | Advisory until A2 manifests land. Post-step updates log loudly on failure but never fail a tick whose data-plane writes succeeded. |
| Archetype signatures (component names, schema descriptor, fingerprint) | Authoritative descriptor for cold reads; guarded by fingerprint check (section 5). |

### Catalog location is a pure function of the storage URI

The same `StorageConfig` always resolves to the same catalog file, so
restarts and crashes converge on one catalog with no coordination:

- Local URIs: `<uri>/<namespace>/.archetype-catalog-<backend>.db`, beside
  the data it describes.
- Remote URIs (e.g. `s3://`): `~/.archetype/catalogs/<fingerprint>.db`,
  where the fingerprint hashes the normalized storage identity. Override the
  directory with `ARCHETYPE_CATALOG_DIR`.

The storage identity is uri + namespace + backend — the same key
`StorageService` pools stores by. Two configs that resolve to different
stores (LanceDB vs Iceberg on the same uri and namespace) never share a
catalog, so one backend cannot discover descriptors whose rows live in the
other.

The catalog opens with WAL journaling, `synchronous=FULL`, a busy timeout,
and `BEGIN IMMEDIATE` transactions. Concurrent processes racing to register
the same world converge on exactly one row: identical re-registration is a
no-op; a differing record for the same `world_id` raises
`CatalogConflictError`.

## 3. Gated discovery API

Two read-gated operations on `iCommandService`:

```python
async def discover_worlds(
    ctx: ActorCtx, storage_config: StorageConfig
) -> list[WorldInfo]: ...

async def open_world_readonly(
    ctx: ActorCtx, storage_config: StorageConfig, world_id: str | UUID
) -> WorldInfo: ...
```

- `discover_worlds` is gated as `LIST_WORLDS`. Unlike `list_worlds` (live
  process registry), it answers from the catalog: a fresh process sees every
  world ever registered against that storage identity, including destroyed
  ones.
- `open_world_readonly` is gated as `GET_WORLD_INFO`. It returns the world's
  durable descriptor as the existing `WorldInfo` boundary type and raises
  `KeyError` for unrecorded worlds. It never constructs a live mutable
  world — fenced mutable resume is a separate, A2-gated capability.

Both operations respect the info-class downgrade: callers get `WorldInfo`,
never a world handle.

## 4. Cold subset queries

`query_components` unions two sources:

1. The live querier path (unchanged): signatures registered in this process.
2. Catalog-discovered tables: signature records whose component sets cover
   the request and whose tables are not already live.

Catalog tables are read through a dedicated store seam —
`get_existing_table_schema` / `get_existing_table_df` — that **opens and
never creates**. Read paths cannot allocate tables, on either backend
(LanceDB, Iceberg). Projection uses the durable schema descriptor, so a cold
process needs Python classes only for the components it is asking about, not
for every component the writing process defined.

```python
# Process B, sharing nothing with the writer but the storage config:
infos = await gate.discover_worlds(ctx, storage_config)
df = await runtime.query_components(
    [Score], world_id=infos[0].world_id, run_id=infos[0].run_id,
    storage_config=storage_config,
)
```

## 5. Fail-closed schema check

Before reading a catalog-discovered table, the physical table schema is
fingerprinted and compared to the catalog record. A mismatch raises
`CatalogSchemaMismatchError` — the read fails closed rather than returning
rows whose shape the descriptor no longer describes.

The fingerprint hashes the schema's *logical* shape: field names and
normalized logical types, in order. It deliberately excludes nullability and
physical encoding variants (`large_string` vs `string`), because backends
legitimately normalize those on round-trip — Iceberg stores `string` as
`large_string` and forces fields nullable. Renames, reorders, added or
removed columns, and retypes all mismatch.

Schemas never evolve in place: a changed component set is a new archetype
signature and a new table. The stored descriptor for an existing table is
therefore immutable, and a fingerprint mismatch means corruption or
out-of-band interference, not drift.

## 6. What is and is not guaranteed (A1-read)

Guaranteed:

- Worlds registered at create/fork time are discoverable from any process,
  for the life of the storage, including after `destroy_world`.
- Exactly one catalog row per world identity under concurrent registration.
- Cold reads never create tables and fail closed on descriptor mismatch.
- Catalog unavailability degrades reads to live-registry behavior (logged);
  it never corrupts the data plane.

Not guaranteed until A2 (issue #273):

- The catalog tick head is advisory. The durable, atomic tick-visibility
  boundary (commit tokens, writer epochs) is A2's contract.
- No fenced mutable resume of a discovered world. Opens are read-only.
- Crashed physical executions are queryable, not resumable.
