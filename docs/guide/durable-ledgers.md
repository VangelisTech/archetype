# Durable Ledgers

**Document type:** Normative.
**Scope:** durable references, the local control catalog, and A1 read behavior.

## 1. What A1 guarantees

A durable ledger is an immutable reference to one published manifest generation. A caller can create
an empty ledger, lose the returned reference, restart in another process, and recover the exact head
from its `LedgerIdentity`.

A1 guarantees:

- strict, frozen `StorageRef`, component, signature, manifest, and ledger models;
- deterministic `archetype-jcs-v1` SHA-256 identities;
- a credential-free local storage identity;
- cross-process unique immutable records and compare-and-swap through SQLite;
- generation-zero create, head lookup, list, and exact manifest lookup;
- trusted component resolution without importing persisted module paths; and
- physical LanceDB reads that never create a missing table.

A1 does **not** make rows written by the current world step path transactionally visible. A nonempty
pinned query requires A2's commit-tagged batches, manifest publication, crash reconciliation, and
writer fencing. `AsyncWorld.step`, mutation, simulation, AutoResearch, and `runtime.attach()` keep
their existing behavior in A1.

## 2. Storage and control plane

The supported A1 profile is a local LanceDB root. Equivalent relative paths, absolute paths, and
`file://` URIs normalize to one absolute `file://` `StorageRef`. Credentials, `IOConfig`, and cache
settings are never serialized into it.

The immutable component rows remain in LanceDB. Linearizable control records live in:

```text
<absolute-lancedb-root>/<namespace>/.archetype/catalog-v1.sqlite3
```

SQLite uses a composite primary key, `BEGIN IMMEDIATE` for serialized writes, a bounded busy timeout,
foreign keys, and `synchronous=FULL`. The default WAL profile is tested across independent processes
on one host and a local filesystem. It makes no network-filesystem or multi-host claim.

Iceberg and remote LanceDB configurations raise `UnsupportedAtomicInsertError` for ledger catalog
operations until an equivalent shared CAS implementation is supplied and tested.

## 3. Generation zero

`LedgerService.create_ledger()` atomically creates or replays a generation-zero manifest:

- `generation == 0` and no previous manifest;
- `committed_through_tick is None` and `next_tick == 0`;
- `next_entity_id == 1`;
- no signatures, entity directory, lineage, or batches; and
- `writer_epoch == 0`.

The manifest proves that the ledger's committed row set is empty. It does not imply that a mutable
world exists. Creating or reading a ledger never registers a world, allocates an entity, advances a
tick, or acquires writer ownership.

Repeated creation with the same storage/world/run/name returns the original reference. Reusing the
same identity with different content raises `ManifestConflictError`. `get_head()` recovers the latest
reference even when the caller lost its prior response.

## 4. Public use

Runtime operations require explicit storage and pass through `iCommandService`:

```python
from archetype import ArchetypeRuntime, StorageConfig

storage = StorageConfig(uri="./data", namespace="experiments")

async with ArchetypeRuntime() as runtime:
    ref = await runtime.create_ledger(
        "search-lab",
        storage=storage,
        world_id="lab-world",
        run_id="run-001",
    )
    latest = await runtime.get_ledger_head(ref.identity, storage=storage)
    manifest = await runtime.get_ledger_manifest(latest, storage=storage)
    ledgers = await runtime.list_ledgers(storage=storage, name="search-lab")
```

`CREATE_LEDGER` requires operator or admin. Head, list, and manifest reads are available to every
role. Async and sync runtime surfaces are identical.

The lower-level `LedgerService` also requires a caller-supplied `StorageConfig` for every operation.
It never falls back to `StorageConfig()` because doing so could open a different catalog.

## 5. Component trust and reads

Persisted Python paths are data, never import instructions. Applications register already imported
component types in a `ComponentRegistry`; the registry verifies stable component ID, Arrow schema
digest, signature digest, and the existing physical `Archetype.get_name()` table ID.

`QueryService.query_ledger()` can return a correctly typed empty frame for generation zero without
opening a component table. It fails with `LedgerMetadataUnavailableError` for nonempty pinned reads
until A2 lands. Existing world/run query overloads remain compatibility diagnostics and are not an
immutable visibility boundary.

The LanceDB read-existing capability opens a durable physical table ID only after confirming it
exists. Missing-table reads fail and never call the legacy create-on-read path.

## 6. Integrity failures

Storage mismatch, unknown components, schema conflict, malformed control records, and manifest/ref
disagreement fail with typed errors. None are reinterpreted as an empty ledger. A legacy store with
no durable manifest has no `LedgerRef`; exact diagnostic reads remain available, but it cannot claim
pinned visibility or mutable resume.

## 7. Next slice

A2 computes a complete tick before writing, appends immutable commit-tagged batches, publishes the
manifest head last through SQLite CAS, and then clears mutation caches. It adds crash reconciliation,
writer fencing, cold mutable resume, real nonempty pinned queries, and AutoResearch
`lab_ledger_ref`.
