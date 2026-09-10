# Iceberg v3 immutable-flush round trip

An isolated executable oracle for `iceberg` and `iceberg-catalog-sql` **0.10.1**.
It writes local immutable Parquet, publishes its descriptors later through the
real SQLite SQL catalog, reads ordinary columns through Iceberg, and reconciles
the same logical publication after process restarts. It imports no Archetype
or DDlog code. See [RESULTS.md](RESULTS.md) for the actual run and limitations.

## Run

This directory is its own Cargo workspace and owns its lockfile and target.
The root workspace and its dependencies are unchanged. Rust 1.94 or newer
is required; the recorded run uses the already installed Rust 1.94.1.

```sh
cd spikes/iceberg-v3-roundtrip
mkdir -p run-data
python3 run_guarded.py --timeout 900 --log build.log -- cargo build --offline --locked -j 1
python3 run_guarded.py --timeout 120 --log run-data/oracle.log -- target/debug/iceberg-v3-roundtrip-spike oracle run-data/fresh-run
```

The oracle requires a nonexistent run
directory and exits nonzero on a failed assertion. The offline build requires
the locked crates to have been fetched. No new toolchain, cloud service,
provider credentials, Docker, DataFusion, or root workspace build is needed.
The guard stops only its own child process group below 900 MiB free or after
the timeout; it never deletes caches or other work. Debug information and
incremental compilation are disabled.

Inspect a retained table with:

```sh
target/debug/iceberg-v3-roundtrip-spike verify run-data/fresh-run/history
target/debug/iceberg-v3-roundtrip-spike reconcile run-data/fresh-run/history p1
```

`produce DIR p1|p2` and `reconcile DIR p1|p2 [before-commit|after-commit]` are
fixture commands for an already initialized table. Publication IDs and input
transactions are deliberately fixed. These commands are not general import
or ingestion APIs. `oracle` owns table initialization.

## Flush contract and receipt

The grouping prefix is `aabc_wfixture_r1` (archetype signature, world, run).
Each flush gets a distinct publication directory and final file:

```text
external/aabc_wfixture_r1/p1/part-00000.parquet
external/aabc_wfixture_r1/p2/part-00000.parquet
```

A flush can contain multiple completed source transactions. P1 contains
transactions 10 and 11, including an empty transaction; P2 contains transactions
12 and 13. The receipt preserves ordered transaction envelopes and the completed
half-open logical-time interval. Nonempty transactions are separate writer
batches within one flush file. Nothing requires publication once per tick.

The producer creates the publication directory exclusively, writes an
`.inprogress` file through the public Iceberg Parquet writer, closes it,
renames it once to the immutable final path, syncs that directory, verifies
the footer and exact rows, then persists the receipt. A retry reuses the
receipt and file; it never overwrites published bytes. An existing directory
without a receipt fails closed and needs investigation. Recovery from a crash
between directory creation and receipt insertion is intentionally not built.

The SQLite receipt binds table UUID, publication ID, schema/spec IDs,
representation and computation version, source transactions and interval,
complete file URI/size/row count/SHA-256 set, and public Avro-serialized
`DataFile` descriptors. Its SHA-256 covers that persisted manifest. The receipt
moves from `files_durable` to `iceberg_visible` only after a fresh catalog
reload finds exactly the matching committed snapshot and added file set.
The file-durable and catalog-visible boundaries are separate.

The producer and reconciler use a process-shared OS file lock. The fixture
supports one cooperative reconciler per table and retains every snapshot.
Logical publication identity is an application obligation: Iceberg does not
enforce uniqueness of the snapshot properties or deduplicate equal content
in different paths. Native duplicate-file checking is enabled, and application
preflight also rejects repeated paths inside a publication.

## Executable checks

The oracle starts fresh producer/reconciler processes and asserts:

1. A closed, readable external P1 file leaves the catalog pointer, planned
   files, rows, snapshots, and row-ID counter unchanged before publication.
2. P1 publishes two rows. Producing P2's three rows leaves P1 visible until a
   separate catalog commit. The latest table then has exactly five full rows,
   including differences `+2`, `-2`, and `-1`; order is ignored, multiplicity is
   preserved. Signed accumulation leaves only `(entity=42, value=4, count=1)`.
3. The pinned P1 snapshot still reads exactly its original two rows. V3
   snapshot/manifest lineage allocations exist, and `next-row-id` advances
   from 0 to 2 to 5.
4. A process exits immediately after P2's successful catalog call and before
   acknowledging the receipt. Restart adopts the authoritative snapshot.
   Repeating P1/P2 reconciliation creates no snapshot, file reference, row,
   or row-ID allocation. The original bytes remain hash-verified.
5. Duplicate file descriptors, mismatched hashes, and changed content under
   P1's identity are rejected. Exiting before the catalog
   call leaves the table unchanged; restart then publishes once.
6. Two independent SQL catalogs stage metadata against the same empty table.
   A test-only delegating storage barrier delays both metadata writes before
   they perform the real conditional SQL update. With library retries disabled,
   exactly one succeeds and one reports `CatalogCommitConflicts`. Fresh
   reconciliation publishes the loser, giving the exact five-row union.

The schema has eleven required flat fields with explicit Iceberg/Parquet IDs
1–11. Footer checks verify field ID, name, physical type, requiredness, count,
and exact decoded values. The five kernel-shaped columns are synthetic;
`tick` mirrors fixture logical time and `is_active` is true even for negative
change rows. Negative differences are ordinary history values, not Iceberg
delete files or a proposal to reinterpret kernel tombstones.

## Catalog acknowledgment and recovery limits

The pinned SQL catalog performs a real optimistic update conditioned on the
previous metadata location. However, its [`execute(None)` implementation](https://github.com/apache/iceberg-rust/blob/v0.10.1/crates/catalog/sql/src/catalog.rs#L379)
**discards the database COMMIT result**. An apparently successful catalog
return therefore cannot serve as durable acknowledgment.

The reconciler reloads through a fresh catalog after both `Ok` and `Err`,
searches committed snapshot history for the publication ID and digest, verifies
the exact added file set and pinned rows, and only then updates the receipt.
If verification fails, it returns an error and keeps the receipt pending.
The lost-ack test exercises process death after a successful commit, not an
injected SQLite COMMIT failure. This workaround does not repair the upstream
catalog or establish its production acknowledgment contract.

The process lock and retained authoritative history are assumptions. Arbitrary
writers can violate publication-ID uniqueness; expired snapshots can defeat
reconciliation; a partitioned multi-host deployment needs separate fencing,
retention, in-flight commit, and ledger ownership decisions. There is no
multiple-table transaction or cross-table visibility guarantee.

## Dependency and durability scope

The [upstream tag](https://github.com/apache/iceberg-rust/tree/v0.10.1) uses
Arrow/Parquet 58 and Rust 1.94. This lock pins Arrow Array/Schema 58.3.0,
Parquet 58.1.0, SQLx 0.8.6, and Tokio 1.52.1. The consumer explicitly enables
SQLx `any`, `sqlite`, and `runtime-tokio`; the catalog crate's normal features
alone do not enable the SQLite driver. Built-in `LocalFsStorageFactory` avoids
an object-store connector. The writer's public `DataFileBuilder` output and
public Avro descriptor serializer avoid private existing-Parquet import APIs.

**This dependency set does not satisfy the separate Rust kernel experiment's Arrow/Parquet 59 security floor.** It resolves vulnerable `thrift` 0.17.0; Apache identifies
versions before 0.23.0 as affected by excessive allocation on malformed
messages ([CVE-2026-43868 announcement](https://www.openwall.com/lists/oss-security/2026/05/05/2)).
Only the fixture's own locally generated Parquet is read. The run does not
establish safety for external files or waive the production policy.
No `cargo-audit` or `cargo-deny` tool was available; lock inspection is not a
complete dependency audit. Root Arrow 59 was not downgraded.

V3 ordinary-column scans and row-lineage metadata are the acceptance surface.
Virtual `_row_id` and `_last_updated_sequence_number` projection, deletion
vectors, broad v3 types, partitioned imports, and Arrow59-produced file
interoperability are not tested. The inspected 0.10.1 reader does not implement
the required virtual lineage materialization.

Local file writer close syncs data; the library's metadata byte-write path
does not explicitly fsync. SQLite and the receipt are independent commits.
This is a single-host process-crash fixture, with no power-loss durability
certification, cloud-store behavior, orphan cleanup, snapshot expiration,
or completely empty flush publication.

## Relationship to DDlog and Archetype

This fixture tests writing bytes before catalog publication. `produce` invokes
`ParquetWriterBuilder` over `table.file_io().new_output`, closes immutable bytes,
and persists the resulting `DataFile` descriptors with `write_data_files_to_avro`.
`reconcile` later calls `Transaction::fast_append().add_data_files().commit()`.
Thus a catalog snapshot is not required for every source transaction or byte
write. Producing files does not make them visible to Iceberg readers.

This proves the separation with **local FileIO**, not remote object-store
throughput or cloud durability. Production throughput was not benchmarked.
The sibling [DDlog recovery oracle](../ddlog-recovery/README.md) applies this
mechanism to a real DDlog input checkpoint and clean restart. Neither fixture
changes the supported Archetype storage path or standalone DDlog runtime.
Historical Rust kernel experiments used mutable per-world/run Parquet objects;
those cannot be registered directly as immutable Iceberg references. A future
integration must settle immutable exports, row meaning, dependency versions,
catalog acknowledgment and reconciliation ownership explicitly.
