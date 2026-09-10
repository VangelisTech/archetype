# Verified separate byte writing and Iceberg publication

The original September 5, 2026 local oracle passed. The unchanged Rust fixture
was rebuilt offline and rerun from the cleaned package before publication:
**PASS**. It wrote immutable Parquet before catalog publication; a fresh catalog
had no rows/files/snapshots until reconciliation. Two flushes produced exact
five-row history, stable pinned snapshots and v3 row allocations.

Lost acknowledgment after commit was adopted without duplicate publication;
failure before catalog commit published once after restart. A real SQLite CAS
race produced exactly one success and one CatalogCommitConflicts result;
reconciliation restored the exact union. Duplicate descriptors, changed identity
content and file hash mismatch were rejected.

[Sanitized acceptance](results/acceptance.json) retains rows, snapshots, fault
outcomes, allocations and raw receipt hash. Local paths and catalogs are omitted.
The adapter compiled using an existing locked target/dependency cache. Rust
formatting passed. No cloud store, paid provider or throughput benchmark ran.

This validates LocalFsStorageFactory and a cooperative local owner, not remote
object-store throughput, power-loss durability or production recovery. The
DDlog connected-components application is separately verified by the sibling
[recovery oracle](../ddlog-recovery/RESULTS.md). Full limits and dependency
qualification remain in [README.md](README.md).
