# Audit Log

**Document type:** Normative.
**Scope:** `iAuditLog`, `AuditRow`, and user-facing history reads through
`iRuntimeApplication` or `iCommandGateway`.

## 1. Purpose

The audit log is the append-only analytical record of gated operations and
projected command outcomes. It is distinct from the durable command ledger:

- `iCommandScheduler` admits, leases, dispatches, and settles tick-deferred commands.
- Its transactional outbox is authoritative for command state transitions.
- `iAuditLog` records access events and projects outbox events for history and accountability.

Trusted runtime history reads call `iRuntimeApplication.get_audit_history(...)`;
remote reads call `iCommandGateway.get_audit_history(...)`. Both delegate to
`iAuditLog.query(...)`; the runtime does not keep a separate history.

## 2. Append-only invariant

`iAuditLog` has no `drop_*` or `delete_*` methods. Implementations MUST NOT delete audit rows.

Destroying a world does not delete audit rows. Fork cleanup after a rollout does not delete audit rows. Audit rows remain queryable after the live world object has been removed from the registry.

## 3. Storage and batching

Audit rows live in a dedicated Iceberg table named `audit_rows`. They do not
share the Lance world-store lifecycle and they are not written into the SQLite
control manifest. The default is Archetype's concrete local SQLite-catalog
Iceberg lakehouse. A managed deployment injects its configured Daft `Session`
through `StorageService` and supplies audit data-plane credentials directly in
`StorageConfig.io_config`.

Because one injected session is bound to one URI and namespace, a managed host
passes that same storage identity as `ServiceContainer(audit_storage_config=...)`.
Audit isolation comes from the dedicated table, not by mutating the shared
session into a second namespace.

`record(...)` buffers at most one batch (128 rows by default). The batch is
appended when it reaches the threshold; `flush()`, `query()`, and `shutdown()`
also flush the current partial batch. One batch is one Iceberg append/snapshot,
not one snapshot per audit row. If a threshold flush fails, the log retries
that same bounded batch before accepting another row. If storage is still
unavailable, the incoming row is rejected with `AuditBackpressureError`, the
full batch remains intact, and `rejected_rows` increments. The log never grows
an unbounded process-memory queue to conceal a storage outage.

`AuditBackpressureError` implements the public `AvailabilityError` contract so
a host that directly exposes audit operations can classify it without importing
the concrete audit module. The built-in `CommandGateway` does not propagate
audit backpressure to REST callers: the gated operation has already applied by
the time audit emission runs, so returning 503 would invite an unsafe retry.

Concurrent processes append to the same table using Iceberg's optimistic
commit protocol. A conflicting append refreshes table metadata and retries
with bounded backoff; it does not create a process-local audit fork.

Access-audit emission remains advisory at the command gate: `CommandGateway._emit` logs an
audit failure at warning level and suppresses it so telemetry cannot
retroactively fail an applied operation. Artifacts and receipts remain the durable
evidence boundary. Deployments that require lossless audit must monitor
`rejected_rows` and warning logs and restore storage before accepting more
gated work; the built-in gate does not claim lossless delivery during an
outage.

## 4. Audit unit

Every `CommandGateway` call attempts exactly one access-audit emission. Trusted
local runtime calls do not fabricate an actor or authorization event. An accepted row is
persisted once; a row rejected by bounded-buffer backpressure is observable as
described above and is not represented as durable audit history.

Deferred command admission and outcome also create command-outbox events.
Those are projected independently and deduplicated by event identity; they do
not change the one-access-event-per-gated-call rule.

Projection discovers catalogs through the world family's retained durable
storage coordinates, not only through command activity in the current process.
Creating, forking, discovering, opening read-only, or resuming a world records
those coordinates. Consequently, a fresh process that discovers or opens a
world projects its pre-restart outbox on history reads and shutdown even when
that process has not admitted or drained a command for the world.

Multi-step gate methods still emit exactly one audit row:

- `destroy_world` records one row for the lifecycle cleanup.
- `run_rollout` records one row for the rollout call, not one row per fork.

The row payload captures sub-operation outcomes when one gated method performs multiple internal actions.

Spawn metadata is structured rather than encoded in the command name:

- `create_entities` records one `spawn_batch` row with
  `payload_json = {"count": N}`, not one row per entity.
- `spawn_with_reserved_id` records one `spawn_reserved` row with
  `payload_json = {"entity_id": N}`.

## 5. Row Schema

`AuditRow` is defined in `app/models.py`.

Required identity and operation fields:

- `audit_id`
- `command_id`
- `world_id`
- `actor_id`
- `command_type`
- `status`
- `payload_json`
- `accepted_at`
- `applied_at`

Nullable fields:

- `idempotency_key`

## 6. Query Contract

`iAuditLog.query(...)` is read-only and supports filters for:

- `world_id`
- `actor_id`
- `idempotency_key`
- `status`
- `limit`

External callers do not call `iAuditLog` directly. They use
`iCommandGateway.get_audit_history(ctx, ...)`, which applies the read
permission check before delegating.

`tick_range` remains an accepted compatibility parameter because the HTTP and
runtime history APIs already expose it, but it has no filtering effect because
`AuditRow` has no tick field. Adding real tick filtering requires an explicit
row-schema migration.

Queries flush first, apply predicates lazily in Daft, and return rows ordered by
`(accepted_at, audit_id)`. A limit selects the newest rows before restoring that
ascending order. Zero returns an empty frame and a negative limit is invalid.

Compaction and snapshot-retention policy are intentionally outside this
contract and belong in a separate storage-maintenance feature.
