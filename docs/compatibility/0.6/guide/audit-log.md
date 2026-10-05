# Audit log

**Document type:** Normative.
**Scope:** commands-owned `AccessSummary`, `AuditRow`, `AuditLog`,
transactional command-outbox projection, and `GetAuditHistory`.

## 1. Two evidence planes

The commands family exposes one append-only analytical table with two distinct
inputs:

1. Actor-aware dispatcher calls produce bounded advisory access evidence.
2. Durable command state transitions produce transactional outbox events.

The command ledger, tick manifest, artifact index, evaluation receipt, and
other family records remain authoritative for their outcomes. The analytical
audit table is a query projection; it never replaces those records.

Trusted `CommandDispatcher.apply` and `defer` calls do not fabricate an actor
or authorization row. Actor-aware `apply_as`, `defer_as`, batch, and spawn
entry points attempt bounded access evidence. `GetAuditHistory` is a registered
commands-owned, direct-only read that calls `AuditLog.query`.

## 2. Append-only invariant

`AuditLog` has no delete or drop operation. Destroying a world, cleaning up a
rollout fork, cancelling commands, or removing a live registry binding does not
delete analytical rows. Durable rows remain queryable after the live world is
gone.

Rows live in the dedicated Iceberg table `audit_rows`. They are not part of
the Lance world store and are not stored in the control catalog. A managed
deployment supplies the shared storage session and an explicit audit
`StorageConfig`; table isolation does not require a second mutable session.

## 3. Bounded access evidence

An `AccessSummary` contains:

- operation;
- actor identity;
- optional world identity;
- decision (`allowed` or `denied`);
- outcome (`succeeded`, `failed`, `denied`, `rejected`, or `queued`); and
- a small metadata mapping.

The canonical encoded summary is limited to 4096 bytes. The dispatcher accepts
only an allowlist of routing and count keys, bounded scalar strings, finite
numbers, booleans, and null. It does not copy component values, storage
configuration, credentials, callbacks, repository diffs, validator output,
critic findings, arbitrary results, or exception text.

The ordering for an actor-aware call is role preauthorization, exact
availability, bounded coordinate resolution, quota policy, primary effect,
then evidence. A role denial happens before world resolution. Direct-only or
trusted-only rejection happens before scheduler persistence.

Access evidence is advisory. Failure to summarize, encode, or append it cannot
replace the primary success or failure and cannot invite an unsafe retry of an
already-applied operation.

## 4. Bounded buffering and backpressure

`AuditLog.record` converts accepted access evidence into `AuditRow` values and
buffers at most one configured batch (128 rows by default). Reaching the
threshold, `flush`, `query`, and `shutdown` append the current batch through
`StorageService`.

If a full batch cannot flush, the log retries that same batch before accepting
another row. A repeated failure rejects the incoming row with
`AuditBackpressureError`, retains the original bounded batch, and increments
`rejected_rows`. It never hides an outage behind an unbounded process-memory
queue.

`AuditBackpressureError` implements the public `AvailabilityError` contract.
The dispatcher suppresses advisory recording failures after the primary
decision; a host that directly invokes the log may still classify the error.

Concurrent writers rely on Iceberg's optimistic append/retry behavior through
`StorageService`. They do not create process-local audit forks.

## 5. Transactional outbox projection

Durable admission, retry/dead-letter/rejection, cancellation, and applied
settlement append outbox events in the same control-authority transaction as
the state transition.

Projection:

1. reads available outbox events, optionally for one world;
2. flushes already-accepted access rows;
3. appends the event rows directly, without placing them in the bounded access
   buffer; and
4. acknowledges the source watermark only after the append succeeds.

A crash after append but before acknowledgement may replay an event.
`audit_id` is the outbox event identity and query-time deduplication produces
one analytical row. If append fails, the source remains authoritative and
retryable.

The composition root retains durable world-to-catalog coordinates. History
reads and shutdown can therefore project events for discovered or resumed
worlds even when the current process did not admit their commands.

## 6. Row schema

`AuditRow` is defined in `archetype.commands.models` with:

- `audit_id`
- `command_id`
- `world_id`
- `actor_id`
- `command_type`
- `status`
- `payload_json`
- `accepted_at`
- `applied_at`
- nullable `idempotency_key`

Application-scoped access rows have no fabricated world identity. A
world-filtered query therefore does not include an application-scoped
`create_world` decision.

## 7. Query contract

`AuditLog.query` is read-only and supports:

- `world_id`
- `actor_id`
- `idempotency_key`
- `status`
- `limit`

It projects available outbox rows, flushes access rows, reads lazily through
Daft, deduplicates by `audit_id`, applies predicates, and orders by
`(accepted_at, audit_id)`. A positive limit selects the newest rows before
restoring ascending order. Zero returns an empty frame; a negative limit is
invalid.

`tick_range` remains a validated compatibility input because existing runtime
and HTTP history methods expose it. It has no filtering effect until an
explicit row-schema migration adds a tick field.

Compaction and snapshot-retention policy are storage-maintenance concerns
outside this contract.

## Executable contracts

- `tests/commands/test_audit_projection_contracts.py`
- `tests/commands/test_scheduler_audit_contracts.py`
- `tests/commands/test_dispatch_policy_contracts.py`
- `tests/integration/test_fork_destroy_contracts.py`
