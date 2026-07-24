# Durable commands

**Document type:** Normative.
**Scope:** `OperationRegistry`, `CommandDispatcher`, `DurableOptions`,
`CommandScheduler`, durable admission, materialization, settlement, retry,
dead letter, cancellation, and outbox behavior.

The top-level `archetype.commands` family owns governed entry and durable
work. Operation meaning remains with the family that defines the exact
Pydantic model and handler. The scheduler owns durability metadata and never
implements world behavior by switching over a legacy command enum.

```text
trusted caller                         authenticated caller
dispatcher.defer(...)                 dispatcher.defer_as(ActorCtx, ...)
dispatcher.defer_spawn(...)           dispatcher.defer_spawn_as(ActorCtx, ...)
              \                         /
               exact OperationRegistry
                         |
                 CommandScheduler.admit
                         |
                durable command ledger
                         |
         lock-held world tick materializer
                         |
             family durable materializer
                         |
          tick manifest + command settlement
                         |
              transactional outbox
```

## Exact registration and eligibility

Every governed operation has one `OperationSpec`, keyed by its exact model
type and exact discriminator name. Registration contains:

- the family-owned Pydantic model and direct family handler;
- permission, quota scope, and bounded access summarizer;
- trusted and untrusted availability;
- a world-key extractor when the operation is world scoped; and
- optional `DurableOperation` decode/materialize functions.

There is no MRO fallback. Duplicate names or model types fail registration.
The durable decoder must return the registered exact model and round-trip its
canonical JSON byte-for-byte.

Only portable tick mutations are durable:

- `Spawn`
- `SpawnReserved`
- `Despawn`
- `Update`
- `AddComponents`
- `RemoveComponents`

Lifecycle, simulation, query, processor, hook, resource, and live-capability
operations are direct-only. An unregistered model or a direct-only model fails
before catalog persistence. Legacy `MESSAGE`, `CUSTOM`, and `QUERY_WORLD`
envelopes are not durable no-ops.

`SpawnReserved` is an internal exact operation. Actor-aware callers submit
`Spawn`; `defer_spawn_as` authorizes first, and the scheduler owns the
reservation and durable `SpawnReserved` admission. A trusted caller may admit
an already-reserved `SpawnReserved` only when it already owns that reservation.

## Admission and identity

`DurableOptions` owns target tick, priority, and maximum attempts. Those fields
do not belong on family operation models.

Admission canonicalizes and validates the complete operation before any
catalog side effect. A persisted row contains:

- caller-supplied or generated command ID and positive schema version;
- target world and non-negative scheduled tick;
- priority and catalog-assigned monotonic sequence;
- exact operation name and canonical payload JSON;
- an immutable digest over every identity-bearing field;
- principal snapshot and origin (`gateway` or `local`);
- attempt budget; and
- reserved entity ID when applicable.

A non-empty batch is all-or-nothing and cannot span worlds. Replaying one
command ID with identical immutable content returns the existing identity.
Reusing it with different content raises a conflict, including after a failed
first persistence attempt. Live Python capabilities, non-canonical values, and
operation/discriminator mismatches fail before admission.

Component values and component-type references use the component name plus
the fingerprint of the canonical prefixed schema. Schema-identical
definitions remain portable across module moves; same-named definitions with
different schemas are never guessed.

## Ordering and leasing

Due rows are leased in `(scheduled_tick, priority, sequence)` order. Leasing
does not delete a row. The stable scheduler owner and expiry make a crashed
lease recoverable; another owner may continue only after takeover is allowed.

The local control authority is the per-storage SQLite catalog. The remote
authority is the per-world Durable Object. Both implement the same admission,
lease, retry, cancellation, outbox, and settlement state machine. Admission
fails closed unless the durable world is active.

## Materialization and settlement

`AsyncWorld` calls `CommandScheduler.materialize(actual_world, target_tick)`
while the exact world operation lock is already held. The scheduler must not
look the world up again. It verifies the stored world, name, version, digest,
payload, exact model, and reserved identity before calling the registered
durable materializer with that same world.

Materialization happens before `PreTick` and active-signature discovery. A due
spawn can therefore introduce a signature in its scheduled tick.

A successful mutation is staged on the world's commit coordinator. It becomes
`APPLIED` only in the same control transaction that publishes the tick
manifest. A failure before publication leaves the tick retryable. A retry sees
already-staged command IDs and does not duplicate their mutations. The
committed-tick receipt reports the number applied; the catalog remains the
authority for exact command identities and outcomes.

## Failure policy

- `LookupError`, `TypeError`, and `ValueError` are permanent command failures:
  mark `REJECTED`, append the outbox transition, and continue.
- Other ordinary exceptions are `RETRYABLE` until the attempt budget is
  exhausted.
- An exhausted retryable command becomes `DEAD_LETTER`; later independent
  commands may continue.
- On a retryable failure, release the unprocessed leased tail without charging
  attempts and stop that drain.
- Process-fatal `BaseException` values are never translated into product
  state.

Destroy coordinates close admission, terminally cancels unsettled commands,
and then closes the world. An admission racing destroy either commits before
cancellation or observes the closing world and fails; it cannot appear after
the cancellation transaction.

## Audit projection and history

Admission, failure, cancellation, and applied settlement append outbox events
in the same control transaction as the state change. `AuditLog` projects those
events into append-only Iceberg storage and acknowledges them only after the
append succeeds. Projection may lag and replay; event identity deduplicates the
analytical view. The command ledger and manifest remain authoritative.

`CommandScheduler.history` decodes durable records through their exact
registrations. `GetAuditHistory` is a separate commands-owned, direct-only
operation over the analytical projection.

The compatibility `app.models.Command` envelope is a finite state-free
translator for existing API callers. It recognizes only supported portable
mutations and immediately constructs the canonical family model plus
`DurableOptions`; it owns no dispatch, policy, queue, or replay behavior.

## Executable contracts

- `tests/commands/test_registry_contracts.py`
- `tests/commands/test_scheduler_audit_contracts.py`
- `tests/commands/test_durable_runtime_contracts.py`
- `tests/commands/test_integration_contracts.py`
- `tests/integration/test_command_flow.py`
- `evals/suites/idempotency/tasks.py`
