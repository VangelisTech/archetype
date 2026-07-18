# Durable commands

**Document type:** Normative.
**Scope:** `Command`, `iCommandScheduler`, durable admission, dispatch,
settlement, retry, dead-letter, and outbox behavior.

The commands family owns tick-deferred work. `CommandGateway` authorizes
untrusted admission; `RuntimeApplication` admits trusted local work. Neither
object owns queue state.

```text
trusted RuntimeApplication.submit(...)
                  or
CommandGateway.submit(ActorCtx, ...)
                  |
                  v
       iCommandScheduler.admit
                  |
        durable command ledger
                  |
                  v
SimulationService tick-boundary callback
                  |
                  v
 iCommandScheduler.drain_and_apply
                  |
           MutationService
                  |
                  v
 tick manifest + command settlement + outbox
```

## Admission and identity

Admission persists a `PENDING` row before returning a command ID. A batch is
all-or-nothing. Each command records:

- caller-supplied command ID and schema version;
- target world and scheduled tick;
- priority and a catalog-assigned monotonic sequence;
- canonical portable payload plus its digest;
- immutable principal snapshot, when admission came through the gateway;
- origin (`local` for trusted scripting); and
- attempt budget and any reserved entity ID.

Replaying one command ID with identical immutable content returns the existing
identity. Reusing the ID for different content rejects the whole admission.
Live Python capabilities are not a durable payload format.

Component values and component-type references use a portable wire identity:
the component class name plus a fingerprint of its canonical prefixed schema.
Loaded definitions with the same name and schema are interchangeable; a
same-named definition with a different schema is never guessed. If dispatch
cannot resolve the recorded identity, the command is permanently rejected.

## Ordering and leasing

Due work is leased in `(scheduled_tick, priority, sequence)` order. A lease
does not remove the row. Its owner and expiry make process failure recoverable;
the same owner or a later owner after expiry can resume it.

The local reference implementation is the per-storage SQLite control catalog.
The remote implementation is the per-world Cloudflare Durable Object. Both
implement the same command state machine and ordering contract. Admission and
leasing fail closed unless the catalog records the world as active.

## Dispatch and settlement

The dispatcher has an explicit arm for every admitted deferred `CommandType`.
Entity/component mutations stage through `MutationService`. Processor changes
are direct gated operations and the generic deferred submission surface rejects
them before admission; a future portable processor registry may add a versioned
deferred dispatcher without serializing live Python. Message, custom, and query
envelopes currently have explicit no-op dispositions; application extensions
must replace that disposition with a versioned portable handler rather than
deserialize live code.

A successfully dispatched command is staged on the world's commit coordinator.
It becomes `APPLIED` only in the transaction that publishes the tick manifest
making its rows visible. A crash before that transaction leaves the command
lease recoverable and the failed tick retryable.

## Failure policy

- Payload/lookup/type/value failures are permanent: mark the command
  `REJECTED`, append its outbox event, and continue with the batch.
- Availability failures are `RETRYABLE`: preserve the command, release the
  unprocessed leased tail without charging attempts, and stop the drain.
- A transient command that exhausts its attempt budget becomes `DEAD_LETTER`;
  later independent commands may continue.
- Process-fatal `BaseException` values are never converted into product state.

At the per-world command authority, transitioning a world out of `active` and
terminally rejecting its unsettled commands is one transaction. The remote
directory status follows as a discovery index. This closes
admission/cancellation races across hosts; destroying an already-absent world
remains idempotent.

## Audit projection

Admission, failure, cancellation, and applied settlement append authoritative
outbox events in the same control-authority transaction as their state change.
`AuditLog` projects those events to analytical Iceberg storage and acknowledges
the projection watermark. The projection can lag; it is not the authority for
command outcome.

User-facing history comes through `RuntimeApplication` or `CommandGateway`,
not from an in-memory broker history.

## Executable contracts

- `tests/app/test_durable_commands.py`
- `tests/integration/test_command_flow.py`
- `evals/suites/idempotency.py`
- `evals/suites/idempotency_process.py`
