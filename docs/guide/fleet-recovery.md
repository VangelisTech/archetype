# Fleet Recovery Catalog Authority

**Document type:** Normative.

**Scope:** Durable, storage-scoped scheduling authority for fleet recovery.
This document covers world discovery, recurring sweep rows, sparse exception
rows, catalog time, leases, fencing, and local/remote protocol parity. It does
not claim that a recurring recovery service or process host is deployed.

This contract is the catalog substrate for
[#503](https://github.com/VangelisTech/archetype/issues/503). Recovery service
composition, item handlers, retention effects, mission finalization, and
sandbox supervision remain later layers.

## 1. Recovery is scoped by storage identity

A recovery fleet operates against one explicit storage identity: the
normalized URI, namespace, and backend used by `StorageService` and the
corresponding control catalog. It does not search ambient configuration, live
world registries, every catalog on a host, or every tenant known to a process.

The durable world directory for that identity is the discovery authority.
Cold discovery needs no live world handle and no caller-supplied world ID.
`list_worlds_page` returns a stable, bounded page ordered by world ID, using an
exclusive world-ID cursor. Destroyed worlds remain visible because their
durable rows may still own unfinished finalization, outbox, or retention work.

Storage credentials and raw storage URIs are not recovery state. A later host
may retain a non-secret storage fingerprint, but that fingerprint cannot grant
access to a different storage identity.

## 2. Source records remain authoritative

Fleet recovery coordinates due work; it never replaces the record that
defines the work.

| Work | Authoritative source |
|---|---|
| Mission attempt recovery | Mission attempt claim and typed mission authority |
| Artifact publication and indexing | Artifact publication control row and index receipt |
| External-event delivery | The owning durable outbox or event row |
| Retention and deletion | Lifecycle metadata, holds, and tombstone receipt |

A sweep, cursor, lease, exception, pause, or dead-letter row is scheduling and
operator evidence only. It cannot declare a mission settled, an artifact
indexed, an event delivered, or bytes deleted. A handler must reacquire the
source-native row before any effect, and only that row's owning family may
commit its domain transition.

The catalog substrate therefore stores only bounded coordination values:
storage fingerprint, world ID, closed recovery kind, deterministic digest
keys, cursor, owner, lease, fence, cycle, portable counters, bounded error
code, caller-redacted bounded error detail, and catalog-derived times. A
dedicated scheduling field does not exist for prompts, model requests,
outcomes, raw exceptions, filesystem paths, object URLs, checkpoint URLs,
credentials, or provider responses. Callers MUST NOT tunnel those values
through error detail or another coordination field. A caller that supplies
error detail MUST apply the unified pre-durability redaction policy before the
catalog write. The catalog validates the field's type and bound, but it cannot
prove that free text is semantically redacted or free of prohibited content,
and it does not issue or verify a redaction receipt. Catalog acceptance is
therefore not evidence that scanning occurred. Unified scanning and receipt
enforcement remain the separate work tracked by
[#505](https://github.com/VangelisTech/archetype/issues/505); this catalog
slice does not implement them.

## 3. Durable scheduling records

Schema version 9 adds two derived ledgers:

- one recurring sweep row per storage fingerprint, world, and closed recovery
  kind; and
- sparse exception rows only for subjects that require delayed retry,
  dead-letter evidence, or redrive.

Sweep identity and exception identity are versioned SHA-256 digests over
their exact authority fields. Caller-provided keys must equal the catalog's
recomputed relationship; substitution fails before mutation. World discovery
uses a stable exclusive world-ID cursor and a bounded limit. Exception lists
are bounded and deterministically ordered by retry time and exception digest;
this catalog slice does not expose an exception-list pagination cursor.

The sweep graph is closed:

```text
absent + create          -> idle
idle/retry_wait + lease  -> leased
leased + take_over       -> leased
leased + renew           -> leased
leased + checkpoint      -> leased
leased + yield           -> idle
leased + fail            -> retry_wait
leased + exhaust/pause   -> paused
paused + redrive         -> idle
```

The sparse exception graph is also closed:

```text
absent/retry_wait + retry        -> retry_wait
absent/retry_wait + dead_letter  -> dead_letter
retry_wait/dead_letter + resolve -> resolved
dead_letter + redrive            -> retry_wait
```

Every insert and update asks the transition oracle for its edge. Unknown
states, unknown events, and absent edges fail closed before durable mutation.
The Cloudflare Worker mirrors the same graph, and executable parity tests bind
the TypeScript map to the Python authority.

## 4. Catalog clock, leases, and fencing

The control authority performing a mutation supplies the authoritative clock.
Workers submit bounded durations, never a client assertion of current time or
an absolute retry instant. SQLite uses the catalog host's clock; the remote
catalog uses the Worker's clock.

All recovery due, lease, update, expiry, retry, pause, and resolution times are
exact non-boolean integer milliseconds since the Unix epoch in the portable
JavaScript-safe range `0..2^53-1`. Fences, cycles, attempts, and other
cross-backend counters use the same ceiling. Floats, numeric strings,
booleans, negative values, mixed units, and overflow fail closed.

One claimant holds a live sweep lease. Lease acquisition and expired-lease
takeover atomically install the claimant and increment the monotonic fence.
Once leased, renewal, checkpoint, yield, failure, and pause are owner-bound:
they require the exact world, kind, claimant, fence, and an unexpired lease. A
live foreign lease is unavailable. An expired lease may be taken over only by
incrementing the fence; a stale claimant cannot renew, checkpoint, settle,
fail, or pause it.

Paused-sweep redrive is a separate operator-intended compare-and-set. It
requires the exact world and kind to remain paused at the supplied expected
fence, then clears the claimant and lease, returns the sweep to `idle`, and
increments the fence. It does not authenticate the former claimant;
authorization for invoking it belongs to the later operator workflow. A stale
expected fence cannot redrive the sweep.

A successful redrive also stores an internal lost-response receipt binding the
source fence and requested delay. An exact retry may return the already-durable
result only while that receipt still matches. A merely similar `idle` row—for
example, one produced by yield—cannot prove redrive. Every later sweep mutation
clears the receipt.

Checkpointing persists only a deterministic cursor and the active subject
digest. It does not make the sweep row the source authority. A crash may replay
from the predecessor cursor after lease expiry; the eventual handler must rely
on source-native fencing and idempotency.

Transient exception rows carry a catalog-derived `retry_at_ms`, bounded
attempt count, and closed error code. Dead-letter or pause affects only
scheduling. Exception redrive remains bound to the current live sweep's
claimant and fence and compare-and-sets the expected attempt count; it cannot
forge a source transition. Its internal lost-response receipt binds the
dead-letter attempt and requested delay; ordinary retry state cannot satisfy a
redrive replay, and the next exception mutation clears the receipt.

## 5. Local, remote, and Worker parity

`SqliteControlCatalog`, `RemoteControlCatalog`, and the control-catalog Worker
ship as one contract. The remote client probes the versioned
`fleet_recovery_v1` capability before using recovery routes and fails closed
against an older Worker. Request bodies and responses are exact and reject
unknown or lossy fields.

Parity covers schema migration, stable world paging, sweep and exception
state, catalog-clock decisions, concurrent acquisition, lease takeover,
portable counter bounds, cursor validation, and the closed transition maps.
No production layer may treat the SQLite implementation as having stronger
semantics than the Worker.

## 6. Current implementation boundary

This slice implements only catalog authority. It does not yet provide:

- the bounded `run_once` recovery service or capability-limited handler ports;
- artifact, mission-finalization, outbox, or retention adapters;
- recurring host cursor/fairness, deployment, or shutdown behavior;
- authorized operator inspection, pause, or redrive workflows;
- model-bearing sandbox supervision; or
- telemetry as workflow authority.

Those layers may consume this catalog contract but cannot broaden it. In
particular, generic maintenance must not receive a model endpoint, agent
subscription, Git-push credential, or general sandbox capability. Sandbox
resource recovery remains owned by `app.sandboxes`, while mission authority
decides whether workflow state may advance.
