# Fleet Recovery

**Document type:** Normative.

**Scope:** Storage-scoped recovery discovery, bounded recurring passes,
leases and fencing, sparse retry state, operator inspection, retention, and
the separation between maintenance work and model-bearing sandbox recovery.

This specification defines the target contract for
[#503](https://github.com/VangelisTech/archetype/issues/503). It does not make
telemetry authoritative, grant permission to repeat a model call, or define a
durable external event bus.

## 1. Recovery is scoped by storage identity

A recovery fleet operates against one explicit storage identity: the
normalized URI, namespace, and backend used by `StorageService` and the
corresponding control catalog. It does not search ambient configuration, live
world registries, every catalog on a host, or every tenant known to a process.

A cold worker receives a `StorageConfig` or an operator-managed profile that
resolves to exactly one storage identity. It needs no world ID and constructs
no live world merely to discover due work. A deployment serving multiple
storage identities schedules an independent bounded pass for each configured
identity. The storage profile and its non-secret digest may be recorded for
inspection; credentials and the raw storage URI are not recovery state.

The durable world directory for that identity is the discovery authority.
World discovery MUST be paginated and stable, including across process
restart. Destroyed worlds remain eligible when their durable rows still own
unfinished finalization, outbox, or retention work.

## 2. Source records remain authoritative

Fleet recovery coordinates work; it does not replace the records that define
the work.

| Work | Authoritative source |
|---|---|
| Mission attempt recovery | Mission attempt claim and typed transition state |
| Artifact publication and indexing | Artifact publication control row and artifact index |
| External-event delivery | The owning durable outbox or event row |
| Retention and deletion | Artifact/checkpoint lifecycle metadata, holds, and tombstone receipt |

A recovery pass, cursor, lease, exception, or dead-letter row is scheduling
and operator evidence only. It MUST NOT declare a mission settled, an artifact
indexed, an event delivered, or bytes deleted. The selected handler MUST
reread and CAS-acquire the source-native record before performing an effect,
and only that record's owning family may commit its transition.

The baseline queue is derived, not copied. A recurring pass scans bounded
pages of worlds and work kinds for source rows that are due. Recovery persists
sparse coordination rows only while an item has an active lease, a delayed
retry, a pause/operator-intervention requirement, or a dead-letter state. A
converged item settles or removes that sparse row while retaining the terminal
source record and its receipts.

This design avoids a second universal queue whose contents could drift from
the artifact, mission, outbox, or retention authorities.

## 3. Bounded recurring pass

The core operation is one `run_once`-style pass. It MUST be bounded by all of:

- storage identity;
- work kind;
- world pages and items examined;
- items claimed;
- elapsed time; and
- worker concurrency.

The current provider-neutral service dispatches sequentially, so one
invocation's worker concurrency is exactly one. Multiple hosts may invoke it
concurrently; the catalog lease/fence prevents duplicate authority, while
endpoint- and tenant-level admission remains a separate scheduler concern.

The pass does not hide a forever loop inside an API request, runtime handle, or
application service. A process host such as cron, a worker scheduler, systemd,
or Modal invokes bounded passes repeatedly. The provider-neutral recovery
family imports none of those hosts.

Worlds and work kinds are independent lanes. A slow or poisoned item in one
lane MUST NOT abort its page, block unrelated worlds, or require a fleet-wide
lock. The pass records a bounded item failure, schedules its next action, and
continues while budget remains.

Each pass has at most one crash-local **active subject** per worker slot. A
subject names the exact storage identity, world, kind, and source item being
handled. A crash abandons only that subject's lease; completed pages and other
workers' progress remain durable. A cursor advances only after the item or
page has reached a durable scheduling outcome. On restart, the active subject
is rediscovered from its source row or sparse lease and retried after expiry.
The subject key is a versioned deterministic digest of its kind, world, and
source-authority key; model validation rejects any other relationship. The
sweep therefore needs to checkpoint only the source-authority key, while a
resolver cannot substitute a different logical subject during takeover.
Resolution MUST return the same scheduled subject and source-authority keys;
substitution fails the sweep as source corruption before dispatch.
Resolution reconstructs authority only and never advances the discovery
cursor. After a crash, re-scanning from the predecessor is safe because the
source transition or sparse exception is already durable and idempotent; this
prevents changed resolver code from skipping an unseen page tail.

Pagination uses a deterministic exclusive SHA-256 cursor over stable durable
keys. Every discovered subject carries its own non-empty cursor, subject
cursors increase strictly from the requested cursor, and a non-exhausted
page's continuation cursor equals its final subject cursor. Per-subject
cursors let a deadline checkpoint a partially handled page without skipping
its tail. Sources that over-deliver the requested limit, repeat or regress a
cursor, or return an inconsistent continuation fail the sweep as
`source_corrupt` before dispatch. Cursor persistence is an optimization:
restarting an incomplete sweep from an earlier page is correct because source
acquisition and effects are fenced and idempotent.

## 4. Clock, leases, fencing, and backoff

The control authority performing a lease mutation supplies the authoritative
clock. Workers submit bounded durations, not a client-derived assertion of
the current time. Local catalogs use the catalog host's clock; remote catalogs
use the server clock.

Every time in the fleet sweep and sparse-exception ledger, including due,
lease, update, expiry, and tombstone times, is an exact non-boolean integer
number of milliseconds since the Unix epoch in the portable JavaScript-safe
range `0..2^53-1`. Fences, cycles, attempts, and other cross-backend counters
use that same ceiling. Floats, numeric strings, booleans, and mixed time units
fail closed. An owning source family may retain an older versioned time
representation—artifact publication leases are seconds while retry deadlines
are integer milliseconds—but its catalog authority still derives every due or
lease mutation from its own clock and validates that representation exactly.

One live owner holds a lease and monotonically increasing fence for an active
subject. Renewal and completion compare the owner and fence. An expired or
superseded worker MUST NOT mutate scheduling state or invoke a source handler.
Before an external effect, the handler also acquires or authenticates the
source family's own lease/fence and current phase. A recovery lease never
upgrades scheduling ownership into domain authority.

Transient failures use bounded exponential backoff with deterministic jitter
derived from a non-secret stable item identity and attempt number. The control
authority computes `next_attempt_at_ms` from its own `now_ms`. Policy defines
the initial delay, maximum delay, and attempt ceiling. A maximum recovery-age
policy is not implemented in this slice and is listed explicitly below rather
than inferred from retry count.
Exhaustion moves only the sparse scheduling row to dead letter; it does not
forge a terminal source transition or release a retention hold.

Operator retry of a paused or dead-letter item is a new fenced scheduling
attempt against the same authoritative source. It does not reset source
identity or permit an otherwise forbidden effect.

The catalog substrate has fenced redrive primitives, but this slice does not
yet expose them through a typed, authorized operator workflow. Direct catalog
access is internal implementation machinery, not the supported operator API.

## 5. Recovery lanes and capabilities

Discovery classifies due rows into explicit handler requirements. Capability
separation is part of correctness, not merely deployment hardening.

| Lane | Permitted capabilities | Forbidden capabilities |
|---|---|---|
| Artifact finalization | Control catalog, object store/index, and checkpoint read/restore solely for already-bound file extraction | Model endpoint, agent subscription, Git push, or repository-edit credentials |
| Durable outbox projection | Owning outbox and a policy-approved delivery adapter | Model, sandbox execution, or artifact mutation outside the outbox contract |
| Retention | Lifecycle/hold reads and narrowly scoped object or checkpoint deletion | Model, Git, agent subscription, and publication authority |
| Sandbox/model supervision | Only a separate supervisor implementing the mission recovery action | Generic maintenance dispatch |

Maintenance workers MUST NOT receive model-provider endpoint credentials,
Codex or Claude subscription material, repository push credentials, or a
general-purpose sandbox execution capability. A provider credential used only
to read an already-bound checkpoint is distinct from the credentials that can
run an agent or submit inference.

The maintenance composition fails closed on capability shape as well as the
declared lane: a source or handler that exposes the model-recovery method is
rejected even if it labels itself as artifact, outbox, or retention work.

Mission claim state determines the recovery classification:

- `finalizing` may enter the artifact-maintenance lane and may publish only
  the exact persisted artifact projection;
- `possibly_submitted` and `provider_acknowledged` require provider-aware
  reconciliation by the checkpoint-aware sandbox supervisor in
  [#504](https://github.com/VangelisTech/archetype/issues/504);
- a terminal `settled` claim authorizes no provider or publication work; and
- metadata or an apparently idempotent provider API never authorizes generic
  recovery to repeat a model call.

The #503 service may discover and report model-bearing work as deferred or
paused, but it MUST NOT dispatch it until a #504 implementation supplies the
typed supervisor capability. No model endpoint or model credential is
required to implement, test, or operate the maintenance lanes or the first
artifact vertical slice.

## 6. First vertical slice: one artifact publication

The first executable vertical slice is item-scoped artifact recovery, not a
fleet-wide model supervisor. For one exact publication item, the handler:

1. discovers only a publication digest from a server-clock due page;
2. acquires the current recovery fence and the exact source-native publication
   lease without copying `request_json` into fleet scheduling state;
3. rereads the durable artifact source row and authenticates its request and
   storage coordinates;
4. reauthorizes the same digest and claimant immediately before external I/O,
   so a stalled worker cannot cross lease takeover or retry-window expiry;
5. resumes `PENDING` from the exact durable request, using only an already
   bound checkpoint when source material is still required;
6. resumes `UPLOADED` by verifying and indexing deterministic rows without a
   sandbox or checkpoint;
7. expires a publication only when the source authority's clock proves the
   durable retry window elapsed;
8. records the source-native `INDEXED` or `EXPIRED` receipt, then settles the
   sparse scheduling row; and
9. reports an item-scoped failure with a bounded retry duration without
   aborting the remaining page or pass.

The initial retry window, retry backoff, due scan, publication lease, and
pre-effect reauthorization all use catalog-clock decisions. A worker submits
durations and, at initial publication only, may supply the checkpoint's
external `not-after` bound. It never supplies its current time or an absolute
backoff instant. `UPLOADED` rows never expire: their deterministic metadata is
already durable and must converge to `INDEXED` without reopening a sandbox.
The artifact source row uses a fresh invocation-unique claimant plus its lease;
the outer fleet sweep's monotonic fence remains scheduling authority and is not
misrepresented as an artifact fence.

Content-addressed placement, byte verification, index verification, and
source-native CAS make retries safe. A process-local receipt, exception, or
fleet scheduling row cannot substitute for the terminal artifact row.

When a mission claim is `finalizing`, its owning mission finalizer may bind
the matching terminal artifact row and settle the claim as specified by
[Agent Mission Transitions](agent-missions.md). Recovery still cannot advance
the visible task transition outside the ordinary world tick commit.

## 7. Retention is mark, verify, and sweep

Retention evaluates durable policy, expiration, legal/operational holds,
mission resume references, and checkpoint TTL before deletion. Eligibility is
not proof of deletion. A fenced worker MUST recheck those values immediately
before invoking the narrow delete capability.

Physical deletion is idempotent: already-absent bytes are success only after
the target identity is authenticated. After deletion or verified absence, the
owning family appends or CAS-publishes a tombstone receipt. Artifact/index
metadata, object identity, content hash when known, policy, lifecycle times,
and the tombstone remain queryable; garbage collection does not erase the
audit trail.

A provider checkpoint MUST NOT be deleted until its artifact bundle is
`INDEXED` and no task, branch, resume, investigation, or recovery hold still
references it. A transient outage, abandoned lease, or dead-letter scheduling
row never releases a hold early. Bucket lifecycle rules may provide a longer
disaster-recovery ceiling, but they do not replace policy-aware retention.

## 8. Durable operator inspector

The recovery inspector is read-only and safe to expose through an authorized
operator boundary. It is reconstructed from durable source and sparse
scheduling rows and may include:

- storage profile and non-secret storage-identity digest;
- world ID, run ID, work kind, and a safe source-row identifier or digest;
- source phase/status and scheduling status;
- attempt count and configured ceiling;
- due, lease-expiry, updated, retention, and checkpoint-expiry integer-ms
  times, including a derived TTL margin;
- current lease owner digest only while the row is leased, its fence, required
  handler class, bounded error code, and pause/dead-letter reason category;
- terminal receipt or tombstone summary; and
- pass/page cursor, bounded scan counts, and last completed-pass time.

The inspector MUST NOT expose prompts, normalized model requests, outcomes,
raw exception text, response JSON, filesystem paths, object/checkpoint URLs,
headers, credentials, agent-auth material, or arbitrary provider metadata.
Error values are closed, bounded categories; narrative diagnostics belong only
in a separately redacted artifact when policy permits.

A process-local sandbox handle, heartbeat, or telemetry span is not durable
liveness and MUST NOT be presented as such. The inspector distinguishes a
valid durable lease from an advisory observation of a live process.

## 9. Telemetry and external events

Recovery emits only the bounded, vendor-neutral signals admitted by
[Observability](observability.md). Useful advisory measurements include pass
duration, worlds/items examined, outcomes by bounded work kind, lease
contention, retry delay buckets, dead-letter counts, and TTL risk buckets.
World, run, mission, attempt, artifact, tenant, and storage identifiers are not
metric labels. Trace loss or exporter failure cannot change a lease, retry,
retention, or source transition.

Durable source rows and the inspector remain the operational truth. Telemetry
helps an operator find the row; it does not prove completion or liveness.

[#491](https://github.com/VangelisTech/archetype/issues/491) owns the durable
external live-event bus, delivery ordering, replay, and consumer semantics.
Fleet recovery may eventually reconcile an outbox already defined by that
contract, but #503 does not invent event authority, derive durable events from
spans, or make a volatile live stream resumable.

## 10. Guarantees and non-goals

Conforming recovery provides:

- restart-safe, bounded discovery from a cold process;
- at-least-once handler attempts with one current lease/fence;
- item-local failure containment and deterministic retry scheduling;
- source-native, idempotent maintenance effects; and
- durable, safe operator evidence without requiring inference.

It does **not** claim exactly-once external effects. A crash can leave the
result of an object-store, provider, index, or event-delivery call unknown.
Recovery rereads authority, verifies the target, and repeats only an operation
whose owning contract makes repetition idempotent. Paid model execution is
never included in that inference.

Same-sandbox process restoration, provider liveness, checkpoint-aware agent
resume, and resolution of `possibly_submitted` uncertainty belong to #504.
External live-event durability belongs to #491. Multi-task missions, HTN, and
same-sandbox multi-agent coordination are downstream orchestration and do not
broaden this contract.

## 11. Current implementation boundary

The first vertical slice is implemented. `FleetRecoveryService` binds to one
storage identity, pages the durable world directory, leases recurring
per-world/kind sweeps, checkpoints a crash-local authority digest, and records
sparse deterministic retry/dead-letter state. Its safe list operations expose
durable sweep and exception projections without raw claimants, error details,
paths, URLs, or requests. Local and remote control catalogs implement the same
server-clock integer-millisecond CAS contract.

`ArtifactPublicationRecovery` is the first registered maintenance adapter. It
discovers digest-only due publications and invokes exact item reconciliation;
the artifact publication row remains the `INDEXED` or `EXPIRED` authority. The
credential-free capability evaluation fault-injects process loss after the
active subject checkpoint and before source I/O, discards every live handle,
then cold-restarts and indexes the exact item. No model endpoint is involved.

**CURRENT GAP:** A production deployment still needs a recurring fleet process
host, durable host-level cursor/fairness policy across repeated crashes,
fleet-paged operator inspection beyond the authoritative per-world views,
an authorized typed pause/redrive workflow, maximum recovery-age policy,
dedicated maintenance-host composition that physically excludes model and
sandbox credentials rather than relying only on a narrow Python surface,
mission-finalization and outbox adapters, policy-aware
artifact/checkpoint/local staging retention handlers, deletion receipts, and
a garbage-collection runbook. The #504 sandbox supervisor and #491 external
event bus remain separate work. Generic maintenance continues to reject every
model recovery kind until #504 supplies its distinct capability.
