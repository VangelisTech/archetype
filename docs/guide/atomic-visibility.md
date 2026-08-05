# Atomic Tick Visibility

**Document type:** Normative.
**Scope:** Commit identity, manifests, writer fencing, append receipts, legacy epoch-0 semantics. Issue #273 (v0.3.0 slice A2). Amends the storage base schema defined in the umbrella specification.

## 1. The visibility rule

A tick is visible if and only if its manifest is published in the control
catalog. Everything else follows from this one rule:

- A crash during a tick's appends leaves rows on disk but no manifest —
  the tick did not happen, and no reader ever sees the partial write.
- A crash after appends but before publish: same outcome. The retried tick
  recomputes from the last visible tick and re-appends under a fresh commit
  token; only the attempt that published is ever visible. Exactly one
  visible attempt per tick, even with multiple physical attempts on disk.
- A stale writer (one that lost the fence) fails at publish, atomically,
  and its rows stay invisible — including rows it appends late at a tick
  that is already visible.

## 2. Commit identity columns

The storage base schema carries two commit-identity columns on every row:

| Column | Type | Meaning |
|---|---|---|
| `commit_token` | string | Names the tick-commit attempt that wrote the row. |
| `writer_epoch` | int64 | The fenced writer epoch the attempt ran under. |

Schema-is-identity is preserved: schemas never evolve on tables, so these
columns join the base schema of **new-generation tables** — the table id is
a hash of the full schema, and the new columns produce new ids. v0.2 tables
keep their old ids and are read through a legacy-name fallback as **implicit
epoch-0** history: always visible, token filtering never applies to them,
and nothing ever writes to a legacy table id again.

Component projections (`query_components`, `query_archetype` with a
`components` argument) exclude the commit columns — they are storage
metadata, not part of the component shape. Raw `query_archetype` reads
expose them: commit identity is in the ledger and queryable like everything
else.

## 3. The tick commit protocol

Coordinated worlds (every world created through the service layer) commit a
tick in this order:

1. **Materialize** due durable commands against the exact locked world, then
   fire advisory `PreTick` hooks and capture active signatures. A materializer
   infrastructure failure leaves the tick, caches, and settlement retryable.
2. **Compute** every archetype's frame. No writes, no cache consumption.
3. **Mint** one `CommitContext` (fresh `commit_token`, the writer's epoch)
   for the whole tick.
4. **Append** every frame, stamped with the tick's commit identity. Appends
   return `AppendReceipt`s (row counts, staged/durable, an auxiliary backend
   reference such as a Lance version — never the visibility mechanism).
5. **Flush** the store. A caching store drains its memtables; a manifest
   must never claim RAM-only rows are durable.
6. **Publish** the manifest — after durable data flush, one transaction in the
   target world's control authority (a) verifies the writer still holds the
   fence, (b) put-if-absent inserts the manifest row
   `(world, run, tick, commit_token, writer_epoch, table_ids)`, and (c)
   advances the world's durable tick head. It may also atomically settle the
   target world's commands and append its durable control outbox. It does not
   span a directory Durable Object or the separate Iceberg data commit. Stale
   epoch → `StaleWriterError`; a different already-published attempt →
   `CatalogConflictError`; the identical attempt → idempotent no-op.
7. **Reconcile** an uncertain publish response before admitting any other
   world operation. After append and flush, the world retains one prepared
   commit containing the exact context and computed frames. An authoritative
   read that returns that exact token proves the commit and permits only an
   in-process acknowledgment that releases coordinator-local staging; it MUST
   NOT issue a second fenced catalog write merely to finish local cleanup.
   Command materialization, processor execution, and append MUST NOT replay.
   This remains true if another writer acquired the fence after the proven
   commit; the old writer may finalize that receipt but cannot publish new
   work. An explicit empty result for that tick from the fenced authority
   proves the publish had no effect and permits a later fresh-token
   recomputation. An unreadable result, a pre-coordination `None`, or a
   different token is not proof of absence: the prepared identity remains
   retained and all entity, processor, hook, and resource mutation stays
   frozen until exact reconciliation succeeds or explicit absence is proved.
8. **Consume** spawn/despawn caches — only after publication is confirmed.
   Failure at any earlier point leaves mutations intact for the retried tick.
9. **Advance** the in-memory tick and record a `CommittedTickReceipt` whose
   identity is
   `(world_id, run_id, committed_tick, visibility_token)`.
10. **Fire** advisory `PostTick` hooks and return that already-recorded
    receipt. Cancellation after publication cannot erase the durable receipt;
    managed execution retains it for required projection before propagating
    caller cancellation.

The managed world layer then retains that exact receipt for its required
projector. A projector failure is post-commit: the manifest remains visible,
processors and command materialization are not replayed, and the same retained
receipt is retried before another tick. Required projection is not a public
hook. It runs under exact-world authority and is limited to idempotently
persisting deterministic intent keyed by its consumer and receipt identity; it
MUST NOT perform provider or sandbox I/O. An Activity worker may claim that
intent and perform external effects outside the world lock. When that work is
durably coordinated into a later committed observation, it follows the
[Activity](activities.md) contract: at-least-once delivery, fenced control
writes, provider reconciliation, durable result reference, and exact-receipt
settlement.

Worlds constructed without a coordinator (bare core usage, the sync stack)
run uncoordinated: rows stamp the implicit epoch-0 identity (`""`/`0`), no
manifests are written, and nothing is filtered — v0.2 semantics, unchanged.
Their tokenless receipts do not provide the pinned source or observation
snapshot required by the [Activity](activities.md) contract.

## 4. Reader-side allowlist

Readers resolve a token allowlist per `(world, run)` segment — fork lineage
segments each resolve against their own ancestor's manifests:

- No manifests and no writer fence → pre-#273 or uncoordinated history:
  unfiltered (implicitly visible).
- A writer fence exists but no manifests → a coordinated world whose first
  commit has not published: nothing current-generation is visible.
- Manifests exist → rows must match the published token for their tick.
  Legacy epoch-0 rows are always admitted.

The allowlist matches manifests across history, not the head epoch alone: a
stale writer may append old-epoch rows at an already-visible tick, and those
rows carry an unpublished token, so they never surface.

A corrupt or unreadable catalog fails coordinated reads closed — the error
propagates; it never degrades to unfiltered visibility. (Degraded discovery
returns less data; degraded visibility would return rows no manifest
authorized.) A merely missing catalog is not an error: connecting creates an
empty one, which reports the legacy-unfiltered case.

## 5. Writer fencing

One live writer per world, enforced by catalog CAS: acquiring the fence
increments the epoch and stales every earlier holder. `create_world` and
`fork_world` acquire the fence; fenced mutable resume (A1-resume) acquires
it cold. Publication verifies the epoch inside the manifest transaction, so
fencing has no window: either the manifest lands under the live epoch or
the writer learns it is stale and stops (its world never advances).

## 6. Fork lineage under commit identity

`persist_lineage` writes the fork's full ancestor chain as **one append**,
which is atomic on both backends — a crash leaves either the whole chain or
nothing. A caching store must flush a staged lineage append before fork
activation returns, so a successful activation cannot leave its lineage only
in process memory. Lineage rows are world metadata (negative entity ids,
epoch-0 stamped, never token-filtered). The catalog's world record carries the
authoritative `parent_world_id`; a fork record whose lineage rows are missing
is detectable corruption, and fenced resume must refuse it loudly.

## 7. Retention, optimize, and GC

Visibility is column-and-manifest based, never backend-version based. That
is what makes maintenance safe:

- `optimize()`/compaction may rewrite fragments freely — rows keep their
  commit columns, manifests keep matching them.
- Backend version pruning is safe: no reader ever pins a Lance/Iceberg
  version to decide visibility (versions appear only as auxiliary
  diagnostics in receipts).
- Append-only holds: no maintenance operation deletes rows. Unmanifested
  rows from crashed attempts are dead weight, not dangerous weight; a
  future vacuum contract may reclaim them, and MUST key on "token absent
  from manifests", never on age or version.

## 8. What A2 does not do

- No physical resumability: a crashed MuJoCo (or any external-process)
  execution is queryable up to its last published tick, not resumable
  mid-physics. Physical evidence worlds are durably registered with immutable
  `writer_mode="cleanup_only"` while retaining `status="active"` for ordinary
  tick materialization. Mutable resume requires `writer_mode="resumable"`, so
  a process crash cannot reactivate the serialized provider processors.
- Cross-host fencing requires the remote control catalog
  (``ARCHETYPE_CONTROL_CATALOG_URL`` plus
  ``ARCHETYPE_CONTROL_CATALOG_TOKEN``, captured once by the application
  composition root; issue #281): with it configured,
  discovery, fencing, and visibility hold across hosts; the default local
  SQLite catalog remains single-host authority.
- Mutable cold resume is delivered on top of this contract — see
  [World Lifecycle](world-lifecycle.md) § Resume: `resume_world`
  reconstructs a live world from visible rows + manifests and acquires the
  fence, staling the previous writer.
