# Durable Facts

**Document type:** Normative.
**Scope:** `IngestionService`, fact claims, exactly-once visibility, asset references. Issue #274 (v0.3.0 slice B). Builds on [Atomic Visibility](atomic-visibility.md).

## 1. What a fact is

A fact is an externally-produced record — a sensor event, a provider
webhook, an export — ingested into a world's history **exactly once**.
Exactly-once here has a precise meaning: **exactly one logically visible
fact per (storage, world, run, producer, external_id)**. Physical appends
may retry freely; duplicates stay invisible because visibility, not the
append, is the unit of truth.

Facts are not entities. They use catalog-allocated ids in the negative
metadata band, never enter `entity2sig`, never join active simulation, and
are excluded from resume's entity directory — otherwise every immutable
fact would re-append per tick and history would grow quadratically. They
are ordinary queryable rows in every other respect.

## 2. Why not MutationService

Mutations stage RAM state that persists on a later simulation step. Either
crash outcome for a fact routed that way is wrong: a completed claim over
RAM-only rows (visibility outran durability), or ingestion driving a full
simulation tick through every processor. A deliberately small
`IngestionService` owns the fact lifecycle behind a **direct gated**
method — never the deferred broker path, whose drain failures are logged,
not requeued.

## 3. The claim lifecycle

Claims live in the control catalog, keyed by a deterministic scope
(world, run, producer, external_id), and carry their own commit token:

1. **Acquire** (CAS, put-if-absent): a fresh claim is PENDING with a lease
   and the current writer-fence epoch recorded. Same id + same digest
   already COMPLETE → the original receipt (idempotent duplicate). Same id
   + **different digest** → loud `ClaimConflictError`, at acquire time and
   forever after. A live lease → `ClaimPendingError`; concurrent identical
   callers converge on the winner's receipt rather than erroring.
2. **Append**: one row per fact, stamped with the claim's commit token.
   The payload digest is **server-computed** from the components
   (caller-supplied hashes are never trusted). `FactMeta` rides the data
   plane on every fact row — producer, external id, digest, commit id —
   so recovery can identify an already-appended orphan by its embedded key.
3. **Flush**: staged rows become durable before any visibility claim
   (the same rule ticks obey).
4. **Complete** (CAS): one catalog transaction marks the claim COMPLETE —
   which publishes its commit token into the visible set. Readers change
   nothing: `visible_tokens` unions tick manifests with completed claims.

## 4. Crash recovery

Stranded PENDING claims are recovered by **lease takeover, never blind
retry**:

+ Crash before the append: the taker-over finds no rows under the claim's
  token and appends fresh — under the **original** token, so any late
  writes by the presumed-dead claimant remain part of the same single
  visible identity.
+ Crash after the append, before completion: the taker-over finds the
  orphan rows by token on the data plane and completes the claim
  **without re-appending**. No duplicates, ever.
+ Crash after completion: the claim is COMPLETE; every retry receives the
  original receipt.

## 5. Assets

External artifacts are content-addressed: `AssetRef` carries the sha256
digest (the identity), a uri (a hint that may rot), media type, and size.
Fact components embed asset references; helpers (`digest_file`,
`asset_ref_for_file`) compute digests. Index rows may be persisted in
tables like any component data.

## 6. Surfaces

```python
# Gate (operator+; writes durable history)
receipt = await gate.ingest_fact(
    ctx, world_id, [Reading(value=21.5)], external_id="sensor-1:evt-9",
    producer="sensor-1",
)

# Runtime
receipt = await world.ingest(
    Reading(value=21.5), external_id="sensor-1:evt-9", producer="sensor-1",
)
```

The receipt is the durable outcome: commit token, fact entity id, tick,
table, digest, and whether this call deduplicated against an existing
visible fact. Ingestion works against live worlds and cold (catalog-
recorded) ones; facts land at the world's last visible tick and never
advance it.
