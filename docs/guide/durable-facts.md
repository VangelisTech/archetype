# Durable Facts

**Document type:** Normative.
**Scope:** claim-backed `IngestionService` facts, asset references, evaluation\nreceipts, and typed Iceberg fact tables. Builds on\n[Atomic Visibility](atomic-visibility.md).

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

## 7. Evaluation receipts: claim-before-grade

A receipt records that ONE grader ran under ONE pinned contract against ONE
pinned subject, and what it concluded. Receipts ride the fact machinery —
same claims, same visibility, same crash recovery — with three identity
rules on top:

+ **The claim precedes the grader.** `evaluate` acquires the claim before
  any grading runs; a matching COMPLETE claim returns the persisted receipt
  **without re-grading**. The guarantee is exactly one **visible** durable
  receipt per `evaluation_id` — never exactly-once grader execution. A
  lease takeover whose orphan probe finds the appended rows completes
  without re-running the grader; a takeover that finds none re-runs it at
  most once.
+ **The subject is pinned, never hashed by content.** Subject identity =
  the immutable snapshot reference (manifest head tick + commit tokens)
  plus the canonical selector (components, ticks, entity ids).
  Materializing a trajectory to hash its rows would break the lazy
  contract; the snapshot reference makes the receipt recomputable and
  attributable without it. Asset references inside subjects bind content
  digests, never path strings.
+ **The contract is versioned or the receipt is refused.** A
  `GraderContract` (stable grader id, implementation/prompt/model version,
  config, thresholds, seed) is required; bare callables get no digest and
  no persisted receipt — `world.grade` remains the ephemeral path. The
  `evaluation_id` is deliberately distinct from subject + contract:
  repeated trials of nondeterministic graders are a feature, and each
  trial is its own id. Same id with a different subject or contract is a
  loud conflict.

Outcomes are typed — pass, fail, invalid, or inconclusive, with an optional
finite score — and empty outcome sets fail closed.

**Receipts are evidence, never authority.** A receipt carries no authority
fields — no accepted, no promote, no approved, no allowed_next_action —
enforced by the spec-contract eval suite. A PASS means one grader passed
under one pinned contract; the layer above owns what that means.

## 8. Typed Iceberg fact tables

New general-purpose facts live in typed Iceberg tables beside the world's ECS
tables. They do not become entities, enter `entity2sig`, run through simulation
processors, or advance a world tick.

Every typed fact table has this service-owned envelope:

| Column | Meaning |
|---|---|
| `fact_id` | UUIDv7 assigned when the row is written |
| `world_id` | world that owns the fact |
| `run_id` | run that owns the fact |
| `source_uri` | canonical source location |
| `content_hash` | lowercase SHA-256 digest of the source content |

The logical key is
`(world_id, run_id, source_uri, content_hash)`. The UUIDv7 is row identity,
not the deduplication key. Changed bytes at one URI create another fact; equal
bytes at different URIs also create distinct facts. There is no generic
`observed_at` column. Domains that need source event time add a typed payload
column with the precise semantics they require.

Typed fact visibility is local to that `world_id` and `run_id`. ECS tick
lineage is not applied to fact tables: a fork starts with an empty typed-fact
view and may ingest the same source under its new identity. Inheriting ancestor
facts without a fact-time boundary would incorrectly expose facts added to the
ancestor after the fork.

Each logical name maps to `facts__<table_name>` in the same catalog and
namespace as the world. The remaining columns are the domain schema. Schema
drift fails before append; fact tables do not silently widen or coerce.

## 9. Authoritative storage boundary

Typed facts require `StorageBackend.ICEBERG`. `FactService` obtains the
world's `IcebergCatalogContext` from `StorageService`, so catalog selection
and data-plane credentials have one source of truth:

+ the caller-configured Daft `Session` owns the catalog;
+ the caller's Daft `IOConfig` passes directly to file reads and Iceberg I/O;
+ Archetype does not translate environment variables or reconstruct managed
  service credentials;
+ the built-in factory remains the concrete local SQLite-catalog option.

LanceDB remains supported by the claim-backed compatibility path, not by typed
fact tables.

## 10. Daft file processors

`ingest_files` builds a lazy Daft pipeline with `daft.from_files`. Each input
row contains:

+ `file`: a lazy `daft.File` reference;
+ `source_uri`: derived with `daft.functions.file_path`;
+ `content_hash`: SHA-256 streamed through `File.open()`.

A `FactProcessor` declares `table_name` and transforms each input into
exactly one typed output row. It must preserve `source_uri` and
`content_hash`. `FactService` verifies the one-to-one identity mapping,
removes the execution-only `file` column, and assigns `world_id`, `run_id`,
and `fact_id`.

```python
import daft
from daft import col


@daft.func(return_dtype=daft.DataType.string())
def read_text(file: daft.File) -> str:
    with file.open() as stream:
        return stream.read().decode("utf-8")


class Documents:
    table_name = "documents"

    def process(self, files: daft.DataFrame) -> daft.DataFrame:
        return files.with_column("text", read_text(col("file")))


receipt = await world.ingest_files("notes/**/*.md", Documents())
documents = await world.facts("documents")
```

Known logical keys are removed before the processor runs. A complete retry is
therefore a no-op: it does not rerun the processor or create an empty Iceberg
snapshot.

## 11. Existing Daft pipelines

Callers that already have a Daft pipeline use `write_facts`. The frame must
contain `source_uri`, `content_hash`, and at least one typed payload column.
The service adds the rest of the envelope.

```python
facts = daft.from_pydict(
    {
        "source_uri": ["sensor://room-1/reading-9"],
        "content_hash": [digest],
        "temperature_c": [21.5],
    }
)
receipt = await world.write_facts("temperatures", facts)
```

`world.facts(table_name)` returns a lazy frame filtered to the handle's
current `world_id` and `run_id`.

## 12. Commit and concurrency semantics

One non-empty service call appends its rows in one Iceberg commit, not one
snapshot per row. `FactService` does not automatically re-execute an arbitrary
Daft pipeline after a commit conflict; the single-writer requirement below
makes such a conflict an operational error for the caller to resolve. Fact
ingestion does not perform compaction; compaction is a separate table-
maintenance feature.

Within one `FactService`, writes to a physical fact table are serialized
around the existing-key check and append. This gives deterministic idempotency
for callers sharing that service. Iceberg append tables do not enforce unique
constraints: independent processes writing the same table must use an external
single-writer lease until Archetype provides a catalog-coordinated
multi-process fact writer. The API does not claim global exactly-once behavior
without that serialization.

Iceberg commits are atomic. After a crash, a visible append is found by its
logical keys on retry; an uncommitted append is absent and can be attempted
again.
