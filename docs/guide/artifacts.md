# Artifacts

**Document type:** Normative.
**Scope:** claim-backed `ArtifactService` artifacts, asset references,
evaluation receipts, and typed Iceberg artifact tables. Builds on
[Atomic Visibility](atomic-visibility.md).

## 1. What an artifact is

An artifact is an externally produced record — a sensor event, a provider
webhook, an export — ingested into a world's history **exactly once**.
Exactly-once here has a precise meaning: **exactly one logically visible
artifact per (storage, world, run, producer, external_id)**. Physical appends
may retry freely; duplicates stay invisible because visibility, not the
append, is the unit of truth.

Artifacts are not entities. They use catalog-allocated ids in the negative
metadata band, never enter `entity2sig`, never join active simulation, and
are excluded from resume's entity directory — otherwise every immutable
artifact would re-append per tick and history would grow quadratically. They
are ordinary queryable rows in every other respect.

## 2. Why not MutationService

Mutations stage RAM state that persists on a later simulation step. Either
crash outcome for an artifact routed that way is wrong: a completed claim over
RAM-only rows (visibility outran durability), or ingestion driving a full
simulation tick through every processor. A deliberately small
`ArtifactService` owns the artifact lifecycle behind a **direct gated**
method — never the deferred command path. Artifact claims have their own
lease/recovery state machine.

## 3. The claim lifecycle

Claims live in the control catalog, keyed by a deterministic scope
(world, run, producer, external_id), and carry their own commit token:

1. **Acquire** (CAS, put-if-absent): a fresh claim is PENDING with a lease
   and the current writer-fence epoch recorded. Same id + same digest
   already COMPLETE → the original receipt (idempotent duplicate). Same id
   + **different digest** → loud `ClaimConflictError`, at acquire time and
   forever after. A live lease → `ClaimPendingError`; concurrent identical
   callers converge on the winner's receipt rather than erroring.
2. **Append**: one row per artifact, stamped with the claim's commit token.
   The payload digest is **server-computed** from the components
   (caller-supplied hashes are never trusted). `ArtifactMeta` rides the data
   plane on every artifact row — producer, external id, digest, commit id —
   so recovery can identify an already-appended orphan by its embedded key.
3. **Flush**: staged rows become durable before any visibility claim
   (the same rule ticks obey).
4. **Complete** (CAS): one catalog transaction marks the claim COMPLETE —
   which publishes its commit token into the visible set. Readers change
   nothing: `visible_tokens` unions tick manifests with completed claims.

For a never-fenced legacy run, the first claim activates token filtering but
keeps the empty epoch-0 token visible. That hides a PENDING artifact's non-empty
token without making pre-coordination rows disappear.

## 4. Crash recovery

Stranded PENDING claims are recovered by **lease takeover, never blind
retry**:

+ Crash before the append: the taker-over finds no rows under the claim's
  token, then atomically re-arms the claim with a **fresh** token before
  rebuilding and appending. Any late write by the expired claimant keeps
  the old token and therefore remains invisible.
+ Crash after the append, before completion: the taker-over finds the
  orphan rows by token on the data plane and completes the claim
  **without re-appending**. No duplicates, ever.
+ Crash after completion: the claim is COMPLETE; every retry receives the
  original receipt.

## 5. Assets

External artifacts are content-addressed: `AssetRef` carries the sha256
digest (the identity), a uri (a hint that may rot), media type, and size.
Artifact components embed asset references; helpers (`digest_file`,
`asset_ref_for_file`) compute digests. Index rows may be persisted in
tables like any component data.

## 6. Surfaces

```python
# Gate (operator+; writes durable history)
receipt = await gate.publish(
    ctx, world_id, [Reading(value=21.5)], external_id="sensor-1:evt-9",
    producer="sensor-1",
)

# Runtime
receipt = await world.publish(
    Reading(value=21.5), external_id="sensor-1:evt-9", producer="sensor-1",
)
```

The receipt is the durable outcome: commit token, artifact entity id, tick,
table, digest, and whether this call deduplicated against an existing
visible artifact. Ingestion works against live worlds and cold (catalog-
recorded) ones; artifacts land at the latest manifest tick, falling back to the
recorded fork/genesis head before the first manifest, and never advance it.

## 7. Evaluation receipts: claim-before-grade

A receipt records that ONE grader ran under ONE pinned contract against ONE
pinned subject, and what it concluded. Receipts ride the artifact machinery —
same claims, same visibility, same crash recovery — with three identity
rules on top:

+ **The claim precedes the grader.** `evaluate` acquires the claim before
  any grading runs; a matching COMPLETE claim returns the persisted receipt
  **without re-grading**. The guarantee is exactly one **visible** durable
  receipt per `evaluation_id` — never exactly-once grader execution. A
  lease takeover whose orphan probe finds the appended rows completes
  without re-running the grader; a takeover that finds none re-arms the
  claim before rebuilding. Grader executions may overlap across an expired
  lease, but only the current claim token can ever publish a receipt.
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
enforced by the `spec.receipt_authority_firewall` repository check. A PASS
means one grader passed under one pinned contract; the layer above owns what
that means.

## 8. Typed Iceberg artifact tables

New general-purpose artifacts live in typed Iceberg tables beside the world's ECS
tables. They do not become entities, enter `entity2sig`, run through simulation
processors, or advance a world tick.

Every typed artifact table has this service-owned envelope:

| Column | Meaning |
|---|---|
| `artifact_id` | UUIDv7 assigned when the row is written |
| `world_id` | world that owns the artifact |
| `run_id` | run that owns the artifact |
| `source_uri` | canonical source location |
| `content_hash` | lowercase SHA-256 digest of the source content |

The logical key is
`(world_id, run_id, source_uri, content_hash)`. The UUIDv7 is row identity,
not the deduplication key. Changed bytes at one URI create another artifact; equal
bytes at different URIs also create distinct artifacts. There is no generic
`observed_at` column. Domains that need source event time add a typed payload
column with the precise semantics they require.

Typed artifact visibility is local to that `world_id` and `run_id`. ECS tick
lineage is not applied to artifact tables: a fork starts with an empty typed-artifact
view and may ingest the same source under its new identity. Inheriting ancestor
artifacts without an artifact-time boundary would incorrectly expose artifacts added to the
ancestor after the fork.

For dataset tables, this envelope describes storage ownership and write
identity; it is not dataset episode identity or original execution provenance.
Dataset payloads retain their natural keys and optional source-runtime slice as
defined by the [Dataset and Evaluation Ontology](dataset-eval-ontology.md).
An ingestion world's `world_id` / `run_id` MUST NOT be presented as the runtime
that originally produced an imported episode.

Each logical name maps to `artifacts__<table_name>` in the same catalog and
namespace as the world. The remaining columns are the domain schema. Schema
drift fails before append; artifact tables do not silently widen or coerce.

## 9. Authoritative storage boundary

Typed artifacts require `StorageBackend.ICEBERG`. `ArtifactTableService` obtains the
world's `IcebergCatalogContext` from `StorageService`, so catalog selection
and data-plane credentials have one source of truth:

+ the caller-configured Daft `Session` owns the catalog;
+ the caller's Daft `IOConfig` passes directly to file reads and Iceberg I/O;
+ Archetype does not translate environment variables or reconstruct managed
  service credentials;
+ the built-in factory remains the concrete local SQLite-catalog option.

LanceDB remains supported by the claim-backed component-publication path, not by typed
artifact tables.

## 10. Daft file processors

`ingest_files` builds a lazy Daft pipeline with `daft.from_files`. Each input
row contains:

+ `file`: a lazy `daft.File` reference;
+ `source_uri`: derived with `daft.functions.file_path`;
+ `content_hash`: SHA-256 streamed through `File.open()`.

An `ArtifactProcessor` declares `table_name` and transforms each input into
exactly one typed output row. It must preserve `source_uri` and
`content_hash`. `ArtifactTableService` verifies the one-to-one identity mapping,
removes the execution-only `file` column, and assigns `world_id`, `run_id`,
and `artifact_id`.

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
documents = await world.artifacts("documents")
```

Known logical keys are removed before the processor runs. A complete retry is
therefore a no-op: it does not rerun the processor or create an empty Iceberg
snapshot.

`ArtifactWriteReceipt.sources_matched` counts the file rows discovered before
logical deduplication and the existing-key filter. `duplicate` is true only
when that count is nonzero and the filter removes every source. An empty path
match is therefore distinct
from an idempotent retry. For `write_artifacts`, `sources_matched` is `None` because
counting an arbitrary input pipeline separately would execute it twice; a
zero-row direct write therefore reports `duplicate=None` rather than guessing.
If that is the first write for the logical table, the service also unwinds the
empty table registration so the no-op cannot lock in a speculative schema.

## 11. Existing Daft pipelines

Callers that already have a Daft pipeline use `write_artifacts`. The frame must
contain `source_uri`, `content_hash`, and at least one typed payload column.
The service adds the rest of the envelope.

```python
artifacts = daft.from_pydict(
    {
        "source_uri": ["sensor://room-1/reading-9"],
        "content_hash": [digest],
        "temperature_c": [21.5],
    }
)
receipt = await world.write_artifacts("temperatures", artifacts)
```

`world.artifacts(table_name)` returns a lazy frame filtered to the handle's
current `world_id` and `run_id`.

## 12. Commit and concurrency semantics

One non-empty service call appends its rows in one Iceberg commit, not one
snapshot per row. `ArtifactTableService` does not automatically re-execute an arbitrary
Daft pipeline after a commit conflict; the single-writer requirement below
makes such a conflict an operational error for the caller to resolve. Artifact
ingestion does not perform compaction; compaction is a separate table-
maintenance feature.

Within one `ArtifactTableService`, writes to a physical artifact table are serialized
around the existing-key check and append. This gives deterministic idempotency
for callers sharing that service. Iceberg append tables do not enforce unique
constraints: independent processes writing the same table must use an external
single-writer lease until Archetype provides a catalog-coordinated
multi-process artifact writer. The API does not claim global exactly-once behavior
without that serialization.

Iceberg commits are atomic. After a crash, a visible append is found by its
logical keys on retry; an uncommitted append is absent and can be attempted
again.
