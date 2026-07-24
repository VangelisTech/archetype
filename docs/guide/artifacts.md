# Artifacts and ingestion

An artifact is a file occurrence that Archetype has copied into durable object
storage and indexed for a world run. The implementation is deliberately a
small data pipeline. There are no artifact claims, leases, publication state
machines, bundle receipts, or reconciler.

## 1. Ownership

The feature is split at the authority boundary:

```text
archetype.artifacts
  values + FileIngestionPipeline + stream scanners
  storage-backed views + exact free handlers

archetype.storage
  Daft execution + Catalog table registration/read/write + Iceberg retry
  durable world/run envelope + published-head authority

archetype.app.missions
  transcript-specific redaction, parsing, and normalized-row publication
```

The cohesive `archetype.artifacts.FileIngestionPipeline` keeps the lazy Daft
graph for scan, persistence, reopening, and every common or specialized index
together. Only pure metadata algorithms live separately in `scanners.py`; they
stream where the format permits it. The family-owned free handlers configure
that graph and call the storage port with explicit durable coordinates. The
family has reviewed dependencies on core and storage only; it owns no live
world, run, control-catalog, or background-job authority.

`StorageService` is the single substrate authority. It owns the
catalog-derived world/run envelope, extends conditional keys with that
identity, admits terminal Daft execution, registers and resolves tables in
`daft.Catalog`, compares schemas, reads and writes Iceberg, and retries
optimistic commit conflicts. Image, audio, video, PDF, text, and diff handling
are branches of one artifact-family workflow, not separate application
services.

## 2. Public file contract

Submit one exact file or a Daft-readable glob with `ArtifactSource`. Express
recursive discovery in the pattern itself, such as `./outputs/**/*`:

```python
from archetype import ArchetypeRuntime, ArtifactSource
from archetype.core.config import StorageBackend, StorageConfig

storage = StorageConfig(
    uri="./archetype-data",
    namespace="factory",
    backend=StorageBackend.ICEBERG,
)

async with ArchetypeRuntime() as runtime:
    world = runtime.world("software-factory", storage=storage)
    (diff,) = await world.ingest_artifacts(
        ArtifactSource(
            source_uri="./worktree/change.diff",
            logical_path="outputs/change.diff",
        )
    )
    print(diff.artifact_id, diff.uri)
```

`ArtifactRef` is the portable handoff:

| Field | Meaning |
|---|---|
| `artifact_id` | UUIDv7 identity for this ingestion occurrence |
| `logical_path` | Portable path meaningful to the submitting workflow |
| `uri` | Durable content-addressed object URI |
| `sha256` | Cryptographic content identity |
| `xxhash3_64` | Fast scan/join fingerprint |
| `media_type` | Detected MIME type |
| `size_bytes` | Exact byte size |
| `ingested_at` | Timestamp derived from `artifact_id` |

There is no separate ingestion ID or ingestion timestamp. UUIDv7 supplies both
occurrence identity and time. SHA-256 and XXH3-64 are calculated during the
same streaming read.

## 3. Source URI, logical path, and object URI

The three paths answer different questions:

| Coordinate | Question |
|---|---|
| `source_uri` | Where did this ingestion read the bytes? |
| `logical_path` | Where does the file belong in the workflow output? |
| `object_uri` | Where are the immutable bytes stored now? |

`logical_path` is relative, slash-normalized, and rejects `..`. It remains
portable when a sandbox disappears or an object-store prefix changes. An
explicit `logical_path` wins; otherwise the resolved Daft file name is used.
Collection patterns therefore require unique file names unless the caller
submits the files separately with explicit logical paths.

Two files in one ingestion may not resolve to the same logical path. The
service fails before publishing either occurrence.

## 4. Occurrence and content identity

Artifact ingestion is intentionally not idempotent. Every submitted file gets
a fresh UUIDv7 `artifact_id`, even when its bytes have been seen before. This
preserves the fact that the software factory observed or produced the file at
a particular time.

Equal bytes do reuse the same immutable object:

```text
{object_root}/objects/sha256/{first_two_hex}/{sha256}
```

The common index can therefore contain several occurrence rows pointing at
one object URI. Analysis can group by `sha256` for content identity or by
`artifact_id` for workflow history without conflating the two.

Every artifact operation carries an explicit `StorageConfig`. Before scanning
or copying a source, the handler resolves the durable `WorldRecord`, requires
its recorded `run_id`, reads the published manifest head, and verifies that it
equals the record's `tick_head`. That published durable head supplies every
occurrence tick. Process-local liveness and an uncommitted in-memory tick
cannot move attribution forward, and the handler never acquires a live-world
registry lock. Missing coordinates, a missing run, or an absent or mismatched
published head fail before file or index effects.

## 5. Catalog and index contract

Artifact publication calls `StorageService.append_world_rows()` with a stable
table name, a typed Daft DataFrame, and `artifact_id` as the conditional key.
At the storage boundary that operation:

1. resolves the durable world and current run from the explicit storage
   configuration
2. rejects caller-supplied `world_id` or `run_id`
3. verifies that every requested key column exists
4. adds the `world_id` and `run_id` envelope
5. performs a plain append when `key_columns` is empty, or a conditional append
   when keys are present

The conditional key is `("world_id", "run_id", *key_columns)`. It describes
row identity for this append-only table view; it is not a global business key
and does not create a claim protocol.

`StorageService` performs the storage half: it creates or resolves the table
through the active `daft.Catalog`, rejects typed schema drift, materializes the
candidate graph once, and writes Iceberg. Conditional writes anti-join against
the current table. If another writer wins an optimistic Iceberg commit,
`StorageService` refreshes the table and recomputes that anti-join before
retrying, so a stale pending set cannot duplicate the same logical key.

The control and data planes remain distinct. The local SQLite control catalog,
or the remote Cloudflare Durable Object implementation, owns world records,
writer fences, command admission, and other small transactional coordination.
Iceberg owns the artifact and ingestion data tables, their atomic snapshots,
and multi-writer optimistic commits. The control catalog does not wrap an
Iceberg append in a second transaction.

The artifact common index is `artifact_files`, keyed by `artifact_id`:

| Column | Purpose |
|---|---|
| `world_id`, `run_id` | Application-owned envelope |
| `artifact_id`, `ingested_at`, `tick` | Occurrence and world coordinates |
| `source_uri`, `logical_path`, `object_uri` | Acquisition, workflow, and storage locations |
| `size_bytes`, `mime_type`, `media_family` | Common file metadata |
| `sha256`, `xxhash3_64` | Integrity and fast fingerprint |

`world.artifacts()` returns the durable current run's common index. Typed
extension tables remain internal artifact/storage surfaces until a specific
supported query API needs them. Other families that publish durable typed rows
define their own workflow meaning and call the same storage substrate; there
is no generic ingestion facade.

Reads may not depend on registration state held by the writer process. Given
the same storage configuration and durable world record, a fresh application
graph must resolve each existing named table through `daft.Catalog` and return
only its current world/run rows. This cold-read rule applies equally to the
common artifact index, typed media extensions, and normalized transcript rows.

## 6. Typed media indexes

The file scan asks `daft.File.mime_type()` for MIME classification; there is no
Python `mimetypes` fallback. Routing may additionally inspect the logical suffix
to recognize source text and patches without rewriting the MIME value. Present
families receive a narrow extension table sharing the same `artifact_id`:

| Table | Built-in metadata |
|---|---|
| `artifact_images` | width, height, format, mode |
| `artifact_audio` | stream metadata and derived duration |
| `artifact_video` | stream metadata and derived duration |
| `artifact_pdf` | page count, encryption flag, title, author |
| `artifact_text` | text kind, language, line count, UTF-8 validity |
| `artifact_diff` | patch format, files, hunks, additions, deletions, binary files |

Nested metadata structs are unnested directly into the table projection. A
`.diff` or `.patch` occurrence has both a text row and a narrower structural
diff row under the same `artifact_id`. Unknown binary files need no extension
table; their common rows are still complete.

The `FileIngestionPipeline` owns these Daft branches together. `scanners.py`
contains only the pure parsers used for hashes, PDF metadata,
text shape, and patch structure. Resize, resample, transcode, thumbnail, OCR,
and embedding helpers are future derivative workflows. They must produce new
artifacts instead of silently changing submitted bytes.

Every specialized scan reads `object_uri`, after persistence, rather than
reopening `source_uri`. This is a real durability boundary: remote source bytes
may change or disappear immediately after the content-addressed copy, while
the typed index must describe the immutable object that Archetype retained.

## 7. Visibility and failure

For each ingestion, execution is ordered:

```text
discover Daft files and occurrence identities
  -> validate required sources and logical paths
  -> stream, hash, and persist content-addressed objects
  -> append present typed media indexes
  -> append artifact_files
  -> return ArtifactRef values
```

`artifact_files` is the visibility root and is written last. A failed media
metadata scan cannot expose a common artifact row. Object bytes may already
exist after such a failure; that is safe because content-addressed objects are
immutable and unreferenced objects are not visible artifacts.

Required sources that match no files fail closed. The local persistence pass
streams through Daft's copy buffer into a same-filesystem temporary file while
computing SHA-256, XXH3-64, and byte size from those same chunks. It then
atomically publishes the resulting content address. A mutable source is
therefore addressed by the bytes actually copied, without a discovery hash,
verification reread, or destination reread.

Daft 0.7.19 exposes read-only `File` values and its `upload()` expression
accepts a Binary column rather than a streaming file source. Remote persistence
therefore performs the same single source read but temporarily materializes
that payload for upload. This implementation limitation is explicit and does
not impose a total artifact-size policy; it can be replaced by Daft's public
writable/multipart file surface when that ships.

No claim or recovery state surrounds this pipeline. Callers retry by making a
new occurrence. If the content copy already completed, the retry reuses the
verified object.

## 8. Transcript ingestion

Coding-agent transcripts are a mission workflow, not an artifact backend.
`TranscriptIngestionService` preserves this exact order:

1. `RedactionService` validates metadata and snapshots a sanitized file before
   durability
2. the missions parser reads only that sanitized copy
3. the workflow redacts normalized session and turn rows
4. it computes the sanitized file digest
5. the artifact-family handler publishes the immutable object, typed indexes,
   and common artifact row
6. it verifies that the returned artifact SHA-256 equals the sanitized digest
7. `StorageService` appends normalized rows to
   `coding_agent_transcript_rows`

Every normalized row carries `source_artifact_id`, so queries can join the
narrative data to the common file index without persisting a duplicate asset
component. `world.transcript_rows()` returns the current run's normalized
session and turn rows. The original source digest may identify the input, while the
artifact SHA-256 always describes the sanitized bytes actually stored.

Quarantine, parse, and row-redaction failures occur before artifact
publication and publish nothing. A digest mismatch occurs after the honest
artifact boundary and therefore leaves the sanitized artifact visible but
fails before any transcript row append. Re-ingesting a valid transcript
records another artifact occurrence and another normalized row set scoped to
that occurrence.

## 9. Evaluation results

Evaluation pins the world's visible component snapshot from explicit storage
coordinates, runs the requested grader, and appends one row to
`evaluation_results`, keyed by `evaluation_id` inside the world run. Its free
family handler writes through `StorageService`; it does not consult the live
registry or the general ingestion facade. A forked subject includes its
current-run visibility and every durable ancestor segment at that segment's
fork-time tick cap. All segment allowlists are captured and reused before
grading; each non-empty segment's world/run, cap, and immutable manifest head
are bound into the receipt subject identity. Equal-cap zero-width ancestry is
pinned for integrity but contributes no subject rows or digest segment.

For a no-lineage fork, evaluation admits child-only rows when that run owns
tick zero or when its parent is absent from the target catalog, as happens
after an intentional cross-store fork. The latter parent-absence test is the
current durable severance signal; a future lifecycle schema can replace that
inference with an explicit lineage-mode marker. If the parent is present and
the child begins later than tick zero, evaluation fails closed rather than
persisting a partial receipt.

Reusing an evaluation ID with the same pinned subject and grader contract
returns the persisted result without grading again. Reusing it for a different
subject or contract fails loudly. The result remains an ordinary Iceberg row;
a narrow evaluation lease in the existing control catalog serializes grader
execution across processes until that row is durable. Failed owners release
immediately, expired owners can be recovered, and recovery checks for an
already-appended result before running the grader again. This coordination is
evaluation-specific and does not add claims or publication state to artifact
ingestion.

## 10. Security boundary

Generic artifact ingestion stores the bytes the caller submits. Workflows that
handle potentially secret-bearing content must sanitize before calling it.
Transcript ingestion does this by construction: unsafe metadata, symlinks,
unsupported containers, and unrewritable secret-bearing inputs are quarantined
before any object or catalog row becomes durable.

The common rule is simple: specialized workflows own pre-durability safety;
the artifacts family owns exact file persistence and indexing; operation
models carry explicit durable coordinates; and `StorageService` owns the
world/run envelope, append choice, Catalog, and terminal Daft execution
authority.

## 11. Task-anchored artifact context

`ArtifactContext` names one task-scoped interpretation of an artifact set. Its
UUIDv7 `context_id` identifies the interpretation; artifact UUIDs continue to
identify the individual ingestion occurrences. The contract does not create a
second storage service or copy the files again.

```python
from daft.ai.provider import load_openai

from archetype import ArtifactContext, ArtifactSource
from archetype.artifacts import analyze_artifacts, synthesize_artifact_context

provider = load_openai()

submitted = await world.ingest_artifacts(
    ArtifactSource(source_uri="./evidence/change.patch", logical_path="change.patch"),
    ArtifactSource(source_uri="./evidence/design.md", logical_path="design.md"),
)
context = ArtifactContext(
    task="Explain whether this change preserves immutable source identity.",
    artifact_ids=tuple(artifact.artifact_id for artifact in submitted),
)
index = await world.artifacts()
analyses = analyze_artifacts(
    index,
    context,
    provider=provider,
    model="gpt-5-mini",
)
synthesis = synthesize_artifact_context(
    analyses,
    context,
    provider=provider,
    model="gpt-5-mini",
)
```

`ArtifactContext` binds the authoritative task and exact artifact occurrence
IDs under one context ID, so the same interpretation identity cannot silently
refer to a different evidence set. The first transform never treats the
complete world index as an implicit context pack: Daft filters it by those IDs
before giving the selected artifacts one prompt each. Files can be analyzed in
parallel without issuing model calls for unrelated run artifacts. Every prompt
carries the task, context ID, logical path, MIME type, and the staged
`daft.File`. Artifact contents are explicitly marked as untrusted evidence
rather than instructions. The second transform applies the same selection and
reduces the attributed observations into one answer while retaining logical
paths and artifact IDs.

These are family-owned DataFrame transforms, not application orchestration.
They do not choose a catalog, persist model output, or decide mission state.
A mission processor may persist the resulting rows or use them as evidence for
a transition. The selected Daft AI provider determines which content
modalities its model accepts; storage and typed indexing support do not imply
that every model can directly interpret every media type.

### Cloud dogfood

The protected infrastructure test sends one bounded context pack through the
real Cloudflare stack:

```text
Hugging Face Markdown + MP3 + MP4 + PDF
local Markdown + Python + git patch + PNG + sanitized Claude transcript
  -> content-addressed R2 objects
  -> Daft Catalog / Iceberg tables whose metadata and data live on R2
  -> fresh catalog + fresh application graph
  -> cold queries of every populated table
```

The cold query result is deliberately reviewable as one small table:

| Table | Rows | Join back to `artifact_files` |
| --- | ---: | --- |
| `artifact_files` | 9 | visibility root |
| `artifact_images` | 1 | `artifact_id` |
| `artifact_audio` | 1 | `artifact_id` |
| `artifact_video` | 1 | `artifact_id` |
| `artifact_pdf` | 1 | `artifact_id` |
| `artifact_text` | 5 | `artifact_id` |
| `artifact_diff` | 1 | `artifact_id` |
| `coding_agent_transcript_rows` | 3 | `source_artifact_id` |

The test checks metadata and logical-path attribution after the restart, UUIDv7
identity-derived timestamps, both content hashes, and Daft's unmodified MIME
classification—not merely that the table names exist. It then destroys its
unique catalog namespace and R2 prefixes. Local contract tests generate real
PNG, WAV, MP4, and PDF fixtures and additionally
delete an acquisition source after object persistence, proving that metadata
scans use the staged object. Live model calls remain an explicit
credential-bearing external check; the deterministic contract tests validate
the task anchoring and source attribution without pretending that a mocked
provider is model evidence.

## 12. Migration from the 0.4 artifact surface

This refactor is an intentional breaking change from the artifact API shipped
in `0.4.1`. The first release containing it must be `0.5.0` or later; it must
not be published as another `0.4.x` release.

The old surface mixed file persistence with claims, publication recovery,
checkpoint bundles, and entity receipts. The replacement keeps file
occurrence identity and content durability while removing that orchestration:

| 0.4 surface | 0.5 direction |
| --- | --- |
| `ArtifactBundleRequest` and `ArtifactCandidate` | one or more `ArtifactSource` values |
| `ArtifactPublishReceipt`, `ArtifactReceipt`, and `MaterializedArtifact` | immutable `ArtifactRef` values |
| `world.publish_artifact_bundle(...)` | `world.ingest_artifacts(...)` |
| `world.artifact_bundles(...)` | `world.artifacts()` and typed artifact indexes |
| `world.reconcile_artifact_bundles(...)` | removed; retry creates a new occurrence and reuses verified content |
| generic `world.ingest_files(...)` / `world.write_artifacts(...)` | `world.ingest_artifacts(...)` for files, or an owning family workflow over `StorageService` for typed rows |
| `world.publish(...)` for external component rows | `world.spawn(...)` for world state, or an owning application workflow for durable tabular data |
| `TranscriptIngestionReceipt` | `TranscriptIngestionResult` linked to the sanitized `ArtifactRef` |

`ArtifactStoreConfig` retains its name but now configures only the object root,
file-ingestion I/O, and upload concurrency. Callers must construct the new model
rather than expecting the former bundle/checkpoint fields. There are deliberately
no compatibility aliases for claim, receipt, bundle-finalization, or reconciler
types: preserving them would retain the machinery this migration removes.
