# Artifacts and ingestion

An artifact is a file occurrence that Archetype has copied into durable object
storage and indexed for a world run. The implementation is deliberately a
small data pipeline. There are no artifact claims, leases, publication state
machines, bundle receipts, or reconciler.

## 1. Ownership

The feature is split at the authority boundary:

```text
archetype.ingestion
  table contracts + lazy file/media DataFrame transforms

archetype.app.ingestion
  world/run envelope + Daft Catalog registration + Iceberg append/read

archetype.app.artifacts
  source discovery + object persistence + typed index composition

archetype.app.missions
  transcript-specific redaction, parsing, and normalized row ingestion
```

`archetype.ingestion` is reusable family code. It can inspect and project a
DataFrame, but it cannot choose a catalog, namespace, world, or run.
`IngestionService` owns those durable application decisions and is the only
layer in this stack that registers tables in `daft.Catalog`.

`ArtifactService` is the one file-artifact service. It composes general
ingestion; image, audio, video, PDF, and text handling are not separate
application services.

## 2. Public file contract

Submit one file, a glob, or a recursive prefix with `ArtifactSource`:

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
portable when a sandbox disappears or an object-store prefix changes. A
recursive source combines `logical_root` with each path relative to the source
root. An explicit single-file `logical_path` wins for that file.

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

## 5. Catalog and index contract

`IngestionService.append()` accepts an `IngestionTable` and a typed Daft
DataFrame. At the application boundary it:

1. resolves the world and current run
2. adds the `world_id` and `run_id` envelope
3. registers the table in the active `daft.Catalog`
4. rejects schema drift
5. anti-joins the table's declared keys
6. appends the remaining rows to Iceberg

`IngestionTable.key_columns` defines write identity inside one world run. It is
not a global business key and does not create a claim protocol.

The artifact common index is `artifact_files`, keyed by `artifact_id`:

| Column | Purpose |
|---|---|
| `world_id`, `run_id` | Application-owned envelope |
| `artifact_id`, `ingested_at`, `tick` | Occurrence and world coordinates |
| `source_uri`, `logical_path`, `object_uri` | Acquisition, workflow, and storage locations |
| `size_bytes`, `mime_type`, `media_family` | Common file metadata |
| `sha256`, `xxhash3_64` | Integrity and fast fingerprint |

`world.artifacts()` returns the current run's common index. General ingestion
and typed extension tables remain internal service-layer surfaces until a
specific supported query API needs them.

## 6. Typed media indexes

The file scan classifies each row once. Present families receive a narrow
extension table sharing the same `artifact_id`:

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

These modules perform metadata scans only. Resize, resample, transcode,
thumbnail, OCR, and embedding helpers are future derivative workflows. They
must produce new artifacts instead of silently changing submitted bytes.

Every specialized scan reads `object_uri`, after persistence, rather than
reopening `source_uri`. This is a real durability boundary: remote source bytes
may change or disappear immediately after the content-addressed copy, while
the typed index must describe the immutable object that Archetype retained.

## 7. Visibility and failure

For each bounded ingestion, execution is ordered:

```text
discover and hash
  -> validate paths and byte limits
  -> persist content-addressed objects
  -> append present typed media indexes
  -> append artifact_files
  -> return ArtifactRef values
```

`artifact_files` is the visibility root and is written last. A failed media
metadata scan cannot expose a common artifact row. Object bytes may already
exist after such a failure; that is safe because content-addressed objects are
immutable and unreferenced objects are not visible artifacts.

The service bounds both each file and the complete submission through
`ArtifactStoreConfig`. Required sources that match no files fail closed.

No claim or recovery state surrounds this pipeline. Callers retry by making a
new occurrence. If the content copy already completed, the retry reuses the
verified object.

## 8. Transcript ingestion

Coding-agent transcripts are a mission workflow, not an artifact backend.
`TranscriptIngestionService` composes three capabilities:

1. `RedactionService` snapshots and sanitizes the source before durability
2. `ArtifactService` stores the sanitized JSONL file
3. `IngestionService` appends normalized session and turn rows to
   `coding_agent_transcript_rows`

Every normalized row carries `source_artifact_id`, so queries can join the
narrative data to the common file index without persisting a duplicate asset
component. `world.transcript_rows()` returns the current run's normalized
session and turn rows. The original source digest may identify the input, while the
artifact SHA-256 always describes the sanitized bytes actually stored.

Quarantine and parse failures occur before artifact ingestion and publish
nothing. Re-ingesting a valid transcript records another artifact occurrence
and another keyed set of normalized rows.

## 9. Evaluation results

Evaluation is another tabular consumer of general ingestion. It pins the
world's visible component snapshot, runs the requested grader, and appends one
row to `evaluation_results`, keyed by `evaluation_id` inside the world run.

Reusing an evaluation ID with the same pinned subject and grader contract
returns the persisted result without grading again. Reusing it for a different
subject or contract fails loudly. This is application-level serialization and
catalog identity, not an artifact claim or cross-process lease protocol.

## 10. Security boundary

Generic artifact ingestion stores the bytes the caller submits. Workflows that
handle potentially secret-bearing content must sanitize before calling it.
Transcript ingestion does this by construction: unsafe metadata, symlinks,
unsupported containers, and unrewritable secret-bearing inputs are quarantined
before any object or catalog row becomes durable.

The common rule is simple: specialized workflows own pre-durability safety;
`ArtifactService` owns exact file persistence and indexing; `IngestionService`
owns catalog authority.

## 11. Task-anchored artifact context

`ArtifactContext` names one task-scoped interpretation of an artifact set. Its
UUIDv7 `context_id` identifies the interpretation; artifact UUIDs continue to
identify the individual ingestion occurrences. The contract does not create a
second storage service or copy the files again.

```python
from daft.ai.provider import load_openai

from archetype import ArtifactContext
from archetype.artifacts import analyze_artifacts, synthesize_artifact_context

context = ArtifactContext(
    task="Explain whether this change preserves immutable source identity."
)
provider = load_openai()

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

The first transform gives Daft one prompt per artifact, so files can be
analyzed in parallel. Every prompt carries the task, context ID, logical path,
MIME type, and the staged `daft.File`. Artifact contents are explicitly marked
as untrusted evidence rather than instructions. The second transform reduces
the attributed observations into one answer while retaining logical paths and
artifact IDs.

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
local Markdown + Python + git patch + sanitized Claude transcript
  -> content-addressed R2 objects
  -> R2 Data Catalog / Iceberg typed tables
  -> fresh catalog + fresh application graph
  -> cold artifact discovery
```

The test asserts the concrete audio, video, PDF, text, diff, transcript, and
common-index rows, then destroys its unique catalog namespace and R2 prefixes.
Local contract tests generate real WAV, MP4, and PDF fixtures and additionally
delete an acquisition source after object persistence, proving that metadata
scans use the staged object. Live model calls remain an explicit
credential-bearing external check; the deterministic contract tests validate
the task anchoring and source attribution without pretending that a mocked
provider is model evidence.
