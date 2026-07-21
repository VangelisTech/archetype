# Storage, ingestion, and artifact execution refactor

**Status:** Implemented and dogfooded; PR verification awaits the concurrent documentation rewrite
**Date:** 2026-07-21  
**Tracking pull request:** #619

## Objective

Finish the Agent Missions cleanup by making the data path as coherent as the
mission state machine:

- `StorageService` is the single application authority for Archetype-owned
  Daft execution, catalog access, table persistence, ordering, and retry.
- `IngestionService` adds application identity and selects a storage
  operation; it does not execute Daft or implement storage mechanics.
- `ArtifactService` is the one file-occurrence workflow, composed over general
  ingestion rather than a second durability kernel.
- `archetype.ingestion` contains reusable lazy file and media transforms, not
  application table descriptors or durable authority.
- SQLite remains the single-host control authority and the Durable Object its
  cross-host implementation. Iceberg and object storage remain the data plane.

This plan preserves the already-completed Mission V1, orphan-family moves,
legacy attempt-kernel removal, artifact claim removal, and first multimodal R2
proof.

## Implementation result

The implementation now matches the target dependency flow:

- `StorageService` owns the reentrant application Daft lane, app-table Catalog
  operations, schema alignment, plain and conditional append, and optimistic
  conflict retry.
- `IngestionService` owns only the world/run envelope and append selection.
- one `FileIngestionPipeline` owns the visible file graph; `scanners.py` owns
  only pure bounded stream parsing; one `ArtifactService` composes the workflow.
- architecture enforcement rejects app-layer Daft collection, direct Iceberg
  operations, Catalog table creation, retry ownership, and unmediated Python
  row conversion outside storage.
- 1,667 non-documentation source contracts pass, the static profile passes,
  the documentation site builds, and the contract/spec eval suites pass.
- the live Cloudflare R2 dogfood uploaded eight file occurrences plus one
  sanitized transcript artifact, cold-opened a fresh application graph, and
  queried all eight populated tables. Image, audio, video, PDF, Markdown, code,
  patch/diff, and transcript rows were joined back to the common index; UUIDv7
  timestamps, SHA-256, XXH3-64, and Daft MIME classification were verified.

Automatic capture of arbitrary sandbox outputs by `MissionService` was not
added. Mission V1 already persists the state-transition evidence it owns;
choosing which sandbox files become artifacts remains a separate explicit
workflow rather than hidden post-processing in the mission kernel.

## Contract card

**Behavior:** Archetype-owned terminal Daft work is admitted through one
storage-owned execution boundary. Typed ingestion and artifact workflows build
lazy plans above it. Durable rights come from the existing control catalog;
Iceberg commits data. No artifact claim, finalizer, publication receipt, or
reconciler is reintroduced.

**Owning layer:** `archetype.app.storage`, with semantic plan construction in
`archetype.app.ingestion`, `archetype.app.artifacts`, and
`archetype.ingestion`.

**Normative sources:** application architecture, service protocols, atomic
visibility, artifacts and ingestion, and the umbrella specification.

**Existing executable oracles:** storage pooling and identity contracts,
atomic tick visibility and stale-writer tests, ingestion contracts, artifact
ingestion contracts, evaluation lease tests, and the Cloudflare R2
infrastructure dogfood.

**Invariants at risk:** lazy Daft execution, one materialization of generated
artifact identity, append-only world history, tick commit ordering, stale
writer exclusion, injected-session identity, cold table discovery, explicit
`IOConfig`, transcript redaction before durability, and common-index-last
artifact visibility.

**Required validation:** focused unit and integration contracts, a real
two-writer Iceberg conflict test, tick-versus-ingestion admission, SQLite and
Durable Object parity for changed control semantics, static ownership audits,
full PR verification, and the cold R2 round trip.

**Documentation affected:** application architecture, service protocols,
artifacts and ingestion, specification, contract traceability, generated API
reference, and examples that teach the affected path.

## Final dependency flow

```text
Mission / transcript workflow
            |
            v
      ArtifactService
            |
            v
 FileIngestionPipeline
            |
            v
     IngestionService  <--- evaluation and other typed-row producers
            |
            v
       StorageService  <--- application-driven ticks and owned queries
       |             |
       |             +--- SQLite / Durable Object control catalog
       |                  fences, leases, manifests, commands, outbox
       |
       +--- Daft Session / Catalog / execution admission
            |
            +--- Iceberg tables and content-addressed object storage
```

The control catalog decides whether a logical operation may publish. The
storage service decides when and how Archetype submits Daft work. Daft decides
how that work is parallelized. Iceberg decides whether a data commit is atomic.

## Ownership

### StorageService

`StorageService` owns:

- the configured Daft `Session`, `Catalog`, namespace, and `IOConfig`;
- pooling and identity binding for stores and control catalogs;
- the application-wide admission gate for Archetype-owned Daft execution;
- narrow materialization boundaries;
- direct `read_iceberg` and `write_iceberg` calls;
- table registration, discovery, schema comparison, and snapshot lookup;
- plain append and conditional append;
- retry of optimistic commit conflicts; and
- access to durable coordination implemented by SQLite or the Durable Object.

It does not own artifact identity, evaluation identity, mission transitions,
redaction policy, or media classification.

The managed-execution contract applies to terminal work initiated by Archetype
services. A caller that receives a lazy Daft `DataFrame` and collects it
directly owns that execution. The supported DataFrame-first query surface is
not wrapped in a proxy merely to hide Daft.

### IngestionService

`IngestionService` owns:

- resolving the durable world and current run;
- adding the `world_id` and `run_id` envelope;
- selecting plain or conditional append; and
- supplying semantic identity columns for a conditional append.

It owns no execution lock, catalog facade, Iceberg call, retry loop, or
materialization. `IngestionTable` and `TableVersion` are not reusable family
contracts and will be removed. Table names remain next to their application
producer. Daft's catalog validates identifiers; Archetype does not duplicate
that validation with a regular expression.

### FileIngestionPipeline

The reusable pipeline under `archetype.ingestion` owns lazy transforms for:

- file discovery with `daft.from_files`;
- `daft.File.mime_type()` classification;
- one UUIDv7 artifact occurrence identity and its derived timestamp;
- portable logical paths;
- one streaming pass for SHA-256, XXH3-64, and byte size;
- content-addressed object destinations;
- common file metadata; and
- image, audio, video, PDF, text/code, and diff metadata projections.

It has no world, run, catalog, storage service, lock, or durable publication
state. Daft expressions that compose a media branch remain in the same primary
pipeline module. Pure stream parsers may live in a small scanner module.
Resize, resample, transcode, OCR, thumbnail, and embedding workflows are added
only when implemented and produce derivative artifacts rather than changing
submitted bytes in place.

### ArtifactService

`ArtifactService` is the sole file-artifact application service. It owns:

- source and logical-path policy;
- artifact byte and batch limits;
- object-root selection;
- configuring the file pipeline once per ingestion;
- requesting storage-owned materialization of discovery and persistence;
- ordering typed indexes before the common visibility root; and
- returning supported `ArtifactRef` values.

`ArtifactSource`, `ArtifactRef`, `ArtifactStoreConfig`, and `ArtifactContext`
remain supported Pydantic contracts under `archetype.artifacts`.

### Consumers

- Transcript ingestion redacts first, stores the sanitized JSONL through
  `ArtifactService`, then sends normalized rows through `IngestionService`.
- Evaluation pins visible world state, obtains its durable execution right,
  grades, and sends the typed result through `IngestionService`.
- Audit retains locks that protect its in-memory bounded buffer, but delegates
  table execution and commit retry to `StorageService`.
- Mission state stores commit, checkpoint, filesystem, friction, transcript,
  and artifact references; file bytes remain in object storage.
- Application-driven ticks enter the same local Daft admission boundary while
  preserving the existing compute, append, flush, and manifest protocol.

## Target files

```text
src/archetype/
  ingestion/
    __init__.py
    pipeline.py       # FileIngestionPipeline and the visible Daft graph
    scanners.py       # pure stream parsers only
  artifacts/
    __init__.py
    contracts.py      # supported Pydantic values
    context.py        # task-anchored lazy analysis transforms
  app/
    storage/
      service.py      # execution, catalog, table I/O, ordering, retry
      interfaces.py
      session.py
      catalog.py      # SQLite control authority
      remote_catalog.py
      commit.py       # existing world fence and manifest coordinator
      signatures.py
    ingestion/
      service.py      # world/run envelope and append selection
      interfaces.py
    artifacts/
      service.py      # complete file-occurrence workflow
      interfaces.py
    missions/
      service.py
      transcript_service.py
      trajectory_service.py
      interfaces.py
```

`src/archetype/app/storage/iceberg.py`,
`src/archetype/ingestion/contracts.py`, and the one-wrapper-per-media modules
are removed after their behavior moves to the owning files above.

## Coordination semantics

### Local execution admission

The first implementation uses one storage-owned serial admission lane for
Archetype-owned terminal Daft jobs in a service container. Lazy plan
construction remains outside the lane. The coordinated section begins when a
plan is materialized or a table operation executes and ends after durable
visibility is known.

The boundary can later become a scheduler or multiple explicit non-conflicting
lanes without changing producers. This refactor does not design distributed
Daft scheduling.

### Durable rights

Durable coordination stays scope-specific:

| Operation | Local admission | Durable authority |
|---|---:|---|
| World tick | yes | existing writer epoch and manifest |
| Evaluation | yes | existing evaluation execution lease |
| Artifact/transcript occurrence append | yes | none |
| Audit append | yes | none |
| Conditional append | yes | no cross-host uniqueness claim unless explicitly added |
| Caller collection of returned lazy data | caller-owned | none |

A fence is a monotonically increasing generation that makes every earlier
holder stale. It does not stop already-running work. Existing world rows carry
the fence epoch plus a commit-attempt token; the control catalog rejects stale
manifest publication, so stale physical rows remain invisible.

No generic execution ledger or artifact claim table is introduced merely to
serialize append-only ingestion.

### Ordered table writes

Storage provides two mechanisms:

1. Plain append materializes once, appends, and reports the resulting durable
   table state needed by the caller.
2. Conditional append reads current keys, anti-joins, materializes once, and
   appends. An optimistic conflict retries the complete read/anti-join/write
   operation against the latest snapshot.

The producer supplies semantic keys. Storage owns the execution sequence.
Ordinary artifact occurrences use plain append because every submission has a
fresh UUIDv7 identity.

## Artifact flow

```text
submit ArtifactSource values
  -> resolve world, run, tick, object root, and limits
  -> build lazy discovery / identity / hash graph
  -> StorageService materializes once
  -> build content-addressed persistence graph
  -> StorageService materializes durable objects
  -> reopen the immutable object for specialized metadata
  -> build typed index frames
  -> IngestionService adds world/run identity
  -> StorageService appends typed indexes
  -> StorageService appends artifact_files last
  -> return ArtifactRef values
```

The common index is the visibility root. Unreferenced content-addressed bytes
or typed rows from a failed attempt are safe dead data and may be reclaimed by
a future maintenance contract. A retry records a new artifact occurrence and
reuses verified content bytes.

## Implementation sequence

1. Add executable storage contracts for admission, direct table I/O, and real
   optimistic-conflict behavior.
2. Move application table reads, appends, schema comparison, snapshot lookup,
   and retry into `StorageService`; delete `IcebergCatalogContext`.
3. Move the ingestion anti-join sequence into storage and reduce
   `IngestionService` to world/run enrichment and operation selection.
4. Consolidate file and media transforms into `FileIngestionPipeline`; remove
   Python MIME fallback and fake ingestion contracts.
5. Simplify `ArtifactService` around the pipeline and storage materialization.
6. Migrate transcript, evaluation, audit, application query, and mission
   artifact paths.
7. Admit application-driven ticks through the same local execution boundary
   without changing `src/archetype/core`.
8. Add architecture and static checks for materialization, direct Iceberg
   access, table registration, and storage-lock ownership.
9. Reconcile normative docs, contract registry, generated references, and
   teaching examples.
10. Run focused concurrency tests, complete PR verification, cold R2 dogfood,
    and the redacted transcript-to-artifact handoff through a cold query.

Each step is an atomic commit and is pushed to the existing branch. The PR is
opened and reviewed normally; it is not auto-merged manually.

## Acceptance criteria

- `IcebergCatalogContext`, `IngestionTable`, and `TableVersion` are gone.
- No application family other than storage owns a table-execution lock or
  optimistic Iceberg retry loop.
- Archetype-owned app materializations are admitted through `StorageService`.
- Direct app-layer `read_iceberg` and `write_iceberg` calls live in storage.
- Ingestion contains no catalog facade or execution mechanics.
- File ingestion has one readable primary Daft pipeline.
- Artifact ingestion remains one service and the common index remains last.
- World atomic-visibility and stale-writer contracts remain unchanged.
- Transcript redaction still precedes all durability.
- Evaluation concurrency remains one paid grader execution per identity.
- Fresh-application local and Cloudflare R2 reads discover every durable table.
- Static, contract, process, integration, documentation, and R2 profiles pass.

## Explicit non-goals

- changing core processor or tick semantics;
- designing a distributed Daft scheduler;
- wrapping the supported lazy DataFrame API in an execution proxy;
- adding cross-host uniqueness to every append;
- adding media preprocessing before concrete derivative workflows exist;
- reintroducing artifact claims, receipts, finalizers, bundles, or recovery
  state machines; or
- implementing the V1 task decomposer or HTN planner.
