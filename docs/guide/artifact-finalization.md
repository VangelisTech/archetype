# Artifact finalization

This specification defines the durable boundary between a sandbox-backed agent
attempt and Archetype's query layer. It is intentionally independent of Modal,
Apple Container, Codex, Claude Code, and any particular object store.

## 1. What is authoritative

An attempt has three different records. They are complementary; none replaces
the others.

| Record | System of record | Purpose |
|---|---|---|
| Archetype component rows | World storage | Attempt, validator, task-gate, commit, and finalization facts at each tick |
| Provider checkpoint | Modal image, Apple rootfs export, or another sandbox provider | Complete resumable filesystem and process substrate |
| Portable artifact bundle | Object storage plus the artifact Iceberg index | Independently readable evidence: manifests, traces, patches, Git bundles, `.context`, and declared outputs |

The full checkpoint is **not** copied into Parquet. Real Apple Container
exports are already multiple gigabytes, and some providers do not expose their
snapshot as bytes at all. The artifact index stores a
`storage_kind=provider_checkpoint` row containing the restorable provider
reference, its provider, and its expiration. Portable files are uploaded as
ordinary objects and carry hashes, sizes, and MIME types.

Telemetry is operational evidence, not the state authority. Losing a trace
must not make an otherwise indexed bundle disappear; losing the control record
or artifact index is a durability failure.

## 2. Identities and primary keys

One publication request is identified by:

```text
bundle_id = SHA256(domain, world_id, run_id, idempotency_key)
```

Reusing that key with the same canonical request returns the original receipt.
Reusing it with a different request digest is a conflict and fails closed.
The control row's immutable claim time supplies `created_at_ms` and any derived
retention deadline, so replaying a `PENDING` upload produces the same manifest
and index rows rather than resetting their lifecycle clock.

Each portable object is content-addressed within its bundle:

```text
artifact_id = SHA256(bundle_id, logical_path, content_sha256)
```

The artifact index primary identities are therefore:

- bundle: `(world_id, run_id, bundle_id)`
- artifact: `(bundle_id, artifact_id)`
- attempt lookup: `(world_id, run_id, attempt_id, artifact_id)`

The object destination is stable up to Daft's generated terminal filename:

```text
<object-root>/
  worlds/<world_id>/runs/<run_id>/attempts/<attempt_id>/
  bundles/<bundle_id>/objects/<artifact_id>/<generated-file-name>
```

The content-addressed folder is the retry key. If a worker crashes after an
upload but before recording it, the next worker lists that folder and reuses
the existing object instead of creating another logical artifact. Multiple
physical files in that folder are harmless orphans; reconciliation chooses a
stable first path and lifecycle cleanup may remove the rest.

## 3. Request and index contracts

`ArtifactBundleRequest` is immutable and credential-free. It carries:

- `world_id`, `run_id`, `entity_id`, `tick`, `attempt_id`, and
  `idempotency_key`;
- provider checkpoint identity, restorable flag, creation, and expiration;
- whether the attempt was accepted and its retention class;
- declared `ArtifactCandidate` values with immutable `source_ref`, portable
  `logical_path`, free-form `kind`, and required/recursive flags.

Provider-qualified source references are data, not live handles. The built-in
resolver supports direct files and
`apple-container-rootfs://<archive>#<path>`. The Modal experiment supplies a
resolver that reads a live sandbox or restores a `modal-image://` checkpoint.
Other providers implement `ArtifactSourceResolver`.

Every `ArtifactIndexRecord` is scalar and Parquet-friendly:

| Group | Fields |
|---|---|
| Identity | `schema_version`, `artifact_id`, `bundle_id`, `world_id`, `run_id`, `entity_id`, `tick`, `attempt_id`, `idempotency_key` |
| Classification | `kind`, `logical_path`, `storage_kind`, `mime_type` |
| Location | `source_ref`, `object_uri`, `checkpoint_provider`, `checkpoint_ref` |
| Integrity | `content_hash`, `size_bytes`, `restorable` |
| Lifecycle | `accepted`, `retention`, `created_at_ms`, `expires_at_ms` |

Portable objects require lowercase SHA-256 and a non-negative size. A provider
checkpoint uses an empty content hash and `size_bytes=-1` when the provider
does not expose those values. The generated `bundle_manifest` describes every
payload object and the checkpoint. It does not recursively include its own
record.

MIME detection uses `daft.functions.guess_mime_type` on the bytes and falls
back to the logical filename's registered MIME type. Upload uses
`daft.functions.upload`; R2 is supplied through Daft's S3-compatible
`IOConfig`. Credentials remain in that process-local configuration and are
never serialized into requests, control rows, manifests, or the index.

## 4. Publication state machine

The control catalog records the canonical request **before external I/O**.

```text
             upload every declared object
PENDING  ───────────────────────────────────▶  UPLOADED
   │                                                │
   │ retry window elapsed                          │ append/confirm Iceberg rows
   ▼                                                ▼
EXPIRED                                          INDEXED
```

- `PENDING`: the checkpoint-qualified replay request is durable. A reconciler
  may still need to reopen the checkpoint and upload files.
- `UPLOADED`: `records_json` and the bundle-manifest URI are durable in the
  control catalog. Indexing no longer depends on the sandbox or checkpoint.
- `INDEXED`: all deterministic rows are present in the Iceberg artifact index.
  This is the portable publication success state.
- `EXPIRED`: the retry window elapsed while still `PENDING`. `UPLOADED`
  publications never expire; they no longer need the provider and must be
  driven to `INDEXED`.

`published` is intentionally not part of this state machine. Git branch push,
PR creation, and merge are software-delivery states. They may reference the
same attempt and bundle but do not alter whether evidence is durably indexed.

The Iceberg append is the query visibility point. If a process dies after the
append but before marking the control row `INDEXED`, a retry reads and verifies
the deterministic bundle rows, performs no second append, and completes the
control row. A claimant can still lose its lease after an Iceberg commit but
before catalog completion; in that narrow split-brain case an identical
physical append is possible because Iceberg does not enforce row uniqueness.
The service query applies a lazy exact-row `distinct`, so the artifact key has
exactly-once logical visibility. Periodic Iceberg compaction may remove those
rare physical duplicates without changing the index contract.

### Crash matrix

| Crash point | Durable state | Recovery action |
|---|---|---|
| Before claim | Nothing | Submit the same request |
| After claim, before upload | `PENDING` + replay request | Reopen checkpoint, upload |
| During upload | `PENDING` + some content-addressed folders | Reuse present folders, upload missing objects |
| After upload metadata | `UPLOADED` + complete records | Index without reopening checkpoint |
| After Iceberg commit | Query rows visible, control row `UPLOADED` | Verify rows, mark `INDEXED` |
| After completion | `INDEXED` | Return duplicate receipt |

## 5. Reconciler contract

`ArtifactService.reconcile(world_id, limit=N)` is one bounded, idempotent pass.
It does not run forever inside an API request.

1. Enumerate nonterminal publications whose lease is due.
2. CAS-acquire one publication, retaining its current phase and incrementing
   `attempt_count`.
3. If `PENDING`, expire it only when `retry_until_ms` has elapsed; otherwise
   materialize the recorded sources, reuse/upload objects, and atomically store
   `records_json` as `UPLOADED`.
4. If `UPLOADED`, append or verify all deterministic Iceberg rows.
5. Mark `INDEXED`; on a transient error, record `last_error` and a new
   `lease_expires_at` for backoff.

The SQLite catalog is the single-host reference implementation. The remote
catalog implements the same CAS transitions in the per-world Durable Object.
A fleet reconciler enumerates worlds from the directory object, then performs
bounded per-world passes. It can shard by world without cross-world locking.

The default lease is 15 minutes and the default retry window is seven days.
Long operations renew the lease. Reconciler attempts are expected to be
at-least-once; object placement and index verification make their effects
idempotent.

## 6. Lifecycle policy for review

There are two independent clocks:

1. **Provider checkpoint TTL.** Set by the provider and recorded exactly. It
   must outlive the publication retry window. Modal currently defaults to 30
   days; local Apple exports have no automatic deletion until a cleanup policy
   is configured.
2. **Portable object retention.** Derived from the request class unless an
   explicit `artifact_expires_at_ms` is supplied.

Default portable retention:

| Class | Default | Intended use |
|---|---:|---|
| `attempt` | 30 days | Rejected/abandoned attempts and debugging evidence |
| `run` | 180 days | Accepted attempts and completed mission evidence |
| `durable` | No automatic expiry | Promoted datasets, audit evidence, or legal retention |

Control-catalog rows and artifact-index metadata are not automatically deleted;
they are the audit trail even after object expiry. A garbage collector should
query `expires_at_ms`, delete object bytes idempotently, and retain or tombstone
the row. An R2 bucket-wide lifecycle rule longer than the largest ordinary
retention (for example 365 days) is a disaster-recovery ceiling, not the
primary policy, because lifecycle rules cannot evaluate index columns. Provider
checkpoints must not be deleted until the bundle is `INDEXED` and no task or
branch resume policy still references them.

## 7. Query, authorization, and telemetry

- Operators publish and reconcile through the existing `INGEST_FACT`
  permission class. Viewers query through `QUERY_WORLD`.
- `RuntimeWorld.artifacts()` queries the handle's current run. The lower-level
  service accepts explicit world/run keys and works after world destruction or
  process restart.
- `artifact.publish`, `artifact.upload`, and `artifact.index` OpenTelemetry
  spans carry `world_id`, `run_id`, `entity_id`, `tick`, `attempt_id`, and
  `idempotency_key`; stage spans also carry bundle and artifact counts.
- Agent CLI JSONL and sandbox-side OTel output are portable artifacts. Shipping
  live sandbox spans to an OTel collector is complementary and must use the
  same correlation attributes. It is not required for publication correctness.
- Provider and model secrets are process capabilities. They must not appear in
  sandbox manifests, artifact requests, control rows, object paths, traces, or
  index rows.
- A reconciler restoring a checkpoint for artifact extraction receives only
  the sandbox-provider credential. It must not require or inject the model or
  GitHub secret; the Modal resolver enforces this separation.

## 8. Relationship to tick progression

An Archetype tick records an attempt whether it is accepted or rejected. A
validator failure does not abort persistence of that tick. What is gated is the
task state transition: a mission may require `checkpointed` or `indexed`
finalization before moving to the next task.

The example currently proves both boundaries explicitly: its processor gates
task advancement on a restorable provider checkpoint, and its driver publishes
every recoverable accepted or rejected attempt and queries the portable bundles
before teardown. Attempts whose provider checkpoint failed remain visible as
Archetype facts but cannot claim durable artifact publication. A mission that requires
`indexed` atomically with task advancement should invoke the same artifact
service from its transition authority before returning the next task state;
the service contract and idempotency key do not change.
