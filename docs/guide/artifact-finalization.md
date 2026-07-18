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

### Live sandbox observation

A remote coding attempt must expose progress before it completes. Modal mission
sandboxes maintain two fixed files under
`<workspace>/.archetype-agent/live/`:

- `session.json` is the latest status event and is safe to poll.
- `events.jsonl` is the append-only phase and heartbeat history.

The active canonical agent stdout and stderr paths are carried in each status
event. Codex and Claude Code output is streamed to the launching process as it
arrives and tee'd to those in-sandbox files. While the agent process is quiet,
an `agent_running` heartbeat is emitted at least every configured heartbeat
interval. Heartbeats report stdout, stderr, and total byte counts plus the age
of the most recent agent output, so a connected-but-silent CLI is distinguishable
from an agent that is actively producing events. Phase events cover sandbox
readiness, attempt start, agent execution, each validator, commit, recovery
evidence capture, checkpoint, artifact publication, completion, and teardown. Every event
carries `sandbox_id`, harness, attempt identity when known, and the world/run/
entity/tick correlation map supplied by the caller.

The transport closes the child process's stdin immediately after launch.
Remote process APIs commonly expose stdin as an open pipe; Codex and Claude
Code may wait for additional prompt input and emit no session event until that
pipe reaches EOF. Supplying the complete prompt as an argument and closing
stdin is therefore part of the noninteractive execution contract.

An independent operator can attach by provider sandbox ID; no model credential
is required for the monitor process. Observation is deliberately read-only and
must not extend the sandbox idle timeout by executing commands inside it.
Modal may briefly reject filesystem reads while it snapshots the sandbox. A
following monitor preserves its byte offsets and retries for a bounded grace
period, emits interruption and reconnection records, and treats
`sandbox_closing` as the clean terminal event. Exhausting the grace period is an
explicit `monitor_disconnected` result, never an implied mission success.

These files are operational evidence, not the attempt authority. A checkpoint
captures the stream through `checkpoint_started`; events emitted after the
provider returns the checkpoint reference remain on the live sandbox and are
ingested before teardown. The persisted attempt receipt, Archetype component
row, checkpoint reference, and portable artifact index remain authoritative if
the live observer disconnects.

## 2. Identities and primary keys

One publication request is identified by:

```text
bundle_id = SHA256(domain, world_id, run_id, idempotency_key)
```

Reusing that key with the same producer request returns the original receipt.
Reusing it with a different producer request digest is a conflict and fails
closed. The digest excludes the service-bound `redaction_policy_id`: upgrading
the scanner must not turn replay of an already indexed bundle into an identity
conflict. The persisted canonical request still records the exact policy that
processed the payload.
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
`apple-container-rootfs://<archive>#<path>`. The Modal provider supplies a
resolver that reads a live sandbox or restores a `modal-image://` checkpoint.
Other providers implement the stable
`ArtifactSourceResolver.materialize(candidates, destination)` contract.

Resolvers that can preflight provider objects implement the optional
`BoundedArtifactSourceResolver.materialize_bounded` capability. The service
passes the configured per-artifact and aggregate bundle byte limits through
that capability. Archive resolvers must reject a member from its declared
size before copying its bytes and must account for repeated and recursive
selections cumulatively. Legacy two-argument resolvers remain supported and
are checked after materialization, but they cannot provide resource control
during the copy itself. The service always validates materialized files again
before Daft reads them; bounded preflight is the resource control, while the
second check defends against legacy resolvers and stale or dishonest metadata.

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

## 4. Pre-durability secret redaction

`RedactionService` is the single provider-neutral authority for data that may
cross from an agent or sandbox into a durable control row, object, index,
trace, or external event stream. Artifact publication consumes only its
`iRedactionService` port. Provider adapters, collectors, and proxies must use
the same port rather than implementing separate regular-expression filters.

The artifact handoff applies the following dispositions:

| Input | Pre-durability disposition |
|---|---|
| Request identity, source/checkpoint references, logical paths, object-store roots, returned object URIs, index strings | Scan before the catalog claim or next durable write; quarantine on any finding because rewriting identity would break recovery |
| UTF-8 text, JSON/JSONL, logs, patches, and declared text artifacts | Copy into a controlled local snapshot, redact deterministically, then hash and upload the sanitized bytes |
| Opaque binary | Scan the complete byte stream for the shared high-confidence corpus; quarantine on a finding because byte replacement may corrupt the artifact |
| Tar or ZIP archive | Traverse regular members without extraction, validate every member name, enforce declared and observed member/count/expanded-byte bounds, and quarantine on a finding, link or special member, encrypted member, unsafe member path, nested/disguised container, or incomplete inspection |
| Known credential path such as `.codex/auth.json`, `.claude/.credentials.json`, `.config/opencode/auth.json`, `.aws/credentials`, `.git-credentials`, `.netrc`, or a private SSH key | Quarantine by path even when content patterns do not recognize the credential |

The scanner covers Codex/OpenAI, Claude/Anthropic, GitHub, Modal, OpenRouter,
AWS and common cloud formats, OAuth access/refresh tokens, signed URLs,
private-key blocks, Authorization/Bearer headers, and generic sensitive
assignments. This is defense in depth, not a claim that arbitrary encrypted or
proprietary binary encodings can be semantically decoded. Uninspectable
containers fail closed; other opaque binaries receive complete byte-pattern
scanning. Credential files must still be excluded from mission checkpoints and
artifact declarations by construction.

The order is authoritative:

1. Bind the active scanner `policy_id` into a copy of the immutable request.
2. Scan all request and destination metadata before the control-catalog claim.
3. Persist the credential-free claim before provider or object-store I/O.
4. Materialize sources, copy each file into a controlled snapshot, and scan or
   redact that copy.
5. Compute MIME type, size, and content hash from exactly the approved copy;
   upload those same bytes.
6. Generate and scan the manifest, validate every index string, then record
   upload metadata and append the index.

Every retry diagnostic is also redacted and bounded before the control catalog
records it. Quarantine exceptions retain only rule identifiers—not the matched
value or caller-provided scope—and the HTTP adapter maps them to a fixed 422
detail. Reconciler logs emit error types rather than raw provider diagnostics,
so an untrusted exception cannot turn the retry ledger or reconciliation log
into a credential side channel.

The caller's source and the provider checkpoint are never mutated. This both
preserves resumability and prevents a source-file change between scan and
upload from bypassing the gate. Snapshot copying opens a verified regular-file
handle without following symlinks and rejects an inode swap between inspection
and open; hashes and uploads then consume only that controlled copy.

`ArtifactBundleRequest.redaction_policy_id` is empty at ingress and bound by
the service. Its canonical durable form always contains the active policy ID.
A caller-supplied mismatch fails before the claim. A reconciler may resume a
`PENDING` publication only when that exact policy implementation remains
available; policy deployments must therefore retain old implementations or
drain their pending claims. Reapplying a compatible policy is deterministic,
and content-addressed placement reuses prior sanitized uploads. `UPLOADED`
recovery and `INDEXED` replay no longer need the retired implementation because
their approved bytes and safe records are already durable; the current policy
still scans their metadata before index or receipt visibility.

Deployments upgrading an existing catalog must treat rows without a bound
policy explicitly. Legacy `INDEXED` rows remain queryable historical evidence,
but their payload bytes are not retroactively certified by this gate and must
not be labeled redaction-approved. Legacy `PENDING` rows fail closed rather
than reopening a checkpoint under an unknown policy; an operator must expire
and republish them through the current scanner. Legacy `UPLOADED` rows may be
indexed only after their durable metadata passes the current scanner, while
retaining the same historical limitation for already-uploaded payload bytes.

Successful manifests contain only safe evidence: policy ID, clean/redacted
status, file and byte counts, redaction count, rule IDs, and per-file receipts.
Receipts and quarantine errors never retain or echo matched text. A text
finding is redacted; a metadata, credential-file, archive, or opaque-binary
finding raises `SecretQuarantineError`. Metadata quarantine creates no claim.
Payload quarantine leaves a retryable `PENDING` claim with a safe diagnostic,
no object/index visibility, and the original provider checkpoint available for
operator recovery.

Sandbox live events, sandbox-side spans, OTel export, and policy-controlled L7
traffic capture are required to consume this same authority before their
respective durable/external writes. Those integrations extend this contract;
they do not weaken the artifact gate while their transports are developed.

## 5. Publication state machine

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

## 6. Reconciler contract

`ArtifactBundleService.reconcile(world_id, limit=N)` is one bounded,
idempotent pass.
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

## 7. Lifecycle policy for review

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

## 8. Query, authorization, and telemetry

- `RuntimeApplication` owns actor-free publish, reconcile, and query semantics;
  an untrusted API adapter must apply gateway authorization before invoking it.
- `RuntimeWorld.artifact_bundles()` queries the handle's current run. The
  lower-level service accepts explicit world/run keys and works after world
  destruction or process restart.
- `artifact.publish`, `artifact.upload`, and `artifact.index` OpenTelemetry
  spans carry `world_id`, `run_id`, `entity_id`, `tick`, `attempt_id`, and
  `idempotency_key`; stage spans also carry bundle and artifact counts.
- Agent CLI JSONL and sandbox-side OTel output are portable artifacts. Shipping
  live sandbox spans to an OTel collector is complementary and must use the
  same correlation attributes. It is not required for publication correctness.
- Every portable payload and generated manifest passes the pre-durability gate
  in section 4. Live telemetry exporters must apply the same policy before
  enqueueing or sending a span; an observability outage or quarantine never
  changes mission state authority.
- Provider and model secrets are process capabilities. They must not appear in
  sandbox manifests, artifact requests, control rows, object paths, traces, or
  index rows.
- A Modal subscription credential lives in a named Volume mounted only into a
  separate auth-broker Sandbox. The broker stages only the selected CLI's
  credential file for agent execution, atomically persists any refresh, and
  removes the mission copy before validators, filesystem manifests, or provider
  snapshots. The auth Volume itself is never part of a mission checkpoint.
- A reconciler restoring a checkpoint for artifact extraction receives only
  the sandbox-provider credential. It must not require or inject the model or
  GitHub secret; the Modal resolver enforces this separation.

## 9. Relationship to tick progression

An Archetype tick records an attempt whether it is accepted or rejected. A
validator failure does not abort persistence of that tick. What is gated is the
task state transition: a mission may require `checkpointed` or `indexed`
finalization before moving to the next task.

The example currently proves both boundaries explicitly: `MissionService` gates
task advancement on a restorable provider checkpoint, and its driver publishes
every recoverable accepted or rejected attempt and queries the portable bundles
before teardown. Attempts whose provider checkpoint failed remain visible as
Archetype facts but cannot claim durable artifact publication. A mission that requires
`indexed` atomically with task advancement should invoke the same artifact
bundle service through the application path before returning the next task state;
the service contract and idempotency key do not change.
