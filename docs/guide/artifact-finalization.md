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

One portable object is the sanitized full-worktree archive at
`recovery/full-worktree.tar` (`kind=worktree_archive`). It is distinct from
the provider checkpoint, sanitized Git bundle, binary patch, declared result
files, and generated bundle manifest; each retains its own artifact identity,
object URI, hash, and lifecycle row. Provider checkpoint expiry or deletion
therefore cannot erase the indexed worktree archive.

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

This value is both the bundle ID and the prepared request's deterministic
`publication_key`. Reusing it with the same producer request returns the
original receipt. Reusing it with a different producer request is a conflict
and fails closed.

Preparation binds two deliberately different digests:

- `producer_digest` hashes the logical producer request without the
  service-bound `redaction_policy_id`. It is the stable catalog conflict
  identity across scanner upgrades.
- `request_digest` hashes the exact canonical `request_json`, including the
  bound `redaction_policy_id`. It authenticates the bytes that one in-flight
  publication must replay.

`PreparedArtifactBundleRequest` contains that canonical JSON, both digests,
the publication key, and the policy ID. The five values are validated as one
identity. Preparation binds and scans metadata but performs no source,
object-store, catalog, or index I/O. Publication revalidates all five values
before its first catalog operation. The artifact control row retains the exact
canonical request even though its historical conflict-digest field uses the
policy-independent producer digest.

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
an existing object only after reading it back and verifying its exact bytes
and size. A truncated, corrupt, or same-size wrong payload is replaced before
metadata can reach `UPLOADED`; the bundle manifest is verified the same way.
Multiple physical files in that folder are harmless orphans; reconciliation
chooses a stable verified path and lifecycle cleanup may remove the rest.

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

Before upload metadata becomes durable, and again before recovery, indexing,
or receipt construction, the service authenticates the complete record set
against the exact request. All rows must preserve its identity and lifecycle;
portable IDs and content-addressed object folders must be deterministic; there
must be exactly one bound checkpoint and one bound manifest; and every payload
row must match exactly one declared direct or recursive candidate. A resolver
or corrupted control row cannot redirect evidence into another publication.

MIME detection uses `daft.functions.guess_mime_type` on the bytes and falls
back to the logical filename's registered MIME type. Upload uses
`daft.functions.upload`; R2 is supplied through Daft's S3-compatible
`IOConfig`. Credentials remain in that process-local configuration and are
never serialized into requests, control rows, manifests, or the index.

### Portable full-worktree archive

The archive format is `archetype-worktree-tar-v1`: an uncompressed POSIX PAX
tar with normalized `uid`, `gid`, owner/group names, and `mtime`. Members are
ordered by portable path; modes are retained; links and special members are
forbidden. Identical approved repository state, Git identities, exclusions,
and redaction policy therefore produce identical bytes.

`archive-manifest.json` has `schema_version=1` and records:

- baseline and end-state `HEAD` identities plus the archive-format identity;
- every approved path, type, mode, byte size, and SHA-256 hash;
- every policy exclusion with path, observed type, and reason; and
- the bound redaction policy plus aggregate and per-file safe receipts.

The `worktree/` subtree captures tracked, untracked, ignored, rejected, and
uncommitted repository files, including policy-allowed `.context` content.
Raw `.git` internals are excluded. `recovery/` instead contains the separately
generated status, binary patch, and sanitized Git bundle needed to reconstruct
Git state. Credential paths, auth material, caches, provider internals, and
archive staging are excluded by construction and recorded. A symbolic link,
hard link, device, FIFO, socket, path escape, or unreadable/incomplete file
fails the capture rather than being represented ambiguously.

Capture inventories the tree before and after copying. Every regular file is
opened without following links, its device/inode/size/time identity is checked
against the inventory, and that same handle supplies the staged bytes. A
change before, during, or after a read fails the attempt's evidence capture.
The raw provider-side tar is not durable evidence. During artifact publication
the application validates its manifest and exact member set, sanitizes every
regular member through `iRedactionService`, rebuilds the deterministic tar,
binds the active policy and safe receipts into the manifest, and only then
hashes and uploads it. Text findings are rewritten; credential paths, opaque
secret-bearing bytes, nested containers, invalid metadata, and incomplete
inspection quarantine before object or index visibility.

Restore requires a clean directory and optionally the expected indexed object
hash. It validates the complete tar and manifest before writing, rejects any
undeclared, missing, linked, special, oversized, escaped, or hash-mismatched
member, and creates every output beneath no-follow directory handles. It then
rehashes the reconstructed bytes. This round-trip is the executable proof that
the approved tree and recovery material remain usable without the provider
snapshot.

## 4. Pre-durability secret redaction

`RedactionService` is the single provider-neutral authority for content-bearing
data that may cross from an agent or sandbox into a durable control row,
object, index, or external event record. Artifact publication consumes only
its `iRedactionService` port. Provider adapters, collectors, and proxies must
use the same port rather than implementing separate regular-expression
filters.

The lower `archetype._obs` signal boundary is also imported by core and cannot
depend on this application family. It prevents signal leakage structurally:
fixed names, fixed keys, exact value validators, bounded enums, no arbitrary
string conversion, and no raw exception recording. A content-bearing outer
export adapter applies `iRedactionService` as defense in depth before its own
external write. See the [Observability contract](observability.md).

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

Every resume re-scans the active process-local object-store root before object
reuse, upload, or index access. A changed or secret-bearing `StorageConfig`
cannot use an already-durable safe request to redirect publication; the row
remains retryable and only a sanitized diagnostic may be recorded.

For mission-owned indexed finalization there are two ordered durable claims.
First, the mission claim stages the complete `PreparedArtifactBundleRequest`
identity and sanitized provisional outcome, reaching `finalizing`. The outcome
carries four independently checked linkage markers: publication key, exact
request digest, producer digest, and redaction-policy identity. Only then may
the artifact service acquire its publication row and start step 3 above.
After artifact-catalog acquisition, the catalog's exact `request_json` is
authoritative and the caller's decoded object is discarded. This upstream
mission claim and downstream artifact row form the finalization outbox; neither
may regenerate a request from current world state during recovery.

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

Content-bearing sandbox live events, portable sandbox-side span records, and
policy-controlled L7 capture consume this same authority before their durable
or external writes. Closed-schema `_obs` signals carry no such content. Those
integrations extend this contract; they do not weaken the artifact gate while
their transports are developed.

## 5. Publication state machine

The artifact control catalog records the canonical request **before external
I/O**. For a mission requiring indexed evidence, its claim records the exact
prepared request even earlier:

```text
provider_acknowledged
        │ stage sanitized outcome + exact prepared request
        ▼
    finalizing  ── authenticate terminal row + sealed settle ──▶  settled
        │
        └── artifact outbox: PENDING ──▶ UPLOADED ──▶ INDEXED

    settled  ── require_settled + private row transform + tick ──▶ completed row
```

The `finalizing` edge is authoritative authorization for publication. It is
available to recoverable provider-accepted and provider-rejected attempts; a
rejected attempt is indexed for review but remains rejected and never advances
the task. Both require a restorable checkpoint. An accepted outcome also
requires a commit SHA. Changed prepared identity or provisional outcome is a
conflict, while an exact lost-response staging retry is idempotent.

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
  driven to `INDEXED`. When an indexed mission owns the exact expired row, its
  finalizer preserves the staged provider evidence, adds the generic
  `artifact_publication_expired` marker, and settles that attempt incomplete or
  rejected. It never synthesizes indexed authority and never reruns the same
  attempt.

Legacy mission phase `published` is intentionally not part of this state
machine. It predates the artifact outbox and remains sufficient only for the
historical pending, captured, checkpointed, and published mission gates. It is
never evidence of artifact `UPLOADED` or `INDEXED`. Under an indexed gate it is
only eligible staging input and must traverse this outbox. Git branch push, PR
creation, and merge are software-delivery states; they may reference the same
attempt and bundle but cannot satisfy portable publication.

An unbound sandbox outcome that claims `indexed` is rejected even when the
mission's minimum gate is lower. The staged request and claim-owned linkage,
not the provider's string, establish authority. Existing terminal legacy rows
remain readable and queryable; they are not rewritten into the new outbox. The
v7-to-v8 migration records `legacy_unbound_eligible` only for claims already
settled under an indexed gate before these artifact-authority columns existed.
Non-indexed v7 claims remain ordinary historical replays. A narrowly valid
legacy replay exposes `Finalization.legacy_unbound=true` while preserving its
canonical outcome unchanged. That flag is an explicit compatibility
classification, not a claim that historical artifact provenance was recovered.
Claim contract v9 requires `worktree_archive_ref` on new replayable outcomes.
Previously durable v7/v8 outcomes remain readable under their original field
set and project an empty archive reference when none was recorded. If a
nonterminal v7/v8 claim reconciles into indexed finalization, its authenticated
contract version also makes the archive bundle candidate optional; current v9
claims still require that candidate before staging.

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

The indexed mission path has the following complete finalization matrix. A
direct artifact caller enters at the artifact-publication claim row.

| Crash point | Durable state | Recovery action |
|---|---|---|
| Before artifact preparation | Mission `provider_acknowledged` plus recoverable sandbox receipt | Reconcile the matching receipt, sanitize it, and prepare deterministically; do not call the model again. |
| After preparation, before mission staging | Mission `provider_acknowledged`; prepared value was only in memory | Reproduce the exact preparation and retry `stage_finalization`; no artifact I/O occurred. |
| During or after staging response loss | Mission is either `provider_acknowledged` or `finalizing` | Re-read the claim. Retry only byte-identical outcome and prepared identity; changed evidence fails closed. |
| After mission reaches `finalizing`, before artifact claim | Exact request JSON, digests, key, policy, and provisional outcome are durable | Cold `FINALIZE` from the projection. Do not invoke the attempt runner, model, validators, repository finalization, or checkpoint creation. |
| After artifact claim, before upload | Mission `finalizing`; artifact `PENDING` plus exact replay request | Materialize from the already-bound checkpoint and upload. |
| Artifact retry window elapses before upload | Mission remains `finalizing`; its exact artifact publication is `EXPIRED` | Authenticate the terminal row, preserve the staged evidence, add only `artifact_publication_expired`, seal and settle incomplete or rejected, then authenticate the durable winner through `require_settled`. Do not rerun or republish the same attempt. |
| During upload | Artifact `PENDING` plus some content-addressed folders | Byte-verify present payload and manifest objects; reuse exact objects and replace truncated or corrupt ones before upload metadata. |
| After upload metadata | Artifact `UPLOADED` plus complete records | Index without reopening a sandbox or checkpoint. |
| After Iceberg commit, before artifact completion | Query rows visible; artifact control row `UPLOADED` | Verify deterministic rows and mark `INDEXED`; do not append logically new evidence. |
| After artifact `INDEXED`, before finalized outcome construction | Mission `finalizing` plus exact terminal artifact row | Reread the row, validate its staged identity and positive snapshot, and add authoritative indexed fields. |
| After finalized outcome construction, before mission settlement | Mission remains `finalizing`; finalized value was only in memory | Reconstruct and seal the same value from the staged claim and terminal row, then settle; projection is still forbidden. |
| During or after settlement response loss | Mission is either `finalizing` or `settled` | Re-read. If still finalizing, retry only the exact sealed settlement; if settled, use `require_settled` to authenticate the winning stored outcome before private projection. Conflicts fail closed. |
| After mission `settled`, before world commit | Canonical terminal outcome is durable | Call `require_settled(world_id, claim_key)`, then let the execution service invoke the private row transformer; no runner, repository, checkpoint, or artifact work is permitted. A supplied claim DTO is not authority. |
| After world commit | `settled` claim plus visible completed-attempt row | Return/replay the committed result without external work. |

## 6. Reconciler contract

`ArtifactBundleService.reconcile(world_id, limit=N)` is one bounded,
idempotent pass. It does not run forever inside an API request.

1. Ask the catalog for a digest-only page of nonterminal publications whose
   lease is due according to the catalog clock. Discovery accepts no caller
   clock and returns no replay request.
2. CAS-acquire one exact publication by world ID and publication digest,
   retaining its current phase and incrementing `attempt_count` on takeover.
   The source row returned after acquisition is the only replay authority.
3. Immediately before external I/O, repeat that exact acquisition. This
   reauthorizes a claimant that stalled after discovery or initial acquisition
   and closes the race with lease takeover or retry-window expiry.
4. If `PENDING`, expire it only when the catalog clock proves
   `retry_until_ms` elapsed; otherwise materialize the recorded sources,
   reuse/upload objects, and atomically store `records_json` as `UPLOADED`.
5. If `UPLOADED`, append or verify all deterministic Iceberg rows. An uploaded
   publication never expires because it no longer depends on the checkpoint.
6. Mark `INDEXED`; on a transient error, send a bounded retry duration and let
   the catalog derive the new `lease_expires_at` from its own clock.

Initial publication likewise sends a retry-window duration and an optional
provider checkpoint `not-after` bound. The catalog derives the persisted
`retry_until_ms` from its clock, capped by that external deadline. Recovery
never echoes `request_json`, supplies an absolute retry time, or asserts what
time it is; it carries only the publication digest, claimant, and bounded
durations. The durable row is reread and authenticated after every successful
acquisition before files, objects, or the index are touched.

The artifact row's source-native guard is a lease plus an invocation-unique
claimant token, not the fleet sweep's monotonic fence epoch. Built-in publishers
and reconcilers generate a fresh claimant for every invocation and never reuse
one after takeover. Any future adapter calling this internal catalog contract
MUST preserve that uniqueness; a public or mutually untrusted claimant API
would require adding a source-native fence token before exposure.

The SQLite catalog is the single-host reference implementation. The remote
catalog implements the same CAS transitions in the per-world Durable Object.
Iceberg snapshot identity is an exact Python `int` in the range
`1..2^63-1`; booleans, floats, numeric strings, zero, and larger values are
rejected before completion. The remote catalog transports and stores that
identity as canonical decimal text under `artifact_snapshot_decimal_v1`.
Before snapshot-bearing mutations, `GET /status` must report
`catalog_protocol_version >= 3` and that capability; `renew-v2`, `uploads-v2`,
`complete-v2`, and `expire-v2` therefore fail closed against a Worker that
could round the snapshot through a JavaScript number.

Server-clock publication scheduling additionally requires
`catalog_protocol_version >= 6` and
`artifact_publication_server_clock_v1`. The `acquire-v3`, `due-v1`,
`recover-v1`, and `fail-v3` routes fail closed against an older Worker that
accepts a caller-derived clock, request echo, or absolute retry instant. The
SQLite reference and per-world Durable Object apply the same status, deadline,
lease-owner, and retry-duration transitions.

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
  spans carry validated canonical world/run UUIDs and entity/tick values.
  Upload/index stage spans also carry the bundle digest and bounded artifact
  count. Raw attempt IDs and idempotency keys are omitted; no attempt or
  idempotency digest is claimed until the owning family supplies one explicitly.
- Agent CLI JSONL and sandbox-side OTel output are portable artifacts. Shipping
  live sandbox spans to an OTel collector is complementary and must use the
  same correlation attributes. It is not required for publication correctness.
- Every portable payload and generated manifest passes the pre-durability gate
  in section 4. Content-bearing live-event/export adapters apply the same
  policy before their external write, while `_obs` signals admit no content by
  schema. An observability outage or quarantine never changes mission state
  authority.
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

For an indexed gate, the authoritative application path is:

1. Assess and sanitize the complete provider outcome. Recoverable accepted and
   rejected outcomes are both eligible; both require a restorable checkpoint,
   and acceptance also requires a commit SHA.
2. Prepare the exact policy-bound artifact request without publication I/O and
   stage it with the provisional outcome in the mission claim. The durable
   claim reaches `finalizing` before the artifact service is called.
3. Publish only that persisted projection through
   `PENDING -> UPLOADED -> INDEXED`. Legacy mission phase `published` is merely
   eligible staging input, never index evidence.
4. Reread the exact durable `INDEXED` row and validate it against the staged
   bundle/publication key, request JSON and digest, producer digest, policy,
   attempt identity, manifest, and positive index snapshot. Upgrade the
   provisional outcome and settle the claim. A publication receipt cannot
   substitute for this read.
5. Call `iMissionAttemptClaimService.require_settled(world_id, claim_key)` to
   re-read the winning terminal claim and authenticate its canonical stored
   JSON, digest, status, and any explicit legacy compatibility classification.
   The execution service then invokes the mission service's private settled-row
   transformer. Only the world's ordinary two-phase tick commit makes the
   completed attempt and task edge visible. A detached or caller-replaced
   `AttemptClaim` DTO cannot substitute for `require_settled`.

Public `MissionService.apply_attempt` cannot shortcut steps 4–5. It
categorically rejects an `indexed` phase and any artifact staging, linkage,
finalized authority, or nonzero snapshot. Public `iMissionService` has no
settled-projection operation; only the execution workflow's immediate
`require_settled` plus private row-transform sequence may project the durable
winner.

Indexing a rejected attempt preserves portable review evidence but does not
change its rejection or advance the task. An accepted attempt advances only
after the exact terminal row has been bound, the claim has settled, and the
world commit publishes the typed edge. Attempts with a failed or non-restorable
checkpoint cannot enter `finalizing`; they remain persistable incomplete or
failed mission facts under the ordinary evidence gate.

A crash anywhere after staging is recoverable from control state. `finalizing`
recovery never invokes the attempt runner, model, validators, repository
finalization, or checkpoint creation. A `PENDING` artifact row may read the
already-recorded checkpoint solely for extraction; `UPLOADED` and later phases
do not require it. A crash after claim settlement but before the world commit
reapplies the stored canonical outcome without repeating artifact or provider
work. An exact `EXPIRED` row settles the attempt with no indexed authority; the
same attempt never runs again, though the retryable world state may identify a
new next attempt.
