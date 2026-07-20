# Artifact bundle publication

**Document type:** Normative.

This specification defines how Archetype turns provider-native or local files
into portable, content-addressed artifacts. It is independent of Agent
Missions task progression. Publishing an artifact never accepts a task, settles
an execution, or changes a world transition.

## 1. What is authoritative

Artifact publication has four complementary records:

| Record | Authority |
|---|---|
| Canonical publication row | Retry, lease, request identity, and publication phase |
| Portable objects | The exact bytes selected for long-term handoff |
| Artifact index rows | Query visibility, provenance, hashes, sizes, and locations |
| Provider checkpoint | Optional recovery source; never copied wholesale into component rows |

The catalog row authorizes publication work. The object store owns bytes. The
index makes those bytes queryable. A provider checkpoint remains a recovery
object owned by its sandbox provider.

World state may reference a published artifact by digest and URI, but the
artifact family does not infer workflow state from publication. Agent Missions
records its own AgentArtifact, Checkpoint, and FilesystemManifest observations
and may compose this service later as an explicit handoff.

## 2. Identities and primary keys

One logical publication is identified within a world run by:

```text
publication_key = SHA256(
    "archetype.catalog.v1",
    world_id,
    run_id,
    idempotency_key,
)
```

ArtifactBundleRequest.canonical_json() is the exact replay request.
request_digest authenticates that complete JSON, including the bound redaction
policy. producer_digest preserves the producer's logical identity across
compatible scanner upgrades.

attempt_id remains an opaque producer correlation field in the reusable
artifact schema. It does not refer to an Agent Missions aggregate and carries
no transition authority.

Portable objects use deterministic content-addressed placement:

```text
<object-root>/
  worlds/<world_id>/runs/<run_id>/attempts/<attempt_id>/
  bundles/<bundle_id>/objects/<artifact_id>/<generated-file-name>
```

If a worker crashes after upload but before catalog update, a later worker may
reuse an object only after reading it back and verifying its bytes and size.

## 3. Request and index contracts

ArtifactBundleRequest is immutable and credential-free. It carries:

- world, run, entity, tick, producer-correlation, and idempotency identity;
- an optional provider checkpoint reference and lifecycle;
- a retention class; and
- one or more ArtifactCandidate values.

Each candidate names an immutable source_ref, a portable relative logical_path,
a free-form kind, and required/recursive behavior. Logical paths must be unique
and may not be absolute or contain parent traversal.

ArtifactSourceResolver.materialize is the stable provider boundary. Resolvers
that can reject oversized provider objects before copying implement
BoundedArtifactSourceResolver.materialize_bounded. The service validates
materialized files again before Daft reads them.

Every ArtifactIndexRecord is scalar and Parquet-friendly:

| Group | Fields |
|---|---|
| Identity | artifact, bundle, world, run, entity, tick, producer correlation, idempotency key |
| Classification | kind, logical path, storage kind, MIME type |
| Location | source reference, object URI, checkpoint provider and reference |
| Integrity | SHA-256, byte size, restorable flag |
| Lifecycle | retention, acceptance label, creation and expiration |

Portable objects require lowercase SHA-256 and a non-negative size. A provider
checkpoint may use an empty hash and size_bytes=-1 when its provider does not
expose bytes.

Before upload metadata becomes durable—and again before recovery, indexing, or
result construction—the service authenticates the complete record set against
the exact request. Every payload must match one declared candidate, and the
bundle must contain exactly one generated manifest plus its checkpoint record.

## 4. Pre-durability secret redaction

RedactionService is the provider-neutral authority for content-bearing data
crossing into a durable control row, object, index, or external event.
Artifact publication consumes only iRedactionService; providers do not fork
scanner policy.

The order is fixed:

1. Bind the active scanner policy to a copy of the immutable request.
2. Scan request and destination metadata before creating the publication row.
3. Persist the credential-free canonical request before provider or object I/O.
4. Materialize into a controlled local snapshot.
5. Redact text or quarantine unsafe metadata, archives, credential paths, and
   opaque binary findings.
6. Hash and upload exactly the approved bytes.
7. Generate and scan the manifest, validate every index value, then record
   upload metadata and append the index.

Identity-bearing metadata is quarantined rather than rewritten. UTF-8 text,
JSON, JSONL, logs, and patches are deterministically redacted. Archives are
walked without unsafe extraction and fail closed on links, special members,
encrypted content, nested containers, unsafe paths, or resource-limit
violations.

Every resume rechecks the process-local object-store root and authenticates the
durable canonical request. A PENDING publication may resume only while its
bound redaction policy implementation remains available. UPLOADED recovery no
longer needs source bytes, but its metadata is still checked before index
visibility.

Quarantine creates no object or index visibility. Diagnostics are bounded and
redacted; they retain rule identifiers, never matched secret text.

## 5. Publication state machine

The artifact catalog records the canonical request before external I/O:

```text
             upload every declared object
PENDING  ───────────────────────────────────▶  UPLOADED
   │                                                │
   │ retry window elapsed                          │ append/confirm index rows
   ▼                                                ▼
EXPIRED                                          INDEXED
```

- PENDING: request identity is durable; source materialization or upload may
  still be required.
- UPLOADED: complete record metadata and manifest location are durable;
  indexing no longer depends on the sandbox or checkpoint.
- INDEXED: every deterministic row is present in the artifact index.
- EXPIRED: the retry window elapsed before upload completed.

UPLOADED publications do not expire: their portable bytes already exist and
must be driven to INDEXED. A Git commit, branch push, pull request, mission
status, or sandbox status cannot substitute for INDEXED.

The index append is the query-visibility point. If a worker dies after append
but before catalog completion, recovery verifies the deterministic rows and
marks the publication INDEXED. The query applies an exact-row distinct so a
rare identical physical append cannot create duplicate logical visibility.

| Crash point | Durable state | Recovery |
|---|---|---|
| Before catalog acquisition | Nothing | Prepare and acquire normally. |
| After acquisition, before upload | PENDING request | Reacquire after lease expiry, authenticate, materialize, and upload. |
| During upload | PENDING plus possible objects | Verify and reuse exact objects; replace corrupt or truncated objects. |
| After upload metadata | UPLOADED records | Index without reopening the source. |
| After index append | Rows visible, row still UPLOADED | Verify rows and complete the catalog record. |
| After completion | INDEXED | Return the authoritative publication result without external work. |

## 6. Reconciler contract

ArtifactBundleService.reconcile(world_id, limit=N) performs one bounded,
idempotent pass:

1. Ask the catalog for a digest-only page of due nonterminal publications.
2. Acquire one exact publication by world ID and publication key.
3. Reread and authenticate the authoritative row before external I/O.
4. For PENDING, expire only when the catalog clock proves the retry deadline;
   otherwise materialize, verify/upload, and store complete records.
5. For UPLOADED, append or verify deterministic index rows.
6. Mark INDEXED; on a transient failure, persist a bounded retry duration.

Discovery accepts no caller clock and returns no replay request. Recovery
accepts only identity plus bounded durations; the durable row supplies the
canonical request. Each invocation uses a fresh claimant token, and long
operations renew their lease.

The SQLite catalog is the single-host reference. The remote per-world Durable
Object implements the same phases and server-clock behavior. Snapshot identity
is an exact integer in 1..2^63-1; the remote protocol transports it as canonical
decimal text to avoid JavaScript-number rounding.

Reconciliation is at-least-once. Content-addressed placement and exact index
verification make its externally visible effects idempotent.

## 7. Lifecycle, query, and authorization

Provider checkpoint TTL and portable-object retention are separate clocks.
Checkpoint expiry may cap the retry window for a PENDING publication. UPLOADED
and INDEXED publications no longer require the checkpoint.

Default portable retention:

| Class | Default | Intended use |
|---|---:|---|
| attempt | 30 days | Short-lived producer output and debugging evidence |
| run | 180 days | Completed run evidence |
| durable | No automatic expiry | Promoted datasets, audit evidence, or legal retention |

Catalog rows and index metadata remain as the audit trail after object expiry.
A garbage collector deletes bytes idempotently and retains or tombstones the
row.

RuntimeApplication owns actor-free publish, reconcile, and query semantics. An
untrusted adapter authorizes before invoking it. The lower-level service accepts
explicit world/run coordinates and remains usable after world destruction or
process restart.

## Companion contracts

- [Artifacts](artifacts.md)
- [Application Architecture](application-architecture.md)
- [Service Protocols](service-protocols.md)
- [Agent Missions V1](agent-missions.md)
