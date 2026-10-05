# Storage migration

**Document type:** Normative.

**Scope:** Local v1 whole-storage-identity migration between already-composed
Archetype endpoints.

Storage migration moves one complete Archetype storage identity to another.
It is not a World fork, backup-file copy, synchronization job, or ongoing
multi-authority protocol.

The governing invariant is:

> Migration either preserves every durable plane in the local v1 profile, or
> refuses before destination World activation. It never reports a partial
> Archetype identity as successful.

## 1. Local v1 profile

Local v1 is deliberately narrow. A conforming migration MUST be:

- offline and whole-identity, never scoped to selected Worlds or tables;
- local Iceberg to local Iceberg;
- SQLite-backed Iceberg catalog to SQLite-backed Iceberg catalog;
- local SQLite control catalog to local SQLite control catalog;
- between different normalized storage identities;
- into an empty destination namespace and control identity;
- inclusive of every table, supported control record, and visible Artifact
  object in the source identity; and
- resumable only for the exact same `migration_id` and immutable plan digest.

The storage identity remains the existing tuple of normalized storage URI,
namespace, and backend. Both endpoints are invocation-scoped, already-composed
capabilities. A migration plan or receipt MUST NOT serialize catalog sessions,
filesystem authority, credentials, `IOConfig`, secret-bearing URIs, or other
live capabilities.

Each endpoint MUST bind its audit storage configuration explicitly. Local v1
requires that configuration to name the same storage identity; omission is not
treated as co-location because the runtime's default audit lakehouse is a
separate identity.

Remote catalogs, object stores, and control authorities are deferred. Local v1
MUST reject them; their existing use for ordinary Archetype storage does not
imply migration support. LanceDB, live migration, merge, continuous
synchronization, selected-World migration, and arbitrary Parquet import are
also outside this profile.

## 2. Durable identity and admitted planes

Migration preserves logical state, not physical Iceberg layout. A successful
migration MUST preserve every World, run, entity, command, evaluation,
Artifact, commit-token, tick, lineage, table row, and application-owned logical
identity. Physical snapshot IDs, metadata files, data-file paths, and file
layout may differ at the destination.

Local v1 admits these durable planes:

| Plane | Required contents | Owning family |
|---|---|---|
| Iceberg data | Every table in the attached namespace, including ECS rows outside current manifest visibility and unknown application tables | `archetype.storage` |
| Control | Worlds, signatures, manifests, terminal commands, evaluations, outbox state, and writer-fence epoch floors | `archetype.storage` |
| Artifact objects | Every distinct content object referenced by `artifact_files` | `archetype.artifacts` |
| Activity history | No admissions, attempts, or provider-operation records are admitted in local v1; storage-owned inspection must prove the physical catalog is empty | `archetype.storage` |

Unknown tables MUST be copied and verified without interpreting or rewriting
their fields. Their existence is not permission to omit them. A future family
whose durable state includes relocatable external objects must supply an
explicit migration participant or force preflight to fail closed.

The source remains intact as the rollback copy. Migration MUST NOT delete,
rewrite, or automatically seal it.

## 3. Preflight, quiescence, and destination emptiness

The caller MUST close all source runtimes and caller-owned services, flush
write-behind and audit buffers, and keep the source offline for the complete
migration. Preflight MUST reject:

- identical source and destination storage identities;
- a populated destination Iceberg namespace or control identity, except the
  exact durable reservation left by a retry of the same plan;
- a pre-existing destination content object whose complete bytes do not verify
  against its content address;
- unsettled commands or a running evaluation lease;
- any source or destination Activity admission, attempt, or provider operation,
  including settled, completed, or failed history;
- an unreadable or corrupt indexed Artifact object; and
- an endpoint outside the local v1 profile.

Checking only for unsettled Activities is insufficient. Settled Activity rows
are durable history, and local v1 has no typed Activity export/import contract.
Any Activity record therefore rejects the migration before destination data or
control mutation.

Planning acquires a new source writer-fence epoch for every active World under
a migration holder. This stales previously constructed writers but is not a
global online-migration lock. The source MUST remain offline by convention.

Before any destination mutation, planning freezes:

1. a typed, versioned control snapshot;
2. every source table and its pinned snapshot evidence;
3. the complete verified `artifact_files` object inventory; and
4. a credential-free canonical plan and digest.

Immediately before destination activation, the workflow MUST recapture the
source control state, recapture table snapshot identities, and compare the
Artifact inventory with the plan. Any drift other than the migration's known
source-fence changes invalidates the plan and aborts activation.

## 4. Data-plane transfer

The attached Iceberg catalog is the inventory authority. Migration MUST
enumerate the namespace; it MUST NOT use a hard-coded table list or copy the
warehouse directory, PyIceberg metadata database, or source catalog records.

For each table, the immutable plan binds:

- exact table name and classification;
- source snapshot identity;
- logical Arrow schema and schema fingerprint;
- row count; and
- deterministic content digest.

The content digest MUST be schema-bound, row-order independent, Arrow
chunk-boundary independent, duplicate-sensitive, deterministic for nested
values, and explicit about nulls and scalar type classes. Logically equivalent
string and binary width variants may normalize only where the schema
fingerprint uses the same logical type. The destination table is successful
only after a fresh read reproduces its expected logical schema, count, and
digest. Logical-schema equality binds column order, names, normalized types,
and nullability. Adapter-added Arrow and Iceberg field metadata is physical
evidence and is not required to match.

One atomic destination commit per table is preferred. A populated destination
table with conflicting evidence MUST fail. If a commit response is ambiguous,
the workflow MUST inspect the destination table and compare its complete
evidence: matching content converges; absent content may retry; conflicting
content fails. It MUST NOT blindly append after an unknown outcome.

## 5. Artifact relocation without re-ingestion

`artifact_files` is the authoritative visible Artifact occurrence inventory.
For every occurrence, migration MUST read the recorded source `object_uri` and
verify SHA-256, XXH3-64, and byte size against the row. Each distinct SHA-256
is copied once to the destination content address, then read back and verified
in full. An existing destination object may be reused only when its bytes
verify; conflicting content at a digest path MUST NOT be overwritten.

Every source `object_uri` MUST be the exact content address beneath the source
endpoint's configured Artifact root. Source and destination Artifact roots
MUST be disjoint. Content objects MUST be regular files; symlinks, directories,
and other filesystem entries at a content address fail closed. Destination
URIs remain lexically bound beneath the normalized destination root.

Migration is relocation, not ingestion. It MUST NOT call
`ingest_artifacts()`, mint a new occurrence, or rescan media. The destination
`artifact_files` table changes only `object_uri`. It preserves `artifact_id`,
`ingested_at`, `source_uri`, `logical_path`, World/run/tick attribution,
hashes, size, media classification, typed index joins, and downstream
`source_artifact_id` joins. Source-table evidence and relocated-destination
evidence are distinct because the allowed URI transformation changes the
table digest.

`artifact_files` is copied last among Artifact tables, after all referenced
objects and typed indexes verify. Unreferenced objects left by a failed
ingestion are not visible Artifact occurrences and need not migrate.

## 6. Control import and writer fencing

Control state MUST be exported and imported through a versioned
administrative contract. Ordinary control mutations are not import APIs:
replaying command admission, a manifest commit, or evaluation transitions
would mint or alter history.

Import MUST preserve exact logical records and ordering. It MUST NOT import the
source process's active fence holder. For each World it imports an epoch floor
at least as high as both the source's current fence epoch and every imported
manifest writer epoch. The first destination mutable resume MUST acquire a
strictly higher epoch.

The destination reserves `(migration_id, plan_digest)` before copying. The
same pair resumes idempotently. A changed plan for that ID, a different
migration, or conflicting staged content fails. Global signatures and
per-World control bundles are staged while destination Worlds remain
undiscoverable. World directory activation occurs only after data, Artifact,
control, and source-stability verification.

Migration reservation and staging rows are endpoint-local administrative
evidence, not source application identity, and are not recursively included in
a later source control snapshot.

## 7. Required execution order

A conforming workflow executes in this order:

1. validate local endpoints, source quiescence, Activity emptiness, and
   destination emptiness;
2. acquire source fences and build the immutable plan;
3. reserve the exact plan at the destination;
4. copy and read back every referenced Artifact object;
5. copy all non-`artifact_files` tables;
6. copy relocated `artifact_files`;
7. read back and verify every destination table;
8. recheck the source against the frozen plan;
9. stage exact control state and writer-fence floors;
10. activate destination World discovery last;
11. verify from a fresh destination-only process; and
12. emit and durably complete the immutable receipt.

A crash may leave verified objects, tables, or staged control bundles. It may
also occur during last-stage World activation. Retrying the exact plan MUST
read and verify durable evidence, skip matching work, complete missing work,
and reject conflicting work. No success receipt exists until every World is
activated and cold verification succeeds.

Once the reservation records `ACTIVATED`, the destination is the complete
recovery authority. A retry MUST rehydrate the exact reserved plan, recover
the imported historical table snapshots, run destination-only cold
verification, and complete the receipt without reading or validating the
source endpoint, its Activity catalog, or its Artifact authority. Source
availability remains required through activation, but not afterward.

Completion MUST atomically store the canonical credential-free receipt and its
digest with the destination reservation. If that commit succeeds but its
response is lost, retrying the completed plan MUST return the exact stored
receipt. It MUST NOT rerun cold verification or commit another verification
tick. Missing, non-canonical, digest-invalid, or plan-mismatched stored receipt
evidence fails closed.

## 8. Cold verification and receipt

Cold verification MUST use fresh destination resources without a readable
source path. It proves destination discovery, complete table evidence, all
referenced Artifact hashes, and destination-visible queries, including
fork-aware visibility when lineage exists. When the migrated identity includes
an active resumable World, it MUST also resume that World at its durable head,
acquire an epoch above the imported floor, and commit a later tick. An identity
with no eligible World records that fact as not applicable rather than
fabricating a resume.

The cold verifier is a trusted, host-injected composition capability because
resume may require application Components and hooks to be installed in the
fresh process. It MUST be present during planning, before source fencing or any
destination mutation; use no source authority; return digest-bound bounded
evidence; and tolerate an already-completed verification tick on retry by
finding the frozen imported snapshots in destination history and acquiring a
still-higher writer epoch.

The immutable, credential-free receipt contains at least:

- migration and format identity, Archetype version, and timestamps;
- source and destination storage fingerprints;
- plan and source-stability digests;
- per-table source/destination snapshot identity, classification, schema
  fingerprint, row count, and content digest;
- Artifact occurrence/content counts, verified bytes, and inventory digest;
- control counts including the fence-floor count, snapshot digest, and
  activation status;
- `activity_disposition="empty-v1"`;
- cold-verification evidence; and
- a digest binding the complete receipt.

A copy request returning success, the appearance of destination tables, or an
opened fork is not completion evidence.

## 9. Ownership and invocation

`archetype.migration` owns planning, quiescence, ordering, retry convergence,
verification, and receipts. It consumes only the declared lower
`archetype.storage` and `archetype.artifacts` families. Storage owns table
enumeration/transfer evidence and SQLite control snapshot, reservation,
staging, activation, and completion. Artifacts owns object inventory,
relocation, read-back verification, and the exact `artifact_files`
transformation. Storage MUST NOT import artifacts.

The administrative Python workflow operates on already-composed endpoints:

```python
plan = await plan_storage_migration(
    source=source_endpoint,
    destination=destination_endpoint,
    migration_id=migration_id,
)
receipt = await migrate_storage(plan)  # Includes the required cold verification.

# Optional: repeat verification independently at a later time.
verification = await verify_storage_migration(
    receipt,
    destination=destination_endpoint,
)
```

`migrate_storage()` does not return a complete receipt until the required cold
verification has succeeded and its evidence is bound into that receipt. The
separate `verify_storage_migration()` call above is an optional later
re-verification; omitting it does not weaken the completed migration receipt.

Endpoint composition remains in `archetype.wiring`. Direct construction of a
`MigrationEndpoint` is a trusted composition seam, not an adversarial security
boundary; the concrete local builder derives the control and Activity paths
and requires explicit audit identity. Migration is not a
`RuntimeWorld` method, deferred simulation command, REST route, or CLI command
in local v1. Any future adapter transports endpoint references understood by
its host and does not construct catalogs or acquire workflow authority.

## 10. Architecture and executable evidence

The registered family graph is:

```text
migration -> artifacts -> storage
          -> storage
```

`quality/architecture.d/migration.toml` is the machine authority for those two
outgoing edges. The architecture contract rejects imports from migration to
world, commands, activities, runtime, API, CLI, wiring, or any other
undeclared family.

Focused table-transfer tests prove deterministic evidence, exact destination
verification, empty-table handling, conflict refusal, and ambiguous-commit
reconciliation. The local integration contract constructs a multi-plane
identity, migrates it, destroys all source-side process objects, and proves
destination-only discovery, query, Artifact hashing, higher-epoch resume, and
a later committed tick. Remote migration remains deferred until equivalent
typed control/Activity administration and clean-process infrastructure
evidence exist.
