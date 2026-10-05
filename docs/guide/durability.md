# Durability and retry

Native completion does not establish Iceberg visibility. Observe an admission's
`frozen` state, retain its `Boundary`, publish the complete cut and confirm that
exact cut. Keep the admission key, generation, boundary and cut receipt until
settlement. On uncertain publication use `world.reconcile(boundary)`. Inspect and
confirm the exact returned result; do not submit the inputs again.

Every cut includes all declared components, even empty outputs. Reads select a
cut before tables. Retractions therefore stay absent in a later empty cut.
`world.resume(cut, expected_generation=...)` verifies the bound native checkpoint
and publication identity. Analytical parent cut IDs and native receipt digests
are distinct identities and both remain verified internally.

`RuntimeOperationError.code` is bounded. Its `outcome` is `not_dispatched` for
local rejection or `unknown` after dispatch when completion is uncertain. Neither
message wording nor cancellation proves rollback. Stop and close drain through
the native owner and preserve unresolved publication evidence.

Distributed fencing, generic administrative uncertainty resolution and garbage
collection are outside this contract. Transport envelope limits do not bound
native execution time or analytical scan memory.

## Remote data placement

An operator may configure immutable `RemoteData(version=1, uri=..., region=...,
path_style_access=True, credential_source="aws_environment", endpoint=...)` on
`ArchetypeRuntime` or `ServerConfig`. The URI names one assigned `s3://bucket/prefix`
namespace. HTTP and MCP server startup also accepts `ARCHETYPE_REMOTE_DATA_PATH`
pointing to a TOML profile with those fields; credentials never belong in it.
The native owner captures `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` and optional
`AWS_SESSION_TOKEN` when it activates. Provider endpoints require HTTPS.

The profile is immutably bound to the local store. Changing it or redirecting
an existing local data store fails. Iceberg metadata, manifests, Parquet and
original artifact bytes use this one provider path. Every object is created
conditionally and verified by bounded exact readback. An uncertain write with
unreadable or conflicting content remains unresolved; retain the exact intent
and repair the provider before retrying it.

SQLite catalogs, ownership locks, journals, context identities, origins and
DDlog registry/build state remain local. Cold reads require that retained local
authority and the same remote profile. Local artifact staging is used by typed
scanners during ingestion; it does not grant published visibility. This mode
provides no remote-only discovery, loss-of-machine recovery, distributed fence,
cross-host writer coordination or automatic provider cleanup.

Fresh-process reopening retains that local control state and selects the same
exact context and cut receipts; it does not replay the simulation or require
the original artifact staging files. Preserving remote objects alone is
insufficient. A missing catalog cannot recover a known context or cut receipt,
and a missing cut journal prevents checkpoint recovery even when analytical
rows remain visible. Those recovery attempts fail rather than reconstructing
execution state from analytical data.

Opening a new local root may initialize a new catalog; successful opening is
not evidence of recovery. Do not point a new host at an already-owned remote
prefix to discover or recreate its authority. Retain the catalog, journals,
identity/origin state and DDlog registry/build state with the matching remote
profile. The fresh-process regression uses a synthetic provider and establishes
this local-control boundary; it does not establish actual-provider acceptance.
