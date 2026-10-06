# Historical DDlog forks

This local preview forks an explicit immutable published cut into a different
native world and analytical world/run. It uses the existing DDlog WorldManager
and CutStore. It does not change latest-only resume or the retained Python ECS.

## Ownership and durable identity

WorldManager verifies the source manifest, checkpoint bytes and pinned program
before child effects, then durably reserves one child ID for an exact request.
The request binds source context, destination scope, definition, checkpoint and
external receipt. Same-key exact retries return the same child; changed source,
destination or definition conflicts. Another request cannot occupy that same
destination. Reservation is durable before child creation. A separate durable
materialization fence distinguishes an unfinished birth from missing progressed
control state. Missing `world.json` or a whole child directory after that fence
fails closed. It never recreates a progressed child at generation zero.
Every materialization retry fences the retained child record, child directory
and build root before accepting or creating that marker, including after a
successful record rename followed by a directory-sync failure.

CutStore publishes one immutable origin per child world/run. It binds the full
native reservation, original source CutReceipt and source ExternalReceipt.
The lineage digest is the canonical native reservation identity. A source
receipt is never relabeled as a child receipt; source journal, checkpoint,
component objects, snapshot IDs and provenance remain unchanged. Origin
publication is serialized with ordinary cut publication and rejects occupied
destinations, including unpublished journals. Exact retries verify/adopt the
same bytes and repeat the durability fence.

The manager's fork readiness flag is separate from pending input work. Restore
may run while that flag is false, but new input and ordinary activation reject.
Archetype verifies the durable origin before returning the private confirmation
ticket. The manager persists the exact confirmation before releasing admission.
Failed confirmation keeps admission blocked; neither missing acknowledgment nor
native process loss permits input replay.

## Inherited reads and first publication

Child history consists of the requested source lineage through selected tick
`t`, followed by child-owned cuts. Before the first child cut, the selected
original source receipt is its read head. Origin-only children can be forked;
an inherited receipt retains its physical source owner even when selected
through an intermediate child's lineage. Traversal rejects cycles and is bounded
to 32 ancestry links under the existing operation read budget. The prospective
33rd link rejects before reservation and is checked again before origin write.

The first child cut MUST be tick `t+1`. Its analytical parent MUST be the source
cut ID; its native parent MUST independently equal the source external-receipt
digest. These hashes are different identities. Later child cuts use the ordinary
publication chain. Reads select complete pinned cuts, so an empty child cut
stays empty; source rows are never merged back after retraction. Physical files
may share a catalog/table, but child cut identities and objects are distinct and
reads remain pinned to their exact snapshots. Parent advancement does not change
the selected origin or child history.

Origins and all referenced source objects/checkpoints must be retained. There
is no retention/GC workflow in this slice. Analytical history and reads can
reopen without a live parent; native activation still requires the configured
registry and manager.

## Trusted Python and shared ingress

`Host.fork(source_binding, receipt, world=..., run=..., label=...,
request_key=..., expected_generation=0)` verifies source membership in that
binding's resolved lineage before reserving a child. Caller-selected receipt
coordinates never authorize opening an unrelated origin. It publishes the
immutable origin, invokes the existing asynchronous native restore, and returns
factual status. A `starting` response has not completed activation. Repeat the
same exact request/generation to observe completion and confirm readiness; this
is an exact lookup, never automatic input replay or a newly chosen generation.

`Host.fork_binding(world, run, components)` resolves the native child from its
durable origin and checks the configured component/program binding. No mutable
Python child map is authoritative. A confirmed origin-only child can resume
after stop/reopen through `fork` with its explicitly observed current generation.
Repeating its original generation leaves it stopped. Once child input exists,
native guards prohibit restoring the historical source. After an own child cut,
ordinary `restore` still requires the exact latest same-world/run cut.

Shared ingress adds `fork` to the existing version-1 operation contract:

| Field | Meaning |
|---|---|
| Outer `resource` | Operator-configured fork destination |
| `source_resource` | Exact configured source lineage |
| `receipt` | Original `{world, run, tick, cut_id}` selector |
| `request_key` | Stable exact retry identity |
| `expected_generation` | Canonical decimal generation; initially `"0"` |

The destination is an immutable `Resource` with `native_world=None`; its scope,
components and input schema are operator configuration. Source and destination
configurations must agree on components/inputs and have distinct scopes. The
principal needs `simulation:fork` plus that exact capability grant on BOTH
resources before either is looked up. A receipt may name an inherited ancestor;
the backend proves membership in the granted source lineage. Other operations
resolve the destination's native ID from its origin on demand, without mutating
ingress configuration. There is no new HTTP route or MCP tool.

Public results expose request/source/destination, lineage digest, readiness,
state and exact decimal generation. Status for a fork also exposes readiness;
`running` alone does not imply input admission. Native IDs, physical paths,
checkpoint bytes and diagnostics remain private. Cancellation retains the
existing shielded operation/Host lease; errors after dispatch report an unknown
outcome and never authorize another child or replay.

## Executable evidence

Native manager contracts inject reservation-before-child and
child-record-before-materialization failures, readiness-confirmation write
failure, rename-success/directory-sync failure, missing control state and cold
reopen. Archetype's
`tests/historical_fork.rs` uses simulated native transport with real Iceberg for
historical parent advancement, unchanged source receipts, nested origins,
ancestry bounds, retraction, independent storage and partial first-publication
reconciliation. Installed Python/C ABI tests cover exact requests, origin-only
resume, readiness, cold lookup, unrelated-scope rejection and grant/projection
contracts. HTTP/MCP reuse the same ingress authorization.

The separate ignored `installed_compiler_historical_fork` test exercises the
same storage contract with the actual DDlog compiler. The upstream ignored
native test directly checks the restored historical fixed point before child
retraction. Compiling or skipping these tests is not actual compiler evidence.
The broader supported-runtime facade, live types, artifact-only contexts and
consumer migration remain separate stages.
