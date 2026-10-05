# Published artifact contexts

This additive local preview gives file occurrences an explicit published data
context. An artifact collection has no executable program, tick, checkpoint,
revision, admission state or mutable head. A hosted context retains verified
program/declaration/publication-policy evidence before activation or a first
cut. Existing full-cut and v1 cut-artifact contracts remain unchanged.

## Ownership and visibility

`CutStore` owns immutable descriptors, objects, typed indexes and visibility.
`HostedCutAdapter.context_draft()` mints hosted evidence from its verified native
binding. A collection draft only names a world/run data scope; it never creates
a native world. `archetype.storage.context_artifacts` owns Python execution and
metadata transport; `archetype.artifacts.context_attachments` owns file workflow
orchestration through that port. The file pipeline remains lazy through its
intrinsic common and typed projections.

A version-1 descriptor binds world/run, origin and a canonical context ID. The
hosted origin retains ABI and native-code revision provenance, native owner,
processor, native program, component schemas, publication policy and program
digest. Cold verification uses persisted evidence and requires no live registry
or manager. None of these fields imply an active process.

Before durable scope preparation, the complete descriptor and encoded root are
admitted under the existing metadata and Parquet budgets. Immutable preparation
is keyed by world/run; a changed origin cannot escape a conflict by hashing to a
different context ID. Preparation grants no visibility. The final fixed-schema
`published_contexts_v1` root is published through existing object staging and
Iceberg append/adoption. Readback verifies its pinned table, schema, snapshot,
object, complete descriptor and retained preparation. Exact lost-acknowledgment
retry adopts the same root. A missing preparation beneath a root fails closed.

The existing owner lock and publication mutex serialize contexts with cuts and
origins. A collection cannot acquire cuts or a fork origin. Hosted contexts
permit only matching future cuts. Existing cuts, pending journals and inherited
origins must agree before a descriptor claims their scope. A fresh fork cannot
reserve a child into a scope already claimed by another context; exact retries
with matching retained origin remain valid. This adds no execution owner,
claim/lease service, input ledger or scheduler.

## Optional cut attribution

`ArtifactTarget` binds the exact published context reference plus either no cut
or an exact `{tick, cut_id}`. No cut means **cutless**, never latest. A hosted
cutless occurrence stays cutless after later simulation publication. An exact
cut must be a fully verified member of that context's retained history. For a
fork, an inherited source receipt keeps its original physical owner while the
occurrence belongs to the child's context.

New `context_artifact_files_v1` and typed tables require context/world/run.
Both tick and cut ID are nullable in every row schema, and must be absent or
present together. Existing `cut_artifact_*_v1` nonnullable schemas, receipts,
`CutCoordinates` and `ArtifactRef` are not redefined.

The existing FileIngestionPipeline persists immutable originals and produces
intrinsic metadata. Its new `intrinsic_common_index()` needs no tick;
`common_index()` retains its 0.6 projection. Storage freezes UUID discovery and
content persistence once, and retains encoded occurrence bytes. Preparation
binds the whole target and common/typed inventory before appends. Typed indexes
commit first, each common root last. A call may expose a prefix and is not batch
atomic. Retry requires the same UUID, target and payload. A new ingestion creates
a new occurrence even when original bytes are shared. Retained snapshot evidence
prevents UUID reuse across table versions even if preparation is missing.

Cold reads verify context, optional complete cut, preparation digest, complete
typed inventory, pinned common/typed snapshots, schemas, objects and original
content. Run-wide reads return each occurrence's own attribution; exact-cut and
cutless reads use explicit selectors. Shared operation budgets and existing
32-occurrence/page limits apply.

## Local and authenticated surfaces

`Host.publish_collection(world, run)` and
`Host.publish_hosted_context(binding)` publish descriptors;
`Host.context(world, run)` verifies an existing descriptor. `Store(library=...,
store_root=...)` uses the same C ABI handle, lease and close protocol while owning
only CutStore and its executor. It neither constructs a manager/registry nor
requires a native driver. Native mutations require a Host. A Store also exposes
storage-only v1 reads through `read_cut_artifacts` without weakening that format's
verification. A live Host borrows its existing store; a second opener still
respects the exclusive owner lock.

`PublishedContextRef`, `ArtifactTarget`, `ContextArtifactStorage`, and
`prepare_context_attachments` / `publish_context_attachments` supply the trusted
file workflow. Target verification precedes source discovery or file effects.
Prepared values retain immutable original `ArtifactRef`s and exact occurrence
metadata for caller-controlled retry.

Shared version-1 ingress has a separate immutable `ContextResource` configuration:

| Operation | Capability | Arguments |
|---|---|---|
| `publish_context` | `artifacts:publish` | `source_resource=null` for a collection, or a configured execution source in the same scope |
| `read_context` | `artifacts:read` | Empty object |
| `context_artifacts` | `artifacts:read` | Exact context ID; explicit all/optional-cut selector; decimal offset and limit |

Operator configuration pins `ContextResource.source_resource`: absent means a
collection; present names the required execution source in the same scope. A
context sharing an execution resource's scope must pin that source. Caller
arguments cannot change this choice or claim a hosted scope as a collection.
The context grant precedes lookup; hosted publication also requires the exact
source grant before either resource lookup. Source and destination scope must
match. Public results contain context/occurrence IDs, origin kind, optional
exact cut, content hash, media type and decimal sizes. Native identifiers,
declaration payloads, local paths, Parquet and diagnostics remain private. File
discovery/publication stays in the trusted local file workflow. HTTP and MCP
reuse the existing ingress route/tool and cancellation shield.

## Executable contract

Focused native store tests cover descriptor failures after preparation/object/
root publication, exact cold adoption, format rejection before effects,
occurrence preparation/typed/common failures, changed targets/inventories,
missing preparation and cross-version UUID exclusion. Hosted integration checks
context ownership before native fork effects. Python/C ABI file-pipeline tests
cover collection-only work, hosted context before first cut, cutless/exact
distinction, old v1 reads with registry/manager absent and nonempty safe ingress
pages. Scope/grant tests prove denial before lookup. These contracts are separate
from future logical creation, live types and supported runtime migration.
