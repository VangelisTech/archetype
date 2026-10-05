# Files attached to committed DDlog cuts

Status: trusted local preview. This contract adds later file attachments to the
existing hosted DDlog `CutStore`. It requires the native library built from this
source revision and the matching `archetype-ecs` artifact/storage modules. The
stdlib-only `archetype-ddlog-preview` wheel is version 0.1.3. Its raw
request method is an internal integration port, not the intended beginner API.

The additive [published-context format](ddlog-published-contexts.md) supports
artifact collections without execution and optional exact-cut attribution for
hosted contexts. This page retains the unchanged v1 cut-bound contract.

## Exact attribution and ownership

An attachment names an existing `{world, run, tick, cut_id}`. Native storage
resolves the canonical catalog receipt and verifies the complete frozen journal,
component inventory, pinned snapshots, and hosted scope/program/schema binding
before source discovery. It repeats verification before index publication.
Unknown, unpublished, mismatched or corrupt cuts fail closed. Historical cuts
are valid targets even after the live world advances or stops.

An attachment never modifies the cut's FULL component inventory, checkpoint,
tick, native generation or admission state. The artifact family reuses
`FileIngestionPipeline` for discovery, streaming copy/hash, and media metadata.
`archetype.storage.cut_artifacts` owns bounded Daft execution and Parquet
conversion. It borrows the sole existing Host; Rust `CutStore` owns physical
schemas, object durability, Iceberg commits and root verification. Importing
these modules does not initialize the retained Python execution engine.

The retained 0.6 handlers in [Artifacts](artifacts.md) still use their existing
current-run contract. This preview does not route through those handlers or
fabricate a retained `WorldRecord`.

## Local workflow

Given a previously bound Host and its committed cut receipt:

```python
from archetype.artifacts.models import ArtifactSource
from archetype.artifacts.cut_attachments import (
    prepare_attachments,
    publish_attachments,
)
from archetype.storage.cut_artifacts import CutArtifactStorage, CutCoordinates

storage = CutArtifactStorage(host, binding)
cut = CutCoordinates(**{
    name: committed_receipt[name]
    for name in ("world", "run", "tick", "cut_id")
})
prepared = prepare_attachments(
    storage, cut,
    (ArtifactSource(source_uri="/absolute/results/report.txt"),),
)
receipts = publish_attachments(storage, prepared)
page = storage.read(cut)
for item in page["items"]:
    common = storage.decode(item["common"])
    text_metadata = storage.decode(item["typed"]["text"])
```

This is synchronous local batch work. Async callers may use `asyncio.to_thread`;
cancelling its waiter does not roll back native publication. Keep the Host
alive through the call and close it explicitly afterward. The storage port
copies the binding configuration so later caller mutation cannot retarget it.

## Publication, durability and retry

Discovery materializes each UUIDv7 occurrence exactly once. Persistence derives
SHA-256, XXH3-64 and byte size from the same streaming copy. Scanners reopen the
stored object. Equal bytes share an object, while fresh preparation creates new
occurrences even when the path and cut are unchanged.

Local persistence flushes and fsyncs the file before atomic rename, then fsyncs
the destination directory chain. Before an index commit, native storage checks
the operator-owned object path, SHA-256 and size and syncs the file and its
directories. Failed sync prevents visibility; an unreferenced object may remain.

The versioned physical tables are `cut_artifact_files_v1` and
`cut_artifact_{audio,diff,images,pdf,text,video}_v1`. Legacy artifact tables retain
their schemas. Native storage stamps exact `world`, `run`, `tick` and `cut_id` on
every row; incoming coordinates are rejected.

1. Validate the complete submission and its fixed v1 metadata schemas.
2. Persist an immutable per-occurrence preparation object containing the exact
   attributed common and typed Parquet payloads. It grants no visibility.
3. Commit all typed index rows, using the occurrence UUID as the physical retry
   key. The simulation cut ID is a separate row coordinate.
4. Commit each common row last, including the preparation digest and
   native-generated typed table UUIDs, snapshots, schemas and object digests.

There is no cross-table or cross-occurrence atomic transaction. A multi-file
call may partially publish common rows. A lost response may mean that publication
succeeded. Retain `prepared.occurrences` and its exact cut for retry: those frozen
values contain only bytes and strings and may be serialized by the caller for
restart. Repeat publication of those exact values to adopt prior commits.
Reusing a UUID with changed content, attribution, common metadata, or added,
removed or changed typed indexes fails. Rerunning discovery is a new submission,
not recovery of the previous occurrence.

## Reads and supported values

Reads select the cut's common roots, verify exact snapshot/table/object identity,
verify the immutable preparation and complete typed inventory, regenerate and
compare the physical metadata objects, then verify stored content. Missing or
corrupt evidence fails the read. Raw typed-table scans are not supported artifact
reads. Receipts report factual committed coordinates; caller-supplied receipt
metadata never grants authority.

The current file indexes have fixed flat nullable metadata fields: strings,
Int64, Bool and Float64, with a UTC microsecond occurrence timestamp. Image
dimensions widen losslessly from UInt32 to Int64. Iceberg field IDs and UTC
timezone spelling are normalized by native storage. Parquet carries Float64
metadata. Live DDlog relations separately support required Int64/string/Bool/finite
Float64 fields with Int64 entity keys; file indexes do not imply arbitrary Arrow
schema evolution or nested metadata support.

A submission contains at most 32 occurrences, with at most 192 KiB per encoded
metadata object and the existing 1 MiB native request limit. A read page contains
at most 32 roots and 2 MiB, within the existing 16 MiB ABI response envelope.
The [operation-scoped read limits](ddlog-python-preview.md#storage-read-limits-v1)
also bound metadata scans, full-cut verification, preparation decoding and
streamed content verification; exceeding a bound returns an explicit failure.
Offsets refer to the
current occurrence list; concurrent new attachments are not a frozen page cursor.

Artifact-only world contexts, forks, public runtime facade migration, remote
object persistence, HTTP/MCP artifact routes and legacy table migration remain
separate work. No fabricated tick is supplied for a world without a committed
cut.

## Executable contract

`crates/archetype-ddlog/src/store/attachments.rs` tests interrupted typed/common
commits, exact restart adoption, inventory changes, malformed first-use schemas,
content/typed/journal corruption and unchanged cut history.
`tests/artifacts/test_cut_attachments_native.py` runs the family
pipeline through the real C ABI and local Iceberg with the existing simulated
DDlog driver. It covers historical attribution, multiple occurrences, Float64
audio metadata, source deletion, fresh-process retry and failures before file
effects. These storage tests do not claim another actual-DDlog compiler run.
