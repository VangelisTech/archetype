# Artifacts and verified index facts

Install `archetype-ecs[analysis]` for ingestion and Daft analysis. Cold occurrence
facts use the native storage reader and do not require Daft.

```python
from archetype import ArtifactSource

files = world.artifacts("experiment_files")
await files.publish()                       # immutable hosted context
uploaded = await files.upload(
    b"evidence\n", logical_path="note.txt", artifact_id=uuid7_string, cut=cut,
)
page = await files.occurrences(cut=cut)
common = dict(page.items[0].facts())
frame = await files.analyze(index="text", cut=cut)
```

Inline upload accepts exact bytes up to 32 KiB, a canonical portable logical path
and explicit canonical UUIDv7 occurrence. Retrying with the same occurrence and
bytes preserves ingestion time and source identity; changed metadata conflicts.

For larger files or patterns use the existing offline batch graph:

```python
collection = runtime.artifacts("collection")
await collection.publish()                  # no live world or invented tick
prepared = await collection.prepare_files((
    ArtifactSource(source_uri="/absolute/data/*.wav"),
    ArtifactSource(source_uri="/absolute/data/report.pdf"),
))
receipts = await collection.publish_files(prepared)
# Retain `prepared`; exact publication retry uses the same metadata and UUIDs.
assert await collection.publish_files(prepared) == receipts
```

Discovery and indexing run outside live execution. Publication allows at most 32
occurrences and uses native read admission of 64 MiB per file and 256 MiB aggregate.
The graph's discovery and parser memory are not bounded by HTTP envelope limits.
Sources must resolve to distinct logical paths. These local paths never become
HTTP/MCP arguments. Remote clients use bounded inline uploads or an operator-run
batch preparation; there is no public arbitrary filesystem request tunnel.

`occurrences()` selects cutless rows; `occurrences(cut=cut)` selects that exact cut;
`occurrences(all=True)` selects all occurrences within the context, with bounded
pagination. Future world cuts never retroactively attribute cutless occurrences.

Common facts include logical path, UUID-derived ingestion time, hashes, size,
media kind and context/world/run/cut attribution. Typed facts include image
width/height/format/mode; audio sample rate/channels/frames/duration/format/subtype;
video size/frame count/fps/time base/duration; PDF page count/encryption/title/author;
text kind/language/line count/UTF-8; and diff format/file/hunk/line/binary counts.
Unknown metadata remains `None`. `occurrence.facts("audio")` returns an immutable
field/value tuple. `analyze(index="audio")` creates a lazy bounded Daft frame.
Wire integer, Bool and Float64 tags remain exact; timestamps use decimal
`timestamp_us` since UTC epoch. Physical object paths and publication proofs are
excluded; semantic titles and authors are preserved.

Cold reads need only the native library and store. Removing source files,
registry, build directory and driver does not change published facts. Forks create
a distinct hosted context; original occurrences remain attributed to their source
context and are not silently copied as new child occurrences.
