# Bookmarks Example

A domain example showing how to build an ECS ingestion pipeline on top of
Archetype's core runtime. Pulls X (Twitter) bookmarks, enriches them with an
LLM, and makes them searchable.

## Why this lives in `examples/`

This is **not** part of the `archetype` framework. It does not go through
`CommandService`/`CommandBroker` and is not part of the public API. It is a
reference application that demonstrates:

- Defining custom `Component` types (`Bookmark`, `KnowledgeEntry`, ...)
- Writing `AsyncProcessor` pipeline stages (dedup → enrich → index)
- Using `daft.functions.prompt` inside a processor for LLM calls
- Composing a high-level pipeline API around the core runtime

## Layout

```
examples/bookmarks/
├── __init__.py      # package exports
├── client.py        # X API v2 client
├── components.py    # Bookmark, KnowledgeEntry, Media, UrlEntity
├── processors.py    # DeduplicationProcessor, EnrichmentProcessor, IndexingProcessor
├── pipeline.py      # BookmarksPipeline high-level API
└── tests/           # Unit tests (run with pytest from this directory)
```

## Usage

From the repo root:

```python
import sys
sys.path.insert(0, "examples")

from bookmarks import BookmarksPipeline

pipeline = BookmarksPipeline(
    name="my-knowledge-base",
    bearer_token="your-x-bearer-token",
    storage_uri="s3://your-bucket/bookmarks",
)

await pipeline.fetch()   # Pull from X API
await pipeline.run()     # Dedup → Enrich → Index
results = await pipeline.search("recursive self-improvement")
```

## Running tests

```bash
uv run pytest examples/bookmarks/tests
```

## Known follow-ups

- `DeduplicationProcessor` currently `.collect()`s (tweet_id, entity_id)
  into Python memory and runs a row-wise UDF. Fine at bookmark scale, but
  should be rewritten as a pure groupby+join for larger corpora.
- `IndexingProcessor` does not yet generate embeddings — it formats a
  search-friendly document. Real vector embedding happens when LanceDB's
  native indexing is wired in.
- `BookmarksPipeline` only constructs a LanceDB `StorageConfig`. Iceberg
  backend selection is not yet exposed.
