# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Tests for bookmark processors — dedup, indexing."""

import json

import daft
import pytest

from archetype.bookmarks.components import Bookmark, KnowledgeEntry
from archetype.bookmarks.processors import (
    DeduplicationProcessor,
    IndexingProcessor,
)


def _make_df(bookmarks: list[Bookmark], entries: list[KnowledgeEntry]) -> daft.DataFrame:
    """Build a DataFrame matching what the ECS would produce for (Bookmark, KnowledgeEntry) entities."""
    rows = []
    for bm, entry in zip(bookmarks, entries, strict=True):
        row = {}
        for field_name, val in bm.model_dump().items():
            row[f"bookmark__{field_name}"] = val
        for field_name, val in entry.model_dump().items():
            row[f"knowledgeentry__{field_name}"] = val
        row["entity_id"] = len(rows)
        row["is_active"] = True
        rows.append(row)
    return daft.from_pylist(rows)


def _make_bookmarks(n: int, *, duplicate_ids: list[int] | None = None) -> list[Bookmark]:
    """Create n bookmarks. duplicate_ids lists indices that share tweet_id with index 0."""
    duplicate_ids = duplicate_ids or []
    bookmarks = []
    for i in range(n):
        tid = "tweet_0" if i in duplicate_ids else f"tweet_{i}"
        bookmarks.append(
            Bookmark(
                tweet_id=tid,
                author_id=f"author_{i}",
                author_username=f"user{i}",
                author_name=f"User {i}",
                text=f"This is tweet number {i} about topic {i % 3}",
                created_at=f"2026-01-{i + 1:02d}T10:00:00Z",
                lang="en",
                metrics_json=json.dumps({"like_count": i * 10, "retweet_count": i * 2}),
            )
        )
    return bookmarks


@pytest.mark.asyncio
async def test_dedup_marks_duplicates():
    """DeduplicationProcessor should mark later entities with same tweet_id as duplicates."""
    bookmarks = _make_bookmarks(4, duplicate_ids=[2, 3])
    entries = [KnowledgeEntry() for _ in bookmarks]
    df = _make_df(bookmarks, entries)

    proc = DeduplicationProcessor()
    result = await proc.process(df)
    collected = result.collect().to_pylist()

    assert len(collected) == 4  # All rows preserved

    dupes = {r["entity_id"]: r["knowledgeentry__is_duplicate"] for r in collected}
    # entity 0 has tweet_0 (first seen) — not a duplicate
    assert dupes[0] is False
    # entity 1 has tweet_1 — unique
    assert dupes[1] is False
    # entity 2 has tweet_0 — duplicate of entity 0
    assert dupes[2] is True
    # entity 3 has tweet_0 — duplicate of entity 0
    assert dupes[3] is True


@pytest.mark.asyncio
async def test_dedup_all_unique():
    """All unique tweet_ids should result in no duplicates."""
    bookmarks = _make_bookmarks(3)
    entries = [KnowledgeEntry() for _ in bookmarks]
    df = _make_df(bookmarks, entries)

    proc = DeduplicationProcessor()
    result = await proc.process(df)
    collected = result.collect().to_pylist()

    assert all(not r["knowledgeentry__is_duplicate"] for r in collected)
    assert all(r["knowledgeentry__deduplicated"] for r in collected)


@pytest.mark.asyncio
async def test_dedup_empty_tweet_id():
    """Bookmarks with empty tweet_id should not be marked as duplicates of each other."""
    bookmarks = [Bookmark(tweet_id="", text="no id 1"), Bookmark(tweet_id="", text="no id 2")]
    entries = [KnowledgeEntry() for _ in bookmarks]
    df = _make_df(bookmarks, entries)

    proc = DeduplicationProcessor()
    result = await proc.process(df)
    collected = result.collect().to_pylist()

    assert all(not r["knowledgeentry__is_duplicate"] for r in collected)


@pytest.mark.asyncio
async def test_indexing_builds_search_doc():
    """IndexingProcessor should build a composite search document for processed bookmarks."""
    bookmarks = [
        Bookmark(
            tweet_id="t1",
            text="Original tweet about Daft DataFrames",
            author_username="daftdev",
        )
    ]
    entries = [
        KnowledgeEntry(
            summary="A post about Daft DataFrame performance",
            topics_json=json.dumps(["daft", "dataframes", "performance"]),
            processed=True,
            is_duplicate=False,
        )
    ]
    df = _make_df(bookmarks, entries)

    proc = IndexingProcessor()
    result = await proc.process(df)
    collected = result.collect().to_pylist()

    doc = collected[0]["knowledgeentry__summary"]
    assert "@daftdev" in doc
    assert "Daft DataFrame performance" in doc
    assert "daft" in doc


@pytest.mark.asyncio
async def test_indexing_skips_unprocessed():
    """IndexingProcessor should not modify unprocessed bookmarks."""
    bookmarks = [Bookmark(tweet_id="t1", text="Unprocessed tweet")]
    entries = [KnowledgeEntry(processed=False, summary="original")]
    df = _make_df(bookmarks, entries)

    proc = IndexingProcessor()
    result = await proc.process(df)
    collected = result.collect().to_pylist()

    # Summary should be unchanged for unprocessed rows
    assert collected[0]["knowledgeentry__summary"] == "original"


@pytest.mark.asyncio
async def test_indexing_skips_duplicates():
    """IndexingProcessor should not modify duplicate bookmarks."""
    bookmarks = [Bookmark(tweet_id="t1", text="Duplicate tweet")]
    entries = [KnowledgeEntry(processed=True, is_duplicate=True, summary="original")]
    df = _make_df(bookmarks, entries)

    proc = IndexingProcessor()
    result = await proc.process(df)
    collected = result.collect().to_pylist()

    assert collected[0]["knowledgeentry__summary"] == "original"
