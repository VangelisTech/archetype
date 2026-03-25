# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Tests for the memory service."""

import json
import pytest
import daft
from daft import col
from daft.functions import minhash

from archetype.app.memory.components import MemoryChunk
from archetype.app.memory.processors import (
    JACCARD_THRESHOLD,
    NUM_HASHES,
    DedupeProcessor,
    MinHashProcessor,
)


# ── Component tests ───────────────────────────────────────────────────────────


class TestMemoryChunk:
    def test_defaults(self):
        c = MemoryChunk(chunk_id="c1", session_id="s1", content="hello")
        assert c.sig_json == "[]"
        assert c.is_duplicate is False
        assert c.canonical_id == ""

    def test_prefix(self):
        assert MemoryChunk.get_prefix() == "memorychunk__"


# ── MinHashProcessor tests ────────────────────────────────────────────────────


class TestMinHashProcessor:
    def _df(self, contents, sigs=None):
        n = len(contents)
        sigs = sigs or ["[]"] * n
        return daft.from_pydict({
            "entity_id": list(range(n)),
            "memorychunk__chunk_id": [f"c{i}" for i in range(n)],
            "memorychunk__session_id": ["s1"] * n,
            "memorychunk__content": contents,
            "memorychunk__source": ["test"] * n,
            "memorychunk__sig_json": sigs,
            "memorychunk__is_duplicate": [False] * n,
            "memorychunk__canonical_id": [""] * n,
            "memorychunk__similarity_score": [0.0] * n,
        })

    @pytest.mark.asyncio
    async def test_fills_empty_sigs(self):
        df = self._df(["hello world foo bar", "different content here"])
        proc = MinHashProcessor()
        result = await proc.process(df)
        rows = result.collect().to_pylist()
        for row in rows:
            sig = json.loads(row["memorychunk__sig_json"])
            assert len(sig) == NUM_HASHES
            assert all(isinstance(x, int) for x in sig)

    @pytest.mark.asyncio
    async def test_preserves_existing_sigs(self):
        existing_sig = json.dumps([999] * NUM_HASHES)
        df = self._df(["hello world"], sigs=[existing_sig])
        proc = MinHashProcessor()
        result = await proc.process(df)
        rows = result.collect().to_pylist()
        assert rows[0]["memorychunk__sig_json"] == existing_sig

    @pytest.mark.asyncio
    async def test_identical_texts_same_sig(self):
        text = "the quick brown fox jumps over the lazy dog"
        df = self._df([text, text])
        proc = MinHashProcessor()
        result = await proc.process(df)
        rows = result.collect().to_pylist()
        assert rows[0]["memorychunk__sig_json"] == rows[1]["memorychunk__sig_json"]


# ── DedupeProcessor tests ─────────────────────────────────────────────────────


class TestDedupeProcessor:
    def _signed_df(self, contents, session_ids=None):
        n = len(contents)
        session_ids = session_ids or ["s1"] * n

        # Compute real MinHash sigs
        raw = daft.from_pydict({"text": contents})
        sigs = (
            raw.with_column("sig", minhash(col("text"), num_hashes=NUM_HASHES, ngram_size=3, seed=42))
            .collect()
            .to_pylist()
        )
        sig_jsons = [json.dumps(list(r["sig"])) for r in sigs]

        return daft.from_pydict({
            "entity_id": list(range(n)),
            "memorychunk__chunk_id": [f"c{i}" for i in range(n)],
            "memorychunk__session_id": session_ids,
            "memorychunk__content": contents,
            "memorychunk__source": ["test"] * n,
            "memorychunk__sig_json": sig_jsons,
            "memorychunk__is_duplicate": [False] * n,
            "memorychunk__canonical_id": [""] * n,
            "memorychunk__similarity_score": [0.0] * n,
        })

    @pytest.mark.asyncio
    async def test_marks_exact_duplicate(self):
        text = "the exact same text repeated verbatim for dedup testing purposes"
        df = self._signed_df([text, text])
        proc = DedupeProcessor()
        result = await proc.process(df)
        rows = result.collect().to_pylist()
        dup_count = sum(1 for r in rows if r["memorychunk__is_duplicate"])
        assert dup_count == 1

    @pytest.mark.asyncio
    async def test_different_texts_not_duplicate(self):
        df = self._signed_df([
            "completely unrelated text about quantum physics",
            "totally different content about cooking recipes",
        ])
        proc = DedupeProcessor()
        result = await proc.process(df)
        rows = result.collect().to_pylist()
        assert not any(r["memorychunk__is_duplicate"] for r in rows)

    @pytest.mark.asyncio
    async def test_duplicate_points_to_canonical(self):
        text = "shared text for canonical reference test in deduplication"
        df = self._signed_df([text, text])
        proc = DedupeProcessor()
        result = await proc.process(df)
        rows = sorted(result.collect().to_pylist(), key=lambda r: r["entity_id"])
        original = rows[0]
        duplicate = rows[1]
        assert not original["memorychunk__is_duplicate"]
        assert duplicate["memorychunk__is_duplicate"]
        assert duplicate["memorychunk__canonical_id"] == original["memorychunk__chunk_id"]

    @pytest.mark.asyncio
    async def test_cross_session_not_deduped(self):
        """Chunks in different sessions should not be marked as duplicates of each other."""
        text = "identical content across sessions"
        df = self._signed_df([text, text], session_ids=["s1", "s2"])
        proc = DedupeProcessor()
        result = await proc.process(df)
        rows = result.collect().to_pylist()
        assert not any(r["memorychunk__is_duplicate"] for r in rows)
