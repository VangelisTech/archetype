# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Tests for the recursive embeddings improvement service."""

import json

import daft
import pytest
from daft import DataFrame, col
from daft.functions import when

from archetype.app.embeddings.components import Document, Embedding, EmbeddingFrontier
from archetype.app.embeddings.processors import (
    EmbedProcessor,
    FrontierProcessor,
    QualityProcessor,
)


# ── Component Tests ────────────────────────────────────────────────────────────


class TestComponents:
    def test_document_defaults(self):
        doc = Document(doc_id="d1", content="hello world")
        assert doc.doc_id == "d1"
        assert doc.content == "hello world"
        assert doc.chunk_index == 0

    def test_embedding_defaults(self):
        emb = Embedding(doc_id="d1")
        assert emb.vector_json == "[]"
        assert emb.quality_score == 0.0
        assert emb.generation == 0

    def test_frontier_defaults(self):
        frontier = EmbeddingFrontier(model_name="all-MiniLM-L6-v2")
        assert frontier.avg_quality == 0.0
        assert frontier.generation == 0
        assert frontier.centroid_json == "[]"

    def test_component_prefixes(self):
        assert Document.get_prefix() == "document__"
        assert Embedding.get_prefix() == "embedding__"
        assert EmbeddingFrontier.get_prefix() == "embeddingfrontier__"


# ── EmbedProcessor Tests ───────────────────────────────────────────────────────


class TestEmbedProcessor:
    @pytest.mark.asyncio
    async def test_embed_fills_vector_json(self):
        """EmbedProcessor should populate vector_json for all rows."""
        df = daft.from_pydict(
            {
                "document__content": ["hello world", "foo bar"],
                "document__doc_id": ["d1", "d2"],
                "document__chunk_index": [0, 0],
                "embedding__doc_id": ["d1", "d2"],
                "embedding__model_name": ["", ""],
                "embedding__vector_json": ["[]", "[]"],
                "embedding__generation": [0, 0],
                "embedding__quality_score": [0.0, 0.0],
            }
        )

        proc = EmbedProcessor(model_name="all-MiniLM-L6-v2")
        result = await proc.process(df, tick=0)

        rows = result.collect().to_pylist()
        assert len(rows) == 2
        for row in rows:
            vec = json.loads(row["embedding__vector_json"])
            assert isinstance(vec, list)
            assert len(vec) > 0, "vector should be non-empty"

    @pytest.mark.asyncio
    async def test_embed_sets_generation(self):
        df = daft.from_pydict(
            {
                "document__content": ["test"],
                "document__doc_id": ["d1"],
                "document__chunk_index": [0],
                "embedding__doc_id": ["d1"],
                "embedding__model_name": [""],
                "embedding__vector_json": ["[]"],
                "embedding__generation": [0],
                "embedding__quality_score": [0.0],
            }
        )
        proc = EmbedProcessor()
        result = await proc.process(df, tick=3)
        rows = result.collect().to_pylist()
        assert rows[0]["embedding__generation"] == 3

    @pytest.mark.asyncio
    async def test_embed_sets_model_name(self):
        df = daft.from_pydict(
            {
                "document__content": ["test"],
                "document__doc_id": ["d1"],
                "document__chunk_index": [0],
                "embedding__doc_id": ["d1"],
                "embedding__model_name": [""],
                "embedding__vector_json": ["[]"],
                "embedding__generation": [0],
                "embedding__quality_score": [0.0],
            }
        )
        proc = EmbedProcessor(model_name="test-model")
        result = await proc.process(df, tick=0)
        rows = result.collect().to_pylist()
        assert rows[0]["embedding__model_name"] == "test-model"


# ── QualityProcessor Tests ─────────────────────────────────────────────────────


class TestQualityProcessor:
    def _make_df(self, vec_a, centroid):
        return daft.from_pydict(
            {
                "document__content": ["test"],
                "document__doc_id": ["d1"],
                "document__chunk_index": [0],
                "embedding__doc_id": ["d1"],
                "embedding__model_name": ["m"],
                "embedding__vector_json": [json.dumps(vec_a)],
                "embedding__generation": [1],
                "embedding__quality_score": [0.0],
                "embeddingfrontier__model_name": ["m"],
                "embeddingfrontier__avg_quality": [0.5],
                "embeddingfrontier__generation": [0],
                "embeddingfrontier__centroid_json": [json.dumps(centroid)],
            }
        )

    @pytest.mark.asyncio
    async def test_identical_vectors_score_one(self):
        vec = [1.0, 0.0, 0.0]
        df = self._make_df(vec, vec)
        proc = QualityProcessor()
        result = await proc.process(df)
        rows = result.collect().to_pylist()
        score = rows[0]["embedding__quality_score"]
        assert abs(score - 1.0) < 1e-6

    @pytest.mark.asyncio
    async def test_orthogonal_vectors_score_zero(self):
        df = self._make_df([1.0, 0.0], [0.0, 1.0])
        proc = QualityProcessor()
        result = await proc.process(df)
        rows = result.collect().to_pylist()
        score = rows[0]["embedding__quality_score"]
        assert abs(score) < 1e-6

    @pytest.mark.asyncio
    async def test_empty_centroid_preserves_score(self):
        """Empty frontier centroid → quality_score unchanged."""
        df = daft.from_pydict(
            {
                "document__content": ["test"],
                "document__doc_id": ["d1"],
                "document__chunk_index": [0],
                "embedding__doc_id": ["d1"],
                "embedding__model_name": ["m"],
                "embedding__vector_json": [json.dumps([1.0, 0.0])],
                "embedding__generation": [0],
                "embedding__quality_score": [0.42],
                "embeddingfrontier__model_name": ["m"],
                "embeddingfrontier__avg_quality": [0.0],
                "embeddingfrontier__generation": [0],
                "embeddingfrontier__centroid_json": [None],
            }
        )
        proc = QualityProcessor()
        result = await proc.process(df)
        rows = result.collect().to_pylist()
        assert rows[0]["embedding__quality_score"] == pytest.approx(0.42)


# ── FrontierProcessor Tests ────────────────────────────────────────────────────


class TestFrontierProcessor:
    def _make_df(self, quality_scores, frontier_avg, tick=1):
        n = len(quality_scores)
        return daft.from_pydict(
            {
                "embedding__doc_id": [f"d{i}" for i in range(n)],
                "embedding__model_name": ["m"] * n,
                "embedding__vector_json": [json.dumps([1.0, 0.0])] * n,
                "embedding__generation": [tick] * n,
                "embedding__quality_score": quality_scores,
                "embeddingfrontier__model_name": ["m"] * n,
                "embeddingfrontier__avg_quality": [frontier_avg] * n,
                "embeddingfrontier__generation": [0] * n,
                "embeddingfrontier__centroid_json": [json.dumps([1.0, 0.0])] * n,
            }
        )

    @pytest.mark.asyncio
    async def test_frontier_advances_when_quality_improves(self):
        # avg of [0.8, 0.9] = 0.85 > frontier 0.5
        df = self._make_df([0.8, 0.9], frontier_avg=0.5, tick=1)
        proc = FrontierProcessor()
        result = await proc.process(df, tick=1)
        rows = result.collect().to_pylist()
        for row in rows:
            if row.get("embeddingfrontier__centroid_json") not in (None, "[]"):
                assert row["embeddingfrontier__avg_quality"] == pytest.approx(0.85)
                assert row["embeddingfrontier__generation"] == 1

    @pytest.mark.asyncio
    async def test_frontier_unchanged_when_quality_regresses(self):
        # avg of [0.3, 0.4] = 0.35 < frontier 0.5
        df = self._make_df([0.3, 0.4], frontier_avg=0.5, tick=1)
        proc = FrontierProcessor()
        result = await proc.process(df, tick=1)
        rows = result.collect().to_pylist()
        for row in rows:
            if row.get("embeddingfrontier__centroid_json") not in (None, "[]"):
                assert row["embeddingfrontier__avg_quality"] == pytest.approx(0.5)
                assert row["embeddingfrontier__generation"] == 0
