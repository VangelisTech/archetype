# Copyright 2025 Vangelis Technologies Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Tests for the Embedding component and SimilarityIndex resource."""

import json

import pytest

from archetype.core.embedding import Embedding, SimilarityIndex

# ── Embedding component ─────────────────────────────────────────


class TestEmbeddingComponent:
    def test_default_empty(self):
        e = Embedding()
        assert e.get_vector() == []
        assert e.source_text == ""
        assert e.model_name == ""

    def test_from_vector(self):
        vec = [0.1, 0.2, 0.3]
        e = Embedding.from_vector(vec, source_text="hello", model_name="test")
        assert e.get_vector() == pytest.approx(vec)
        assert e.source_text == "hello"
        assert e.model_name == "test"

    def test_roundtrip_json(self):
        vec = [1.0, -0.5, 0.0, 0.25]
        e = Embedding.from_vector(vec)
        decoded = json.loads(e.vector_json)
        assert decoded == pytest.approx(vec)

    def test_prefix(self):
        assert Embedding.get_prefix() == "embedding__"

    def test_to_row_dict(self):
        e = Embedding.from_vector([1.0, 2.0], source_text="test")
        d = e.to_row_dict()
        assert "embedding__vector_json" in d
        assert "embedding__source_text" in d
        assert d["embedding__source_text"] == "test"


# ── SimilarityIndex resource ────────────────────────────────────


class TestSimilarityIndex:
    def test_upsert_and_get(self):
        idx = SimilarityIndex(dim=3)
        idx.upsert(1, [1.0, 0.0, 0.0])
        assert idx.get(1) == [1.0, 0.0, 0.0]
        assert idx.size == 1

    def test_upsert_overwrite(self):
        idx = SimilarityIndex(dim=2)
        idx.upsert(1, [1.0, 0.0])
        idx.upsert(1, [0.0, 1.0])
        assert idx.get(1) == [0.0, 1.0]
        assert idx.size == 1

    def test_remove(self):
        idx = SimilarityIndex(dim=2)
        idx.upsert(1, [1.0, 0.0])
        idx.remove(1)
        assert idx.get(1) is None
        assert idx.size == 0

    def test_dim_mismatch(self):
        idx = SimilarityIndex(dim=3)
        with pytest.raises(ValueError, match="dim"):
            idx.upsert(1, [1.0, 0.0])

    def test_cosine_identical(self):
        idx = SimilarityIndex(dim=3)
        v = [1.0, 2.0, 3.0]
        assert idx.cosine(v, v) == pytest.approx(1.0, abs=1e-5)

    def test_cosine_orthogonal(self):
        idx = SimilarityIndex(dim=2)
        assert idx.cosine([1.0, 0.0], [0.0, 1.0]) == pytest.approx(0.0, abs=1e-5)

    def test_cosine_opposite(self):
        idx = SimilarityIndex(dim=2)
        assert idx.cosine([1.0, 0.0], [-1.0, 0.0]) == pytest.approx(-1.0, abs=1e-5)

    def test_topk_basic(self):
        idx = SimilarityIndex(dim=2)
        idx.upsert(10, [1.0, 0.0])  # points right
        idx.upsert(20, [0.7, 0.7])  # 45 degrees
        idx.upsert(30, [0.0, 1.0])  # points up

        results = idx.topk([1.0, 0.0], k=2)
        assert len(results) == 2
        # Entity 10 should be most similar (identical direction)
        assert results[0][0] == 10
        assert results[0][1] == pytest.approx(1.0, abs=1e-5)
        # Entity 20 should be second
        assert results[1][0] == 20

    def test_topk_exclude(self):
        idx = SimilarityIndex(dim=2)
        idx.upsert(1, [1.0, 0.0])
        idx.upsert(2, [0.9, 0.1])
        idx.upsert(3, [0.0, 1.0])

        results = idx.topk([1.0, 0.0], k=2, exclude={1})
        assert all(eid != 1 for eid, _ in results)

    def test_topk_empty(self):
        idx = SimilarityIndex(dim=2)
        assert idx.topk([1.0, 0.0], k=5) == []

    def test_pairwise(self):
        idx = SimilarityIndex(dim=2)
        idx.upsert(1, [1.0, 0.0])
        idx.upsert(2, [0.0, 1.0])
        idx.upsert(3, [1.0, 0.0])

        pw = idx.pairwise()
        # (1, 2) should be orthogonal
        assert pw[(1, 2)] == pytest.approx(0.0, abs=1e-5)
        # (1, 3) should be identical
        assert pw[(1, 3)] == pytest.approx(1.0, abs=1e-5)
        # (2, 3) should be orthogonal
        assert pw[(2, 3)] == pytest.approx(0.0, abs=1e-5)

    def test_pairwise_too_few(self):
        idx = SimilarityIndex(dim=2)
        idx.upsert(1, [1.0, 0.0])
        assert idx.pairwise() == {}

    def test_clear(self):
        idx = SimilarityIndex(dim=2)
        idx.upsert(1, [1.0, 0.0])
        idx.upsert(2, [0.0, 1.0])
        idx.clear()
        assert idx.size == 0


# ── Rust backend validation ─────────────────────────────────────


class TestRustBackend:
    """Verify the Rust archetype_similarity module works correctly."""

    def test_import(self):
        import archetype_similarity

        assert hasattr(archetype_similarity, "cosine_similarity")
        assert hasattr(archetype_similarity, "topk_similar")
        assert hasattr(archetype_similarity, "pairwise_matrix")
        assert hasattr(archetype_similarity, "batch_cosine")

    def test_cosine_similarity(self):
        import archetype_similarity as sim

        assert sim.cosine_similarity([1.0, 0.0], [1.0, 0.0]) == pytest.approx(1.0)
        assert sim.cosine_similarity([1.0, 0.0], [0.0, 1.0]) == pytest.approx(0.0)

    def test_dimension_mismatch(self):
        import archetype_similarity as sim

        with pytest.raises(ValueError):
            sim.cosine_similarity([1.0], [1.0, 0.0])

    def test_topk_similar(self):
        import archetype_similarity as sim

        matrix = [1.0, 0.0, 0.7, 0.7, 0.0, 1.0]
        results = sim.topk_similar([1.0, 0.0], matrix, 2, 2)
        assert len(results) == 2
        assert results[0][0] == 0  # first vector is most similar

    def test_pairwise_matrix(self):
        import archetype_similarity as sim

        pw = sim.pairwise_matrix([1.0, 0.0, 0.0, 1.0], 2)
        assert len(pw) == 4
        assert pw[0] == pytest.approx(1.0)  # self-sim
        assert pw[3] == pytest.approx(1.0)  # self-sim
        assert pw[1] == pytest.approx(0.0)  # orthogonal

    def test_batch_cosine(self):
        import archetype_similarity as sim

        # 2 queries, 2 targets, dim=2
        result = sim.batch_cosine(
            [1.0, 0.0, 0.0, 1.0],  # queries: [1,0] and [0,1]
            [1.0, 0.0, 0.0, 1.0],  # targets: [1,0] and [0,1]
            2,
        )
        assert len(result) == 4
        assert result[0] == pytest.approx(1.0)  # [1,0]·[1,0]
        assert result[1] == pytest.approx(0.0)  # [1,0]·[0,1]
        assert result[2] == pytest.approx(0.0)  # [0,1]·[1,0]
        assert result[3] == pytest.approx(1.0)  # [0,1]·[0,1]
