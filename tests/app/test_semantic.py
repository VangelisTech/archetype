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

"""Tests for the semantic embedding and routing module."""

import json
import os

import pytest

# Ensure CPU-only torch before any torch import
os.environ.setdefault("CUDA_VISIBLE_DEVICES", "")


class TestMiniTransformerEncoder:
    """Test the from-scratch PyTorch transformer encoder."""

    @pytest.fixture
    def encoder(self):
        from archetype.app.semantic import _build_encoder

        return _build_encoder(vocab_size=256, dim=64, n_heads=2, n_layers=1, max_seq_len=128)

    def test_encode_returns_correct_dim(self, encoder):
        vec = encoder.encode("hello world")
        assert len(vec) == 64

    def test_encode_is_normalized(self, encoder):
        import math

        vec = encoder.encode("test string")
        norm = math.sqrt(sum(x * x for x in vec))
        assert norm == pytest.approx(1.0, abs=1e-4)

    def test_encode_empty_string(self, encoder):
        vec = encoder.encode("")
        assert len(vec) == 64

    def test_encode_unicode(self, encoder):
        vec = encoder.encode("こんにちは世界 🌍")
        assert len(vec) == 64

    def test_encode_deterministic(self, encoder):
        v1 = encoder.encode("same text")
        v2 = encoder.encode("same text")
        for a, b in zip(v1, v2, strict=False):
            assert a == pytest.approx(b, abs=1e-6)

    def test_different_texts_different_embeddings(self, encoder):
        v1 = encoder.encode("the cat sat on the mat")
        v2 = encoder.encode("quantum chromodynamics")
        # Not identical (extremely unlikely for random init)
        assert v1 != v2

    def test_encode_batch(self, encoder):
        texts = ["hello", "world", "test"]
        vecs = encoder.encode_batch(texts)
        assert len(vecs) == 3
        assert all(len(v) == 64 for v in vecs)

    def test_encode_batch_matches_single(self, encoder):
        """Batch encoding should produce same results as single encoding."""
        texts = ["hello world", "foo bar"]
        batch = encoder.encode_batch(texts)
        singles = [encoder.encode(t) for t in texts]
        for b, s in zip(batch, singles, strict=False):
            for bv, sv in zip(b, s, strict=False):
                assert bv == pytest.approx(sv, abs=1e-4)

    def test_encode_batch_empty(self, encoder):
        assert encoder.encode_batch([]) == []

    def test_long_text_truncation(self, encoder):
        """Text longer than max_seq_len should be truncated, not error."""
        long_text = "a" * 10000
        vec = encoder.encode(long_text)
        assert len(vec) == 64


class TestEmbeddingUDF:
    """Test the Daft class-UDF wrapper."""

    def test_instantiation(self):
        from archetype.app.semantic import EmbeddingUDF

        udf = EmbeddingUDF()
        # @daft.cls() wraps the class; verify it has the embed method
        assert hasattr(udf, "embed")

    def test_embed_returns_json(self):
        from archetype.app.semantic import EmbeddingUDF

        udf = EmbeddingUDF()
        result = udf.embed("Alice", "researcher")
        vec = json.loads(result)
        assert isinstance(vec, list)
        assert len(vec) == 128  # default dim

    def test_embed_normalized(self):
        import math

        from archetype.app.semantic import EmbeddingUDF

        udf = EmbeddingUDF()
        result = udf.embed("Bob", "engineer")
        vec = json.loads(result)
        norm = math.sqrt(sum(x * x for x in vec))
        assert norm == pytest.approx(1.0, abs=1e-4)


class TestSemanticConfig:
    def test_defaults(self):
        from archetype.app.semantic import SemanticConfig

        cfg = SemanticConfig()
        assert cfg.similarity_threshold == 0.3
        assert cfg.max_recipients == 5
        assert cfg.dim == 128

    def test_custom(self):
        from archetype.app.semantic import SemanticConfig

        cfg = SemanticConfig(similarity_threshold=0.8, max_recipients=3, dim=64)
        assert cfg.similarity_threshold == 0.8


class TestSemanticComponents:
    def test_semantic_profile_prefix(self):
        from archetype.app.semantic import SemanticProfile

        assert SemanticProfile.get_prefix() == "semanticprofile__"

    def test_semantic_inbox_prefix(self):
        from archetype.app.semantic import SemanticInbox

        assert SemanticInbox.get_prefix() == "semanticinbox__"

    def test_semantic_profile_defaults(self):
        from archetype.app.semantic import SemanticProfile

        p = SemanticProfile()
        assert p.description == ""
        assert p.topics_json == "[]"


class TestIntegrationRustAndTransformer:
    """End-to-end: transformer produces embeddings, Rust computes similarity."""

    def test_embed_and_compare(self):
        import archetype_similarity as sim

        from archetype.app.semantic import _build_encoder

        encoder = _build_encoder(vocab_size=256, dim=64, n_heads=2, n_layers=1)

        v1 = encoder.encode("machine learning researcher")
        v2 = encoder.encode("deep learning scientist")
        v3 = encoder.encode("professional basketball player")

        # All should be valid similarities
        s12 = sim.cosine_similarity(v1, v2)
        s13 = sim.cosine_similarity(v1, v3)

        assert -1.0 <= s12 <= 1.0
        assert -1.0 <= s13 <= 1.0

    def test_embed_topk_via_similarity_index(self):
        """Full pipeline: encode text → index → topk search."""
        from archetype.app.semantic import _build_encoder
        from archetype.core.embedding import SimilarityIndex

        encoder = _build_encoder(vocab_size=256, dim=64, n_heads=2, n_layers=1)
        index = SimilarityIndex(dim=64)

        agents = {
            1: "quantum physics researcher",
            2: "molecular biology scientist",
            3: "basketball team coach",
            4: "particle physics professor",
            5: "organic chemistry lecturer",
        }

        for eid, desc in agents.items():
            vec = encoder.encode(desc)
            index.upsert(eid, vec)

        # Query for physics-related
        query = encoder.encode("theoretical physics")
        results = index.topk(query, k=3)
        assert len(results) == 3
        # Results are (entity_id, similarity) tuples
        assert all(isinstance(r, tuple) and len(r) == 2 for r in results)
