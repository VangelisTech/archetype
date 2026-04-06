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

"""
Embedding: Vector representations for entity data.

Provides:
- Embedding component for storing dense vectors on entities
- SimilarityIndex resource for fast nearest-neighbor lookups

The SimilarityIndex wraps the Rust `archetype_similarity` crate for
high-performance cosine similarity, with a numpy fallback.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field

import numpy as np

from archetype.core.component import Component

logger = logging.getLogger(__name__)


class Embedding(Component):
    """Dense vector embedding stored on an entity.

    Vectors are stored as JSON-encoded float lists (the ``_json`` convention)
    so they survive Arrow/Lance serialization without schema gymnastics.
    """

    vector_json: str = "[]"
    source_text: str = ""
    model_name: str = ""

    def get_vector(self) -> list[float]:
        return json.loads(self.vector_json)

    @classmethod
    def from_vector(
        cls, vector: list[float], source_text: str = "", model_name: str = ""
    ) -> Embedding:
        return cls(
            vector_json=json.dumps(vector),
            source_text=source_text,
            model_name=model_name,
        )


# ── Rust backend (optional, fast path) ──────────────────────────


def _load_rust_backend():
    """Try to import the Rust similarity module."""
    try:
        import archetype_similarity

        return archetype_similarity
    except ImportError:
        return None


# ── Numpy fallback ──────────────────────────────────────────────


def _np_cosine(a: np.ndarray, b: np.ndarray) -> float:
    denom = np.linalg.norm(a) * np.linalg.norm(b)
    if denom < 1e-8:
        return 0.0
    return float(np.dot(a, b) / denom)


def _np_topk(query: np.ndarray, matrix: np.ndarray, k: int) -> list[tuple[int, float]]:
    norms = np.linalg.norm(matrix, axis=1)
    query_norm = np.linalg.norm(query)
    safe = (norms * query_norm) > 1e-8
    sims = np.zeros(matrix.shape[0])
    sims[safe] = (matrix[safe] @ query) / (norms[safe] * query_norm)
    top_idx = np.argsort(sims)[::-1][:k]
    return [(int(i), float(sims[i])) for i in top_idx]


# ── SimilarityIndex resource ────────────────────────────────────


@dataclass
class SimilarityIndex:
    """World-scoped resource for fast vector similarity lookups.

    Maintains an in-memory index of entity embeddings that processors
    can query during a tick.  Uses the Rust ``archetype_similarity``
    crate when available; falls back to numpy otherwise.

    Usage::

        # In world setup
        index = SimilarityIndex(dim=384)
        world.resources.insert(index)

        # In a processor
        index = resources.require(SimilarityIndex)
        index.upsert(entity_id=42, vector=[0.1, 0.2, ...])
        matches = index.topk(query=[0.1, 0.2, ...], k=5)
    """

    dim: int = 384
    _vectors: dict[int, list[float]] = field(default_factory=dict)
    _rust: object = field(default=None, init=False, repr=False)

    def __post_init__(self):
        self._rust = _load_rust_backend()
        backend = "rust" if self._rust else "numpy"
        logger.info("SimilarityIndex: dim=%d, backend=%s", self.dim, backend)

    def upsert(self, entity_id: int, vector: list[float]) -> None:
        """Insert or update an entity's embedding."""
        if len(vector) != self.dim:
            raise ValueError(f"Vector dim {len(vector)} != index dim {self.dim}")
        self._vectors[entity_id] = vector

    def remove(self, entity_id: int) -> None:
        self._vectors.pop(entity_id, None)

    def get(self, entity_id: int) -> list[float] | None:
        return self._vectors.get(entity_id)

    @property
    def size(self) -> int:
        return len(self._vectors)

    def cosine(self, a: list[float], b: list[float]) -> float:
        """Cosine similarity between two vectors."""
        if self._rust:
            return self._rust.cosine_similarity(a, b)
        return _np_cosine(np.array(a, dtype=np.float32), np.array(b, dtype=np.float32))

    def topk(
        self, query: list[float], k: int = 5, exclude: set[int] | None = None
    ) -> list[tuple[int, float]]:
        """Find the k most similar entities to a query vector.

        Returns list of (entity_id, similarity) sorted descending.
        """
        if not self._vectors:
            return []

        exclude = exclude or set()
        ids = [eid for eid in self._vectors if eid not in exclude]
        if not ids:
            return []

        if self._rust:
            flat = []
            for eid in ids:
                flat.extend(self._vectors[eid])
            results = self._rust.topk_similar(query, flat, self.dim, k)
            return [(ids[idx], sim) for idx, sim in results]
        else:
            matrix = np.array([self._vectors[eid] for eid in ids], dtype=np.float32)
            q = np.array(query, dtype=np.float32)
            results = _np_topk(q, matrix, k)
            return [(ids[idx], sim) for idx, sim in results]

    def pairwise(self) -> dict[tuple[int, int], float]:
        """Compute pairwise similarity between all indexed entities.

        Returns dict mapping (entity_a, entity_b) → similarity.
        Only includes pairs where a < b (symmetric).
        """
        if len(self._vectors) < 2:
            return {}

        ids = sorted(self._vectors.keys())
        n = len(ids)

        if self._rust:
            flat = []
            for eid in ids:
                flat.extend(self._vectors[eid])
            pw = self._rust.pairwise_matrix(flat, self.dim)
            result = {}
            for i in range(n):
                for j in range(i + 1, n):
                    result[(ids[i], ids[j])] = pw[i * n + j]
            return result
        else:
            matrix = np.array([self._vectors[eid] for eid in ids], dtype=np.float32)
            norms = np.linalg.norm(matrix, axis=1, keepdims=True)
            norms = np.maximum(norms, 1e-8)
            normed = matrix / norms
            sim_matrix = normed @ normed.T
            result = {}
            for i in range(n):
                for j in range(i + 1, n):
                    result[(ids[i], ids[j])] = float(sim_matrix[i, j])
            return result

    def clear(self) -> None:
        self._vectors.clear()
