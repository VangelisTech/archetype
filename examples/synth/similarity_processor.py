# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0
"""k-nearest neighbor search over embeddings."""

import daft
import numpy as np
from daft import DataFrame, DataType, col

from archetype.core.sync.processor import SyncProcessor

from .components import Embedding, Neighbors


@daft.cls
class KNNIndex:
    """Stateful Daft class — builds normalized matrix once, queries per-row."""

    def __init__(self, vectors: list[list[float]], entity_ids: list[str], k: int):
        arr = np.array(vectors)
        norms = np.linalg.norm(arr, axis=1, keepdims=True).clip(min=1e-8)
        self._normed = arr / norms
        self._ids = entity_ids
        self._k = min(k, len(vectors) - 1)

    @daft.method(
        return_dtype=DataType.struct({
            "neighbor_ids": DataType.list(DataType.string()),
            "distances": DataType.list(DataType.float64()),
        }),
        unnest=True,
    )
    def query(self, vector: list[float]) -> dict:
        v = np.array(vector)
        v = v / max(np.linalg.norm(v), 1e-8)
        sims = self._normed @ v
        # Exclude self-match (sim ≈ 1.0 exact match)
        top_indices = np.argsort(sims)[-(self._k + 1) :][::-1]
        neighbor_ids: list[str] = []
        distances: list[float] = []
        for idx in top_indices:
            if len(neighbor_ids) >= self._k:
                break
            neighbor_ids.append(self._ids[idx])
            distances.append(float(1.0 - sims[idx]))
        return {"neighbor_ids": neighbor_ids, "distances": distances}


class SimilarityProcessor(SyncProcessor):
    components = (Embedding, Neighbors)
    priority = 51

    def __init__(self, k: int = 5):
        self.k = k

    def process(self, df: DataFrame, **kwargs) -> DataFrame:
        # Collect only the columns needed for index construction
        rows = (
            df.select("embedding__vector", "entity_id")
            .collect()
            .to_pylist()
        )
        vectors = [r["embedding__vector"] for r in rows]
        entity_ids = [r.get("entity_id") or str(i) for i, r in enumerate(rows)]
        if len(vectors) < 2:
            return df

        # Build index once, query per-row via daft.cls
        index = KNNIndex(vectors, entity_ids, self.k)
        return df.select(col("*"), index.query(col("embedding__vector"))).with_columns_renamed({
            "neighbor_ids": "neighbors__neighbor_ids",
            "distances": "neighbors__distances",
        })
