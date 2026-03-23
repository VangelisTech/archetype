# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0
"""k-nearest neighbor search over embeddings."""
import numpy as np
from daft import DataFrame

from archetype.core.sync.processor import SyncProcessor

from .components import Embedding, Neighbors


class SimilarityProcessor(SyncProcessor):
    components = (Embedding, Neighbors)
    priority = 51

    def __init__(self, k: int = 5):
        self.k = k

    def process(self, df: DataFrame, **kwargs) -> DataFrame:
        rows = df.collect().to_pylist()
        vectors = np.array([r["embedding__vector"] for r in rows])
        entity_ids = [r.get("entity_id", str(i)) for i, r in enumerate(rows)]
        if len(vectors) < 2:
            return df
        norms = np.linalg.norm(vectors, axis=1, keepdims=True).clip(min=1e-8)
        normed = vectors / norms
        sim_matrix = normed @ normed.T
        k = min(self.k, len(vectors) - 1)
        neighbor_ids_list = []
        distances_list = []
        for i in range(len(vectors)):
            sims = sim_matrix[i].copy()
            sims[i] = -np.inf
            top_k = np.argsort(sims)[-k:][::-1]
            neighbor_ids_list.append([entity_ids[j] for j in top_k])
            distances_list.append([float(1.0 - sims[j]) for j in top_k])
        return df.with_columns({
            "neighbors__neighbor_ids": neighbor_ids_list,
            "neighbors__distances": distances_list,
        })
