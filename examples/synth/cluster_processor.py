# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0
"""Cluster embeddings via k-means."""

import daft
import numpy as np
from daft import DataFrame, DataType, col

from archetype.core.sync.processor import SyncProcessor

from .components import Cluster, Embedding


def cluster_embeddings(vectors: list[list[float]], n_clusters: int = 8) -> dict[str, list]:
    """Standalone helper — kept for use in recurse.py and tests."""
    if len(vectors) < n_clusters:
        return {"cluster_id": [-1] * len(vectors), "centroid_distance": [0.0] * len(vectors)}
    from sklearn.cluster import KMeans

    X = np.array(vectors)
    km = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
    labels = km.fit_predict(X)
    distances = np.linalg.norm(X - km.cluster_centers_[labels], axis=1)
    return {"cluster_id": labels.tolist(), "centroid_distance": distances.tolist()}


@daft.cls
class KMeansScorer:
    """Stateful Daft class — fits KMeans once, predicts per-row."""

    def __init__(self, centers: list[list[float]]):
        self._centers = np.array(centers)

    @daft.method(
        return_dtype=DataType.struct({
            "cluster_id": DataType.int64(),
            "centroid_distance": DataType.float64(),
        }),
        unnest=True,
    )
    def predict(self, vector: list[float]) -> dict:
        v = np.array(vector)
        dists = np.linalg.norm(self._centers - v, axis=1)
        idx = int(np.argmin(dists))
        return {"cluster_id": idx, "centroid_distance": float(dists[idx])}


class ClusterProcessor(SyncProcessor):
    components = (Embedding, Cluster)
    priority = 50

    def __init__(self, n_clusters: int = 8):
        self.n_clusters = n_clusters

    def process(self, df: DataFrame, **kwargs) -> DataFrame:
        # Fit step: collect vectors once to compute centroids
        rows = df.select("embedding__vector").collect().to_pylist()
        vectors = [r["embedding__vector"] for r in rows if r["embedding__vector"]]
        if not vectors or len(vectors) < self.n_clusters:
            return df
        from sklearn.cluster import KMeans

        X = np.array(vectors)
        km = KMeans(n_clusters=self.n_clusters, random_state=42, n_init=10)
        km.fit(X)

        # Score step: per-row via daft.cls (no second collect)
        scorer = KMeansScorer(km.cluster_centers_.tolist())
        return df.select(col("*"), scorer.predict(col("embedding__vector"))).with_columns_renamed({
            "cluster_id": "cluster__cluster_id",
            "centroid_distance": "cluster__centroid_distance",
        })
