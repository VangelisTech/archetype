# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0
"""Cluster embeddings via k-means."""
import numpy as np
from daft import DataFrame

from archetype.core.sync.processor import SyncProcessor

from .components import Cluster, Embedding


def cluster_embeddings(vectors: list[list[float]], n_clusters: int = 8) -> dict[str, list]:
    if len(vectors) < n_clusters:
        return {"cluster_id": [-1] * len(vectors), "centroid_distance": [0.0] * len(vectors)}
    from sklearn.cluster import KMeans
    X = np.array(vectors)
    km = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
    labels = km.fit_predict(X)
    distances = np.linalg.norm(X - km.cluster_centers_[labels], axis=1)
    return {"cluster_id": labels.tolist(), "centroid_distance": distances.tolist()}


class ClusterProcessor(SyncProcessor):
    components = (Embedding, Cluster)
    priority = 50

    def __init__(self, n_clusters: int = 8):
        self.n_clusters = n_clusters

    def process(self, df: DataFrame, **kwargs) -> DataFrame:
        rows = df.select("embedding__vector").collect().to_pylist()
        vectors = [r["embedding__vector"] for r in rows if r["embedding__vector"]]
        if not vectors:
            return df
        result = cluster_embeddings(vectors, n_clusters=self.n_clusters)
        return df.with_columns({
            "cluster__cluster_id": result["cluster_id"],
            "cluster__centroid_distance": result["centroid_distance"],
        })
