# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0
"""Cluster embeddings via k-means."""

import daft
import numpy as np
from daft import DataFrame, DataType, Series, col

from archetype.core.sync.processor import SyncProcessor

from .components import Cluster, Embedding


def cluster_embeddings(vectors: list[list[float]], n_clusters: int = 8) -> dict[str, list]:
    """Standalone helper — kept for use in tests."""
    if len(vectors) < n_clusters:
        return {"cluster_id": [-1] * len(vectors), "centroid_distance": [0.0] * len(vectors)}
    from sklearn.cluster import KMeans

    X = np.array(vectors)
    km = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
    labels = km.fit_predict(X)
    distances = np.linalg.norm(X - km.cluster_centers_[labels], axis=1)
    return {"cluster_id": labels.tolist(), "centroid_distance": distances.tolist()}


@daft.func.batch(
    return_dtype=DataType.struct({
        "cluster_id": DataType.int64(),
        "centroid_distance": DataType.float64(),
    }),
    unnest=True,
)
def kmeans_cluster(vectors: Series, n_clusters: int) -> list[dict]:
    """Batch UDF: fits KMeans on the full partition, returns per-row assignments."""
    from sklearn.cluster import KMeans

    vecs = vectors.to_pylist()
    X = np.array(vecs)
    if len(X) < n_clusters:
        return [{"cluster_id": -1, "centroid_distance": 0.0}] * len(X)
    km = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
    labels = km.fit_predict(X)
    dists = np.linalg.norm(X - km.cluster_centers_[labels], axis=1)
    return [
        {"cluster_id": int(label), "centroid_distance": float(dist)}
        for label, dist in zip(labels, dists, strict=True)
    ]


class ClusterProcessor(SyncProcessor):
    components = (Embedding, Cluster)
    priority = 50

    def __init__(self, n_clusters: int = 8):
        self.n_clusters = n_clusters

    def process(self, df: DataFrame, **kwargs) -> DataFrame:
        return df.select(
            col("*"),
            kmeans_cluster(col("embedding__vector"), self.n_clusters),
        ).with_columns_renamed({
            "cluster_id": "cluster__cluster_id",
            "centroid_distance": "cluster__centroid_distance",
        })
