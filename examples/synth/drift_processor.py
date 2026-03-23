# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0
"""Detect embedding distribution drift across time windows."""
import numpy as np
from daft import DataFrame

from archetype.core.sync.processor import SyncProcessor

from .components import Drift, Embedding


class DriftProcessor(SyncProcessor):
    components = (Embedding, Drift)
    priority = 53

    def process(self, df: DataFrame, **kwargs) -> DataFrame:
        rows = df.select("embedding__vector").collect().to_pylist()
        vectors = [r["embedding__vector"] for r in rows]
        if len(vectors) < 4:
            return df.with_columns({
                "drift__divergence": [0.0] * len(vectors),
                "drift__window": ["insufficient_data"] * len(vectors),
            })
        mid = len(vectors) // 2
        first_half = np.array(vectors[:mid])
        second_half = np.array(vectors[mid:])
        centroid_a = first_half.mean(axis=0)
        centroid_b = second_half.mean(axis=0)
        cos_sim = np.dot(centroid_a, centroid_b) / (
            np.linalg.norm(centroid_a) * np.linalg.norm(centroid_b) + 1e-8
        )
        divergence = float(1.0 - cos_sim)
        windows = ["first_half"] * mid + ["second_half"] * (len(vectors) - mid)
        return df.with_columns({
            "drift__divergence": [divergence] * len(vectors),
            "drift__window": windows,
        })
