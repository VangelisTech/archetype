# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0
"""Detect embedding distribution drift across time windows."""

import daft
import numpy as np
from daft import DataFrame, col

from archetype.core.sync.processor import SyncProcessor

from .components import Drift, Embedding


@daft.func
def _assign_window(row_index: int, midpoint: int) -> str:
    return "first_half" if row_index < midpoint else "second_half"


class DriftProcessor(SyncProcessor):
    components = (Embedding, Drift)
    priority = 53

    def process(self, df: DataFrame, **kwargs) -> DataFrame:
        n = df.count_rows()
        if n < 4:
            return df.with_columns({
                "drift__divergence": daft.lit(0.0),
                "drift__window": daft.lit("insufficient_data"),
            })

        mid = n // 2

        # Collect only the vector column to compute centroids
        vectors = (
            df.select("embedding__vector").collect().to_pylist()
        )
        vecs = [r["embedding__vector"] for r in vectors]
        first_half = np.array(vecs[:mid])
        second_half = np.array(vecs[mid:])
        centroid_a = first_half.mean(axis=0)
        centroid_b = second_half.mean(axis=0)
        cos_sim = float(
            np.dot(centroid_a, centroid_b)
            / (np.linalg.norm(centroid_a) * np.linalg.norm(centroid_b) + 1e-8)
        )
        divergence = 1.0 - cos_sim

        # Assign window labels and divergence via expressions (no second collect)
        df = df._add_monotonically_increasing_id("__row_idx")
        df = df.with_columns({
            "drift__divergence": daft.lit(divergence),
            "drift__window": _assign_window(col("__row_idx"), mid),
        })
        return df.exclude("__row_idx")
