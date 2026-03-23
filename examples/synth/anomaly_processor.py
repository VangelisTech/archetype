# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0
"""Flag outlier entities based on cluster centroid distance."""
import numpy as np
from daft import DataFrame

from archetype.core.sync.processor import SyncProcessor

from .components import Anomaly, Cluster


class AnomalyProcessor(SyncProcessor):
    components = (Cluster, Anomaly)
    priority = 52

    def __init__(self, threshold_percentile: float = 90.0):
        self.threshold_percentile = threshold_percentile

    def process(self, df: DataFrame, **kwargs) -> DataFrame:
        rows = df.select("cluster__centroid_distance").collect().to_pylist()
        distances = [r["cluster__centroid_distance"] for r in rows]
        if not distances:
            return df
        dists = np.array(distances)
        threshold = np.percentile(dists, self.threshold_percentile)
        scores = (dists / max(threshold, 1e-8)).tolist()
        return df.with_columns({"anomaly__outlier_score": scores})
