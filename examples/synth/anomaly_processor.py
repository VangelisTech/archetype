# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0
"""Flag outlier entities based on cluster centroid distance."""

import daft
from daft import DataFrame, col

from archetype.core.sync.processor import SyncProcessor

from .components import Anomaly, Cluster


@daft.func
def _score(distance: float, threshold: float) -> float:
    return distance / max(threshold, 1e-8)


class AnomalyProcessor(SyncProcessor):
    components = (Cluster, Anomaly)
    priority = 52

    def __init__(self, threshold_percentile: float = 90.0):
        self.threshold_percentile = threshold_percentile

    def process(self, df: DataFrame, **kwargs) -> DataFrame:
        pct = self.threshold_percentile / 100.0
        threshold_row = (
            df.agg(col("cluster__centroid_distance").approx_percentiles([pct]))
            .collect()
            .to_pylist()[0]
        )
        threshold = threshold_row["cluster__centroid_distance"][0]
        return df.with_columns({
            "anomaly__outlier_score": _score(
                col("cluster__centroid_distance"), threshold
            ),
        })
