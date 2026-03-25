# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

import daft

from examples.synth.anomaly_processor import AnomalyProcessor


def test_anomaly_processor_scores_outliers_higher():
    distances = [1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 4.5, 5.0, 50.0]
    df = daft.from_pydict({"cluster__centroid_distance": distances})
    proc = AnomalyProcessor(threshold_percentile=90.0)
    result = proc.process(df).collect().to_pylist()
    scores = [r["anomaly__outlier_score"] for r in result]
    # The outlier (50.0) should have the highest score
    assert scores[-1] == max(scores)
    # Normal points should score <= 1.0 (below the threshold)
    assert all(s <= 1.1 for s in scores[:-1])


def test_anomaly_processor_handles_uniform_distances():
    distances = [5.0] * 10
    df = daft.from_pydict({"cluster__centroid_distance": distances})
    proc = AnomalyProcessor(threshold_percentile=90.0)
    result = proc.process(df).collect().to_pylist()
    scores = [r["anomaly__outlier_score"] for r in result]
    # All scores should be approximately equal
    assert max(scores) - min(scores) < 0.01
