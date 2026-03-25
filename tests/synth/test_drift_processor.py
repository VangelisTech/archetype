# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

import daft
import numpy as np

from examples.synth.drift_processor import DriftProcessor


def test_drift_processor_detects_distribution_shift():
    rng = np.random.default_rng(42)
    # Two distinct clusters to create measurable drift
    first_half = (rng.standard_normal((10, 8)) + 5.0).tolist()
    second_half = (rng.standard_normal((10, 8)) - 5.0).tolist()
    df = daft.from_pydict({"embedding__vector": first_half + second_half})
    proc = DriftProcessor()
    result = proc.process(df).collect().to_pylist()
    assert all("drift__divergence" in r for r in result)
    assert all("drift__window" in r for r in result)
    # Should detect significant divergence between halves
    assert result[0]["drift__divergence"] > 0.5
    # Window labels
    assert result[0]["drift__window"] == "first_half"
    assert result[-1]["drift__window"] == "second_half"


def test_drift_processor_low_divergence_for_similar_data():
    rng = np.random.default_rng(42)
    # Same distribution both halves
    vectors = (rng.standard_normal((20, 8)) + 10.0).tolist()
    df = daft.from_pydict({"embedding__vector": vectors})
    proc = DriftProcessor()
    result = proc.process(df).collect().to_pylist()
    # Should have low divergence
    assert result[0]["drift__divergence"] < 0.1


def test_drift_processor_insufficient_data():
    df = daft.from_pydict({"embedding__vector": [[1.0, 2.0], [3.0, 4.0]]})
    proc = DriftProcessor()
    result = proc.process(df).collect().to_pylist()
    assert all(r["drift__divergence"] == 0.0 for r in result)
    assert all(r["drift__window"] == "insufficient_data" for r in result)
