# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

import daft
import numpy as np

from examples.synth.similarity_processor import KNNIndex, SimilarityProcessor


def test_knn_index_returns_k_neighbors():
    rng = np.random.default_rng(42)
    vectors = rng.standard_normal((10, 8)).tolist()
    ids = [f"e{i}" for i in range(10)]
    index = KNNIndex(vectors, ids, k=3)
    result = index.query(vectors[0])
    assert len(result["neighbor_ids"]) == 3
    assert len(result["distances"]) == 3
    assert all(d >= 0 for d in result["distances"])


def test_similarity_processor_adds_neighbor_columns():
    rng = np.random.default_rng(42)
    vecs = rng.standard_normal((10, 8)).tolist()
    df = daft.from_pydict({
        "entity_id": [f"e{i}" for i in range(10)],
        "embedding__vector": vecs,
    })
    proc = SimilarityProcessor(k=3)
    result = proc.process(df).collect().to_pylist()
    assert all("neighbors__neighbor_ids" in r for r in result)
    assert all("neighbors__distances" in r for r in result)
    assert all(len(r["neighbors__neighbor_ids"]) == 3 for r in result)


def test_similarity_processor_skips_tiny_input():
    df = daft.from_pydict({
        "entity_id": ["e0"],
        "embedding__vector": [[1.0, 2.0]],
    })
    proc = SimilarityProcessor(k=3)
    result = proc.process(df).collect().to_pylist()
    assert "neighbors__neighbor_ids" not in result[0]
