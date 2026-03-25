import daft
import numpy as np

from examples.synth.cluster_processor import ClusterProcessor, cluster_embeddings


def test_cluster_embeddings_finds_groups():
    rng = np.random.default_rng(42)
    centers = [rng.standard_normal(8) * 10 for _ in range(3)]
    vectors = []
    for c in centers:
        for _ in range(10):
            vectors.append((c + rng.standard_normal(8) * 0.1).tolist())
    result = cluster_embeddings(vectors, n_clusters=3)
    assert len(result["cluster_id"]) == 30
    assert len(set(result["cluster_id"])) == 3
    assert all(d >= 0 for d in result["centroid_distance"])


def test_cluster_embeddings_returns_negative_one_for_tiny_input():
    result = cluster_embeddings([[1.0, 2.0]], n_clusters=3)
    assert result["cluster_id"] == [-1]


def test_cluster_processor_adds_columns_via_daft_cls():
    rng = np.random.default_rng(42)
    centers = [rng.standard_normal(8) * 10 for _ in range(3)]
    vectors = []
    for c in centers:
        for _ in range(10):
            vectors.append((c + rng.standard_normal(8) * 0.1).tolist())
    df = daft.from_pydict({"embedding__vector": vectors})
    proc = ClusterProcessor(n_clusters=3)
    result = proc.process(df).collect().to_pylist()
    assert all("cluster__cluster_id" in r for r in result)
    assert all("cluster__centroid_distance" in r for r in result)
    assert len(set(r["cluster__cluster_id"] for r in result)) == 3
