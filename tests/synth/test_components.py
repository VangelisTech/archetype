from examples.synth.components import (
    Anomaly,
    Cluster,
    Drift,
    Embedding,
    Neighbors,
    TrainingMetric,
    Triplet,
)


def test_triplet_row_dict():
    t = Triplet(anchor_text="a", positive_text="b", negative_text="c", label_source="voice")
    row = t.to_row_dict()
    assert row["triplet__anchor_text"] == "a"
    assert row["triplet__label_source"] == "voice"


def test_embedding_row_dict():
    e = Embedding(vector=[0.1] * 128, model_version="v1")
    row = e.to_row_dict()
    assert len(row["embedding__vector"]) == 128
    assert row["embedding__model_version"] == "v1"


def test_cluster_row_dict():
    c = Cluster(cluster_id=3, centroid_distance=0.42)
    row = c.to_row_dict()
    assert row["cluster__cluster_id"] == 3


def test_all_components_have_prefix():
    for cls in [Triplet, Embedding, Cluster, Neighbors, Anomaly, Drift, TrainingMetric]:
        prefix = cls.__name__.lower() + "__"
        schema = cls.get_prefixed_schema()
        for name in schema.names:
            assert name.startswith(prefix), f"{cls.__name__} field {name} missing prefix"
