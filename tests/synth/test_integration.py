import tempfile
from pathlib import Path

import daft

from archetype.models.embed_udf import EmbedCls
from examples.synth.cluster_processor import cluster_embeddings
from examples.synth.pair_generator import generate_triplets
from examples.synth.train_processor import train_encoder


def test_full_pipeline_smoke():
    segments = []
    for i in range(30):
        lens = ["objective", "subjective", "abjective"][i % 3]
        segments.append({
            "segment__content": f"This is test segment number {i} about {lens} topics with enough text",
            "perspective__lens": lens,
        })

    df = daft.from_pydict({k: [s[k] for s in segments] for k in segments[0]})

    triplets_df = generate_triplets(df, label_col="perspective__lens", min_per_group=2)
    triplet_rows = triplets_df.collect().to_pylist()
    assert len(triplet_rows) > 0

    with tempfile.TemporaryDirectory() as tmpdir:
        model_path = Path(tmpdir) / "models" / "encoder_v0.pt"
        cfg = {
            "vocab_size": 100256,
            "context_length": 64,
            "emb_dim": 32,
            "n_heads": 2,
            "n_layers": 1,
            "drop_rate": 0.0,
            "qkv_bias": False,
        }
        metrics = train_encoder(triplets=triplet_rows, model_path=model_path, config=cfg, num_epochs=3, batch_size=8)
        assert metrics["final_loss"] < metrics["initial_loss"]

        embed = EmbedCls(model_path=str(model_path), config=cfg)
        texts = [s["segment__content"] for s in segments]
        embeddings = embed(texts)
        assert len(embeddings) == 30
        assert len(embeddings[0]) == 32

        result = cluster_embeddings(embeddings, n_clusters=3)
        assert len(set(result["cluster_id"])) >= 2
