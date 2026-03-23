import tempfile
from pathlib import Path

import torch

from archetype.models.encoder import ENCODER_CONFIG
from examples.synth.train_processor import train_encoder


def test_train_encoder_loss_decreases():
    cfg = {**ENCODER_CONFIG, "vocab_size": 256, "context_length": 32, "n_layers": 1}
    n_per_cluster = 10
    triplets = []
    for _ in range(n_per_cluster):
        a_text = " ".join(str(x) for x in range(1, 17))
        b_text = " ".join(str(x) for x in range(51, 67))
        c_text = " ".join(str(x) for x in range(101, 117))
        triplets.append({"anchor_text": a_text, "positive_text": a_text, "negative_text": b_text})
        triplets.append({"anchor_text": b_text, "positive_text": b_text, "negative_text": c_text})
        triplets.append({"anchor_text": c_text, "positive_text": c_text, "negative_text": a_text})

    with tempfile.TemporaryDirectory() as tmpdir:
        model_path = Path(tmpdir) / "encoder_v0.pt"
        metrics = train_encoder(triplets=triplets, model_path=model_path, config=cfg, num_epochs=5, batch_size=8, lr=1e-3)
        assert model_path.exists()
        assert metrics["final_loss"] < metrics["initial_loss"]
        assert metrics["num_triplets"] == len(triplets)


def test_train_encoder_produces_loadable_weights():
    cfg = {**ENCODER_CONFIG, "vocab_size": 256, "context_length": 32, "n_layers": 1}
    triplets = [{"anchor_text": "1 2 3", "positive_text": "1 2 3", "negative_text": "99 98 97"} for _ in range(20)]
    with tempfile.TemporaryDirectory() as tmpdir:
        model_path = Path(tmpdir) / "encoder_v0.pt"
        train_encoder(triplets=triplets, model_path=model_path, config=cfg, num_epochs=1)
        from archetype.models.encoder import BidirectionalEncoder
        model = BidirectionalEncoder(cfg)
        model.load_state_dict(torch.load(model_path, weights_only=True))
