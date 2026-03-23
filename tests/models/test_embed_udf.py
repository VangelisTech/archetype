import tempfile
from pathlib import Path

import torch

from archetype.models.embed_udf import EmbedCls
from archetype.models.encoder import ENCODER_CONFIG, BidirectionalEncoder


def test_embed_cls_produces_vectors():
    cfg = {**ENCODER_CONFIG, "vocab_size": 100256, "context_length": 32, "n_layers": 1}
    with tempfile.TemporaryDirectory() as tmpdir:
        model_path = Path(tmpdir) / "encoder_v0.pt"
        model = BidirectionalEncoder(cfg)
        torch.save(model.state_dict(), model_path)

        embed = EmbedCls(model_path=str(model_path), config=cfg)
        results = embed(["hello world", "test sentence"])
        assert len(results) == 2
        assert len(results[0]) == cfg["emb_dim"]
