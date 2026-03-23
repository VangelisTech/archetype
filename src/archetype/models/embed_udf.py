"""Daft stateful class UDF for batch embedding inference."""

import tiktoken
import torch

from .encoder import ENCODER_CONFIG, BidirectionalEncoder


class EmbedCls:
    def __init__(self, model_path: str | None = None, config: dict | None = None):
        cfg = config or ENCODER_CONFIG
        self.model = BidirectionalEncoder(cfg)
        if model_path:
            self.model.load_state_dict(torch.load(model_path, weights_only=True, map_location="cpu"))
        self.model.eval()
        self.tokenizer = tiktoken.get_encoding("cl100k_base")
        self.max_len = cfg["context_length"]
        self.emb_dim = cfg["emb_dim"]

    def __call__(self, texts: list[str]) -> list[list[float]]:
        encoded = [self.tokenizer.encode(t)[:self.max_len] for t in texts]
        max_seq = max(len(e) for e in encoded) if encoded else 1
        padded = []
        masks = []
        for enc in encoded:
            pad_len = max_seq - len(enc)
            padded.append(enc + [0] * pad_len)
            masks.append([1.0] * len(enc) + [0.0] * pad_len)

        tokens = torch.tensor(padded)
        mask = torch.tensor(masks)

        with torch.no_grad():
            embeddings = self.model.encode(tokens, padding_mask=mask)
        return embeddings.tolist()
