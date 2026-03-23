# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Train the BidirectionalEncoder with triplet margin loss."""

from pathlib import Path

import tiktoken
import torch
import torch.nn as nn
from daft import DataFrame

from archetype.core.sync.processor import SyncProcessor
from archetype.models.encoder import ENCODER_CONFIG, BidirectionalEncoder

from .components import TrainingMetric, Triplet


def _tokenize_and_pad(
    texts: list[str], tokenizer, max_len: int, vocab_size: int | None = None
) -> tuple[torch.Tensor, torch.Tensor]:
    encoded = [tokenizer.encode(t)[:max_len] for t in texts]
    padded = []
    masks = []
    for enc in encoded:
        if vocab_size is not None:
            enc = [min(tok, vocab_size - 1) for tok in enc]
        pad_len = max_len - len(enc)
        padded.append(enc + [0] * pad_len)
        masks.append([1] * len(enc) + [0] * pad_len)
    return torch.tensor(padded), torch.tensor(masks, dtype=torch.float32)


def train_encoder(
    triplets: list[dict],
    model_path: Path,
    config: dict | None = None,
    num_epochs: int = 10,
    batch_size: int = 32,
    lr: float = 3e-4,
    margin: float = 0.2,
) -> dict:
    cfg = config or ENCODER_CONFIG
    model = BidirectionalEncoder(cfg)
    optimizer = torch.optim.AdamW(model.parameters(), lr=lr)
    loss_fn = nn.TripletMarginWithDistanceLoss(
        distance_function=lambda a, b: 1.0 - nn.functional.cosine_similarity(a, b),
        margin=margin,
    )
    tokenizer = tiktoken.get_encoding("cl100k_base")
    max_len = cfg["context_length"]
    vocab_size = cfg.get("vocab_size")

    initial_loss = None
    final_loss = None

    for _epoch in range(num_epochs):
        model.train()
        epoch_loss = 0.0
        n_batches = 0

        for i in range(0, len(triplets), batch_size):
            batch = triplets[i : i + batch_size]
            anchors, a_mask = _tokenize_and_pad([t["anchor_text"] for t in batch], tokenizer, max_len, vocab_size)
            positives, p_mask = _tokenize_and_pad([t["positive_text"] for t in batch], tokenizer, max_len, vocab_size)
            negatives, n_mask = _tokenize_and_pad([t["negative_text"] for t in batch], tokenizer, max_len, vocab_size)

            a_emb = model.encode(anchors, padding_mask=a_mask)
            p_emb = model.encode(positives, padding_mask=p_mask)
            n_emb = model.encode(negatives, padding_mask=n_mask)

            loss = loss_fn(a_emb, p_emb, n_emb)
            optimizer.zero_grad()
            loss.backward()
            optimizer.step()

            epoch_loss += loss.item()
            n_batches += 1

        avg_loss = epoch_loss / max(n_batches, 1)
        if initial_loss is None:
            initial_loss = avg_loss
        final_loss = avg_loss

    model_path.parent.mkdir(parents=True, exist_ok=True)
    torch.save(model.state_dict(), model_path)

    return {
        "initial_loss": initial_loss or 0.0,
        "final_loss": final_loss or 0.0,
        "num_triplets": len(triplets),
        "num_epochs": num_epochs,
    }


class TrainProcessor(SyncProcessor):
    components = (Triplet, TrainingMetric)
    priority = 30

    def __init__(self, model_dir: Path, model_version: str = "v0", **train_kwargs):
        self.model_dir = model_dir
        self.model_version = model_version
        self.train_kwargs = train_kwargs

    def process(self, df: DataFrame, **kwargs) -> DataFrame:
        rows = df.select("triplet__anchor_text", "triplet__positive_text", "triplet__negative_text").collect().to_pylist()
        triplets = [
            {"anchor_text": r["triplet__anchor_text"], "positive_text": r["triplet__positive_text"], "negative_text": r["triplet__negative_text"]}
            for r in rows if r["triplet__anchor_text"]
        ]
        if len(triplets) < 100:
            print(f"TrainProcessor: only {len(triplets)} triplets, need >= 100. Skipping.")
            return df
        model_path = self.model_dir / f"encoder_{self.model_version}.pt"
        metrics = train_encoder(triplets=triplets, model_path=model_path, **self.train_kwargs)
        print(f"TrainProcessor: loss {metrics['initial_loss']:.4f} → {metrics['final_loss']:.4f}")
        return df.with_columns({
            "trainingmetric__loss": [metrics["final_loss"]] * len(rows),
            "trainingmetric__epoch": [metrics["num_epochs"]] * len(rows),
            "trainingmetric__num_triplets": [metrics["num_triplets"]] * len(rows),
            "trainingmetric__model_version": [self.model_version] * len(rows),
        })
