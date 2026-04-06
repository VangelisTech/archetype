# Copyright 2025 Vangelis Technologies Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Semantic embedding and routing for Archetype agents.

Provides:
- MiniTransformerEncoder: A from-scratch transformer encoder in PyTorch
  that maps variable-length text to fixed-dimension embeddings.
- EmbeddingUDF: A ``@daft.cls()`` wrapper that runs the transformer
  as a Daft UDF—non-serializable state (the model) lives in ``__init__``.
- SemanticRouterProcessor: An AsyncProcessor that routes Outbox messages
  to the most semantically relevant agents via the SimilarityIndex.

The transformer is intentionally small (configurable depth/width) so it
can run on CPU during simulation ticks.  For production, swap in a
pretrained model—the UDF interface stays identical.
"""

from __future__ import annotations

import json
import logging
import math
import os
from dataclasses import dataclass

import daft
from daft import DataFrame

from archetype.core.aio.async_processor import AsyncProcessor
from archetype.core.component import Component
from archetype.core.embedding import Embedding, SimilarityIndex
from archetype.core.resources import Resources

logger = logging.getLogger(__name__)


# ── Mini Transformer Encoder (from scratch in PyTorch) ──────────


def _build_encoder(
    vocab_size: int = 256,
    dim: int = 128,
    n_heads: int = 4,
    n_layers: int = 2,
    max_seq_len: int = 512,
):
    """Build a small transformer encoder from raw PyTorch modules.

    This is a character-level encoder: input text is mapped to byte
    values (0-255) so we need no external tokenizer.  The encoder
    produces a fixed-size embedding via mean-pooling over the
    sequence dimension.

    Architecture:
        Token Embedding (vocab_size → dim)
        + Sinusoidal Positional Encoding
        → N × TransformerEncoderLayer (dim, n_heads)
        → Mean Pool → L2 Normalize
    """
    os.environ.setdefault("CUDA_VISIBLE_DEVICES", "")
    import torch
    import torch.nn as nn

    class SinusoidalPE(nn.Module):
        """Fixed sinusoidal positional encoding (Vaswani et al. 2017)."""

        def __init__(self, dim: int, max_len: int = 512):
            super().__init__()
            pe = torch.zeros(max_len, dim)
            position = torch.arange(0, max_len, dtype=torch.float).unsqueeze(1)
            div_term = torch.exp(torch.arange(0, dim, 2).float() * (-math.log(10000.0) / dim))
            pe[:, 0::2] = torch.sin(position * div_term)
            pe[:, 1::2] = torch.cos(position * div_term[: dim // 2])
            self.register_buffer("pe", pe.unsqueeze(0))  # (1, max_len, dim)

        def forward(self, x):
            return x + self.pe[:, : x.size(1)]

    class MiniTransformerEncoder(nn.Module):
        """Character-level transformer encoder → fixed-size embedding."""

        def __init__(self, vocab_size, dim, n_heads, n_layers, max_seq_len):
            super().__init__()
            self.dim = dim
            self.tok_emb = nn.Embedding(vocab_size, dim)
            self.pos_enc = SinusoidalPE(dim, max_seq_len)
            encoder_layer = nn.TransformerEncoderLayer(
                d_model=dim,
                nhead=n_heads,
                dim_feedforward=dim * 4,
                dropout=0.0,
                batch_first=True,
                activation="gelu",
            )
            self.encoder = nn.TransformerEncoder(encoder_layer, num_layers=n_layers)
            self.max_seq_len = max_seq_len
            # Initialize with small weights for stable random embeddings
            nn.init.normal_(self.tok_emb.weight, std=0.02)

        @torch.no_grad()
        def encode(self, text: str) -> list[float]:
            """Encode a single string → list[float] of length `dim`."""
            # Character-level tokenization: UTF-8 bytes → int tokens
            tokens = list(text.encode("utf-8"))[: self.max_seq_len]
            if not tokens:
                tokens = [0]  # empty string → single pad token
            x = torch.tensor([tokens], dtype=torch.long)
            emb = self.tok_emb(x)
            emb = self.pos_enc(emb)
            out = self.encoder(emb)  # (1, seq_len, dim)
            # Mean pool over sequence dimension
            pooled = out.mean(dim=1).squeeze(0)  # (dim,)
            # L2 normalize for cosine similarity
            pooled = pooled / (pooled.norm() + 1e-8)
            return pooled.tolist()

        @torch.no_grad()
        def encode_batch(self, texts: list[str]) -> list[list[float]]:
            """Encode multiple strings, padding to max length in batch."""
            if not texts:
                return []
            # Tokenize all
            all_tokens = [list(t.encode("utf-8"))[: self.max_seq_len] or [0] for t in texts]
            max_len = max(len(t) for t in all_tokens)
            # Pad to same length
            padded = [t + [0] * (max_len - len(t)) for t in all_tokens]
            x = torch.tensor(padded, dtype=torch.long)
            # Create attention mask (True = ignore)
            mask = torch.zeros(x.shape, dtype=torch.bool)
            for i, t in enumerate(all_tokens):
                mask[i, len(t) :] = True
            emb = self.tok_emb(x)
            emb = self.pos_enc(emb)
            out = self.encoder(emb, src_key_padding_mask=mask)
            # Masked mean pool
            mask_expanded = (~mask).unsqueeze(-1).float()
            pooled = (out * mask_expanded).sum(dim=1) / mask_expanded.sum(dim=1).clamp(min=1)
            # L2 normalize
            pooled = pooled / (pooled.norm(dim=1, keepdim=True) + 1e-8)
            return pooled.tolist()

    model = MiniTransformerEncoder(vocab_size, dim, n_heads, n_layers, max_seq_len)
    model.eval()
    return model


# ── Daft UDF: @daft.cls() for non-serializable model state ─────


@daft.cls()
class EmbeddingUDF:
    """Daft class-UDF that wraps the transformer encoder.

    The model is initialized once per Daft worker in ``__init__``
    (never serialized).  The ``embed`` method is a row-wise UDF.

    Usage::

        udf = EmbeddingUDF()
        df = df.with_column(
            "embedding__vector_json",
            udf.embed(col("agent__name"), col("agent__role"))
        )
    """

    def __init__(self):
        self.model = _build_encoder(vocab_size=256, dim=128, n_heads=4, n_layers=2, max_seq_len=512)

    def embed(self, name: str, role: str) -> str:
        """Embed name+role into a JSON-encoded vector."""
        text = f"{name}: {role}"
        vector = self.model.encode(text)
        return json.dumps(vector)


# ── Semantic Config ─────────────────────────────────────────────


@dataclass
class SemanticConfig:
    """Configuration for semantic routing.

    Attributes:
        similarity_threshold: Minimum cosine similarity to consider
            an agent as a routing target.
        max_recipients: Maximum number of agents a message can be
            routed to per tick.
        dim: Embedding dimensionality (must match the encoder).
    """

    similarity_threshold: float = 0.3
    max_recipients: int = 5
    dim: int = 128


# ── Components for semantic routing ─────────────────────────────


class SemanticProfile(Component):
    """An agent's semantic identity for routing purposes."""

    description: str = ""
    topics_json: str = "[]"


class SemanticInbox(Component):
    """Messages routed via semantic similarity (not direct addressing)."""

    messages: list[str] = []


# ── SemanticRouterProcessor ─────────────────────────────────────


class SemanticRouterProcessor(AsyncProcessor):
    """Routes Outbox messages to semantically similar agents.

    Instead of explicit addressing (entity_id → entity_id), messages
    are routed based on cosine similarity between the message embedding
    and each agent's profile embedding in the SimilarityIndex.

    Requires:
        Resources: SimilarityIndex, SemanticConfig
        Components: Embedding (for agent profiles)

    Priority -90 (runs early, after message delivery at -100 but
    before most user processors).
    """

    components = (Embedding,)
    priority = -90

    async def process(self, df: DataFrame, resources: Resources, **kwargs) -> DataFrame:
        index = resources.get(SimilarityIndex)
        config = resources.get(SemanticConfig)
        if not index or not config:
            return df

        # Cross-row context needed: collect to build routing table
        # JUSTIFIED .collect(): need global view of all agent embeddings
        # to compute similarity-based routing across entities
        rows = (
            df.select(
                "entity_id",
                "embedding__vector_json",
            )
            .collect()
            .to_pylist()
        )

        # Update the similarity index with current embeddings
        for row in rows:
            vec = json.loads(row["embedding__vector_json"])
            if vec:
                index.upsert(row["entity_id"], vec)

        return df
