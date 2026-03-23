import torch

from archetype.models.encoder import ENCODER_CONFIG, BidirectionalEncoder


def test_encoder_forward_shape():
    """Encoder produces (batch, seq_len, emb_dim) hidden states."""
    cfg = {**ENCODER_CONFIG, "vocab_size": 256, "context_length": 32, "n_layers": 1}
    model = BidirectionalEncoder(cfg)
    tokens = torch.randint(0, 256, (2, 16))
    hidden = model(tokens)
    assert hidden.shape == (2, 16, cfg["emb_dim"])


def test_encoder_encode_shape_and_norm():
    """encode() produces (batch, emb_dim) L2-normalized embeddings."""
    cfg = {**ENCODER_CONFIG, "vocab_size": 256, "context_length": 32, "n_layers": 1}
    model = BidirectionalEncoder(cfg)
    tokens = torch.randint(0, 256, (2, 16))
    emb = model.encode(tokens)
    assert emb.shape == (2, cfg["emb_dim"])
    norms = torch.norm(emb, dim=1)
    assert torch.allclose(norms, torch.ones(2), atol=1e-5)


def test_encoder_is_bidirectional():
    """Changing a later token should affect the embedding of an earlier token."""
    cfg = {**ENCODER_CONFIG, "vocab_size": 256, "context_length": 32, "n_layers": 1}
    model = BidirectionalEncoder(cfg)
    model.eval()

    tokens_a = torch.tensor([[1, 2, 3, 4, 5]])
    tokens_b = torch.tensor([[1, 2, 3, 4, 99]])  # only last token differs

    with torch.no_grad():
        hidden_a = model(tokens_a)
        hidden_b = model(tokens_b)

    # In a causal model, hidden[:, 0, :] would be identical because token 0
    # can't see token 4. In bidirectional, they differ.
    assert not torch.allclose(hidden_a[:, 0, :], hidden_b[:, 0, :], atol=1e-6)
