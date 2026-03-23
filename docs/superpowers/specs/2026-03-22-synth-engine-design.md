# Synthetic Data Engine — Domain-Specific Embeddings from Archetype ECS

**Date**: 2026-03-22
**Status**: Approved
**Author**: Everett Kleven + Claude

## Summary

A self-improving embedding pipeline built on Archetype's ECS. Uses Raschka's LLMs-from-scratch transformer code (modified for bidirectional attention) to train a tiny (~2M param) contrastive encoder on conversation segments labeled by the existing mind extraction pipeline. Deploys the trained model as a Daft stateful class UDF (`@daft.cls`) so embeddings are just another column operation. Downstream processors use the embeddings for clustering, similarity search, anomaly detection, and drift tracking — all without API calls. Cluster-derived labels feed back into training, creating a recursive self-improvement loop.

## Motivation

The mind extraction pipeline (`examples/mind/`) proves that LLM processors over DataFrames work. But every tick costs API calls. The synthetic data engine inverts this: use API calls once to generate labeled training data, train a local model, then run all future inference locally. The model improves itself by discovering structure in its own output and using that structure as new training signal.

## Model Architecture

**BidirectionalEncoder** — a modified Raschka GPT:

- Copy `MultiHeadAttention` and `TransformerBlock` into `encoder.py` (do not import from `llms_from_scratch` — we need to modify them). Remove the causal mask: replace `torch.triu(torch.ones(...), diagonal=1)` with `torch.zeros(...)` in the attention `__init__`, so all tokens attend to all positions bidirectionally.
- `LayerNorm`, `GELU`, `FeedForward` are imported from `llms_from_scratch.ch04` unchanged.
- 4 blocks, 4 heads, 128 embedding dim, ~2M params
- Replace LM head with mean pooling + L2 normalize → 128-dim embedding vector
- Tokenizer: `tiktoken` (cl100k_base, same as Raschka)

```python
ENCODER_CONFIG = {
    "vocab_size": 100256,    # cl100k_base
    "context_length": 256,   # conversation segments are short
    "emb_dim": 128,
    "n_heads": 4,
    "n_layers": 4,
    "drop_rate": 0.1,
    "qkv_bias": False,
}
```

Source: `src/archetype/models/encoder.py`

## Daft Class UDF Integration

The trained model deploys as a `@daft.cls` stateful UDF. Model loads once per worker in `__init__`, batched inference in `__call__`, zero-copy Arrow in/out.

```python
@daft.cls(gpus=0)
class EmbedCls:
    def __init__(self):
        self.model = BidirectionalEncoder(ENCODER_CONFIG)
        self.model.load_state_dict(torch.load(self._model_path()))
        self.model.eval()
        self.tokenizer = tiktoken.get_encoding("cl100k_base")

    @daft.method.batch(return_dtype=DataType.fixed_size_list(DataType.float32(), 128))
    def __call__(self, text: Series) -> Series:
        texts = text.to_arrow().to_pylist()
        tokens = [self.tokenizer.encode(t)[:256] for t in texts]
        # pad, tensorize, forward, mean pool
        with torch.no_grad():
            embeddings = self.model.encode(padded)
        return Series.from_arrow(embeddings_as_arrow)
```

Processor usage:

```python
embed = EmbedCls()
df = df.select(embed(col("segment__content")))
```

Model artifacts are stored at `{world_storage_uri}/models/encoder_v{tick}.pt`. The `EmbedCls` constructor accepts an optional `model_path` parameter; if omitted, it resolves the latest version from the world's storage directory. The `Embedding` component's `model_version` field records which artifact produced it.

Source: `src/archetype/models/embed_udf.py`

## Training Data Pipeline

### Contrastive pairs from ECS labels

Three supervision signals from the existing mind extraction pipeline:

| Label | Positive pair | Negative pair |
|-------|--------------|---------------|
| `perspective__lens` | Two segments both classified `subjective` | One `subjective`, one `objective` |
| `voice__classification` | Two `actor` segments | One `actor`, one `observer` |
| `extraction__memory_type` | Two `feedback` segments | One `feedback`, one `project` |

### Pipeline as ECS processors

1. **`SegmentLoader`** — loads conversation JSONs into Segment entities (exists: `examples/mind/loader.py`)
2. **`LabelProcessor`** — runs existing mind extraction pipeline (Extract, Voice, Perspective) to get labels. One-time API cost.
3. **`PairGenerator`** — pure DataFrame processor. Takes labeled segments, produces `(anchor, positive, negative)` triplets by sampling within/across label groups. No LLM calls.

500 labeled segments produce thousands of triplets. Small model, small data.

## Training Loop

**Loss**: Triplet margin loss.

```
loss = max(0, d(anchor, positive) - d(anchor, negative) + margin)
```

Cosine distance, margin = 0.2.

**TrainProcessor** — a SyncProcessor that runs PyTorch training as a single ECS tick (sync because PyTorch training is blocking CPU/GPU work, no benefit from async):

- Input: `Triplet` entities with `anchor_text`, `positive_text`, `negative_text` columns
- `__init__` builds model, optimizer (AdamW, lr=3e-4), tokenizer
- `process()` iterates DataFrame in batches, forward pass on all three texts, triplet loss, backprop
- Output: `encoder_v{tick}.pt` to `{world_storage_uri}/models/`, `TrainingMetric` component (loss, epoch, num_triplets)

No separate training script. Training is just another processor. 10 epochs, batch size 32, early stopping on loss plateau. Trains in seconds on CPU.

## The Recursion

```
Tick 0: Raw conversation segments
         |
         v
Tick 1: Mind extraction (API calls: GPT-5-mini)
        -> Segment entities gain labels: voice, perspective, memory_type
         |
         v
Tick 2: PairGenerator (pure DataFrame transform, no API)
        -> Triplet entities: (anchor, positive, negative)
         |
         v
Tick 3: TrainProcessor (local PyTorch, no API)
        -> BidirectionalEncoder weights saved to LanceDB as artifact
         |
         v
Tick 4: EmbedCls loads trained weights
        -> NEW conversation segments get embeddings WITHOUT API calls
         |
         v
Tick 5: ClusterProcessor (pure DataFrame, no API)
        -> Entities clustered by embedding similarity
        -> Clusters reveal NEW label structure the original labels missed
         |
         v
Tick 6: PairGenerator AGAIN — now using cluster-derived labels
        -> Richer triplets, informed by structure the model discovered
         |
         v
Tick 7: TrainProcessor AGAIN — better data -> better model
         |
         v
Tick 8: EmbedCls reloads — better embeddings -> better clusters -> ...
```

Labels train embeddings. Embeddings discover structure. Structure becomes new labels. New labels train better embeddings. After Tick 1, zero API calls.

## Downstream Processors

All pure DataFrame transforms over the 128-dim vector column. No LLM, no API:

- **`SimilarityProcessor`** — k-nearest neighbors via LanceDB vector search. Output: `Neighbors` component (entity IDs, distances). "Find conversations like this one."
- **`ClusterProcessor`** — k-means or HDBSCAN over embedding column. Output: `Cluster` component (cluster_id, centroid_distance). Discovers natural groupings. Feeds back into PairGenerator.
- **`AnomalyProcessor`** — flag entities far from any centroid. Output: `Anomaly` component (outlier_score). Surfaces unusual conversation moments.
- **`DriftProcessor`** — compare embedding distributions across time windows. Output: `Drift` component (divergence, window). "You've been more observer than actor this month."

## ECS Components

| Component | Fields | Purpose |
|-----------|--------|---------|
| `Triplet` | `anchor_text`, `positive_text`, `negative_text`, `label_source` | Training input |
| `Embedding` | `vector: Vector(128)`, `model_version: str` | 128-dim output from EmbedCls (uses `lancedb.pydantic.Vector` for native vector search) |
| `Cluster` | `cluster_id: int`, `centroid_distance: float` | Group membership |
| `Neighbors` | `neighbor_ids: list[str]`, `distances: list[float]` | k-NN results |
| `Anomaly` | `outlier_score: float` | Distance from nearest centroid |
| `Drift` | `divergence: float`, `window: str` | Distribution shift over time |
| `TrainingMetric` | `loss: float`, `epoch: int`, `num_triplets: int` | Training run summary |

## File Layout

```
src/archetype/
├── models/                          # NEW — local model code
│   ├── __init__.py
│   ├── encoder.py                   # BidirectionalEncoder (modified Raschka ch03/ch04)
│   └── embed_udf.py                 # EmbedCls (@daft.cls wrapper)
│
├── core/                            # UNTOUCHED
├── app/                             # UNTOUCHED

examples/
├── mind/                            # EXISTING — mind extraction (unchanged)
├── synth/                           # NEW — synthetic data engine
│   ├── __init__.py
│   ├── components.py                # Triplet, Embedding, Cluster, Neighbors, Anomaly, Drift, TrainingMetric
│   ├── pair_generator.py            # PairGenerator processor
│   ├── train_processor.py           # TrainProcessor processor
│   ├── cluster_processor.py         # ClusterProcessor
│   ├── similarity_processor.py      # SimilarityProcessor
│   ├── anomaly_processor.py         # AnomalyProcessor
│   ├── drift_processor.py           # DriftProcessor
│   └── run.py                       # Orchestrates the full loop
```

## CLI Interface

```bash
uv run python -m examples.synth.run [conversation_dir] --phases all --recurse 3
```

- `--phases label,train,embed,analyze,recurse` — run subsets
- `--recurse N` — self-improvement cycles (default 1, no recursion)
- Conversation dir defaults to `$MIND_CONVERSATION_DIR` or `~/.claude/projects/...`

## Failure Modes

- **Embedding collapse**: All embeddings converge to the same point (degenerate model). Detect by checking variance of embedding vectors after training. If variance < threshold, abort recursion and log warning. The `TrainingMetric` component records this.
- **No trained model**: `EmbedCls` can't load weights on first run. The `--phases` flag handles this — run `label,train` before `embed`. If `embed` phase finds no model artifact, skip gracefully and report.
- **Insufficient triplets**: PairGenerator needs at least N segments per label group to produce meaningful triplets. Minimum threshold: 10 segments per label value, 100 triplets total. Below this, skip training and report.
- **Degenerate clusters**: HDBSCAN finds 1 cluster or k-means produces empty clusters. Fall back to label-only pairs (no cluster-derived signal) for that recursion cycle.
- **Parse errors in mind extraction**: Some segments may produce unparseable LLM output. Filter these out before PairGenerator — only segments with valid labels become training data.

## Testing

- **Unit: encoder forward pass** — verify `BidirectionalEncoder(config).forward(tokens)` produces shape `(batch, seq_len, emb_dim)` and `encode(tokens)` produces shape `(batch, 128)` with L2 norm = 1.
- **Unit: PairGenerator** — given mock labeled segments, verify correct triplet count and that positive pairs share labels while negative pairs don't.
- **Unit: contrastive loss** — verify loss = 0 when positive is closer than negative by > margin, loss > 0 otherwise.
- **Integration: training convergence** — on synthetic triplets (3 clusters of random vectors), verify loss decreases over 5 epochs.
- **End-to-end: `run.py --phases label,train,embed`** — on a small conversation fixture (~20 segments), verify the pipeline produces Embedding components with correct dimensionality.

## Future Extensions (B-D corpora)

Once the loop works on conversations (A), the same architecture extends to:

- **B: Codebase evolution** — embed diffs/commits, cluster refactors vs features vs bugfixes
- **C: ECS state snapshots** — embed serialized entity states, find similar entities across worlds/ticks
- **D: Mixed corpus** — all sources through the same model, component tag distinguishes type

Each corpus adds new training signal. The encoder learns a unified representation across all of Archetype's data.

## Dependencies

- `torch` — model training and inference
- `tiktoken` — tokenization (cl100k_base)
- `daft` — DataFrame processing, `@daft.cls` UDF (requires version with class UDF support — verify against pinned version)
- `lancedb` — vector storage and k-NN search
- `scikit-learn` or `hdbscan` — clustering (ClusterProcessor)
- `llms_from_scratch` — Raschka's ch03/ch04 modules (MultiHeadAttention, TransformerBlock, LayerNorm, GELU, FeedForward)
