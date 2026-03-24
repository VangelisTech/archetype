# Synthetic Data Engine

Self-improving embedding pipeline for Archetype ECS. Trains a tiny bidirectional encoder on conversation labels, deploys it as a Daft class UDF, and recurses — cluster-derived labels feed back into training with zero API calls.

## Architecture

```
Tick 1: Mind extraction (API) → labels: voice, perspective, memory_type
Tick 2: PairGenerator          → contrastive triplets from labels
Tick 3: TrainProcessor         → BidirectionalEncoder (2M params, cosine triplet loss)
Tick 4: EmbedCls               → 128-dim vectors as a DataFrame column
Tick 5: ClusterProcessor       → discovers structure the labels missed
Tick 6: PairGenerator (again)  → cluster labels → richer triplets → retrain → ...
```

After Tick 1, zero API calls. The model improves itself.

## Quick Start

```bash
uv sync --extra synth
uv run pytest tests/models/ tests/synth/ -v
uv run python -m examples.synth.run --help
```

## Components

| Component | Fields | Purpose |
|-----------|--------|---------|
| `Triplet` | `anchor_text`, `positive_text`, `negative_text`, `label_source` | Training input |
| `Embedding` | `vector: Vector(128)`, `model_version` | Output from EmbedCls |
| `Cluster` | `cluster_id`, `centroid_distance` | Group membership |
| `Neighbors` | `neighbor_ids`, `distances` | k-NN results |
| `Anomaly` | `outlier_score` | Distance from nearest centroid |
| `Drift` | `divergence`, `window` | Distribution shift over time |
| `TrainingMetric` | `loss`, `epoch`, `num_triplets`, `model_version` | Training run summary |

## Model

`BidirectionalEncoder` — modified Raschka GPT (ch03/ch04):
- Causal mask removed → all tokens attend bidirectionally
- LM head replaced with mean pooling → L2-normalized 128-dim embedding
- 4 blocks, 4 heads, 128 emb_dim, ~2M params
- Deploys as `@daft.cls` UDF: model loads once per worker, batched inference

## Files

```
src/archetype/models/
├── encoder.py         # BidirectionalEncoder nn.Module
└── embed_udf.py       # EmbedCls (Daft stateful class UDF)

examples/synth/
├── components.py      # ECS component definitions
├── pair_generator.py  # Label → contrastive triplets
├── train_processor.py # Triplet margin loss training
├── cluster_processor.py
├── similarity_processor.py
├── anomaly_processor.py
├── drift_processor.py
└── run.py             # CLI orchestrator
```

## Pipeline Phases

| Phase | Processor | API calls | Input → Output |
|-------|-----------|-----------|----------------|
| Label | Mind extraction (existing) | Yes (one-time) | Conversations → labeled segments |
| Generate | `generate_triplets()` | No | Labels → triplets |
| Train | `train_encoder()` | No | Triplets → `encoder_v{tick}.pt` |
| Embed | `EmbedCls` | No | Text → 128-dim vectors |
| Analyze | Cluster + Similarity + Anomaly + Drift | No | Vectors → structure |
| Recurse | PairGenerator + Train (again) | No | Cluster labels → better model |

## Training

Contrastive learning with triplet margin loss (cosine distance, margin=0.2):

```python
from examples.synth.train_processor import train_encoder

metrics = train_encoder(
    triplets=triplet_rows,    # [{"anchor_text", "positive_text", "negative_text"}]
    model_path=Path("encoder_v0.pt"),
    config=ENCODER_CONFIG,
    num_epochs=10,
)
```

Three supervision signals from mind extraction labels:
- `perspective__lens` — objective/subjective/abjective
- `voice__classification` — actor/observer/observed
- `extraction__memory_type` — feedback/project/user/reference

## Inference

```python
from archetype.models import EmbedCls

embed = EmbedCls(model_path="encoder_v0.pt")
vectors = embed(["segment one", "segment two"])  # → list[list[float]]
```

As a Daft column operation:
```python
embed = EmbedCls(model_path="encoder_v0.pt")
df = df.select(embed(col("segment__content")))
```

## Spec

Full design document: `docs/superpowers/specs/2026-03-22-synth-engine-design.md`
