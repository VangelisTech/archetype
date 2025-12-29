# Datasets

Archetype includes dataset “jobs” that are written as Daft pipelines, configured with Archetype’s `StorageConfig` (so I/O configuration is centralized).

## Image understanding curation (less biased VLM eval set)

See:

- code: `src/archetype/app/datasets/image_understanding/builder.py`
- example runner: `examples/build_image_understanding_dataset.py`

High-level pipeline:

1. read the source dataset (Cauldron format)
2. run an ablation (same answer model with-image vs text-only)
3. filter to cases where the image helped
4. label and filter out text bias (text-only judge)
5. label and filter out ambiguous/mislabeled items (image+text judge)
6. write a stable, training-friendly output schema

Run a small local test:

```bash
cd archetype
PYTHONPATH=src uv run python examples/build_image_understanding_dataset.py --limit 50
```

