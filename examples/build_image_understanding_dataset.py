"""
Archetype job: build a less-biased image understanding dataset.

This uses Archetype's `StorageConfig` (I/O + destination) and runs a Daft-native
curation pipeline based on the ablation methodology described here:

- https://www.daft.ai/blog/multimodal-structured-outputs-evaluating-vlm-image-understanding-at-scale

Run (local quick test):
  python archetype/examples/build_image_understanding_dataset.py --limit 50
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

_SRC = Path(__file__).resolve().parents[1] / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from archetype.app.datasets.image_understanding.builder import BuildConfig, build_curated_dataset
from archetype.core.config import StorageBackend, StorageConfig


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--category", default=os.getenv("CATEGORY", "textbook_academic_questions"))
    p.add_argument("--subset", default=os.getenv("SUBSET", "ai2d"))
    p.add_argument("--limit", default=os.getenv("LIMIT", None))

    p.add_argument(
        "--answer-model", default=os.getenv("ANSWER_MODEL_ID", "Qwen/Qwen3-VL-4B-Instruct")
    )
    p.add_argument("--bias-judge-model", default=os.getenv("BIAS_JUDGE_MODEL_ID", "gpt-5-mini"))
    p.add_argument("--image-judge-model", default=os.getenv("IMAGE_JUDGE_MODEL_ID", "gpt-5-mini"))

    p.add_argument("--provider", default=os.getenv("DAFT_PROVIDER", "daft"))
    p.add_argument(
        "--dest-uri",
        default=os.getenv(
            "DEST_URI", "./archetype_data/datasets/image_understanding/curated.parquet"
        ),
    )

    p.add_argument("--storage-uri", default=os.getenv("ARCT_STORAGE_URI", "./archetype_data"))
    p.add_argument("--storage-namespace", default=os.getenv("ARCT_STORAGE_NAMESPACE", "archetypes"))
    p.add_argument(
        "--storage-backend", default=os.getenv("ARCT_STORAGE_BACKEND", StorageBackend.ICEBERG.value)
    )
    return p.parse_args()


def main() -> None:
    args = _parse_args()

    source_uri = (
        f"s3://daft-public-datasets/the_cauldron/original/{args.category}/{args.subset}/*.parquet"
    )

    answer_params = {"temperature": 0.0, "max_tokens": 2}

    storage_config = StorageConfig(
        uri=args.storage_uri,
        namespace=args.storage_namespace,
        backend=StorageBackend(args.storage_backend),
    )

    cfg = BuildConfig(
        category=args.category,
        subset=args.subset,
        source_uri=source_uri,
        answer_model_id=args.answer_model,
        answer_params=answer_params,
        bias_judge_model_id=args.bias_judge_model,
        image_judge_model_id=args.image_judge_model,
        limit=int(args.limit) if args.limit is not None else None,
        dest_uri=args.dest_uri,
    )

    print(
        json.dumps(
            {"storage": storage_config.model_dump(), "job": cfg.__dict__}, indent=2, default=str
        )
    )
    build_curated_dataset(storage_config=storage_config, cfg=cfg, provider=args.provider)


if __name__ == "__main__":
    main()
