#!/usr/bin/env python3
# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0
"""
Synthetic Data Engine — Self-Improving Embeddings

Usage:
    uv run python -m examples.synth.run [conversation_dir] --phases all --recurse 1
"""
import argparse
from pathlib import Path


def parse_args():
    parser = argparse.ArgumentParser(description="Synthetic Data Engine")
    parser.add_argument("conversation_dir", nargs="?", default=None)
    parser.add_argument(
        "--phases",
        default="all",
        help="Comma-separated: label,generate,train,embed,analyze,recurse",
    )
    parser.add_argument("--recurse", type=int, default=1, help="Number of self-improvement cycles")
    parser.add_argument("--model-dir", default="synth_models", help="Directory for model artifacts")
    parser.add_argument("--output", default="synth_results.json", help="Output JSON path")
    return parser.parse_args()


def main():
    args = parse_args()
    phases = (
        args.phases.split(",")
        if args.phases != "all"
        else ["label", "generate", "train", "embed", "analyze", "recurse"]
    )
    model_dir = Path(args.model_dir)
    model_dir.mkdir(parents=True, exist_ok=True)
    print("Synthetic Data Engine")
    print(f"Phases: {phases}")
    print(f"Recurse: {args.recurse} cycles")
    print(f"Model dir: {model_dir}")
    print()
    print("Pipeline orchestrator ready.")
    print("For full execution, run individual phases or integrate with Archetype world.")
    print(f"Results will be written to {args.output}")


if __name__ == "__main__":
    main()
