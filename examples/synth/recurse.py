#!/usr/bin/env python3
# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Recursive self-improvement loop for the synthetic data engine.

Cycle 0: Train on original mind extraction labels (perspective, voice, type)
Cycle N: Train on original labels + cluster-derived labels from cycle N-1

Each cycle: generate triplets → train → embed → cluster → measure
The model discovers structure the original labels missed, then uses
that structure as new training signal.
"""

import json
from collections import Counter
from pathlib import Path

import daft
import numpy as np

from archetype.models.embed_udf import EmbedCls
from examples.synth.cluster_processor import cluster_embeddings
from examples.synth.pair_generator import generate_triplets
from examples.synth.train_processor import train_encoder


def load_labeled_segments(mind_json_path: str) -> list[dict]:
    with open(mind_json_path) as f:
        data = json.load(f)
    segments = []
    for lens, entries in data["by_perspective"].items():
        for entry in entries:
            segments.append({
                "segment__content": entry.get("memory", ""),
                "perspective__lens": lens,
                "voice__classification": entry.get("voice", "unknown"),
                "extraction__memory_type": entry.get("type", "unknown"),
            })
    return segments


def embedding_variance(embeddings: list[list[float]]) -> float:
    """Check for embedding collapse — low variance means degenerate model."""
    arr = np.array(embeddings)
    return float(arr.var(axis=0).mean())


def cluster_purity(segments: list[dict], cluster_ids: list[int], label_col: str) -> float:
    """Average purity across clusters: how well clusters align with a label."""
    cluster_to_labels: dict[int, list[str]] = {}
    for i, cid in enumerate(cluster_ids):
        cluster_to_labels.setdefault(cid, []).append(segments[i][label_col])

    purities = []
    for labels in cluster_to_labels.values():
        if not labels:
            continue
        most_common_count = Counter(labels).most_common(1)[0][1]
        purities.append(most_common_count / len(labels))

    return sum(purities) / len(purities) if purities else 0.0


def run_cycle(
    cycle: int,
    segments: list[dict],
    model_dir: Path,
    n_clusters: int = 4,
    num_epochs: int = 15,
) -> dict:
    """Run one cycle of the recursion loop. Returns metrics."""

    print(f"\n{'=' * 70}")
    print(f"CYCLE {cycle}")
    print(f"{'=' * 70}")

    # ── Build DataFrame with all available labels ──
    cols = {
        "segment__content": [s["segment__content"] for s in segments],
        "perspective__lens": [s["perspective__lens"] for s in segments],
        "voice__classification": [s["voice__classification"] for s in segments],
        "extraction__memory_type": [s["extraction__memory_type"] for s in segments],
    }

    # Add cluster labels from previous cycle if they exist
    label_cols = ["perspective__lens", "voice__classification", "extraction__memory_type"]
    cluster_col = f"cluster__cycle_{cycle - 1}"
    if cluster_col.replace(f"cluster__cycle_{cycle - 1}", "") == "" and cycle > 0:
        if cluster_col in {k for s in segments for k in s}:
            cols[cluster_col] = [str(s.get(cluster_col, "")) for s in segments]
            label_cols.append(cluster_col)

    df = daft.from_pydict(cols)

    # ── Generate triplets from all label sources ──
    all_triplets = []
    for label_col in label_cols:
        triplets = generate_triplets(df, label_col=label_col, min_per_group=2, max_triplets_per_anchor=3)
        rows = triplets.collect().to_pylist()
        if rows:
            print(f"  {label_col}: {len(rows)} triplets")
            all_triplets.extend(rows)

    print(f"  Total: {len(all_triplets)} triplets")

    if len(all_triplets) < 10:
        print("  Insufficient triplets. Stopping.")
        return {"stopped": True}

    # ── Train ──
    model_path = model_dir / f"encoder_v{cycle}.pt"
    metrics = train_encoder(
        triplets=all_triplets,
        model_path=model_path,
        num_epochs=num_epochs,
        batch_size=16,
        lr=3e-4,
    )
    print(f"  Loss: {metrics['initial_loss']:.4f} → {metrics['final_loss']:.4f}")

    # ── Embed ──
    embed = EmbedCls(model_path=str(model_path))
    texts = [s["segment__content"] for s in segments]
    embeddings = embed(texts)

    # ── Check for collapse ──
    var = embedding_variance(embeddings)
    print(f"  Embedding variance: {var:.6f}")
    if var < 1e-6:
        print("  EMBEDDING COLLAPSE detected. Stopping recursion.")
        return {"stopped": True, "reason": "collapse", "variance": var}

    # ── Cluster ──
    nc = min(n_clusters, len(embeddings) // 3)
    result = cluster_embeddings(embeddings, n_clusters=nc)
    cluster_counts = Counter(result["cluster_id"])
    print(f"  Clusters: {dict(sorted(cluster_counts.items()))}")

    # ── Measure purity against original labels ──
    p_purity = cluster_purity(segments, result["cluster_id"], "perspective__lens")
    v_purity = cluster_purity(segments, result["cluster_id"], "voice__classification")
    print(f"  Purity — perspective: {p_purity:.0%}, voice: {v_purity:.0%}")

    # ── Attach cluster labels to segments for next cycle ──
    new_cluster_col = f"cluster__cycle_{cycle}"
    for i, s in enumerate(segments):
        s[new_cluster_col] = result["cluster_id"][i]

    return {
        "cycle": cycle,
        "triplets": len(all_triplets),
        "loss_initial": metrics["initial_loss"],
        "loss_final": metrics["final_loss"],
        "variance": var,
        "n_clusters": len(cluster_counts),
        "perspective_purity": p_purity,
        "voice_purity": v_purity,
        "model_path": str(model_path),
        "cluster_counts": dict(sorted(cluster_counts.items())),
    }


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Recursive self-improvement loop")
    parser.add_argument("--cycles", type=int, default=3, help="Number of recursion cycles")
    parser.add_argument("--clusters", type=int, default=4, help="Number of clusters per cycle")
    parser.add_argument("--epochs", type=int, default=15, help="Training epochs per cycle")
    args = parser.parse_args()

    mind_json = Path("mind_extraction.json")
    model_dir = Path("synth_models")
    model_dir.mkdir(parents=True, exist_ok=True)

    segments = load_labeled_segments(str(mind_json))
    print(f"Loaded {len(segments)} segments")
    print(f"Running {args.cycles} recursive cycles, {args.clusters} clusters, {args.epochs} epochs each")

    history = []
    for cycle in range(args.cycles):
        result = run_cycle(
            cycle=cycle,
            segments=segments,
            model_dir=model_dir,
            n_clusters=args.clusters,
            num_epochs=args.epochs,
        )
        history.append(result)

        if result.get("stopped"):
            print(f"\nRecursion stopped at cycle {cycle}: {result.get('reason', 'insufficient data')}")
            break

    # ── Summary ──
    print(f"\n{'=' * 70}")
    print("RECURSION SUMMARY")
    print(f"{'=' * 70}")
    print(f"{'Cycle':>6} {'Triplets':>9} {'Loss':>12} {'Variance':>10} {'Persp%':>7} {'Voice%':>7}")
    print(f"{'─' * 6} {'─' * 9} {'─' * 12} {'─' * 10} {'─' * 7} {'─' * 7}")
    for r in history:
        if r.get("stopped"):
            print(f"  STOPPED: {r.get('reason', 'insufficient data')}")
            break
        loss_str = f"{r['loss_initial']:.4f}→{r['loss_final']:.4f}"
        print(
            f"{r['cycle']:>6} {r['triplets']:>9} {loss_str:>12} "
            f"{r['variance']:>10.6f} {r['perspective_purity']:>6.0%} {r['voice_purity']:>6.0%}"
        )

    # ── Show what changed across cycles ──
    if len(history) >= 2 and not history[-1].get("stopped"):
        first = history[0]
        last = history[-1]
        pp_delta = last["perspective_purity"] - first["perspective_purity"]
        vp_delta = last["voice_purity"] - first["voice_purity"]
        loss_delta = last["loss_final"] - first["loss_final"]
        print(f"\nDelta (cycle 0 → {last['cycle']}):")
        print(f"  Perspective purity: {pp_delta:+.0%}")
        print(f"  Voice purity:       {vp_delta:+.0%}")
        print(f"  Final loss:         {loss_delta:+.4f}")

    # Export
    output_path = Path("synth_recursion.json")
    with open(output_path, "w") as f:
        json.dump(history, f, indent=2, default=str)
    print(f"\nFull history written to {output_path}")


if __name__ == "__main__":
    main()
