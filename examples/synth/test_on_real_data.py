#!/usr/bin/env python3
# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Test the synthetic data engine on real .claude conversation data.

Uses existing mind_extraction.json as the label source (skips API calls).
Runs: labels → triplets → train → embed → cluster → analyze.
"""

import json
from pathlib import Path

import daft

from archetype.models.embed_udf import EmbedCls
from examples.synth.cluster_processor import cluster_embeddings
from examples.synth.pair_generator import generate_triplets
from examples.synth.train_processor import train_encoder


def load_labeled_segments(mind_json_path: str) -> list[dict]:
    """Convert mind_extraction.json into labeled segments for triplet generation."""
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
                "confidence": entry.get("confidence", 0.0),
                "source_preview": entry.get("source", "")[:60],
            })
    return segments


def main():
    mind_json = Path("mind_extraction.json")
    model_dir = Path("synth_models")
    model_dir.mkdir(parents=True, exist_ok=True)

    # ── Phase 1: Load labeled segments ──
    print("=" * 70)
    print("SYNTHETIC DATA ENGINE — Real .claude Session Data")
    print("=" * 70)
    print()

    segments = load_labeled_segments(str(mind_json))
    print(f"Loaded {len(segments)} labeled memories from mind_extraction.json")

    # Show distribution
    from collections import Counter
    lens_dist = Counter(s["perspective__lens"] for s in segments)
    voice_dist = Counter(s["voice__classification"] for s in segments)
    type_dist = Counter(s["extraction__memory_type"] for s in segments)
    print(f"\nPerspective: {dict(lens_dist)}")
    print(f"Voice:       {dict(voice_dist)}")
    print(f"Type:        {dict(type_dist)}")

    # ── Phase 2: Generate triplets ──
    print(f"\n{'─' * 70}")
    print("PHASE 2: Generating contrastive triplets")
    print(f"{'─' * 70}")

    df = daft.from_pydict({k: [s[k] for s in segments] for k in ["segment__content", "perspective__lens", "voice__classification", "extraction__memory_type"]})

    all_triplets = []
    for label_col in ["perspective__lens", "voice__classification", "extraction__memory_type"]:
        triplets = generate_triplets(df, label_col=label_col, min_per_group=2, max_triplets_per_anchor=3)
        rows = triplets.collect().to_pylist()
        print(f"  {label_col}: {len(rows)} triplets")
        all_triplets.extend(rows)

    print(f"\n  Total triplets: {len(all_triplets)}")

    if len(all_triplets) < 10:
        print("  Not enough triplets to train. Need more labeled data.")
        return

    # ── Phase 3: Train ──
    print(f"\n{'─' * 70}")
    print("PHASE 3: Training BidirectionalEncoder")
    print(f"{'─' * 70}")

    model_path = model_dir / "encoder_v0.pt"
    metrics = train_encoder(
        triplets=all_triplets,
        model_path=model_path,
        num_epochs=15,
        batch_size=16,
        lr=3e-4,
    )

    print(f"\n  Loss: {metrics['initial_loss']:.4f} → {metrics['final_loss']:.4f}")
    print(f"  Triplets: {metrics['num_triplets']}")
    print(f"  Epochs: {metrics['num_epochs']}")
    print(f"  Model saved: {model_path}")

    improved = metrics["final_loss"] < metrics["initial_loss"]
    print(f"  Converged: {'YES' if improved else 'NO'}")

    # ── Phase 4: Embed ──
    print(f"\n{'─' * 70}")
    print("PHASE 4: Embedding all segments")
    print(f"{'─' * 70}")

    embed = EmbedCls(model_path=str(model_path))
    texts = [s["segment__content"] for s in segments]
    embeddings = embed(texts)

    print(f"  Embedded {len(embeddings)} segments → {len(embeddings[0])}-dim vectors")

    # ── Phase 5: Cluster ──
    print(f"\n{'─' * 70}")
    print("PHASE 5: Clustering embeddings")
    print(f"{'─' * 70}")

    n_clusters = min(4, len(embeddings) // 3)
    result = cluster_embeddings(embeddings, n_clusters=n_clusters)

    cluster_counts = Counter(result["cluster_id"])
    print(f"  Found {len(cluster_counts)} clusters:")
    for cid, count in sorted(cluster_counts.items()):
        print(f"    Cluster {cid}: {count} segments")

    # ── Phase 6: Analyze ──
    print(f"\n{'─' * 70}")
    print("PHASE 6: Analysis — what did the model learn?")
    print(f"{'─' * 70}")

    # Show what's in each cluster
    for cid in sorted(cluster_counts.keys()):
        print(f"\n  CLUSTER {cid}:")
        cluster_segments = [
            segments[i] for i in range(len(segments))
            if result["cluster_id"][i] == cid
        ]

        # Show perspective/voice distribution within cluster
        c_lens = Counter(s["perspective__lens"] for s in cluster_segments)
        c_voice = Counter(s["voice__classification"] for s in cluster_segments)
        print(f"    Perspective: {dict(c_lens)}")
        print(f"    Voice: {dict(c_voice)}")

        # Show sample memories
        for s in cluster_segments[:3]:
            preview = s["segment__content"][:100]
            print(f"    [{s['perspective__lens']:>13}|{s['voice__classification']:>8}] {preview}")

    # ── Compare: model clusters vs original labels ──
    print(f"\n{'─' * 70}")
    print("PHASE 7: Label agreement — did the model rediscover the original structure?")
    print(f"{'─' * 70}")

    # For each original label, what cluster do its segments land in?
    for label_col in ["perspective__lens", "voice__classification"]:
        print(f"\n  {label_col}:")
        label_to_clusters = {}
        for i, s in enumerate(segments):
            label = s[label_col]
            cid = result["cluster_id"][i]
            label_to_clusters.setdefault(label, []).append(cid)

        for label, clusters in sorted(label_to_clusters.items()):
            dist = Counter(clusters)
            dominant = dist.most_common(1)[0]
            purity = dominant[1] / len(clusters)
            print(f"    {label:>15}: {dict(dist)}  (purity={purity:.0%})")

    # ── Export ──
    output = {
        "segments": len(segments),
        "triplets": len(all_triplets),
        "training": metrics,
        "clusters": {str(k): v for k, v in cluster_counts.items()},
        "model_path": str(model_path),
    }
    output_path = Path("synth_results.json")
    with open(output_path, "w") as f:
        json.dump(output, f, indent=2)
    print(f"\nResults written to {output_path}")


if __name__ == "__main__":
    main()
