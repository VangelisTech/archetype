#!/usr/bin/env python3
# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Test the synthetic data engine on real .claude conversation data.

Uses existing mind_extraction.json as the label source (skips API calls).
Runs the full Daft-native pipeline: labels → triplets → train → embed → cluster → analyze.
"""

import json
from collections import Counter
from pathlib import Path

import daft
from daft import col

from archetype.models.embed_udf import EmbedColumn
from examples.synth.anomaly_processor import AnomalyProcessor
from examples.synth.cluster_processor import ClusterProcessor
from examples.synth.drift_processor import DriftProcessor
from examples.synth.pair_generator import generate_triplets
from examples.synth.similarity_processor import SimilarityProcessor
from examples.synth.train_processor import train_encoder


def load_labeled_segments(mind_json_path: str) -> daft.DataFrame:
    """Convert mind_extraction.json into a labeled segment DataFrame."""
    with open(mind_json_path) as f:
        data = json.load(f)

    segments: dict[str, list] = {
        "entity_id": [],
        "segment__content": [],
        "perspective__lens": [],
        "voice__classification": [],
        "extraction__memory_type": [],
        "confidence": [],
    }
    idx = 0
    for lens, entries in data["by_perspective"].items():
        for entry in entries:
            segments["entity_id"].append(f"seg_{idx}")
            segments["segment__content"].append(entry.get("memory", ""))
            segments["perspective__lens"].append(lens)
            segments["voice__classification"].append(entry.get("voice", "unknown"))
            segments["extraction__memory_type"].append(entry.get("type", "unknown"))
            segments["confidence"].append(entry.get("confidence", 0.0))
            idx += 1

    return daft.from_pydict(segments)


def main():
    mind_json = Path("mind_extraction.json")
    model_dir = Path("synth_models")
    model_dir.mkdir(parents=True, exist_ok=True)

    # ── Phase 1: Load labeled segments ──
    print("=" * 70)
    print("SYNTHETIC DATA ENGINE — Real .claude Session Data (Daft-native)")
    print("=" * 70)
    print()

    df = load_labeled_segments(str(mind_json))
    n = df.count_rows()
    print(f"Loaded {n} labeled memories from mind_extraction.json")

    rows = df.collect().to_pylist()
    lens_dist = Counter(r["perspective__lens"] for r in rows)
    voice_dist = Counter(r["voice__classification"] for r in rows)
    type_dist = Counter(r["extraction__memory_type"] for r in rows)
    print(f"\nPerspective: {dict(lens_dist)}")
    print(f"Voice:       {dict(voice_dist)}")
    print(f"Type:        {dict(type_dist)}")

    # ── Phase 2: Generate triplets ──
    print(f"\n{'─' * 70}")
    print("PHASE 2: Generating contrastive triplets")
    print(f"{'─' * 70}")

    all_triplets = []
    for label_col in ["perspective__lens", "voice__classification", "extraction__memory_type"]:
        triplets = generate_triplets(df, label_col=label_col, min_per_group=2, max_triplets_per_anchor=3)
        trip_rows = triplets.collect().to_pylist()
        print(f"  {label_col}: {len(trip_rows)} triplets")
        all_triplets.extend(trip_rows)

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

    # ── Phase 4: Embed via EmbedColumn (daft.cls, per-row) ──
    print(f"\n{'─' * 70}")
    print("PHASE 4: Embedding all segments (EmbedColumn @daft.cls)")
    print(f"{'─' * 70}")

    embedder = EmbedColumn(model_path=str(model_path))
    df = df.with_columns({"embedding__vector": embedder.embed(col("segment__content"))})
    emb_dim = len(df.select("embedding__vector").limit(1).collect().to_pylist()[0]["embedding__vector"])
    print(f"  Embedded {n} segments → {emb_dim}-dim vectors")

    # ── Phase 5: Cluster via ClusterProcessor (daft.cls KMeansScorer) ──
    print(f"\n{'─' * 70}")
    print("PHASE 5: Clustering (ClusterProcessor @daft.cls)")
    print(f"{'─' * 70}")

    n_clusters = min(4, n // 3)
    cluster_proc = ClusterProcessor(n_clusters=n_clusters)
    df = cluster_proc.process(df)

    # ── Phase 5b: Similarity via SimilarityProcessor (daft.cls KNNIndex) ──
    print(f"\n{'─' * 70}")
    print("PHASE 5b: k-NN similarity (SimilarityProcessor @daft.cls)")
    print(f"{'─' * 70}")

    sim_proc = SimilarityProcessor(k=min(5, n - 1))
    df = sim_proc.process(df)

    # ── Phase 5c: Anomaly via AnomalyProcessor (daft.func) ──
    print(f"\n{'─' * 70}")
    print("PHASE 5c: Anomaly scoring (AnomalyProcessor @daft.func)")
    print(f"{'─' * 70}")

    anomaly_proc = AnomalyProcessor(threshold_percentile=90.0)
    df = anomaly_proc.process(df)

    # ── Phase 5d: Drift via DriftProcessor (daft.func) ──
    print(f"\n{'─' * 70}")
    print("PHASE 5d: Drift detection (DriftProcessor @daft.func)")
    print(f"{'─' * 70}")

    drift_proc = DriftProcessor()
    df = drift_proc.process(df)

    # ── Materialize and analyze ──
    print(f"\n{'─' * 70}")
    print("PHASE 6: Analysis — what did the model learn?")
    print(f"{'─' * 70}")

    results = df.collect().to_pylist()

    cluster_counts = Counter(r["cluster__cluster_id"] for r in results)
    print(f"\n  Found {len(cluster_counts)} clusters:")
    for cid, count in sorted(cluster_counts.items()):
        print(f"    Cluster {cid}: {count} segments")

    for cid in sorted(cluster_counts.keys()):
        print(f"\n  CLUSTER {cid}:")
        cluster_rows = [r for r in results if r["cluster__cluster_id"] == cid]

        c_lens = Counter(r["perspective__lens"] for r in cluster_rows)
        c_voice = Counter(r["voice__classification"] for r in cluster_rows)
        print(f"    Perspective: {dict(c_lens)}")
        print(f"    Voice: {dict(c_voice)}")

        for r in cluster_rows[:3]:
            preview = r["segment__content"][:100]
            print(f"    [{r['perspective__lens']:>13}|{r['voice__classification']:>8}] {preview}")

    # ── Anomaly report ──
    print(f"\n{'─' * 70}")
    print("PHASE 6b: Anomaly outliers (score > 1.0)")
    print(f"{'─' * 70}")

    outliers = [r for r in results if r["anomaly__outlier_score"] > 1.0]
    print(f"  {len(outliers)} outlier(s) found")
    for r in outliers[:5]:
        print(f"    score={r['anomaly__outlier_score']:.2f}  {r['segment__content'][:80]}")

    # ── Drift report ──
    print(f"\n{'─' * 70}")
    print("PHASE 6c: Embedding drift")
    print(f"{'─' * 70}")

    divergence = results[0]["drift__divergence"]
    window_counts = Counter(r["drift__window"] for r in results)
    print(f"  Divergence: {divergence:.4f}")
    print(f"  Windows: {dict(window_counts)}")

    # ── Label agreement ──
    print(f"\n{'─' * 70}")
    print("PHASE 7: Label agreement — did the model rediscover the original structure?")
    print(f"{'─' * 70}")

    for label_col in ["perspective__lens", "voice__classification"]:
        print(f"\n  {label_col}:")
        label_to_clusters: dict[str, list[int]] = {}
        for r in results:
            label_to_clusters.setdefault(r[label_col], []).append(r["cluster__cluster_id"])

        for label, clusters in sorted(label_to_clusters.items()):
            dist = Counter(clusters)
            dominant = dist.most_common(1)[0]
            purity = dominant[1] / len(clusters)
            print(f"    {label:>15}: {dict(dist)}  (purity={purity:.0%})")

    # ── Export ──
    output = {
        "segments": n,
        "triplets": len(all_triplets),
        "training": metrics,
        "clusters": {str(k): v for k, v in cluster_counts.items()},
        "anomalies": len(outliers),
        "drift_divergence": divergence,
        "model_path": str(model_path),
    }
    output_path = Path("synth_results.json")
    with open(output_path, "w") as f:
        json.dump(output, f, indent=2)
    print(f"\nResults written to {output_path}")


if __name__ == "__main__":
    main()
