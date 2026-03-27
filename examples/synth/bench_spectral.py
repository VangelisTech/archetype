#!/usr/bin/env python3
# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Benchmark spectral clustering approaches inside Daft batch UDFs.

Approaches:
  1. sklearn SpectralClustering (CPU baseline)
  2. MLX spectral: GPU affinity matrix + CPU eigh + MLX k-means
  3. MLX Nyström: subsample landmarks for O(n·m) affinity instead of O(n²)

Usage:
    uv run python -m examples.synth.bench_spectral [--sizes 200,500,1000] [--k 4] [--dim 128]
"""

import argparse
import time
from collections import Counter

import daft
import mlx.core as mx
import numpy as np
from daft import DataType, Series, col

# ─── Approach 1: sklearn baseline ─────────────────────────────────────────────


@daft.func.batch(
    return_dtype=DataType.struct({
        "cluster_id": DataType.int64(),
        "centroid_distance": DataType.float64(),
    }),
    unnest=True,
)
def sklearn_spectral(vectors: Series, n_clusters: int) -> list[dict]:
    """sklearn SpectralClustering — full CPU, O(n²) affinity + ARPACK eigen."""
    from sklearn.cluster import SpectralClustering

    X = np.array(vectors.to_pylist())
    if len(X) < n_clusters:
        return [{"cluster_id": -1, "centroid_distance": 0.0}] * len(X)

    sc = SpectralClustering(
        n_clusters=n_clusters,
        affinity="rbf",
        random_state=42,
        n_init=10,
        assign_labels="kmeans",
    )
    labels = sc.fit_predict(X)

    # Compute centroid distances for comparability
    centers = np.array([X[labels == k].mean(axis=0) for k in range(n_clusters)])
    dists = np.linalg.norm(X - centers[labels], axis=1)
    return [
        {"cluster_id": int(label), "centroid_distance": float(dist)}
        for label, dist in zip(labels, dists, strict=True)
    ]


# ─── Approach 2: MLX spectral (GPU affinity + CPU eigen) ─────────────────────


def _mlx_spectral_core(X_np: np.ndarray, n_clusters: int) -> tuple[np.ndarray, np.ndarray]:
    """
    Spectral clustering via MLX unified memory.

    - GPU: RBF affinity matrix (pairwise distances → exp), degree matrix
    - CPU: eigendecomposition (eigh not on GPU yet)
    - GPU: final k-means on spectral embedding
    """
    n = X_np.shape[0]
    X = mx.array(X_np)

    # GPU: pairwise squared distances via ||a-b||² = ||a||² + ||b||² - 2a·b
    sq_norms = mx.sum(X * X, axis=1, keepdims=True)  # (n, 1)
    dists_sq = sq_norms + sq_norms.T - 2.0 * (X @ X.T)  # (n, n)
    dists_sq = mx.maximum(dists_sq, 0.0)

    # GPU: RBF kernel — sigma = median heuristic
    mx.eval(dists_sq)
    median_dist = mx.sort(mx.reshape(dists_sq, (-1,)))[n * n // 2]
    mx.eval(median_dist)
    gamma = 1.0 / (2.0 * mx.maximum(median_dist, mx.array(1e-8)))
    W = mx.exp(-gamma * dists_sq)
    # Zero diagonal
    W = W - mx.diag(mx.diag(W))
    mx.eval(W)

    # GPU: degree matrix and normalized Laplacian
    d = mx.sum(W, axis=1)
    d_inv_sqrt = 1.0 / mx.sqrt(mx.maximum(d, mx.array(1e-10)))
    D_inv_sqrt = mx.diag(d_inv_sqrt)
    L_norm = mx.eye(n) - D_inv_sqrt @ W @ D_inv_sqrt
    mx.eval(L_norm)

    # CPU: eigendecomposition (eigh is CPU-only in MLX)
    eigenvalues, eigenvectors = mx.linalg.eigh(L_norm, stream=mx.cpu)
    mx.eval(eigenvalues, eigenvectors)

    # Take the k smallest eigenvectors (skip first if near-zero)
    U = eigenvectors[:, :n_clusters]

    # Normalize rows
    row_norms = mx.sqrt(mx.sum(U * U, axis=1, keepdims=True))
    row_norms = mx.maximum(row_norms, mx.array(1e-10))
    U = U / row_norms
    mx.eval(U)

    # Simple k-means on the spectral embedding (GPU)
    labels = _mlx_kmeans(U, n_clusters, max_iter=50)

    # Compute centroid distances in original space (numpy — converting anyway)
    labels_np = np.array(labels.tolist())
    centers_np = np.array([X_np[labels_np == k].mean(axis=0) for k in range(n_clusters)])
    dists_np = np.linalg.norm(X_np - centers_np[labels_np], axis=1)

    return labels_np, dists_np


def _mlx_kmeans(X: mx.array, k: int, max_iter: int = 50) -> mx.array:
    """Simple k-means in MLX (GPU). Returns labels only."""
    n = X.shape[0]
    indices = mx.array(np.random.default_rng(42).choice(n, size=k, replace=False))
    centers = X[indices]
    mx.eval(centers)

    for _ in range(max_iter):
        # Assign: pairwise distances to centers (n, d) vs (k, d) → (n, k)
        diffs = mx.expand_dims(X, axis=1) - mx.expand_dims(centers, axis=0)
        dists = mx.sum(diffs * diffs, axis=2)
        labels = mx.argmin(dists, axis=1)
        mx.eval(labels)

        # Update centers via one-hot scatter (no boolean indexing)
        one_hot = mx.zeros((n, k))
        one_hot = one_hot.at[mx.arange(n), labels].add(1.0)  # (n, k)
        counts = mx.sum(one_hot, axis=0, keepdims=True).T     # (k, 1)
        counts = mx.maximum(counts, mx.array(1.0))
        new_centers = (one_hot.T @ X) / counts                # (k, d)
        mx.eval(new_centers)

        if mx.allclose(centers, new_centers, atol=1e-6).item():
            break
        centers = new_centers

    return labels


@daft.func.batch(
    return_dtype=DataType.struct({
        "cluster_id": DataType.int64(),
        "centroid_distance": DataType.float64(),
    }),
    unnest=True,
)
def mlx_spectral(vectors: Series, n_clusters: int) -> list[dict]:
    """MLX spectral: GPU affinity + CPU eigen + GPU k-means."""
    X = np.array(vectors.to_pylist())
    if len(X) < n_clusters:
        return [{"cluster_id": -1, "centroid_distance": 0.0}] * len(X)

    labels, dists = _mlx_spectral_core(X, n_clusters)
    return [
        {"cluster_id": int(label), "centroid_distance": float(dist)}
        for label, dist in zip(labels, dists, strict=True)
    ]


# ─── Approach 3: MLX Nyström approximation ───────────────────────────────────


def _mlx_nystrom_core(
    X_np: np.ndarray, n_clusters: int, n_landmarks: int = 100,
) -> tuple[np.ndarray, np.ndarray]:
    """
    Nyström-approximated spectral clustering.

    Instead of O(n²) full affinity, subsample m landmarks:
    - Compute W_nm (n × m) and W_mm (m × m)
    - Approximate full Laplacian via Nyström embedding
    - Eigen on m × m matrix instead of n × n

    Complexity: O(n·m) instead of O(n²)
    """
    n = X_np.shape[0]
    m = min(n_landmarks, n)
    rng = np.random.default_rng(42)
    landmark_idx = rng.choice(n, size=m, replace=False)

    X = mx.array(X_np)
    L = X[mx.array(landmark_idx)]  # (m, d)

    # GPU: pairwise distances X to landmarks → (n, m)
    sq_X = mx.sum(X * X, axis=1, keepdims=True)   # (n, 1)
    sq_L = mx.sum(L * L, axis=1, keepdims=True)   # (m, 1)
    dists_nm = sq_X + sq_L.T - 2.0 * (X @ L.T)
    dists_nm = mx.maximum(dists_nm, 0.0)

    # GPU: landmark-to-landmark distances → (m, m)
    dists_mm = sq_L + sq_L.T - 2.0 * (L @ L.T)
    dists_mm = mx.maximum(dists_mm, 0.0)
    mx.eval(dists_nm, dists_mm)

    # Median heuristic for gamma (from landmark distances)
    median_dist = mx.sort(mx.reshape(dists_mm, (-1,)))[m * m // 2]
    mx.eval(median_dist)
    gamma = 1.0 / (2.0 * mx.maximum(median_dist, mx.array(1e-8)))

    # GPU: RBF kernels
    W_nm = mx.exp(-gamma * dists_nm)  # (n, m)
    W_mm = mx.exp(-gamma * dists_mm)  # (m, m)
    W_mm = W_mm - mx.diag(mx.diag(W_mm)) + mx.eye(m) * 1e-6  # regularize
    mx.eval(W_nm, W_mm)

    # CPU: eigen decomposition of W_mm
    evals_mm, evecs_mm = mx.linalg.eigh(W_mm, stream=mx.cpu)
    mx.eval(evals_mm, evecs_mm)

    # Nyström embedding: U_approx = W_nm @ evecs_mm @ diag(1/sqrt(evals_mm))
    evals_mm = mx.maximum(evals_mm, mx.array(1e-10))
    scale = 1.0 / mx.sqrt(evals_mm)
    U_approx = W_nm @ evecs_mm @ mx.diag(scale)
    mx.eval(U_approx)

    # Take k columns, normalize rows
    U_k = U_approx[:, -n_clusters:]  # largest eigenvalues (most significant)
    row_norms = mx.sqrt(mx.sum(U_k * U_k, axis=1, keepdims=True))
    row_norms = mx.maximum(row_norms, mx.array(1e-10))
    U_k = U_k / row_norms
    mx.eval(U_k)

    # GPU: k-means on the Nyström embedding
    labels = _mlx_kmeans(U_k, n_clusters, max_iter=50)

    labels_np = np.array(labels.tolist())
    centers_np = np.array([X_np[labels_np == k].mean(axis=0) for k in range(n_clusters)])
    dists_np = np.linalg.norm(X_np - centers_np[labels_np], axis=1)

    return labels_np, dists_np


@daft.func.batch(
    return_dtype=DataType.struct({
        "cluster_id": DataType.int64(),
        "centroid_distance": DataType.float64(),
    }),
    unnest=True,
)
def mlx_nystrom(vectors: Series, n_clusters: int, n_landmarks: int) -> list[dict]:
    """MLX Nyström-approximated spectral clustering."""
    X = np.array(vectors.to_pylist())
    if len(X) < n_clusters:
        return [{"cluster_id": -1, "centroid_distance": 0.0}] * len(X)

    labels, dists = _mlx_nystrom_core(X, n_clusters, n_landmarks)
    return [
        {"cluster_id": int(label), "centroid_distance": float(dist)}
        for label, dist in zip(labels, dists, strict=True)
    ]


# ─── Approach 4: MLX power iteration (cosine affinity, no eigh) ───────────────


def _mlx_power_spectral_core(
    X_np: np.ndarray,
    n_clusters: int,
    n_iters: int = 30,
    diffusion_steps: int = 3,
    knn: int = 10,
) -> tuple[np.ndarray, np.ndarray]:
    """
    Spectral clustering via power iteration on a sparsified diffusion operator.

    No eigendecomposition. The dominant invariant subspace of D⁻¹W is
    recovered by iterating the random-walk operator, analogous to
    propagating the state transition map in discrete-time optimal control.

    Steps:
      1. Cosine affinity: W = X @ X.T (one GPU matmul)
      2. k-NN sparsification: zero out all but the k nearest neighbors per row
         — creates clean block structure, eliminates weak cross-cluster edges
      3. Random-walk operator: P = D⁻¹W (row-stochastic transition matrix)
      4. Diffusion: P^t sharpens cluster separation (t steps of the Markov chain)
      5. Power iteration + Gram-Schmidt: V_{i+1} = orth(P^t @ V_i) → top-k subspace
      6. k-means on the k-dim embedding
    """
    n = X_np.shape[0]
    X = mx.array(X_np)

    # GPU: L2-normalize rows
    row_norms = mx.sqrt(mx.sum(X * X, axis=1, keepdims=True))
    row_norms = mx.maximum(row_norms, mx.array(1e-10))
    X = X / row_norms

    # GPU: cosine affinity — one matmul
    W = X @ X.T
    W = (W + 1.0) / 2.0
    W = W - mx.diag(mx.diag(W))
    mx.eval(W)

    # GPU: k-NN sparsification — keep only top-knn neighbors per row
    sorted_W = mx.sort(W, axis=1)
    threshold = sorted_W[:, -(knn + 1) : -knn]  # (n, 1): knn-th largest per row
    W = mx.where(W >= threshold, W, mx.array(0.0))
    W = (W + W.T) / 2.0  # symmetrize
    mx.eval(W)

    # GPU: row-stochastic transition matrix P = D⁻¹W
    d = mx.sum(W, axis=1, keepdims=True)
    d = mx.maximum(d, mx.array(1e-10))
    P = W / d
    mx.eval(P)

    # GPU: diffusion — P^t via repeated matmul
    Pt = P
    for _ in range(diffusion_steps - 1):
        Pt = Pt @ P
        mx.eval(Pt)

    # Subspace iteration: block power method with Gram-Schmidt
    V = mx.array(np.random.default_rng(42).standard_normal((n, n_clusters)).astype(np.float32))
    mx.eval(V)

    for _ in range(n_iters):
        V = Pt @ V
        mx.eval(V)
        V = _mlx_gram_schmidt(V)
        mx.eval(V)

    # Normalize rows of the spectral embedding
    row_norms = mx.sqrt(mx.sum(V * V, axis=1, keepdims=True))
    row_norms = mx.maximum(row_norms, mx.array(1e-10))
    V = V / row_norms
    mx.eval(V)

    # GPU: k-means on the spectral embedding
    labels = _mlx_kmeans(V, n_clusters, max_iter=50)

    # Centroid distances in original space
    labels_np = np.array(labels.tolist())
    centers_np = np.array([X_np[labels_np == k].mean(axis=0) for k in range(n_clusters)])
    dists_np = np.linalg.norm(X_np - centers_np[labels_np], axis=1)

    return labels_np, dists_np


def _mlx_gram_schmidt(V: mx.array) -> mx.array:
    """Modified Gram-Schmidt orthogonalization. Operates column-wise."""
    k = V.shape[1]
    for i in range(k):
        vi = V[:, i]
        for j in range(i):
            vj = V[:, j]
            proj = mx.sum(vi * vj)
            vi = vi - proj * vj
        norm = mx.sqrt(mx.sum(vi * vi))
        norm = mx.maximum(norm, mx.array(1e-10))
        V = V.at[:, i].add(vi / norm - V[:, i])
    mx.eval(V)
    return V


@daft.func.batch(
    return_dtype=DataType.struct({
        "cluster_id": DataType.int64(),
        "centroid_distance": DataType.float64(),
    }),
    unnest=True,
)
def mlx_power(vectors: Series, n_clusters: int) -> list[dict]:
    """MLX power iteration: sparse cosine affinity + diffusion + subspace iteration. No eigh."""
    X = np.array(vectors.to_pylist())
    if len(X) < n_clusters:
        return [{"cluster_id": -1, "centroid_distance": 0.0}] * len(X)

    labels, dists = _mlx_power_spectral_core(X, n_clusters)
    return [
        {"cluster_id": int(label), "centroid_distance": float(dist)}
        for label, dist in zip(labels, dists, strict=True)
    ]


# ─── Benchmark harness ───────────────────────────────────────────────────────


def make_synthetic_data(n: int, dim: int, k: int, seed: int = 42) -> daft.DataFrame:
    """Generate k well-separated Gaussian clusters."""
    rng = np.random.default_rng(seed)
    centers = rng.standard_normal((k, dim)) * 10
    vectors = []
    for i in range(n):
        c = centers[i % k]
        vectors.append((c + rng.standard_normal(dim) * 0.5).tolist())
    return daft.from_pydict({"embedding__vector": vectors})


def run_benchmark(
    sizes: list[int], n_clusters: int, dim: int, n_landmarks: int,
) -> list[dict]:
    results = []

    approaches = [
        ("sklearn", lambda df, k: df.select(
            col("*"), sklearn_spectral(col("embedding__vector"), k),
        )),
        ("mlx_spectral", lambda df, k: df.select(
            col("*"), mlx_spectral(col("embedding__vector"), k),
        )),
        ("mlx_nystrom", lambda df, k: df.select(
            col("*"), mlx_nystrom(col("embedding__vector"), k, n_landmarks),
        )),
        ("mlx_power", lambda df, k: df.select(
            col("*"), mlx_power(col("embedding__vector"), k),
        )),
    ]

    for n in sizes:
        print(f"\n{'=' * 60}")
        print(f"n={n}, dim={dim}, k={n_clusters}")
        print(f"{'=' * 60}")

        df = make_synthetic_data(n, dim, n_clusters)

        for name, fn in approaches:
            # Warm up MLX / sklearn
            if n == sizes[0]:
                small = make_synthetic_data(50, dim, n_clusters)
                try:
                    fn(small, n_clusters).collect()
                except Exception:
                    pass

            t0 = time.perf_counter()
            try:
                result_df = fn(df, n_clusters)
                rows = result_df.collect().to_pylist()
                elapsed = time.perf_counter() - t0
                cluster_ids = [r["cluster_id"] for r in rows]
                n_found = len(set(cluster_ids))
                dist_counts = Counter(cluster_ids)

                print(f"\n  {name:20s}  {elapsed:6.3f}s  clusters={n_found}  dist={dict(sorted(dist_counts.items()))}")
                results.append({
                    "approach": name,
                    "n": n,
                    "dim": dim,
                    "k": n_clusters,
                    "time_s": round(elapsed, 4),
                    "clusters_found": n_found,
                })
            except Exception as e:
                elapsed = time.perf_counter() - t0
                print(f"\n  {name:20s}  FAILED ({elapsed:.3f}s): {e}")
                results.append({
                    "approach": name,
                    "n": n,
                    "dim": dim,
                    "k": n_clusters,
                    "time_s": round(elapsed, 4),
                    "error": str(e),
                })

    return results


def main():
    parser = argparse.ArgumentParser(description="Benchmark spectral clustering in Daft UDFs")
    parser.add_argument("--sizes", default="200,500,1000", help="Comma-separated dataset sizes")
    parser.add_argument("--k", type=int, default=4, help="Number of clusters")
    parser.add_argument("--dim", type=int, default=128, help="Embedding dimension")
    parser.add_argument("--landmarks", type=int, default=100, help="Nyström landmark count")
    args = parser.parse_args()

    sizes = [int(s) for s in args.sizes.split(",")]
    print("Spectral Clustering Benchmark — MLX in Daft UDFs")
    print(f"Sizes: {sizes}, k={args.k}, dim={args.dim}, landmarks={args.landmarks}")

    results = run_benchmark(sizes, args.k, args.dim, args.landmarks)

    # Summary table
    print(f"\n{'=' * 60}")
    print("SUMMARY")
    print(f"{'=' * 60}")
    print(f"{'Approach':20s} {'n':>6} {'Time (s)':>10} {'Clusters':>10}")
    print(f"{'─' * 20} {'─' * 6} {'─' * 10} {'─' * 10}")
    for r in results:
        if "error" in r:
            print(f"{r['approach']:20s} {r['n']:>6} {'FAILED':>10}")
        else:
            print(f"{r['approach']:20s} {r['n']:>6} {r['time_s']:>10.4f} {r['clusters_found']:>10}")


if __name__ == "__main__":
    main()
