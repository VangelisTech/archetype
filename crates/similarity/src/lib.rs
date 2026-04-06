// Copyright 2024 Archetype Contributors
// SPDX-License-Identifier: Apache-2.0

//! Fast vector similarity operations for Archetype ECS.
//!
//! Provides cosine similarity, top-k nearest neighbor search,
//! and pairwise similarity matrix computation — all exposed to
//! Python via PyO3.

use pyo3::prelude::*;
use rayon::prelude::*;

/// Compute the cosine similarity between two vectors.
///
/// Returns a value in [-1, 1] where 1 means identical direction,
/// 0 means orthogonal, and -1 means opposite direction.
/// Returns 0.0 if either vector has zero magnitude.
#[pyfunction]
fn cosine_similarity(a: Vec<f32>, b: Vec<f32>) -> PyResult<f32> {
    if a.len() != b.len() {
        return Err(pyo3::exceptions::PyValueError::new_err(
            format!("Vector dimension mismatch: {} vs {}", a.len(), b.len()),
        ));
    }
    Ok(cosine_sim(&a, &b))
}

/// Find the top-k most similar vectors in a matrix to a query vector.
///
/// Returns a list of (index, similarity) tuples sorted by descending similarity.
///
/// Arguments:
///   query: The query vector (1-D, length D).
///   matrix: A flat row-major matrix of shape (N, D) stored as a 1-D list of length N*D.
///   dim: The dimensionality D of each vector.
///   k: Number of top results to return.
#[pyfunction]
fn topk_similar(query: Vec<f32>, matrix: Vec<f32>, dim: usize, k: usize) -> PyResult<Vec<(usize, f32)>> {
    if query.len() != dim {
        return Err(pyo3::exceptions::PyValueError::new_err(
            format!("Query dim {} != declared dim {}", query.len(), dim),
        ));
    }
    if matrix.len() % dim != 0 {
        return Err(pyo3::exceptions::PyValueError::new_err(
            format!("Matrix length {} not divisible by dim {}", matrix.len(), dim),
        ));
    }

    let n = matrix.len() / dim;
    let actual_k = k.min(n);

    // Parallel similarity computation
    let mut scored: Vec<(usize, f32)> = (0..n)
        .into_par_iter()
        .map(|i| {
            let row = &matrix[i * dim..(i + 1) * dim];
            (i, cosine_sim(&query, row))
        })
        .collect();

    // Partial sort: we only need top-k
    scored.select_nth_unstable_by(actual_k.saturating_sub(1), |a, b| {
        b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal)
    });
    scored.truncate(actual_k);
    scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

    Ok(scored)
}

/// Compute a full pairwise cosine similarity matrix.
///
/// Arguments:
///   matrix: A flat row-major matrix of shape (N, D) stored as a 1-D list of length N*D.
///   dim: The dimensionality D of each vector.
///
/// Returns: A flat row-major similarity matrix of shape (N, N) as a list of length N*N.
#[pyfunction]
fn pairwise_matrix(matrix: Vec<f32>, dim: usize) -> PyResult<Vec<f32>> {
    if matrix.len() % dim != 0 {
        return Err(pyo3::exceptions::PyValueError::new_err(
            format!("Matrix length {} not divisible by dim {}", matrix.len(), dim),
        ));
    }

    let n = matrix.len() / dim;
    let mut result = vec![0.0f32; n * n];

    // Compute upper triangle in parallel, mirror to lower
    result
        .par_chunks_mut(n)
        .enumerate()
        .for_each(|(i, row)| {
            let a = &matrix[i * dim..(i + 1) * dim];
            row[i] = 1.0; // self-similarity
            for j in (i + 1)..n {
                let b = &matrix[j * dim..(j + 1) * dim];
                let sim = cosine_sim(a, b);
                row[j] = sim;
            }
        });

    // Mirror upper triangle to lower
    for i in 0..n {
        for j in (i + 1)..n {
            result[j * n + i] = result[i * n + j];
        }
    }

    Ok(result)
}

/// Batch cosine similarity: compute similarity of each query against all rows.
///
/// Arguments:
///   queries: Flat row-major matrix of shape (Q, D).
///   matrix: Flat row-major matrix of shape (N, D).
///   dim: Dimensionality D.
///
/// Returns: Flat row-major matrix of shape (Q, N) — similarity of each query to each row.
#[pyfunction]
fn batch_cosine(queries: Vec<f32>, matrix: Vec<f32>, dim: usize) -> PyResult<Vec<f32>> {
    if queries.len() % dim != 0 {
        return Err(pyo3::exceptions::PyValueError::new_err(
            format!("Queries length {} not divisible by dim {}", queries.len(), dim),
        ));
    }
    if matrix.len() % dim != 0 {
        return Err(pyo3::exceptions::PyValueError::new_err(
            format!("Matrix length {} not divisible by dim {}", matrix.len(), dim),
        ));
    }

    let q = queries.len() / dim;
    let n = matrix.len() / dim;

    let result: Vec<f32> = (0..q)
        .into_par_iter()
        .flat_map(|qi| {
            let query = &queries[qi * dim..(qi + 1) * dim];
            (0..n)
                .map(|ni| {
                    let row = &matrix[ni * dim..(ni + 1) * dim];
                    cosine_sim(query, row)
                })
                .collect::<Vec<f32>>()
        })
        .collect();

    Ok(result)
}

// ── Internal helpers ───────────────────────────────────────────

#[inline]
fn cosine_sim(a: &[f32], b: &[f32]) -> f32 {
    let mut dot = 0.0f32;
    let mut norm_a = 0.0f32;
    let mut norm_b = 0.0f32;

    for i in 0..a.len() {
        dot += a[i] * b[i];
        norm_a += a[i] * a[i];
        norm_b += b[i] * b[i];
    }

    let denom = norm_a.sqrt() * norm_b.sqrt();
    if denom < f32::EPSILON {
        0.0
    } else {
        dot / denom
    }
}

// ── Python module ──────────────────────────────────────────────

#[pymodule]
fn archetype_similarity(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(cosine_similarity, m)?)?;
    m.add_function(wrap_pyfunction!(topk_similar, m)?)?;
    m.add_function(wrap_pyfunction!(pairwise_matrix, m)?)?;
    m.add_function(wrap_pyfunction!(batch_cosine, m)?)?;
    Ok(())
}

// ── Tests ──────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cosine_identical() {
        let v = vec![1.0, 2.0, 3.0];
        let sim = cosine_sim(&v, &v);
        assert!((sim - 1.0).abs() < 1e-6);
    }

    #[test]
    fn test_cosine_orthogonal() {
        let a = vec![1.0, 0.0];
        let b = vec![0.0, 1.0];
        let sim = cosine_sim(&a, &b);
        assert!(sim.abs() < 1e-6);
    }

    #[test]
    fn test_cosine_opposite() {
        let a = vec![1.0, 0.0];
        let b = vec![-1.0, 0.0];
        let sim = cosine_sim(&a, &b);
        assert!((sim + 1.0).abs() < 1e-6);
    }

    #[test]
    fn test_topk() {
        // 3 vectors of dim 2: [1,0], [0.7,0.7], [0,1]
        let matrix = vec![1.0, 0.0, 0.7, 0.7, 0.0, 1.0];
        let query = vec![1.0, 0.0];
        let result = topk_similar(query, matrix, 2, 2).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].0, 0); // [1,0] is most similar to [1,0]
    }

    #[test]
    fn test_pairwise() {
        let matrix = vec![1.0, 0.0, 0.0, 1.0];
        let result = pairwise_matrix(matrix, 2).unwrap();
        assert_eq!(result.len(), 4);
        assert!((result[0] - 1.0).abs() < 1e-6); // self-sim
        assert!((result[3] - 1.0).abs() < 1e-6); // self-sim
        assert!(result[1].abs() < 1e-6); // orthogonal
        assert!(result[2].abs() < 1e-6); // orthogonal (symmetric)
    }

    #[test]
    fn test_zero_vector() {
        let a = vec![0.0, 0.0];
        let b = vec![1.0, 0.0];
        let sim = cosine_sim(&a, &b);
        assert_eq!(sim, 0.0);
    }
}
