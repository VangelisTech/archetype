---
applyTo: "examples/synth/**"
---

# Synth Pipeline Review Guidelines

The synth engine is the showcase for Daft-native processing patterns. It implements a recursive self-improvement loop: triplet generation -> training -> embeddings -> clustering -> anomaly detection -> drift monitoring -> repeat.

## Daft-Native Patterns (CRITICAL)

This pipeline is actively being refactored to eliminate `.collect()` calls. Review for:

1. **No `.collect()` in processors** unless absolutely necessary (model training is the one exception — PyTorch needs materialized tensors).
2. **Use `@daft.cls()` for stateful operations** (model loading, embedding inference). The model loads once per worker in `__init__`.
3. **Use `@daft.func.batch` for vectorized ops** like clustering, scoring — where the operation genuinely benefits from seeing the full batch.
4. **Use plain Daft expressions** for column transforms, filters, aggregations. Don't wrap simple math in UDFs.
5. **Between recursive cycles**, stale columns from previous iterations must be dropped to prevent schema drift.

## Common Issues

- Adding `.collect()` where a Daft expression would work
- Using `@daft.func.batch` but just looping internally (should be `@daft.func`)
- Not cleaning up intermediate columns between recursive cycles
- Forgetting that `@daft.cls()` methods are row-by-row by default
- Model weight files must use `daft.File` pattern for distributed workers
