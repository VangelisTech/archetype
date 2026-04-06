---
name: daft-patterns
description: Enforces correct Daft DataFrame patterns. Auto-triggers when writing or editing Python files that use Daft, DataFrames, UDFs, or column expressions.
paths: "src/**/*.py,tests/**/*.py,examples/**/*.py"
---

## Rules

These are non-negotiable. Violating them produces broken pipelines.

### 1. DataFrames are lazy. Do not break the DAG.

Never `.collect()` mid-pipeline. It materializes a separate plan and downstream columns may be empty.

```python
# WRONG — breaks the DAG
df = df.with_column("response", prompt(col("input"), ...))
debug = df.select("response").limit(1).collect()  # separate plan!
df = df.with_column("next", col("response") + "...")  # response may be empty

# RIGHT — single materialization
df = df.with_column("response", prompt(col("input"), ...))
df = df.with_column("next", col("response") + "...")
result = df.collect()
```

Use `df.explain()` to debug, not intermediate collects.

### 2. UDF selection

| Decorator | When | Example |
|-----------|------|---------|
| DataFrame expressions | Always prefer this | `col("x") + col("y")` |
| `@daft.func` | Row-wise transform, auto type inference | Simple string parsing, JSON encoding |
| `@daft.func.batch` | Operation actually batches (NumPy, vLLM, PyTorch) | Vectorized math, batch inference |
| `@daft.cls()` | Expensive init, once per worker | Model loading, DB connections |
| `@daft.method.batch` | Stateful + actual batching | Model with batch predict |

**`@daft.udf` is removed.** Deprecated 0.7.0, gone 0.8.0. Never use it. Use `@daft.func.batch` instead.

**If you loop inside a batch UDF, you're not batching.** Use `@daft.func` row-wise instead.

```python
# WRONG — looping inside batch, no batching benefit
@daft.func.batch(return_dtype=DataType.string())
def process(values: Series) -> Series:
    return Series.from_pylist([transform(v) for v in values.to_pylist()])

# RIGHT — row-wise with auto type inference
@daft.func
def process(value: str) -> str:
    return transform(value)
```

### 3. Struct access

```python
# WRONG — deprecated
col("result").struct.get("field")

# RIGHT
col("result")["field"]
```

### 4. Column expressions first

The DataFrame is already columnar. Most transforms are just expressions:

```python
df = df.with_column("score", col("reward") * 0.5 + col("bonus"))
df = df.where(col("score") > 0.5)
df = df.groupby("env_id").agg(col("reward").mean())
```

UDFs are the escape hatch, not the default.

### 5. `with_columns` over chained `with_column`

Use `with_columns(dict)` for multiple column updates. Chained `with_column` can cause Daft plan dependency issues.

```python
# Prefer
df = df.with_columns({
    "col_a": col("x") + 1,
    "col_b": col("y") * 2,
})
```
