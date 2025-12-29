# Archetype RL Module Remediation Plan

**Status**: Approved (with API corrections)
**Date**: December 27, 2025
**Authors**: lake-claude-opus-4.5 (proposal), lake-gemini-3-flash (review), lake-claude-opus-4.5 (API verification)

---

## Overview

This document distills the surgical remediation plan for `archetype/src/archetype/rl/` into an executable phased approach. The plan addresses violations of the Daft-native architecture established in `archetype/src/archetype/core/`.

**Guiding Principle**: Minimal diff, maximal correctness.

**API Verification**: All code samples verified against [Daft Functions](https://docs.getdaft.io/en/stable/custom-code/func/) and [Classes & Methods](https://docs.getdaft.io/en/stable/custom-code/cls/) documentation.

---

## Phase Summary

| Phase | Focus | Surgeries | Parallelizable |
|-------|-------|-----------|----------------|
| **1** | Security | S1 | Yes (with Phase 2) |
| **2** | Correctness | S4, S5 | Yes (with Phase 1) |
| **3** | Scalability | S2 | No (depends on Phase 2) |
| **4** | Consistency | S6 | Yes (independent) |

**Note**: Surgery 3 (GRPOBatch removal) has been **withdrawn** after API verification—see rationale below.

**Total Estimated Changes**: ~120 lines modified, ~40 lines added

---

## Phase 1: Security 🔴

### Surgery 1: Remove Unsafe `eval()`

**File**: `archetype/src/archetype/rl/daft_query_training.py`
**Lines**: 471-478
**Risk**: Low (isolated)

**Problem**: Arbitrary code execution via `eval(query)` on model-generated strings.

**Solution**: Replace with AST-validated restricted execution.

```python
# Before
result = eval(query)  # Vulnerable

# After
def _is_safe_daft_query(self, query: str) -> bool:
    """Whitelist-based AST validation."""
    import ast
    try:
        tree = ast.parse(query, mode='eval')
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                if isinstance(node.func, ast.Attribute):
                    if node.func.attr not in self._ALLOWED_METHODS:
                        return False
            if isinstance(node, ast.Name):
                if node.id not in ('df', 'col', 'lit', 'True', 'False', 'None'):
                    return False
        return True
    except SyntaxError:
        return False

_ALLOWED_METHODS = frozenset({
    'select', 'where', 'groupby', 'agg', 'sort', 'limit',
    'mean', 'sum', 'min', 'max', 'stddev', 'count', 'alias',
    'collect', 'to_pylist', 'struct', 'get',
})

def _execute_restricted(self, query: str, df):
    """Execute in sandboxed namespace."""
    from daft import col, lit
    return eval(query, {"__builtins__": {}}, {'df': df, 'col': col, 'lit': lit})

# Usage
if self._is_safe_daft_query(query):
    result = self._execute_restricted(query, df)
```

**Follow-up** (per lake-gemini-3-flash): Replace restricted `eval()` with pure AST walker interpreter for complete safety.

**Validation**:
- [ ] Test rejects `__import__('os').system('...')`
- [ ] Test rejects `().__class__.__subclasses__()`
- [ ] CI grep guard: fail on bare `eval(` in `archetype/rl/`

---

## Phase 2: Correctness 🔴

### Surgery 4: Implement Real GRPO Loss

**File**: `archetype/src/archetype/rl/grpo.py`
**Lines**: 254-293
**Risk**: Medium (requires testing)

**Problem**: `compute_grpo_loss` returns `0.0` placeholder—training doesn't train.

**Solution**: Implement PPO-style clipped objective.

```python
def compute_grpo_loss(
    new_log_probs: torch.Tensor,
    old_log_probs: torch.Tensor,
    advantages: torch.Tensor,
    clip_epsilon: float = 0.2,
) -> Tuple[torch.Tensor, Dict[str, float]]:
    """PPO-style clipped surrogate loss."""
    import torch

    ratio = torch.exp(new_log_probs - old_log_probs)
    clipped_ratio = torch.clamp(ratio, 1.0 - clip_epsilon, 1.0 + clip_epsilon)

    surr1 = ratio * advantages
    surr2 = clipped_ratio * advantages
    policy_loss = -torch.min(surr1, surr2).mean()

    with torch.no_grad():
        clip_fraction = ((ratio - 1.0).abs() > clip_epsilon).float().mean().item()
        approx_kl = (old_log_probs - new_log_probs).mean().item()

    return policy_loss, {
        "policy_loss": policy_loss.item(),
        "clip_fraction": clip_fraction,
        "approx_kl": approx_kl,
    }
```

**Validation**:
- [ ] `loss.backward()` succeeds without error
- [ ] Loss decreases over training iterations
- [ ] Gradients are non-zero

---

### Surgery 5: Implement Real KL Penalty

**File**: `archetype/src/archetype/rl/grpo.py`
**Lines**: 296-308
**Risk**: Low

**Problem**: `compute_kl_penalty` returns `0.0` placeholder.

**Solution**: Implement reverse KL divergence.

```python
def compute_kl_penalty(
    new_log_probs: torch.Tensor,
    ref_log_probs: torch.Tensor,
) -> torch.Tensor:
    """Reverse KL: penalizes low prob where ref is high."""
    return (ref_log_probs - new_log_probs).mean()
```

---

## Phase 3: Scalability 🔴

### Surgery 2: Pure Daft `assign_groups`

**File**: `archetype/src/archetype/rl/grpo.py`
**Lines**: 164-208
**Risk**: Medium (core algorithm)

**Problem**: `assign_groups` uses `.collect().to_pylist()` **outside** of a UDF, causing OOM on large rollouts.

**Solution**: Replace with pure Daft expressions using `monotonically_increasing_id()` and window functions.

**API Reference**: Daft provides [`monotonically_increasing_id()`](https://docs.getdaft.io/en/stable/custom-code/func/) for generating unique row identifiers, and [`row_number()`](https://docs.getdaft.io/en/stable/custom-code/func/) as a window function via `.over()`.

```python
from daft import col, DataType
from daft.functions import monotonically_increasing_id, row_number

def assign_groups(
    trajectories: DataFrame,
    group_size: int,
    group_by_col: Optional[str] = None,
) -> DataFrame:
    """
    Assign samples to groups for GRPO using pure Daft expressions.

    NO .collect().to_pylist() — stays lazy until execution boundary.

    Uses monotonically_increasing_id() for global row numbering
    or row_number().over() for partitioned numbering.
    """
    if group_by_col:
        # Use window function for partitioned row numbers
        # row_number() requires .over() with partition specification
        df = trajectories.with_column(
            "_row_num",
            row_number().over(group_by_col)
        )
        df = df.with_column(
            "group_id",
            (col("_row_num") // group_size).cast(DataType.int64())
        )
        # Make group_id globally unique by combining with partition hash
        df = df.with_column(
            "group_id",
            col("group_id") + (col(group_by_col).hash() % 1_000_000) * 1_000_000
        )
        df = df.exclude("_row_num")
    else:
        # Use monotonically_increasing_id for global row IDs
        df = trajectories.with_column(
            "_row_id",
            monotonically_increasing_id()
        )
        df = df.with_column(
            "group_id",
            (col("_row_id") // group_size).cast(DataType.int64())
        )
        df = df.exclude("_row_id")

    return df
```

**Key API Details**:
- `monotonically_increasing_id()`: Generates unique IDs across partitions (not guaranteed sequential, but unique and increasing within partition)
- `row_number().over(partition_col)`: Sequential row numbers within each partition

**Validation**:
- [ ] No `.collect().to_pylist()` in function body
- [ ] Works with 1M+ row trajectories without OOM
- [ ] Group IDs are stable within session

---

### ~~Surgery 3: Remove GRPOBatch Eager Collection~~ (WITHDRAWN)

**Status**: ❌ **Withdrawn after API verification**

**Original Proposal**: Remove `GRPOBatch`, pass DataFrame directly to `@daft.cls` TrainStep UDF.

**Why This Is Infeasible**:

Per the [Daft Classes & Methods documentation](https://docs.getdaft.io/en/stable/custom-code/cls/):

1. **UDFs receive rows or Series, not DataFrames**: A `@daft.cls` method receives either individual values (row-wise) or `daft.Series` objects (with `@daft.method.batch`). You cannot pass a DataFrame to a UDF.

2. **`.to_pylist()` inside batch UDFs is correct**: The documented pattern for `@daft.method.batch` shows:
   ```python
   @daft.method.batch(return_dtype=DataType.int64())
   def predict(self, x: Series) -> Series:
       predictions = self.model.predict(x.to_arrow().to_numpy())
       return predictions
   ```
   Converting `Series` to Python/NumPy **inside** the UDF is the intended design.

3. **The actual problem is `.collect().to_pylist()` OUTSIDE UDFs**: The violation in `assign_groups` happens at the DataFrame level before any UDF is called. This escapes the lazy execution model.

**Revised Understanding**:

| Location | `.to_pylist()` | Verdict |
|----------|---------------|---------|
| Inside `@daft.func.batch` / `@daft.method.batch` | `series.to_pylist()` | ✅ Correct |
| Outside UDF on DataFrame | `df.collect().to_pylist()` | ❌ Violation (escapes lazy) |

**GRPOBatch Status**: The `GRPOBatch.from_dataframe()` method **is** a problem because it calls `df.collect().to_pylist()` outside a UDF. However, the fix is not to "pass DataFrame to UDF"—it's to restructure the training loop so that:

1. Group assignment happens via pure Daft (Surgery 2)
2. Advantage computation happens via Daft expressions
3. The `@daft.cls` TrainStep receives columns via `@daft.method.batch` and converts `Series → Tensor` inside the UDF

**Deferred**: This requires deeper refactoring of the training loop architecture, not a surgical fix.

---

## Phase 4: Consistency 🟡

### Surgery 6: Pydantic Migration

**Files**:
- `grpo.py`: `GRPOBatch`
- `training.py`: `TrainingMetrics`
- `daft_query_training.py`: `TableSchema`

**Risk**: Low

**Problem**: `@dataclass` doesn't serialize cleanly to Arrow/Lance.

**Solution**: Convert to Pydantic `BaseModel`.

```python
# Before
@dataclass
class TrainingMetrics:
    history: List[Dict[str, Any]] = field(default_factory=list)

# After
from pydantic import BaseModel, Field

class TrainingMetrics(BaseModel):
    """Training metrics with Arrow-compatible serialization."""
    history: List[Dict[str, Any]] = Field(default_factory=list)

    model_config = {"arbitrary_types_allowed": True}
```

---

## Validation Checklist (Pre-Merge)

### Security
- [ ] `grep -r "eval(" archetype/src/archetype/rl/` returns only `_execute_restricted`
- [ ] Injection test suite passes

### Correctness
- [ ] `loss.backward()` succeeds
- [ ] Training loss decreases over epochs
- [ ] KL penalty is non-zero when policies diverge

### Scalability
- [ ] `grep -r "\.collect()\.to_pylist()" archetype/src/archetype/rl/` returns only:
  - Inside `@daft.func.batch` or `@daft.method.batch` UDFs
  - Scalar metrics at epoch boundary
  - Debug samples behind `debug=True`
  - Final storage writes

### Consistency
- [ ] All config/state classes use Pydantic
- [ ] `model.model_dump()` works with LanceDB

---

## Daft API Quick Reference

For implementers, here are the correct Daft patterns:

### Row Numbering (Surgery 2)

```python
from daft.functions import monotonically_increasing_id, row_number

# Global unique IDs (not sequential, but unique)
df = df.with_column("row_id", monotonically_increasing_id())

# Sequential within partition (window function)
df = df.with_column("row_num", row_number().over("partition_col"))
```

### Batch UDFs (Reference)

```python
from daft import DataType, Series

# Stateless batch function
@daft.func.batch(return_dtype=DataType.float64())
def my_batch_fn(x: Series) -> Series:
    # .to_pylist() or .to_arrow() inside UDF is correct
    values = x.to_pylist()
    return Series.from_pylist([v * 2 for v in values])

# Stateful batch method
@daft.cls
class MyModel:
    def __init__(self, model_path: str):
        self.model = load_model(model_path)

    @daft.method.batch(return_dtype=DataType.int64())
    def predict(self, x: Series) -> Series:
        # Convert to numpy inside UDF
        arr = x.to_arrow().to_numpy()
        return self.model.predict(arr)
```

### What NOT to Do

```python
# ❌ WRONG: Escapes lazy execution
def bad_function(df: DataFrame) -> DataFrame:
    rows = df.collect().to_pylist()  # Materializes everything!
    for row in rows:
        # manual processing
    return daft.from_pylist(rows)

# ✅ RIGHT: Pure Daft expressions
def good_function(df: DataFrame) -> DataFrame:
    return df.with_column("result", col("x") * 2)
```

---

## Integration Decision

**Training Architecture** (decided by lake-gemini-3-flash):

> Create a `TrainingWorld` subclass of `AsyncWorld`.

**Rationale**: Simulation and training have distinct lifecycles. `TrainingWorld` manages:
- Epoch concept
- Weight synchronization via `vllm_client.update_named_param()`
- Integration with `WorldOrchestrator` using same interface

---

## Execution Timeline (Revised)

```
Week 1:
├── Phase 1: Surgery 1 (eval removal)
└── Phase 2: Surgery 4, 5 (loss functions)

Week 2:
├── Phase 3: Surgery 2 (assign_groups with monotonically_increasing_id)
└── Phase 4: Surgery 6 (Pydantic migration)

Week 3:
├── Integration testing
├── TrainingWorld scaffold
└── Design review: training loop restructure (deferred Surgery 3)
```

---

## References

- [Architecture Decisions (full)](./rl-architecture-decisions.md)
- [Daft Functions Documentation](https://docs.getdaft.io/en/stable/custom-code/func/)
- [Daft Classes & Methods Documentation](https://docs.getdaft.io/en/stable/custom-code/cls/)
- Section 11: Surgical Remediation Plan (lake-claude-opus-4.5)
- Section 12: Proposal Review (lake-gemini-3-flash)
