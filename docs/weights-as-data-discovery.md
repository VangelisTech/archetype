# Weights as Data: The Discovery

**Date:** 2025-12-27  
**Authors:** Everett (Vangelis), lake-claude-opus-4.5

---

## Executive Summary

We discovered that **RL training IS compatible with Daft's lazy evaluation model** when model weights are treated as data flowing through DataFrames, not as external mutable state.

This insight led to:
1. A clean v2 implementation of Archetype RL (`archetype/src/archetype/rl/v2/`)
2. A proposal for `daft.ModelWeightsFile` datatype
3. A validated pattern for distributed RL training with Daft

---

## The Problem

The original Archetype RL module was "corrupted" - it wasn't using the ECS at all:

```python
# grpo.py line 608 - vanilla Python, not Archetype
for iteration in range(num_iterations):
    trajectories = await rollout.collect(...)  # Eager
    metrics = trainer.train_on_trajectories(trajectories, ...)  # Python objects
```

When asked to integrate with the ECS, we hit a wall:

**"How can training happen inside a Daft UDF when `loss.backward()` mutates model weights?"**

We initially concluded that RL was "fundamentally incompatible" with Daft because:
- Daft builds lazy query plans
- UDFs run on distributed workers
- Workers can't coordinate gradient updates
- Weights would diverge across workers

---

## The Breakthrough

Everett suggested using `daft.File` for safetensors loading. This changed everything.

**The insight:** Don't try to RETURN weights. WRITE them.

```python
@daft.cls
class TrainingUDF:
    @daft.method.batch(return_dtype=DataType.string())
    def train_batch(self, data, weights_file, output_path) -> Series:
        # Load weights via daft.File
        with weights_file.to_tempfile() as temp:
            state_dict = load_file(temp.name)
        
        # Train
        loss.backward()
        optimizer.step()
        
        # WRITE new weights (not return them!)
        save_file(model.state_dict(), output_path)
        
        # Return PATH
        return Series.from_pylist([output_path] * len(data))
```

---

## The Pattern

### daft.File Context Managers

```python
# Pattern 1: file.open() → file-like object
with weights_file.open() as f:
    content = f.read()  # bytes
    content_hash = hash(content)  # For cache invalidation

# Pattern 2: file.to_tempfile() → temp file path
with weights_file.to_tempfile() as temp:
    state_dict = load_file(temp.name)  # safetensors wants path
```

### Stateful UDF with Cache Invalidation

```python
@daft.cls
class InferenceUDF:
    def __init__(self, model: nn.Module):
        self._model = model
        self._weights_hash = None
    
    def __call__(self, input_data, weights_file: daft.File) -> float:
        # Check if weights changed
        with weights_file.open() as f:
            content_hash = hash(f.read())
        
        if self._weights_hash != content_hash:
            with weights_file.to_tempfile() as temp:
                state_dict = load_file(temp.name)
            self._model.load_state_dict(state_dict)
            self._weights_hash = content_hash
        
        return self._model(input_data)
```

### The Training Loop

```python
current_weights = "epoch_0.safetensors"

for epoch in range(num_epochs):
    df = prompts_df.with_column("weights", file(lit(current_weights)))
    
    # === LAZY (distributed) ===
    df = df.with_column("generation", GenerationUDF()(col("prompt"), col("weights")))
    df = df.with_column("reward", compute_reward(...))
    df = df.with_column("advantage", compute_advantage(...))
    
    # === MATERIALIZED (single worker) ===
    df = df.into_partitions(1)
    df = df.with_column("new_path", TrainStepUDF().train_batch(...))
    
    # Collect triggers everything
    result = df.collect()
    
    # New weights for next epoch
    current_weights = f"epoch_{epoch+1}.safetensors"
```

---

## Architecture

```
Epoch N:
┌─────────────────────────────────────────────────────────────────┐
│  df.with_column("weights", file(lit("epoch_N.safetensors")))    │
│    .with_column("gen", GenerationUDF(...))                      │  ← LAZY
│    .with_column("reward", RewardUDF(...))                       │  ← LAZY
│    .with_column("advantage", AdvantageProcessor(...))           │  ← LAZY
│    .into_partitions(1)                                          │  ← CONSOLIDATE
│    .with_column("new_path", TrainStepUDF(...))                  │  ← MATERIALIZE
│                                                                 │
│  Output: epoch_{N+1}.safetensors written to disk                │
└─────────────────────────────────────────────────────────────────┘
                                ↓
Epoch N+1: Uses epoch_{N+1}.safetensors via file(lit(...))
```

---

## Validation

All tests pass:

```
============================================================
RESULTS SUMMARY
============================================================
  ✓ PASS: Load weights via daft.File
  ✓ PASS: Inference with daft.File weights
  ✓ PASS: Train and save weights via UDF
  ✓ PASS: Full epoch chain
  ✓ PASS: Inference with versioned weights

🎉 HYPOTHESIS VALIDATED: Weights can flow as data!
```

---

## Key Takeaways

| Question | Answer |
|----------|--------|
| Is RL compatible with Daft? | **YES**, when weights are data |
| Where is the materialization boundary? | `into_partitions(1)` before training |
| How do weights flow between epochs? | File paths as DataFrame columns |
| What scales horizontally? | Generation, reward, advantage |
| What requires single worker? | Gradient computation (`backward()`) |

---

## Files Created

| File | Purpose |
|------|---------|
| `archetype/src/archetype/rl/v2/` | Clean reimplementation |
| `archetype/src/archetype/rl/experiments/weights_as_data_test.py` | Validation tests |
| `archetype/docs/daft-model-weights-file-proposal.md` | Daft feature proposal |
| `archetype/examples/rl_v2_example.py` | Usage examples |

---

## Next Steps

1. **Production validation**: Test with real LLMs (Qwen, Llama)
2. **Performance benchmarks**: Compare lazy vs eager patterns
3. **daft.ModelWeightsFile**: Propose to Daft maintainers
4. **Deprecate v1**: Mark old RL module as deprecated

---

## Update: Daft + PyTorch Partnership

After discovering the `to_torch_iter_dataset()` bridge (see [Daft blog](https://www.daft.ai/blog/pytorch-data-loader)), we refined the architecture to embrace **both frameworks**:

```
┌─────────────────────────────────────────────────────────────────┐
│                    DAFT (Lazy, Distributed)                     │
│  • daft.File for weight loading                                 │
│  • @daft.cls UDFs for generation                                │
│  • Reward/Advantage computation                                 │
└─────────────────────────────────────────────────────────────────┘
                            │
                            ▼ .to_torch_iter_dataset()
┌─────────────────────────────────────────────────────────────────┐
│                  PYTORCH (Training Ecosystem)                   │
│  • DataLoader (batching, shuffling, pin_memory)                 │
│  • Standard training loop (forward, backward, step)             │
│  • Full ecosystem: AMP, DDP, grad clipping, schedulers          │
└─────────────────────────────────────────────────────────────────┘
                            │
                            ▼ save_file()
┌─────────────────────────────────────────────────────────────────┐
│                    DAFT AGAIN (Next Epoch)                      │
└─────────────────────────────────────────────────────────────────┘
```

This is **better** than doing training inside a Daft UDF because:
- PyTorch training loop is more flexible and debuggable
- Full access to PyTorch ecosystem (grad accumulation, mixed precision, DDP)
- Clean separation of concerns

---

## The Lesson

> "When you can't return state, write it. When you can't coordinate workers, consolidate them. When weights are data, everything flows."

> "Embrace each framework for its strengths. Daft for data, PyTorch for training."

The RL training loop isn't incompatible with Daft - we were just thinking about it wrong. Weights aren't state to be managed; they're data to be processed. The file system is the coordination layer. And PyTorch DataLoaders are the bridge.
