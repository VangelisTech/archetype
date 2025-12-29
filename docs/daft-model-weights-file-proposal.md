# Daft Feature Proposal: `daft.ModelWeightsFile`

**Authors:** Everett (Vangelis), lake-claude-opus-4.5
**Date:** 2025-12-27
**Status:** Proposal

---

## Summary

We propose a new Daft datatype `daft.ModelWeightsFile` (or `daft.Weights`) as a specialized subtype of `daft.File` optimized for machine learning model weights in safetensors format.

This emerged from our discovery that **RL training IS compatible with Daft when weights are treated as data, not state.**

---

## Motivation

### The Discovery

While building Archetype's RL training integration, we discovered that model weights can flow through Daft DataFrames as data:

```python
# Weights as a column
df = df.with_column("weights", file(lit("model.safetensors")))

# Different rows can have different weight versions
df = df.with_column("output", InferenceUDF()(col("input"), col("weights")))
```

This pattern enables:
- Per-row weight versioning (A/B testing, ensembles)
- Lazy weight loading (only load when UDF executes)
- Distributed inference with automatic weight distribution
- Training loops where weights "flow" through the DataFrame

### The Gap

Currently, users must manually handle:
- Content hashing for cache invalidation
- Temp file creation for path-based loaders
- Weight format detection (safetensors, PyTorch, GGUF)
- Sharded weight assembly
- Memory-mapped loading for large models

---

## Proposed API

### Basic Usage

```python
from daft.functions import weights  # or model_weights

# Load weights column
df = df.with_column("model_weights", weights(col("weights_path")))

# In UDF - specialized methods
@daft.cls
class InferenceUDF:
    def __init__(self):
        self._model = None
        self._weights_version = None

    def __call__(self, input: list, w: daft.ModelWeightsFile) -> float:
        # First-class methods on ModelWeightsFile
        if w.version != self._weights_version:
            state_dict = w.load_state_dict()  # Native safetensors support
            self._model.load_state_dict(state_dict)
            self._weights_version = w.version

        return self._model(input)
```

### Proposed Methods

```python
class ModelWeightsFile:
    """Specialized File type for ML model weights."""

    # === Identity & Versioning ===
    @property
    def version(self) -> str:
        """Content hash for cache invalidation."""
        ...

    @property
    def format(self) -> str:
        """Detected format: 'safetensors', 'pytorch', 'gguf', etc."""
        ...

    # === Loading ===
    def load_state_dict(self, device: str = "cpu") -> Dict[str, Tensor]:
        """Load as PyTorch state dict (safetensors native)."""
        ...

    def load_tensor(self, name: str) -> Tensor:
        """Load single tensor by name (memory efficient)."""
        ...

    def mmap(self) -> MappedWeights:
        """Memory-map for zero-copy access (large models)."""
        ...

    # === Metadata ===
    @property
    def num_parameters(self) -> int:
        """Total parameter count."""
        ...

    @property
    def tensor_names(self) -> List[str]:
        """List of tensor names in file."""
        ...

    @property
    def metadata(self) -> Dict[str, Any]:
        """Safetensors metadata dict."""
        ...

    # === Sharding (for large models) ===
    @property
    def is_sharded(self) -> bool:
        """Whether weights are sharded across files."""
        ...

    def shard_paths(self) -> List[str]:
        """Paths to all shards."""
        ...

    # === Context managers (inherited from File) ===
    def open(self) -> BinaryIO:
        """Raw file access."""
        ...

    def to_tempfile(self) -> ContextManager[NamedTemporaryFile]:
        """Temp file for path-based APIs."""
        ...
```

### DataFrame Functions

```python
from daft.functions import weights

# Create weights column from paths
df = df.with_column("w", weights(col("path")))

# Create from HuggingFace
df = df.with_column("w", weights.from_hf(col("model_id")))

# Create from S3/GCS with caching
df = df.with_column("w", weights(col("s3_path"), cache=True))

# Extract metadata as struct
df = df.with_column("meta", col("w").weights.metadata())
df = df.with_column("num_params", col("w").weights.num_parameters())
```

---

## Use Cases

### 1. Distributed Inference with Weight Versioning

```python
# A/B test different model versions
df = daft.from_pydict({
    "prompt": prompts,
    "weights_path": ["v1.safetensors", "v2.safetensors"] * (len(prompts) // 2)
})

df = df.with_column("weights", weights(col("weights_path")))
df = df.with_column("output", InferenceUDF()(col("prompt"), col("weights")))

# Daft distributes - workers load weights on demand
results = df.collect()
```

### 2. Training Loop with Weights as Data

```python
current_weights = "epoch_0.safetensors"

for epoch in range(num_epochs):
    df = prompts_df.with_column("weights", weights(lit(current_weights)))

    # All lazy until training boundary
    df = (df
        .with_column("generation", generate(col("prompt"), col("weights")))
        .with_column("reward", compute_reward(col("generation")))
        .with_column("advantage", compute_advantage(col("reward")))
        .into_partitions(1)
        .with_column("new_weights_path", train_step(col("*"), col("weights")))
    )

    result = df.select("new_weights_path").collect()
    current_weights = result.to_pylist()[0]["new_weights_path"]
```

### 3. Model Registry Integration

```python
# Load from HuggingFace Hub
df = df.with_column("weights", weights.from_hf("Qwen/Qwen2.5-7B-Instruct"))

# Load from MLflow
df = df.with_column("weights", weights.from_mlflow(col("run_id"), "model"))

# Load from Weights & Biases
df = df.with_column("weights", weights.from_wandb(col("artifact_path")))
```

### 4. Sharded Model Loading

```python
# Automatically handles model.safetensors.index.json
df = df.with_column(
    "weights",
    weights("path/to/sharded/model/", sharded=True)
)

# UDF receives assembled view
@daft.cls
class LargeModelUDF:
    def __call__(self, input, w: daft.ModelWeightsFile):
        # w.load_state_dict() assembles from shards automatically
        state_dict = w.load_state_dict()
```

---

## Implementation Considerations

### 1. Format Detection

```python
def detect_format(path: str) -> str:
    if path.endswith(".safetensors"):
        return "safetensors"
    elif path.endswith(".pt") or path.endswith(".pth"):
        return "pytorch"
    elif path.endswith(".gguf"):
        return "gguf"
    elif path.endswith(".bin"):
        return "pytorch"  # HF convention
    else:
        # Sniff magic bytes
        ...
```

### 2. Version/Hash Computation

```python
@property
def version(self) -> str:
    """Content-addressable version for cache invalidation."""
    # Option A: Full content hash (expensive for large files)
    # Option B: Metadata hash (fast but may miss weight changes)
    # Option C: Hybrid - hash file size + mtime + first N bytes
    ...
```

### 3. Memory-Mapped Access

```python
def mmap(self) -> MappedWeights:
    """Zero-copy access for large models."""
    from safetensors import safe_open
    return safe_open(self.path, framework="pt", device="cpu")
```

### 4. Distributed Caching

```python
# Workers cache weights locally after first load
df = df.with_column("weights", weights(col("s3_path"), cache=True))

# Cache key = content hash
# Cache location = worker-local disk or shared filesystem
```

---

## Relationship to Existing Daft Types

```
daft.File (base)
    │
    ├── daft.Image
    │
    ├── daft.Audio
    │
    └── daft.ModelWeightsFile (proposed)
            │
            ├── load_state_dict()
            ├── load_tensor(name)
            ├── mmap()
            ├── version
            ├── format
            └── metadata
```

---

## Why This Matters

1. **ML Training in Daft**: This is the missing piece that makes Daft a first-class ML training framework, not just data processing.

2. **Weights as Data**: Formalizes the pattern we discovered - weights flow through DataFrames like any other data type.

3. **Distributed Training**: Workers can load weights independently, enabling true distributed inference and training.

4. **Ecosystem Integration**: Native HuggingFace, MLflow, W&B support would make Daft the obvious choice for ML pipelines.

---

## Next Steps

1. **Prototype**: Build `ModelWeightsFile` as a Python class wrapping `daft.File`
2. **Validate**: Test with real training loops (our Archetype RL integration)
3. **Propose**: RFC to Daft maintainers
4. **Implement**: Native Rust implementation for performance

---

## Appendix: Validated Pattern (from Archetype RL)

```python
@daft.cls
class InferenceUDF:
    def __init__(self, model: nn.Module):
        self._model = model
        self._weights_version = None

    def __call__(self, input_data: list, weights_file: daft.File) -> float:
        # Manual version tracking (would be automatic with ModelWeightsFile)
        with weights_file.open() as f:
            content_hash = hash(f.read())

        if self._weights_version != content_hash:
            with weights_file.to_tempfile() as temp:
                state_dict = load_file(temp.name)
            self._model.load_state_dict(state_dict)
            self._weights_version = content_hash

        return self._model(input_data).item()
```

With `ModelWeightsFile`:

```python
@daft.cls
class InferenceUDF:
    def __init__(self, model: nn.Module):
        self._model = model
        self._weights_version = None

    def __call__(self, input_data: list, w: daft.ModelWeightsFile) -> float:
        if self._weights_version != w.version:  # ← Built-in versioning
            self._model.load_state_dict(w.load_state_dict())  # ← Native loading
            self._weights_version = w.version

        return self._model(input_data).item()
```

---

*This proposal emerged from building Archetype's RL training integration and discovering that the "weights as data" pattern is the key to making ML training compatible with Daft's lazy evaluation model.*
