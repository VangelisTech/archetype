"""
Hypothesis Validation: Weights as Data
======================================

Can we:
1. Load model weights via daft.File?
2. Use weights in a @daft.cls UDF?
3. Train and WRITE new weights from a @daft.func.batch UDF?
4. Chain epochs where each uses the previous epoch's weights?

Let's find out.
"""

from __future__ import annotations

import logging
import os
import shutil
import tempfile
from pathlib import Path

import daft
import numpy as np
import torch
import torch.nn as nn
from daft import DataType, Series, col, lit
from daft.functions import file
from safetensors.torch import load_file, save_file

logger = logging.getLogger(__name__)

# =============================================================================
# Step 1: Create a simple model and save initial weights
# =============================================================================


class TinyModel(nn.Module):
    """A tiny model we can actually train and test."""

    def __init__(self, input_dim: int = 10, hidden_dim: int = 32, output_dim: int = 1):
        super().__init__()
        self.fc1 = nn.Linear(input_dim, hidden_dim)
        self.fc2 = nn.Linear(hidden_dim, output_dim)

    def forward(self, x):
        x = torch.relu(self.fc1(x))
        return self.fc2(x)


def create_initial_weights(path: str) -> str:
    """Create and save initial model weights."""
    model = TinyModel()
    state_dict = {k: v for k, v in model.state_dict().items()}
    save_file(state_dict, path)
    logger.info("Created initial weights at %s", path)
    return path


# =============================================================================
# Step 2: Test loading weights via daft.File
# =============================================================================


@daft.func(
    return_dtype=DataType.struct(
        {
            "num_params": DataType.int64(),
            "fc1_weight_sum": DataType.float64(),
        }
    )
)
def inspect_weights(weights_file: daft.File) -> dict:
    """
    Load safetensors via daft.File and inspect.

    daft.File provides two context managers:

    1. file.open() → file-like object for reading bytes
       with file.open() as f:
           content = f.read()

    2. file.to_tempfile() → temp file path (cleaner for safetensors!)
       with file.to_tempfile() as temp_path:
           state_dict = load_file(temp_path)

    Using to_tempfile() here since safetensors.load_file() expects a path.
    """
    # to_tempfile() returns a file wrapper, use .name for path
    with weights_file.to_tempfile() as temp:
        state_dict = load_file(temp.name)

    num_params = sum(p.numel() for p in state_dict.values())
    fc1_sum = state_dict["fc1.weight"].sum().item()

    return {
        "num_params": num_params,
        "fc1_weight_sum": fc1_sum,
    }


def test_load_weights(weights_path: str) -> bool:
    """Test: Can we load safetensors via daft.File?"""
    logger.info("TEST 1: Load weights via daft.File")

    df = daft.from_pydict({"weights_path": [weights_path]})
    df = df.with_column("weights_file", file(col("weights_path")))
    df = df.with_column("info", inspect_weights(col("weights_file")))

    result = df.collect().to_pylist()[0]
    logger.info("Loaded weights via daft.File")
    logger.info("num_params=%s", result["info"]["num_params"])
    logger.info("fc1_weight_sum=%.4f", result["info"]["fc1_weight_sum"])

    return True


# =============================================================================
# Step 3: Test inference UDF that uses weights from daft.File
# =============================================================================


@daft.cls
class InferenceUDF:
    """
    Stateful inference UDF that loads weights from daft.File.

    Uses file.to_tempfile() context manager for clean safetensors loading.
    """

    def __init__(self):
        self._model = None
        self._current_content_hash = None

    def __call__(self, input_data: list, weights_file: daft.File) -> float:
        # Read content to detect changes (need hash for cache invalidation)
        with weights_file.open() as f:
            content = f.read()
        content_hash = hash(content)

        # Reload model if weights changed
        if self._current_content_hash != content_hash:
            # Use to_tempfile() for clean path-based loading
            with weights_file.to_tempfile() as temp:
                state_dict = load_file(temp.name)
            self._model = TinyModel()
            self._model.load_state_dict(state_dict)
            self._model.eval()
            self._current_content_hash = content_hash
            logger.debug("InferenceUDF loaded new weights (hash=%s)", content_hash)

        # Run inference
        with torch.no_grad():
            x = torch.tensor(input_data, dtype=torch.float32)
            output = self._model(x)

        return output.item()


def test_inference_with_weights(weights_path: str) -> bool:
    """Test: Can we run inference with weights from daft.File?"""
    logger.info("TEST 2: Inference with daft.File weights")

    # Create test data
    df = daft.from_pydict(
        {
            "id": [1, 2, 3],
            "input": [[1.0] * 10, [2.0] * 10, [3.0] * 10],
            "weights_path": [weights_path] * 3,
        }
    )

    df = df.with_column("weights_file", file(col("weights_path")))
    df = df.with_column("output", InferenceUDF()(col("input"), col("weights_file")))

    results = df.select("id", "output").collect().to_pylist()
    logger.info("Inference completed with daft.File weights")
    for r in results:
        logger.info("id=%s output=%.4f", r["id"], r["output"])

    return True


# =============================================================================
# Step 4: Test training UDF with @daft.cls + @daft.method.batch
# =============================================================================


@daft.cls
class TrainingUDF:
    """
    Stateful training UDF using @daft.cls + @daft.method.batch.

    Key patterns:
    - __init__ runs ONCE per worker (model loaded once)
    - Model state PERSISTS across batches on same worker
    - file.to_tempfile() for clean safetensors loading
    - file.open() for content hashing (cache invalidation)
    """

    def __init__(self):
        # Model loaded ONCE, persists across batches
        self._model = None
        self._optimizer = None
        self._criterion = nn.MSELoss()
        self._loaded_weights_hash = None
        logger.debug("TrainingUDF initialized on worker")

    def _ensure_model_loaded(self, weights_file_obj):
        """Load weights if not already loaded or if content changed."""
        # Hash content to detect changes
        with weights_file_obj.open() as f:
            content_hash = hash(f.read())

        if self._loaded_weights_hash != content_hash:
            # Use to_tempfile() for clean path-based loading
            with weights_file_obj.to_tempfile() as temp:
                state_dict = load_file(temp.name)
            self._model = TinyModel()
            self._model.load_state_dict(state_dict)
            self._model.train()
            self._optimizer = torch.optim.SGD(self._model.parameters(), lr=0.1)
            self._loaded_weights_hash = content_hash
            logger.debug("TrainingUDF loaded weights (hash=%s)", content_hash)

    @daft.method.batch(return_dtype=DataType.string())
    def train_batch(
        self,
        inputs: Series,
        targets: Series,
        weights_file: Series,
        output_path: Series,
    ) -> Series:
        """
        Train on a batch and write new weights.

        With repartition(1) on RayRunner, this processes ALL data in one call.
        """
        # Get data from Series
        input_list = inputs.to_pylist()
        target_list = targets.to_pylist()
        new_weights_path = output_path.to_pylist()[0]

        # Ensure model is loaded with current weights
        weights_file_obj = weights_file.to_pylist()[0]
        self._ensure_model_loaded(weights_file_obj)

        # Record initial state
        initial_fc1_sum = self._model.fc1.weight.data.sum().item()

        # Training loop
        total_loss = 0.0
        for inp, tgt in zip(input_list, target_list, strict=False):
            x = torch.tensor(inp, dtype=torch.float32)
            y = torch.tensor([tgt], dtype=torch.float32)

            self._optimizer.zero_grad()
            pred = self._model(x)
            loss = self._criterion(pred, y)
            loss.backward()
            self._optimizer.step()
            total_loss += loss.item()

        # Record final state
        final_fc1_sum = self._model.fc1.weight.data.sum().item()

        logger.info(
            "TRAIN batch_size=%s loss=%.4f fc1_sum: %.4f -> %.4f",
            len(input_list),
            total_loss / len(input_list),
            initial_fc1_sum,
            final_fc1_sum,
        )

        # WRITE new weights to output path
        new_state_dict = {k: v for k, v in self._model.state_dict().items()}
        save_file(new_state_dict, new_weights_path)
        logger.info("Saved new weights to %s", new_weights_path)

        # Return path to new weights for each row
        return Series.from_pylist([new_weights_path] * len(input_list))


# Legacy wrapper for compatibility
def train_and_save(inputs, targets, weights_file, output_path):
    """Wrapper that uses TrainingUDF."""
    return TrainingUDF().train_batch(inputs, targets, weights_file, output_path)


def test_train_and_save(weights_path: str, output_path: str) -> str:
    """Test: Can we train and write new weights from a UDF?"""
    logger.info("TEST 3: Train and save weights via @daft.cls + @daft.method.batch")

    # Create training data
    df = daft.from_pydict(
        {
            "inputs": [[1.0] * 10, [2.0] * 10, [3.0] * 10, [4.0] * 10],
            "targets": [1.0, 2.0, 3.0, 4.0],
            "weights_path": [weights_path] * 4,
            "output_path": [output_path] * 4,
        }
    )

    df = df.with_column("weights_file", file(col("weights_path")))

    # KEY: repartition(1) ensures single worker for training
    # This prevents race conditions when writing weights
    trainer = TrainingUDF()
    df = df.repartition(1).with_column(
        "new_weights_path",
        trainer.train_batch(
            col("inputs"),
            col("targets"),
            col("weights_file"),
            col("output_path"),
        ),
    )

    result = df.select("new_weights_path").collect().to_pylist()[0]
    logger.info("Training completed, new weights at: %s", result["new_weights_path"])

    return result["new_weights_path"]


# =============================================================================
# Step 5: Test full epoch chain - weights flow through!
# =============================================================================


def test_epoch_chain(initial_weights: str, checkpoint_dir: str, num_epochs: int = 3) -> bool:
    """
    Test: Can weights flow through multiple epochs?

    Each epoch:
    1. Uses weights from previous epoch
    2. Trains
    3. Writes new weights
    4. Next epoch uses those weights
    """
    logger.info("TEST 4: Full epoch chain (%s epochs)", num_epochs)

    current_weights = initial_weights

    # Track weight changes across epochs
    weight_history = []

    for epoch in range(num_epochs):
        logger.info("--- Epoch %s ---", epoch)

        # Create data for this epoch
        df = daft.from_pydict(
            {
                "inputs": [[float(i)] * 10 for i in range(1, 9)],  # 8 samples
                "targets": [float(i) * 0.5 for i in range(1, 9)],
            }
        )

        # Add current weights
        df = df.with_column("weights_path", lit(current_weights))
        df = df.with_column("weights_file", file(col("weights_path")))

        # Prepare output path for this epoch
        new_weights_path = f"{checkpoint_dir}/epoch_{epoch + 1}.safetensors"
        df = df.with_column("output_path", lit(new_weights_path))

        # Train and save - repartition(1) for single-worker training
        trainer = TrainingUDF()
        df = df.repartition(1).with_column(
            "new_weights",
            trainer.train_batch(
                col("inputs"),
                col("targets"),
                col("weights_file"),
                col("output_path"),
            ),
        )

        # Materialize
        df.collect()

        # Verify weights actually changed
        old_state = load_file(current_weights)
        new_state = load_file(new_weights_path)

        old_sum = old_state["fc1.weight"].sum().item()
        new_sum = new_state["fc1.weight"].sum().item()

        weight_history.append(
            {
                "epoch": epoch,
                "weights_path": new_weights_path,
                "fc1_sum": new_sum,
            }
        )

        logger.info("Weights changed: %.4f -> %.4f", old_sum, new_sum)

        # Update for next epoch
        current_weights = new_weights_path

    logger.info("Weight evolution:")
    for h in weight_history:
        logger.info("Epoch %s fc1_sum=%.4f", h["epoch"], h["fc1_sum"])

    # Verify weights actually evolved
    first_sum = weight_history[0]["fc1_sum"]
    last_sum = weight_history[-1]["fc1_sum"]

    if abs(first_sum - last_sum) > 0.01:
        logger.info("SUCCESS: Weights evolved from %.4f to %.4f", first_sum, last_sum)
        return True
    else:
        logger.error("FAILURE: Weights did not change significantly")
        return False


# =============================================================================
# Step 6: Test inference with epoch-versioned weights
# =============================================================================


def test_inference_with_versioned_weights(checkpoint_dir: str) -> bool:
    """
    Test: Can different rows use different weight versions?

    This is the ultimate validation - per-row weight versioning!
    """
    logger.info("TEST 5: Inference with different weight versions per row")

    # Find all checkpoints
    checkpoints = sorted(Path(checkpoint_dir).glob("epoch_*.safetensors"))

    if len(checkpoints) < 2:
        logger.warning("Need at least 2 checkpoints for this test")
        return False

    # Create data where each row uses different weights
    df = daft.from_pydict(
        {
            "id": list(range(len(checkpoints))),
            "input": [[5.0] * 10] * len(checkpoints),  # Same input
            "weights_path": [str(cp) for cp in checkpoints],  # Different weights!
        }
    )

    df = df.with_column("weights_file", file(col("weights_path")))
    df = df.with_column("output", InferenceUDF()(col("input"), col("weights_file")))

    results = df.select("id", "weights_path", "output").collect().to_pylist()

    logger.info("Results with different weight versions:")
    outputs = []
    for r in results:
        epoch = Path(r["weights_path"]).stem
        logger.info("%s output=%.4f", epoch, r["output"])
        outputs.append(r["output"])

    # Verify outputs differ (different weights → different outputs)
    if len(set(f"{o:.4f}" for o in outputs)) > 1:
        logger.info("SUCCESS: Different weights produced different outputs")
        return True
    else:
        logger.error("FAILURE: All outputs were identical despite different weights")
        return False


# =============================================================================
# Main: Run all tests
# =============================================================================


def main():
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    logger.info("HYPOTHESIS VALIDATION: Weights as Data")

    # Create temp directory for test
    test_dir = tempfile.mkdtemp(prefix="weights_test_")
    checkpoint_dir = os.path.join(test_dir, "checkpoints")
    os.makedirs(checkpoint_dir)

    logger.info("Test directory: %s", test_dir)

    try:
        # Create initial weights
        initial_weights = os.path.join(test_dir, "initial.safetensors")
        create_initial_weights(initial_weights)

        # Run tests
        results = []

        results.append(("Load weights via daft.File", test_load_weights(initial_weights)))

        results.append(
            ("Inference with daft.File weights", test_inference_with_weights(initial_weights))
        )

        epoch1_weights = os.path.join(checkpoint_dir, "epoch_1.safetensors")
        test_train_and_save(initial_weights, epoch1_weights)
        results.append(("Train and save weights via UDF", os.path.exists(epoch1_weights)))

        results.append(
            ("Full epoch chain", test_epoch_chain(initial_weights, checkpoint_dir, num_epochs=3))
        )

        results.append(
            (
                "Inference with versioned weights",
                test_inference_with_versioned_weights(checkpoint_dir),
            )
        )

        # Summary
        logger.info("RESULTS SUMMARY")

        all_passed = True
        for name, passed in results:
            status = "✓ PASS" if passed else "✗ FAIL"
            logger.info("%s: %s", status, name)
            all_passed = all_passed and passed

        if all_passed:
            logger.info("HYPOTHESIS VALIDATED: Weights can flow as data!")
            logger.info(
                """
Key findings:
1. daft.File can load safetensors ✓
2. @daft.cls UDFs can use weights from file columns ✓
3. @daft.func.batch can train and write new weights ✓
4. Weights can flow through epochs via file paths ✓
5. Different rows can use different weight versions ✓

The RL training loop IS compatible with Daft when weights are data!
"""
            )
        else:
            logger.error("HYPOTHESIS NOT FULLY VALIDATED")

        return all_passed

    finally:
        # Cleanup
        logger.info("Cleaning up %s", test_dir)
        shutil.rmtree(test_dir)


if __name__ == "__main__":
    success = main()
    raise SystemExit(0 if success else 1)
