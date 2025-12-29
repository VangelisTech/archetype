"""
End-to-end GRPO (text-only) demo using Daft UDFs:
- vLLM offline rollouts in @daft.cls
- PyTorch GRPO training in @daft.cls

This is a template for your multimodal setup: swap the rollout stage to your
`vllm-prefix-caching` prompt provider once it can emit token IDs + logprobs.
"""

from __future__ import annotations

import sys
from pathlib import Path

_SRC = Path(__file__).resolve().parents[1] / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

import daft  # type: ignore[import-not-found]
import torch
from daft import col  # type: ignore[import-not-found]
from daft.functions import file  # type: ignore[import-not-found]
from safetensors.torch import save_file

from archetype.rl.grpo.pipeline import RolloutConfig, build_grpo_rollouts_from_mc_dataset
from archetype.rl.grpo.train_udf import PytorchGrpoTrainer, TrainConfig


def main() -> None:
    # Synthetic MC dataset: prompts that are answerable from text alone (for smoke-test).
    df = daft.from_pydict(
        {
            "question": [
                "Which letter comes first in the alphabet?\nA) A\nB) B\nC) C\nD) D",
                "Which letter comes second in the alphabet?\nA) A\nB) B\nC) C\nD) D",
            ],
            "answer_choice": ["A", "B"],
        }
    )

    # Tiny model so this runs fast locally.
    base_model = "sshleifer/tiny-gpt2"

    # Create an initial checkpoint (weights-as-data) for the demo.
    # In real usage, this is your starting checkpoint (possibly merged LoRA).
    ckpt_path = "./.data/grpo_demo/init.safetensors"
    Path("./.data/grpo_demo").mkdir(parents=True, exist_ok=True)
    # Write a real checkpoint so streamer + strict loaders can work.
    from transformers import AutoModelForCausalLM

    init_model = AutoModelForCausalLM.from_pretrained(base_model, torch_dtype=torch.float32)
    # Some HF models tie weights (shared storage). Safetensors requires de-duplicated tensors.
    save_file({k: v.detach().cpu().clone() for k, v in init_model.state_dict().items()}, ckpt_path)
    del init_model

    # Rollouts
    rollouts = build_grpo_rollouts_from_mc_dataset(
        df,
        cfg=RolloutConfig(
            model=base_model, num_samples_per_prompt=4, temperature=0.7, max_tokens=4
        ),
    )

    # Train (single worker)
    trainer = PytorchGrpoTrainer(model_name=base_model, config=TrainConfig(lr=1e-5))
    out_path = "./.data/grpo_demo/epoch_1.safetensors"
    rollouts = rollouts.with_column("weights_path", daft.lit(ckpt_path))
    rollouts = rollouts.with_column("weights_file", file(daft.lit(ckpt_path)))
    rollouts = rollouts.with_column("output_path", daft.lit(out_path))

    metrics = (
        rollouts.into_partitions(1)
        .with_column(
            "metrics",
            trainer.train_batch(
                col("prompt"),
                col("completion_token_ids"),
                col("old_token_logprobs"),
                col("advantage"),
                col("weights_path"),
                col("weights_file"),
                col("output_path"),
            ),
        )
        .select(
            col("metrics")["loss"].alias("loss"),
            col("metrics")["weights_path"].alias("weights_path"),
        )
    )

    metrics.show()


if __name__ == "__main__":
    main()
