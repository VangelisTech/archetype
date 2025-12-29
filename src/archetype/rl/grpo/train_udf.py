# Copyright 2025 Vangelis Technologies Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
PyTorch GRPO training as a Daft class-UDF.

This follows the "weights as data" pattern:
- weights_file: daft.File passed in per row (all rows same file)
- output_path: where to write a new safetensors checkpoint

The UDF runs batch-wise; for safety we typically call it after `repartition(1)`
so exactly one worker performs the optimizer step + writes the checkpoint.
"""

from __future__ import annotations

import math
from dataclasses import dataclass

import daft  # type: ignore[import-not-found]
from daft import DataType, Series  # type: ignore[import-not-found]

_METRICS_DTYPE = DataType.struct(
    {
        "loss": DataType.float64(),
        "policy_loss": DataType.float64(),
        "approx_kl": DataType.float64(),
        "clip_fraction": DataType.float64(),
        "weights_path": DataType.string(),
    }
)


@dataclass(frozen=True)
class TrainConfig:
    lr: float = 1e-5
    clip_epsilon: float = 0.2
    kl_coeff: float = 0.0
    max_grad_norm: float = 1.0
    use_runai_streamer: bool = True
    runai_distributed: bool = False


@daft.cls()
class PytorchGrpoTrainer:
    """
    Minimal GRPO trainer UDF.

    Notes:
    - This is text-only (causal LM). For VLMs, the forward pass must include image features.
    - For GRPO correctness, you must pass token IDs and old logprobs from the rollout engine.
    """

    def __init__(self, *, model_name: str, config: TrainConfig | None = None):
        import torch
        from transformers import AutoModelForCausalLM, AutoTokenizer

        self._torch = torch
        self._cfg = config or TrainConfig()

        self._tokenizer = AutoTokenizer.from_pretrained(model_name, use_fast=True)
        if self._tokenizer.pad_token_id is None:
            # Make padding explicit for batch collation.
            self._tokenizer.pad_token = self._tokenizer.eos_token

        self._model = AutoModelForCausalLM.from_pretrained(
            model_name,
            torch_dtype=torch.bfloat16 if torch.cuda.is_available() else torch.float32,
        )
        self._device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self._model.to(self._device)
        self._model.train()
        self._optim = torch.optim.AdamW(self._model.parameters(), lr=self._cfg.lr)

        # Cache invalidation: we expect weights_path to change per epoch.
        self._loaded_weights_path: str | None = None

    def _maybe_load_weights(self, *, weights_path: str, weights_file_obj) -> None:
        # weights_file_obj is a daft.File
        from safetensors.torch import load_file

        if self._loaded_weights_path == weights_path:
            return

        with weights_file_obj.to_tempfile() as tmp:
            if self._cfg.use_runai_streamer:
                try:
                    from archetype.rl.grpo.runai_streamer import (
                        is_available,
                        load_safetensors_with_streamer,
                    )

                    if is_available():
                        state = load_safetensors_with_streamer(
                            file_path=tmp.name,
                            device=str(self._device),
                            is_distributed=self._cfg.runai_distributed,
                            clone_tensors=True,
                        )
                    else:
                        state = load_file(tmp.name)
                except Exception:
                    # Fail closed to the safe (standard) load path.
                    state = load_file(tmp.name)
            else:
                state = load_file(tmp.name)
        self._model.load_state_dict(state, strict=False)
        self._loaded_weights_path = weights_path

    @daft.method.batch(return_dtype=_METRICS_DTYPE)
    def train_batch(
        self,
        prompts: Series,
        completion_token_ids: Series,
        old_token_logprobs: Series,
        advantages: Series,
        weights_path: Series,
        weights_file: Series,
        output_path: Series,
    ) -> Series:
        """
        Train on a batch and write a new safetensors checkpoint.

        Required shapes (Python lists):
        - prompts: list[str]
        - completion_token_ids: list[list[int]]
        - old_token_logprobs: list[list[float]] (same length as completion_token_ids)
        - advantages: list[float]
        - weights_file: list[daft.File] (usually 1 unique value)
        - output_path: list[str] (usually 1 unique value)
        """
        import torch
        from safetensors.torch import save_file

        # Daft may call batch UDFs with empty batches; treat as a no-op.
        if len(prompts) == 0:
            return Series.from_pylist([])

        # Load weights if changed.
        wf_list = weights_file.to_pylist()
        wp_list = weights_path.to_pylist()
        out_list = output_path.to_pylist()
        if not wf_list or not wp_list or not out_list:
            return Series.from_pylist([])

        wf = wf_list[0]
        wp = str(wp_list[0])
        self._maybe_load_weights(weights_path=wp, weights_file_obj=wf)

        out_path = str(out_list[0])

        prompt_texts = prompts.to_pylist()
        comp_ids = completion_token_ids.to_pylist()
        old_lps = old_token_logprobs.to_pylist()
        adv = advantages.to_pylist()

        # Tokenize prompts in-training (ok) but we MUST use the rollout's completion token IDs.
        # This still avoids retokenizing the completion tokens.
        prompt_enc = self._tokenizer(
            prompt_texts,
            return_tensors="pt",
            padding=True,
            truncation=False,
            add_special_tokens=True,
        )
        prompt_input_ids = prompt_enc["input_ids"].to(self._device)
        prompt_attn = prompt_enc["attention_mask"].to(self._device)

        # Build full sequences: [prompt_tokens | completion_tokens]
        # We pad to the max length across the batch.
        batch_size = len(prompt_texts)
        prompt_lens = prompt_attn.sum(dim=1).to(torch.int64).tolist()
        comp_lens = [len(x) for x in comp_ids]

        max_full_len = max(pl + cl for pl, cl in zip(prompt_lens, comp_lens, strict=False))
        pad_id = int(self._tokenizer.pad_token_id)

        full_ids = torch.full(
            (batch_size, max_full_len), pad_id, dtype=torch.long, device=self._device
        )
        full_attn = torch.zeros((batch_size, max_full_len), dtype=torch.long, device=self._device)
        action_mask = torch.zeros((batch_size, max_full_len), dtype=torch.bool, device=self._device)

        # Also align old_token_logprobs into a tensor [B, max_full_len]
        old_lp = torch.full(
            (batch_size, max_full_len), math.nan, dtype=torch.float32, device=self._device
        )

        for i in range(batch_size):
            pl = int(prompt_lens[i])
            cl = int(comp_lens[i])

            # Prompt tokens are left-aligned in prompt_input_ids; take first pl.
            p_ids = prompt_input_ids[i, :pl]
            c_ids = torch.tensor(comp_ids[i], dtype=torch.long, device=self._device)

            full_ids[i, :pl] = p_ids
            full_ids[i, pl : pl + cl] = c_ids
            full_attn[i, : pl + cl] = 1

            # Action tokens: completion region
            action_mask[i, pl : pl + cl] = True

            # Old logprobs aligned to completion token positions
            lp = torch.tensor(old_lps[i], dtype=torch.float32, device=self._device)
            old_lp[i, pl : pl + cl] = lp

        # Forward pass
        out = self._model(input_ids=full_ids, attention_mask=full_attn)
        logits = out.logits  # [B, T, V]

        # Logprobs for observed tokens: need token probs for token at position t given logits at t-1
        # Shift: logits[:, :-1] predicts full_ids[:, 1:]
        log_probs = torch.log_softmax(logits[:, :-1, :], dim=-1)
        target = full_ids[:, 1:]
        new_lp = log_probs.gather(dim=-1, index=target.unsqueeze(-1)).squeeze(-1)  # [B, T-1]

        # Shift masks to match [B, T-1]
        act_mask = action_mask[:, 1:]
        old_lp_shift = old_lp[:, 1:]

        # Broadcast advantages
        adv_t = torch.tensor(adv, dtype=torch.float32, device=self._device).unsqueeze(-1)  # [B, 1]

        # GRPO / PPO-style clipped objective
        ratio = torch.exp(new_lp - old_lp_shift)
        clipped = torch.clamp(ratio, 1.0 - self._cfg.clip_epsilon, 1.0 + self._cfg.clip_epsilon)

        obj = torch.minimum(ratio * adv_t, clipped * adv_t)
        eps = 1e-8
        policy_loss = -(obj * act_mask).sum() / (act_mask.sum() + eps)

        approx_kl = ((old_lp_shift - new_lp) * act_mask).sum() / (act_mask.sum() + eps)
        loss = policy_loss + self._cfg.kl_coeff * approx_kl

        self._optim.zero_grad()
        loss.backward()
        torch.nn.utils.clip_grad_norm_(self._model.parameters(), max_norm=self._cfg.max_grad_norm)
        self._optim.step()

        with torch.no_grad():
            clip_fraction = (((ratio - 1.0).abs() > self._cfg.clip_epsilon) * act_mask).sum() / (
                act_mask.sum() + eps
            )

        # Write new weights (weights-as-data)
        # Some HF models tie weights (shared storage). Safetensors requires de-duplicated tensors.
        state = {k: v.detach().cpu().clone() for k, v in self._model.state_dict().items()}
        save_file(state, out_path)

        metrics = {
            "loss": float(loss.detach().cpu().item()),
            "policy_loss": float(policy_loss.detach().cpu().item()),
            "approx_kl": float(approx_kl.detach().cpu().item()),
            "clip_fraction": float(clip_fraction.detach().cpu().item()),
            "weights_path": out_path,
        }

        return Series.from_pylist([metrics] * batch_size)
