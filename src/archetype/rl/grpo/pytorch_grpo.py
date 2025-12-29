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
PyTorch GRPO Step (Training-only)
=================================

This is the training-side counterpart to vLLM (or prompt-provider) rollouts.

Inputs you must have per sample:
- `completion_token_ids` (or full sequence ids and an action mask)
- `old_token_logprobs` (from rollouts)
- `advantages` (computed from rewards, e.g. group-relative)

This module intentionally stays small and explicit: one obvious way.
"""

from __future__ import annotations

from dataclasses import dataclass

import torch
import torch.nn.functional as F


@dataclass(frozen=True)
class GrpoLossConfig:
    clip_epsilon: float = 0.2
    kl_coeff: float = 0.0


def _gather_token_logprobs(logits: torch.Tensor, token_ids: torch.Tensor) -> torch.Tensor:
    """
    logits: [B, T, V]
    token_ids: [B, T]

    Returns: logprobs of the observed tokens at each position: [B, T]
    """
    log_probs = F.log_softmax(logits, dim=-1)
    return log_probs.gather(dim=-1, index=token_ids.unsqueeze(-1)).squeeze(-1)


def compute_grpo_loss(
    *,
    new_token_logprobs: torch.Tensor,
    old_token_logprobs: torch.Tensor,
    advantages: torch.Tensor,
    action_mask: torch.Tensor,
    cfg: GrpoLossConfig | None = None,
) -> tuple[torch.Tensor, dict[str, float]]:
    """
    PPO-style clipped policy gradient over tokens (GRPO-style).

    Shapes:
    - new_token_logprobs: [B, T]
    - old_token_logprobs: [B, T]
    - advantages: [B] or [B, 1]
    - action_mask: [B, T] boolean (True where tokens are part of the sampled action)
    """
    cfg = cfg or GrpoLossConfig()
    if advantages.ndim == 1:
        advantages = advantages.unsqueeze(-1)  # [B, 1]

    # Apply mask
    mask = action_mask.to(dtype=new_token_logprobs.dtype)
    eps = 1e-8

    # Ratio per token
    ratio = torch.exp(new_token_logprobs - old_token_logprobs)

    # Broadcast advantages over time
    adv = advantages.expand_as(new_token_logprobs)

    unclipped = ratio * adv
    clipped = torch.clamp(ratio, 1.0 - cfg.clip_epsilon, 1.0 + cfg.clip_epsilon) * adv

    # Maximize objective => minimize negative
    per_token_obj = torch.minimum(unclipped, clipped)
    policy_loss = -(per_token_obj * mask).sum() / (mask.sum() + eps)

    # Optional KL penalty (tokenwise approximation)
    approx_kl = ((old_token_logprobs - new_token_logprobs) * mask).sum() / (mask.sum() + eps)
    loss = policy_loss + cfg.kl_coeff * approx_kl

    with torch.no_grad():
        clip_fraction = (((ratio - 1.0).abs() > cfg.clip_epsilon).to(mask.dtype) * mask).sum() / (
            mask.sum() + eps
        )

    metrics = {
        "loss": float(loss.detach().cpu().item()),
        "policy_loss": float(policy_loss.detach().cpu().item()),
        "approx_kl": float(approx_kl.detach().cpu().item()),
        "clip_fraction": float(clip_fraction.detach().cpu().item()),
    }
    return loss, metrics


def forward_logprobs_for_causal_lm(
    *,
    model: torch.nn.Module,
    input_ids: torch.Tensor,
    attention_mask: torch.Tensor,
    target_token_ids: torch.Tensor,
) -> torch.Tensor:
    """
    Convenience helper: run a causal LM and return token logprobs for target tokens.

    This assumes the model returns `.logits` shaped [B, T, V].
    """
    out = model(input_ids=input_ids, attention_mask=attention_mask)
    logits = out.logits
    return _gather_token_logprobs(logits, target_token_ids)
