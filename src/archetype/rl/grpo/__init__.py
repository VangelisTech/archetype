# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
GRPO (Group Relative Policy Optimization)
=========================================

Pure PyTorch implementation of the GRPO training step.
Rollouts are handled by daft.functions.prompt with appropriate providers.

Usage:
    from archetype.rl.grpo import compute_grpo_loss, GrpoLossConfig
    
    loss, metrics = compute_grpo_loss(
        new_token_logprobs=new_logprobs,
        old_token_logprobs=old_logprobs,
        advantages=advantages,
        action_mask=mask,
        cfg=GrpoLossConfig(clip_epsilon=0.2),
    )
"""

from archetype.rl.grpo.pytorch_grpo import (
    GrpoLossConfig,
    compute_grpo_loss,
    forward_logprobs_for_causal_lm,
)

__all__ = [
    "GrpoLossConfig",
    "compute_grpo_loss",
    "forward_logprobs_for_causal_lm",
]
