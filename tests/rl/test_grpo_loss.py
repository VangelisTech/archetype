# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import torch


def test_compute_grpo_loss_smoke_and_grad() -> None:
    from archetype.rl.grpo.pytorch_grpo import GrpoLossConfig, compute_grpo_loss

    torch.manual_seed(0)
    b, t = 4, 7
    new_lp = torch.randn(b, t, requires_grad=True)
    old_lp = torch.randn(b, t)
    adv = torch.randn(b)
    mask = torch.zeros(b, t, dtype=torch.bool)
    mask[:, 2:] = True  # only last tokens are “action”

    loss, metrics = compute_grpo_loss(
        new_token_logprobs=new_lp,
        old_token_logprobs=old_lp,
        advantages=adv,
        action_mask=mask,
        cfg=GrpoLossConfig(clip_epsilon=0.2, kl_coeff=0.1),
    )

    assert loss.ndim == 0
    assert set(metrics.keys()) == {"loss", "policy_loss", "approx_kl", "clip_fraction"}
    assert 0.0 <= metrics["clip_fraction"] <= 1.0

    loss.backward()
    assert new_lp.grad is not None
