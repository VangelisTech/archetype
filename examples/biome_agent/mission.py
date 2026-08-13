# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Closed-loop planning and verification for the live Biome mission."""

from __future__ import annotations

import time
from collections.abc import Callable

from .client import BiomeClient, FlecsRemoteError
from .contracts import (
    ExtractionGoal,
    MissionPlan,
    MissionSample,
    MissionTrace,
)
from .policy import GoalDirectedDrillPolicy


def plan_mission(
    client: BiomeClient,
    policy: GoalDirectedDrillPolicy,
    goal: ExtractionGoal,
) -> MissionPlan:
    observation = client.observe()
    return MissionPlan(
        goal=goal,
        observation=observation,
        action=policy.choose(goal, observation),
    )


def monitor_mission(
    client: BiomeClient,
    plan: MissionPlan,
    *,
    timeout: float = 15.0,
    poll_interval: float = 0.25,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> MissionTrace:
    """Poll native state until Biome proves the requested extraction."""

    if timeout <= 0:
        raise ValueError("timeout must be positive")
    if poll_interval < 0:
        raise ValueError("poll_interval must not be negative")

    start = clock()
    initial_amount = plan.target.amount
    samples: list[MissionSample] = []
    last_reason = "waiting for the Drill to become powered and mine its target"

    while True:
        elapsed = clock() - start
        try:
            deposit = client.get_deposit(plan.action.target_path)
            drill = client.get_drill(plan.action.drill_path, plan.action.resource)
            extracted = max(0, initial_amount - deposit.amount)
            samples.append(
                MissionSample(
                    elapsed_seconds=elapsed,
                    deposit_amount=deposit.amount,
                    extracted=extracted,
                    drill=drill,
                )
            )
            bound_to_target = drill.deposit_path == plan.action.target_path
            stored = drill.stored_amount >= plan.goal.amount
            if extracted >= plan.goal.amount and drill.powered and bound_to_target and stored:
                return MissionTrace(
                    plan=plan,
                    samples=tuple(samples),
                    success=True,
                    reason=(
                        f"Biome mined {extracted} {plan.goal.resource}; "
                        "native power and Miner target are active"
                    ),
                )
            last_reason = (
                f"extracted={extracted}/{plan.goal.amount}, powered={drill.powered}, "
                f"target={drill.deposit_path}, stored={drill.stored_amount}/{plan.goal.amount}"
            )
        except FlecsRemoteError as exc:
            last_reason = str(exc)

        if elapsed >= timeout:
            return MissionTrace(
                plan=plan,
                samples=tuple(samples),
                success=False,
                reason=f"timed out after {timeout:.1f}s: {last_reason}",
            )
        sleep(poll_interval)


def run_mission(
    client: BiomeClient,
    policy: GoalDirectedDrillPolicy,
    goal: ExtractionGoal,
    **monitor_options: float,
) -> MissionTrace:
    plan = plan_mission(client, policy, goal)
    client.deploy(plan.action)
    return monitor_mission(client, plan, **monitor_options)
