# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Invoke the deployed RoboSemanticBench Modal app.

Use this after:

    modal deploy bench/robosemantic/runner.py

The deployed app keeps ``run_shard`` eligible for Modal memory snapshots. This
client stays intentionally thin so hackathon eval loops do not rebuild the heavy
RSB image for every run.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from typing import Any

import modal

from bench.robosemantic.protocol import (
    aggregate_summaries,
    build_shard_jobs,
)

DEPLOYED_APP_NAME = "archetype-robosemantic"
DEFAULT_SUITE_ARG = "RSB-Math-4"
DEFAULT_PI05_TRAIN_CONFIG = "pi05_base_aloha_lora"
DEFAULT_PI05_MODEL_NAME = "robotwin_pi05_aloha_agilex_randomized_5tasks_step20000"
DEFAULT_PI05_CHECKPOINT_ID = 20000

client_app = modal.App("archetype-robosemantic-client")


def _parse_policy_overrides(policy_overrides_json: str) -> dict[str, Any]:
    if not policy_overrides_json:
        return {}
    parsed = json.loads(policy_overrides_json)
    if not isinstance(parsed, dict):
        raise ValueError("--policy-overrides-json must decode to a JSON object")
    return parsed


@client_app.local_entrypoint()
def main(
    suites: str = DEFAULT_SUITE_ARG,
    policy_name: str = "pi05",
    ckpt_setting: str = "robotwin-pi05-20k",
    expert_data_num: int = 50,
    checkpoint_num: int = 600,
    policy_seed: int = 0,
    policy_overrides_json: str = "",
    run_id: str = "rsb-deployed-smoke",
    episodes_per_suite: int = 2,
    shards_per_suite: int = 1,
    seed: int = 0,
    commit_every_episodes: int = 1,
    instruction_type: str = "unseen",
    deployed_app_name: str = DEPLOYED_APP_NAME,
):
    """Run RSB shards through the deployed Modal app."""
    suite_names = [suite.strip() for suite in suites.split(",") if suite.strip()]
    policy_overrides = _parse_policy_overrides(policy_overrides_json)
    if policy_name == "DP":
        policy_overrides.setdefault("expert_data_num", expert_data_num)
        policy_overrides.setdefault("checkpoint_num", checkpoint_num)
    if policy_name == "pi05":
        policy_overrides.setdefault("train_config_name", DEFAULT_PI05_TRAIN_CONFIG)
        policy_overrides.setdefault("model_name", DEFAULT_PI05_MODEL_NAME)
        policy_overrides.setdefault("checkpoint_id", DEFAULT_PI05_CHECKPOINT_ID)
        policy_overrides.setdefault("pi0_step", 50)

    jobs = build_shard_jobs(
        suite_names=suite_names,
        policy_name=policy_name,
        ckpt_setting=ckpt_setting,
        run_id=run_id,
        episodes_per_suite=episodes_per_suite,
        shards_per_suite=shards_per_suite,
        seed=seed,
        policy_seed=policy_seed,
        commit_every_episodes=commit_every_episodes,
        instruction_type=instruction_type,
        policy_overrides=policy_overrides,
    )

    run_shard = modal.Function.from_name(deployed_app_name, "run_shard")
    write_aggregate = modal.Function.from_name(deployed_app_name, "write_aggregate")
    summaries = list(run_shard.map([asdict(job) for job in jobs], order_outputs=True))
    aggregate = aggregate_summaries(summaries)
    aggregate_path = write_aggregate.remote(run_id, aggregate)

    print("=== RoboSemanticBench deployed summary ===")
    print(f"app: {deployed_app_name}")
    print(f"episodes: {aggregate['episodes']}")
    print(f"task_success_rate: {aggregate['task_success_rate']:.4f}")
    print(f"grasp_success_rate: {aggregate['grasp_success_rate']:.4f}")
    print(f"aggregate_path: {aggregate_path}")
    for suite in aggregate["suites"]:
        nsg = suite["normalized_semantic_grounding"]
        nsg_text = "undefined" if nsg is None else f"{nsg:.4f}"
        print(
            f"  {suite['suite']}: TSR={suite['task_success_rate']:.4f} "
            f"GSR={suite['grasp_success_rate']:.4f} nSG={nsg_text} "
            f"episodes={suite['episodes']}"
        )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Invoke deployed RoboSemanticBench")
    parser.add_argument("--suites", default=DEFAULT_SUITE_ARG)
    parser.add_argument("--policy-name", default="pi05")
    parser.add_argument("--ckpt-setting", default="robotwin-pi05-20k")
    parser.add_argument("--expert-data-num", type=int, default=50)
    parser.add_argument("--checkpoint-num", type=int, default=600)
    parser.add_argument("--policy-seed", type=int, default=0)
    parser.add_argument("--policy-overrides-json", default="")
    parser.add_argument("--run-id", default="rsb-deployed-smoke")
    parser.add_argument("--episodes-per-suite", type=int, default=2)
    parser.add_argument("--shards-per-suite", type=int, default=1)
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument("--commit-every-episodes", type=int, default=1)
    parser.add_argument("--instruction-type", default="unseen")
    parser.add_argument("--deployed-app-name", default=DEPLOYED_APP_NAME)
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    main(
        suites=args.suites,
        policy_name=args.policy_name,
        ckpt_setting=args.ckpt_setting,
        expert_data_num=args.expert_data_num,
        checkpoint_num=args.checkpoint_num,
        policy_seed=args.policy_seed,
        policy_overrides_json=args.policy_overrides_json,
        run_id=args.run_id,
        episodes_per_suite=args.episodes_per_suite,
        shards_per_suite=args.shards_per_suite,
        seed=args.seed,
        commit_every_episodes=args.commit_every_episodes,
        instruction_type=args.instruction_type,
        deployed_app_name=args.deployed_app_name,
    )
