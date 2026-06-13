# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""RoboSemanticBench protocol helpers."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class RsbSuite:
    """One RSB evaluation suite from the paper/README protocol."""

    name: str
    task_name: str
    eval_config: str
    choices: int


@dataclass(frozen=True)
class RsbShardJob:
    """A single Modal-native eval unit."""

    suite: RsbSuite
    policy_name: str
    ckpt_setting: str
    run_id: str
    shard_idx: int
    episode_start: int
    episodes: int
    seed: int
    policy_seed: int = 0
    commit_every_episodes: int = 1
    instruction_type: str = "unseen"
    policy_overrides: dict[str, Any] = field(default_factory=dict)

    @property
    def job_id(self) -> str:
        safe_suite = self.suite.name.lower().replace("-", "_")
        return f"{self.run_id}-{safe_suite}-shard{self.shard_idx}"


RSB_SUITES: tuple[RsbSuite, ...] = (
    RsbSuite("RSB-Math-4", "rsb_math", "rsb_math_train_500", 4),
    RsbSuite("RSB-Math-10", "rsb_math_10blocks", "rsb_math_10blocks_train_500", 10),
    RsbSuite("RSB-HardMath-4", "rsb_hardmath", "rsb_hardmath_train_700", 4),
    RsbSuite("RSB-HardMath-10", "rsb_hardmath_10blocks", "rsb_hardmath_10blocks_train_700", 10),
    RsbSuite("RSB-General-4", "rsb_general", "rsb_general_test_500", 4),
    RsbSuite("RSB-General-10", "rsb_general_10blocks", "rsb_general_10blocks_test_500", 10),
)

RSB_EVAL_REQUIREMENTS_EXCLUDE = (
    "azure==4.0.0",
    "azure-ai-inference",
    "moviepy",
    "openai",
    "wandb",
)

RSB_EVAL_CONSTRAINTS = (
    "torchvision==0.19.1",
    "opencv-python==4.11.0.86",
    "zarr==3.1.5",
    "pillow==11.3.0",
)

CUROBO_CUDA_ARCH_LIST = "8.9"


def rsb_eval_requirements_install_command(root: str) -> str:
    """Build the Modal image command for the RSB eval dependency subset."""
    exclude_pattern = "|".join(re.escape(requirement) for requirement in RSB_EVAL_REQUIREMENTS_EXCLUDE)
    constraints = "\\n".join(RSB_EVAL_CONSTRAINTS)
    return (
        f"cd {root} && "
        f"grep -v -E '^({exclude_pattern})$' script/requirements.txt "
        "> /tmp/rsb-eval-requirements.txt && "
        f"printf '{constraints}\\n' > /tmp/rsb-eval-constraints.txt && "
        "uv pip install --system "
        "-r /tmp/rsb-eval-requirements.txt "
        "-c /tmp/rsb-eval-constraints.txt"
    )


def curobo_install_command(root: str) -> str:
    """Build the Modal image command for curobo's CUDA extension install."""
    return (
        f"cd {root}/envs/curobo && "
        f"TORCH_CUDA_ARCH_LIST={CUROBO_CUDA_ARCH_LIST} "
        "uv pip install --system -e . --no-build-isolation"
    )


def suite_by_name(name: str) -> RsbSuite:
    """Resolve suite by paper name or RSB task name."""
    normalized = name.lower()
    for suite in RSB_SUITES:
        if normalized in {suite.name.lower(), suite.task_name.lower()}:
            return suite
    known = ", ".join(suite.name for suite in RSB_SUITES)
    raise ValueError(f"Unknown RSB suite {name!r}. Known suites: {known}")


def normalized_semantic_grounding(
    *,
    tsr: float,
    gsr: float,
    choices: int,
) -> float | None:
    """Compute nSG = ((TSR/GSR) - (1/N)) / (1 - (1/N))."""
    if gsr <= 0.0:
        return None
    random_rate = 1.0 / float(choices)
    return ((tsr / gsr) - random_rate) / (1.0 - random_rate)


def parse_result_text(text: str) -> dict[str, float]:
    """Extract RSB result metrics from ``_result.txt``."""
    patterns = {
        "task_success_rate": r"Task Success Rate:\s*([0-9.]+)",
        "grasp_success_rate": r"Grasp Success Rate:\s*([0-9.]+)",
    }
    parsed: dict[str, float] = {}
    for key, pattern in patterns.items():
        match = re.search(pattern, text)
        if match is None:
            raise ValueError(f"Could not parse {key} from RSB result text")
        parsed[key] = float(match.group(1))
    return parsed


def build_shard_jobs(
    *,
    suite_names: list[str],
    policy_name: str,
    ckpt_setting: str,
    run_id: str,
    episodes_per_suite: int,
    shards_per_suite: int,
    seed: int,
    policy_seed: int = 0,
    commit_every_episodes: int = 1,
    instruction_type: str = "unseen",
    policy_overrides: dict[str, Any] | None = None,
) -> list[RsbShardJob]:
    """Build one Modal job per suite shard."""
    if episodes_per_suite < 1:
        raise ValueError("episodes_per_suite must be >= 1")
    if shards_per_suite < 1:
        raise ValueError("shards_per_suite must be >= 1")
    if commit_every_episodes < 0:
        raise ValueError("commit_every_episodes must be >= 0")

    jobs: list[RsbShardJob] = []
    for suite_name in suite_names:
        suite = suite_by_name(suite_name)
        base = episodes_per_suite // shards_per_suite
        remainder = episodes_per_suite % shards_per_suite
        episode_start = 0
        for shard_idx in range(shards_per_suite):
            episodes = base + (1 if shard_idx < remainder else 0)
            if episodes == 0:
                continue
            jobs.append(
                RsbShardJob(
                    suite=suite,
                    policy_name=policy_name,
                    ckpt_setting=ckpt_setting,
                    run_id=run_id,
                    shard_idx=shard_idx,
                    episode_start=episode_start,
                    episodes=episodes,
                    # RSB's stock script uses 100000 * (1 + seed) as seed base.
                    # We allocate independent seed streams per shard.
                    seed=seed * 1000 + shard_idx,
                    policy_seed=policy_seed,
                    commit_every_episodes=commit_every_episodes,
                    instruction_type=instruction_type,
                    policy_overrides=dict(policy_overrides or {}),
                )
            )
            episode_start += episodes
    return jobs


def suite_data_requirements(suite: RsbSuite) -> tuple[str, ...]:
    """Return small semantic-source files needed for an eval suite."""
    if suite.task_name == "rsb_hardmath":
        return ("gsm8k/data/test.json", "data/rsb_math/rsb_math_train_500/scene_info.json")
    if suite.task_name == "rsb_general":
        # RSB-General builds its answer pool from both splits at eval time.
        return (
            "mmluqa2/data/test.json",
            "mmluqa2/data/train.json",
            "data/rsb_math/rsb_math_train_500/scene_info.json",
        )
    if suite.task_name.startswith("rsb_hardmath"):
        return ("gsm8k/data/test.json",)
    if suite.task_name.startswith("rsb_general"):
        return ("mmluqa2/data/test.json", "mmluqa2/data/train.json")
    return ()


def dp_checkpoint_requirement(
    *,
    suite: RsbSuite,
    ckpt_setting: str,
    expert_data_num: int,
    checkpoint_num: int,
    policy_seed: int,
) -> str:
    """Return the RSB DP checkpoint path for a suite/policy setting."""
    return (
        f"policy/DP/checkpoints/{suite.task_name}-{ckpt_setting}-"
        f"{expert_data_num}-{policy_seed}/{checkpoint_num}.ckpt"
    )


def pi05_checkpoint_requirement(
    *,
    train_config_name: str,
    model_name: str,
    checkpoint_id: int,
) -> str:
    """Return the RSB pi05 checkpoint params path for an OpenPI checkpoint."""
    return (
        f"policy/pi05/checkpoints/{train_config_name}/"
        f"{model_name}/{checkpoint_id}/params"
    )


def local_requirements_for_job(
    job: RsbShardJob,
    *,
    expert_data_num: int | None = None,
    checkpoint_num: int | None = None,
) -> tuple[str, ...]:
    """Return local RSB paths that should exist before dispatching a job."""
    requirements = list(suite_data_requirements(job.suite))
    if job.policy_name == "DP" and expert_data_num is not None and checkpoint_num is not None:
        requirements.append(
            dp_checkpoint_requirement(
                suite=job.suite,
                ckpt_setting=job.ckpt_setting,
                expert_data_num=expert_data_num,
                checkpoint_num=checkpoint_num,
                policy_seed=job.policy_seed,
            )
        )
    if job.policy_name == "pi05":
        train_config_name = job.policy_overrides.get("train_config_name")
        model_name = job.policy_overrides.get("model_name")
        checkpoint_id = job.policy_overrides.get("checkpoint_id")
        if train_config_name is not None and model_name is not None and checkpoint_id is not None:
            requirements.append(
                pi05_checkpoint_requirement(
                    train_config_name=str(train_config_name),
                    model_name=str(model_name),
                    checkpoint_id=int(checkpoint_id),
                )
            )
    return tuple(requirements)


def missing_local_requirements(root: str | Path, requirements: tuple[str, ...]) -> tuple[str, ...]:
    """Return requirement paths missing under ``root``."""
    root_path = Path(root)
    return tuple(path for path in requirements if not (root_path / path).exists())


def aggregate_summaries(summaries: list[dict[str, Any]]) -> dict[str, Any]:
    """Aggregate shard summaries by suite and overall."""
    by_suite: dict[str, list[dict[str, Any]]] = {}
    for summary in summaries:
        by_suite.setdefault(str(summary["suite"]), []).append(summary)

    suites: list[dict[str, Any]] = []
    for suite_name, suite_rows in by_suite.items():
        episodes = sum(int(row["episodes"]) for row in suite_rows)
        successes = sum(int(row["successes"]) for row in suite_rows)
        grasps = sum(int(row["grasp_successes"]) for row in suite_rows)
        choices = int(suite_rows[0]["choices"])
        tsr = successes / episodes if episodes else 0.0
        gsr = grasps / episodes if episodes else 0.0
        suites.append(
            {
                "suite": suite_name,
                "task_name": suite_rows[0]["task_name"],
                "choices": choices,
                "episodes": episodes,
                "successes": successes,
                "grasp_successes": grasps,
                "task_success_rate": tsr,
                "grasp_success_rate": gsr,
                "normalized_semantic_grounding": normalized_semantic_grounding(
                    tsr=tsr,
                    gsr=gsr,
                    choices=choices,
                ),
                "shards": suite_rows,
            }
        )

    total_episodes = sum(int(row["episodes"]) for row in summaries)
    total_successes = sum(int(row["successes"]) for row in summaries)
    total_grasps = sum(int(row["grasp_successes"]) for row in summaries)
    return {
        "episodes": total_episodes,
        "successes": total_successes,
        "grasp_successes": total_grasps,
        "task_success_rate": total_successes / total_episodes if total_episodes else 0.0,
        "grasp_success_rate": total_grasps / total_episodes if total_episodes else 0.0,
        "suites": sorted(suites, key=lambda row: row["suite"]),
    }
