# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Archetype-native batched RoboSemanticBench runner on Modal.

This is the high-throughput path. It differs from ``runner.py`` by not calling
RSB's serial ``eval_policy`` loop. A Modal cell owns:

- one warm pi0.5 policy;
- N RSB env instances keyed by env_key;
- one Archetype world with one entity per seed;
- batched policy inference over the live envs;
- batched env stepping over the live envs.

The old ``runner.py`` remains the compatibility/reference path.
"""

# ruff: noqa: E402

import argparse
import json
import sys
from dataclasses import asdict
from pathlib import Path
from typing import Any

import modal

_REMOTE_REPO_ROOT = Path("/root/archetype")
_LOCAL_ENTRYPOINT = Path(__file__).resolve()
REPO_ROOT = (
    _REMOTE_REPO_ROOT
    if _REMOTE_REPO_ROOT.exists()
    else _LOCAL_ENTRYPOINT.parents[2]
)
if str(REPO_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "src"))
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from bench.robosemantic.protocol import (
    RsbSuite,
    aggregate_summaries,
    build_shard_jobs,
    local_requirements_for_job,
    missing_local_requirements,
)
from bench.robosemantic.runner import (
    ARCHETYPE_BENCH_DIR,
    ASSET_CACHE_DIR,
    DEFAULT_PI05_CHECKPOINT_ID,
    DEFAULT_PI05_MODEL_NAME,
    DEFAULT_PI05_TRAIN_CONFIG,
    DEFAULT_RSB_SOURCE,
    GSM8K_DATA_VOLUME_DIR,
    LEGACY_PI05_CHECKPOINT_LAYOUT_CONFIG,
    MMLUQA2_DATA_VOLUME_DIR,
    MODEL_CACHE_DIR,
    RESULTS_DIR,
    ROOT,
    RSB_DATA_VOLUME_DIR,
    RSB_SOURCE_DIR,
    app,
    asset_cache_volume,
    curobo_install_command,
    data_volume,
    gsm8k_data_volume,
    hf_cache_env,
    hf_secret,
    mmluqa2_data_volume,
    model_cache_volume,
    pi05_checkpoints_volume,
    results_volume,
    rsb_eval_requirements_install_command,
    write_aggregate,
)

CANONICAL_NS = "robosemantic_batched"

stable_rsb_image = (
    modal.Image.from_registry("nvidia/cuda:12.4.1-devel-ubuntu22.04", add_python="3.11")
    .apt_install(
        "git",
        "clang",
        "build-essential",
        "ffmpeg",
        "unzip",
        "libgl1",
        "libglib2.0-0",
        "libegl1",
        "libvulkan1",
        "mesa-vulkan-drivers",
        "patchelf",
    )
    .add_local_dir(
        RSB_SOURCE_DIR,
        ROOT,
        copy=True,
        ignore=[
            ".git",
            "**/__pycache__",
            "data",
            "eval_result",
            "wandb",
            "envs/curobo",
        ],
    )
    .run_commands(
        "pip install uv hf_xet",
        rsb_eval_requirements_install_command(ROOT),
        "uv pip install --system wheel ninja flit_core",
        f"cd {ROOT} && uv pip install --system git+https://github.com/facebookresearch/pytorch3d.git@stable --no-build-isolation",
        f"cd {ROOT}/policy/DP && uv pip install --system -e . --no-build-isolation",
        f"cd {ROOT}/policy/pi05 && uv pip install --system "
        "-e packages/openpi-client -e . --prerelease=allow --index-strategy unsafe-best-match",
        "SAPIEN_LOCATION=$(pip show sapien | grep 'Location' | awk '{print $2}')/sapien && "
        "sed -i -E 's/(\"r\")(\\))( as)/\\1, encoding=\"utf-8\") as/g' "
        "$SAPIEN_LOCATION/wrapper/urdf_loader.py",
        "MPLIB_LOCATION=$(pip show mplib | grep 'Location' | awk '{print $2}')/mplib && "
        "sed -i -E 's/(if np.linalg.norm\\(delta_twist\\) < 1e-4 )(or collide )(or not within_joint_limit:)/\\1\\3/g' "
        "$MPLIB_LOCATION/planner.py",
        f"cd {ROOT}/envs && git clone --branch v0.7.8 --depth 1 https://github.com/NVlabs/curobo.git",
        curobo_install_command(ROOT),
        "uv pip install --system warp-lang==1.12.0 setuptools==69.5.1",
    )
    .env(
        {
            "PYTHONPATH": f"/root:{ROOT}:{ROOT}/policy:{ROOT}/description/utils",
            "DISPLAY": "",
            "NVIDIA_DRIVER_CAPABILITIES": "graphics,utility,compute,display",
            "PYTHONWARNINGS": "ignore::UserWarning",
            "PYTORCH_ALLOC_CONF": "expandable_segments:True",
            **hf_cache_env(),
        }
    )
)

batched_image = (
    stable_rsb_image.run_commands(
        "UV_COMPILE_BYTECODE=0 uv pip install --system "
        "'daft[lance]>=0.7.4' "
        "lancedb>=0.22.0 "
        "uuid-utils>=0.11.0 "
        "psutil>=5.9 "
        "pydantic>=2.0 "
        "logfire>=4.32.0"
    )
    .add_local_dir(
        ARCHETYPE_BENCH_DIR,
        "/root/bench",
        copy=True,
        ignore=["**/__pycache__"],
    )
    .add_local_dir(
        REPO_ROOT,
        "/root/archetype",
        copy=True,
        ignore=[
            ".git",
            "**/__pycache__",
            ".claude",
            ".pytest_cache",
            ".ruff_cache",
            ".venv",
            "archetype_data",
            "target",
            "**/*.parquet",
        ],
    )
    .env({"PYTHONPATH": f"/root/archetype:/root/archetype/src:/root:{ROOT}:{ROOT}/policy:{ROOT}/description/utils"})
)


def _parse_policy_overrides(policy_overrides_json: str) -> dict[str, Any]:
    if not policy_overrides_json:
        return {}
    parsed = json.loads(policy_overrides_json)
    if not isinstance(parsed, dict):
        raise ValueError("--policy-overrides-json must decode to a JSON object")
    return parsed


def _split_seed_cells(*, seed_start: int, episodes: int, batch_size: int) -> list[list[int]]:
    if episodes < 1:
        raise ValueError("episodes must be >= 1")
    if batch_size < 1:
        raise ValueError("batch_size must be >= 1")
    seeds = [seed_start + offset for offset in range(episodes)]
    return [seeds[idx : idx + batch_size] for idx in range(0, len(seeds), batch_size)]


def _stage(name: str) -> None:
    print(f"RSB_BATCH_STAGE {name}", flush=True)


def _link_volume_dir(target_relative: str, source_dir: str) -> None:
    import shutil

    target = Path(ROOT) / target_relative
    source = Path(source_dir)
    source.mkdir(parents=True, exist_ok=True)
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.is_symlink():
        target.unlink()
    elif target.exists():
        shutil.rmtree(target)
    target.symlink_to(source, target_is_directory=True)


def _with_modal_lock(lock_name: str, fn):
    import shutil
    import time

    lock_path = Path(MODEL_CACHE_DIR) / f".{lock_name}.lock"
    owns_lock = False
    for _ in range(360):
        try:
            lock_path.mkdir()
            owns_lock = True
            break
        except FileExistsError:
            time.sleep(5)
    else:
        raise RuntimeError(f"Timed out waiting for Modal lock {lock_name}")
    try:
        return fn()
    finally:
        if owns_lock:
            shutil.rmtree(lock_path, ignore_errors=True)


def _ensure_rsb_assets() -> None:
    import shutil
    import subprocess
    import sys
    import time

    from huggingface_hub import snapshot_download

    cache_dir = Path(ASSET_CACHE_DIR)
    cache_dir.mkdir(parents=True, exist_ok=True)
    ready_path = cache_dir / ".robosemantic-assets-ready"
    lock_path = cache_dir / ".robosemantic-assets.lock"
    asset_dirs = ("background_texture", "embodiments", "objects")
    zip_names = tuple(f"{name}.zip" for name in asset_dirs)

    def cache_is_ready() -> bool:
        return ready_path.exists() and all((cache_dir / name).exists() for name in asset_dirs)

    owns_lock = False
    if not cache_is_ready():
        for _ in range(720):
            try:
                lock_path.mkdir()
                owns_lock = True
                break
            except FileExistsError:
                if cache_is_ready():
                    break
                time.sleep(5)
        else:
            raise RuntimeError("Timed out waiting for RSB asset cache bootstrap")

    if owns_lock:
        try:
            if not cache_is_ready():
                snapshot_download(
                    repo_id="TianxingChen/RoboTwin2.0",
                    allow_patterns=list(zip_names),
                    local_dir=str(cache_dir),
                    repo_type="dataset",
                    resume_download=True,
                )
                for zip_name, dir_name in zip(zip_names, asset_dirs, strict=True):
                    if not (cache_dir / dir_name).exists():
                        subprocess.run(
                            ["unzip", "-q", "-o", str(cache_dir / zip_name)],
                            cwd=cache_dir,
                            check=True,
                        )
                ready_path.write_text("ok\n", encoding="utf-8")
                asset_cache_volume.commit()
        finally:
            shutil.rmtree(lock_path, ignore_errors=True)

    for name in asset_dirs:
        target = Path(ROOT) / "assets" / name
        source = cache_dir / name
        if target.is_symlink():
            target.unlink()
        if not target.exists():
            target.symlink_to(source, target_is_directory=True)

    subprocess.run([sys.executable, "./script/update_embodiment_config_path.py"], check=True)


def _patch_pi05_openpi() -> None:
    import subprocess

    try:
        import pytest  # noqa: F401
    except ImportError:
        subprocess.run(["uv", "pip", "install", "--system", "pytest"], check=True)

    policy_config_path = Path(ROOT) / "policy" / "pi05" / "src" / "openpi" / "policies" / "policy_config.py"
    text = policy_config_path.read_text(encoding="utf-8")
    if "data_config.asset_id = robotwin_repo_id" in text:
        text = text.replace("import logging\n", "import dataclasses\nimport logging\n")
        text = text.replace(
            "            data_config.asset_id = robotwin_repo_id\n",
            "            data_config = dataclasses.replace(data_config, asset_id=robotwin_repo_id)\n",
        )
        policy_config_path.write_text(text, encoding="utf-8")


def _ensure_pi05_checkpoint_layout(policy_overrides: dict[str, Any]) -> None:
    train_config_name = str(policy_overrides.get("train_config_name", DEFAULT_PI05_TRAIN_CONFIG))
    model_name = str(policy_overrides.get("model_name", DEFAULT_PI05_MODEL_NAME))
    checkpoint_root = Path(ROOT) / "policy" / "pi05" / "checkpoints"
    legacy_path = checkpoint_root / LEGACY_PI05_CHECKPOINT_LAYOUT_CONFIG / model_name
    target_path = checkpoint_root / train_config_name / model_name
    if (
        train_config_name != LEGACY_PI05_CHECKPOINT_LAYOUT_CONFIG
        and legacy_path.exists()
        and not target_path.exists()
    ):
        target_path.parent.mkdir(parents=True, exist_ok=True)
        target_path.symlink_to(legacy_path, target_is_directory=True)


def _prepare_rsb_runtime() -> None:
    import os
    import sys

    os.chdir(ROOT)
    for path in (ROOT, f"{ROOT}/policy", f"{ROOT}/description/utils"):
        if path not in sys.path:
            sys.path.insert(0, path)
    _link_volume_dir("data", RSB_DATA_VOLUME_DIR)
    _link_volume_dir("gsm8k/data", GSM8K_DATA_VOLUME_DIR)
    _link_volume_dir("mmluqa2/data", MMLUQA2_DATA_VOLUME_DIR)
    _ensure_rsb_assets()
    _patch_pi05_openpi()


def _load_rsb_config(
    suite: RsbSuite,
    max_eval_steps: int,
    payload: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    import importlib

    import yaml

    eval_policy_mod = importlib.import_module("script.eval_policy")
    with open(f"{ROOT}/task_config/{suite.eval_config}.yml", encoding="utf-8") as f:
        args = yaml.safe_load(f)
    if max_eval_steps > 0:
        step_limit_path = Path(ROOT) / "task_config" / "_eval_step_limit.yml"
        step_limits = yaml.safe_load(step_limit_path.read_text(encoding="utf-8"))
        original_limit = int(step_limits.get(suite.task_name, 1000))
        step_limits[suite.task_name] = min(original_limit, max_eval_steps)
        step_limit_path.write_text(yaml.safe_dump(step_limits, sort_keys=False), encoding="utf-8")
    args.update(
        {
            "task_name": suite.task_name,
            "task_config": suite.eval_config,
            "ckpt_setting": payload["ckpt_setting"],
            "policy_name": "pi05",
            "expert_check": False,
            "eval_mode": True,
            "eval_metadata_log": True,
            "eval_video_log": False,
            "eval_save_dir": Path(RESULTS_DIR)
            / payload["run_id"]
            / suite.task_name
            / f"cell{payload['cell_idx']}",
        }
    )
    args["eval_save_dir"].mkdir(parents=True, exist_ok=True)

    embodiment_type = args.get("embodiment")
    with open(f"{ROOT}/task_config/_embodiment_config.yml", encoding="utf-8") as f:
        embodiment_types = yaml.safe_load(f)
    with open(f"{ROOT}/task_config/_camera_config.yml", encoding="utf-8") as f:
        camera_config = yaml.safe_load(f)

    def get_embodiment_file(embodiment: str) -> str:
        robot_file = embodiment_types[embodiment]["file_path"]
        if robot_file is None:
            raise RuntimeError(f"No embodiment file configured for {embodiment}")
        return robot_file

    head_camera_type = args["camera"]["head_camera_type"]
    args["head_camera_h"] = camera_config[head_camera_type]["h"]
    args["head_camera_w"] = camera_config[head_camera_type]["w"]
    if len(embodiment_type) == 1:
        args["left_robot_file"] = get_embodiment_file(embodiment_type[0])
        args["right_robot_file"] = get_embodiment_file(embodiment_type[0])
        args["dual_arm_embodied"] = True
    elif len(embodiment_type) == 3:
        args["left_robot_file"] = get_embodiment_file(embodiment_type[0])
        args["right_robot_file"] = get_embodiment_file(embodiment_type[1])
        args["embodiment_dis"] = embodiment_type[2]
        args["dual_arm_embodied"] = False
    else:
        raise RuntimeError("embodiment items should be 1 or 3")
    args["left_embodiment_config"] = eval_policy_mod.get_embodiment_config(args["left_robot_file"])
    args["right_embodiment_config"] = eval_policy_mod.get_embodiment_config(args["right_robot_file"])

    usr_args = dict(payload["policy_overrides"])
    usr_args.update(
        {
            "task_name": suite.task_name,
            "task_config": suite.eval_config,
            "ckpt_setting": payload["ckpt_setting"],
            "policy_name": "pi05",
            "seed": int(payload.get("policy_seed", 0)),
            "instruction_type": payload.get("instruction_type", "unseen"),
            "expert_check": False,
            "left_arm_dim": len(args["left_embodiment_config"]["arm_joints_name"][0]),
            "right_arm_dim": len(args["right_embodiment_config"]["arm_joints_name"][1]),
        }
    )
    return args, usr_args


class ModalRsbEnvBatch:
    def __init__(self, suite: RsbSuite, args: dict[str, Any], instruction_type: str) -> None:
        import importlib

        self.suite = suite
        self.args = args
        self.instruction_type = instruction_type
        self.envs: dict[int, Any] = {}
        self.eval_policy_mod = importlib.import_module("script.eval_policy")
        self.generate_episode_descriptions = importlib.import_module(
            "generate_episode_instructions"
        ).generate_episode_descriptions

    def reset_batch(self, env_keys: list[int], seeds: list[int]) -> list[dict[str, Any]]:
        import copy

        import numpy as np

        out = []
        for episode_idx, (env_key, seed) in enumerate(zip(env_keys, seeds, strict=True)):
            task_env = self.eval_policy_mod.class_decorator(self.suite.task_name)
            task_args = copy.deepcopy(self.args)
            task_env.setup_demo(now_ep_num=episode_idx, seed=int(seed), is_test=True, **task_args)
            if getattr(task_env, "step_lim", None) is None:
                raise RuntimeError(
                    "RSB eval setup did not load a step limit. "
                    f"task={self.suite.task_name} config={self.suite.eval_config} "
                    "expected eval_mode=True to load task_config/_eval_step_limit.yml"
                )
            if hasattr(task_env, "set_episode_info"):
                episode_info = task_env.set_episode_info()
            else:
                episode_info = task_env.info
            descriptions = self.generate_episode_descriptions(
                self.suite.task_name,
                [episode_info["info"]],
                len(seeds),
            )
            instruction = str(np.random.choice(descriptions[0][self.instruction_type]))
            task_env.set_instruction(instruction=instruction)
            observation = task_env.get_obs()
            self.envs[int(env_key)] = task_env
            out.append(
                {
                    "instruction": instruction,
                    "raw": observation,
                    "state": observation["joint_action"]["vector"],
                    "episode_info": episode_info["info"],
                }
            )
        return out

    def step_batch(
        self,
        env_keys: list[int],
        actions: list[list[float]],
    ) -> list[dict[str, Any]]:
        out = []
        for env_key, action in zip(env_keys, actions, strict=True):
            task_env = self.envs[int(env_key)]
            task_env.take_action(action)
            observation = task_env.get_obs()
            success = bool(task_env.eval_success)
            step_lim = getattr(task_env, "step_lim", None)
            done = success or (step_lim is not None and int(task_env.take_action_cnt) >= int(step_lim))
            grasp_success = bool(getattr(task_env, "any_block_grasp_success", False))
            if done:
                task_env.close_env()
            out.append(
                {
                    "raw": observation,
                    "state": observation["joint_action"]["vector"],
                    "done": done,
                    "success": success,
                    "grasp_success": grasp_success,
                    "episode_info": task_env.get_eval_metadata()
                    if hasattr(task_env, "get_eval_metadata")
                    else {},
                }
            )
        return out


class Pi05BatchPolicy:
    def __init__(self, usr_args: dict[str, Any]) -> None:
        import importlib

        train_config_name = str(usr_args.get("train_config_name", DEFAULT_PI05_TRAIN_CONFIG))
        model_name = str(usr_args.get("model_name", DEFAULT_PI05_MODEL_NAME))
        checkpoint_id = str(usr_args.get("checkpoint_id", DEFAULT_PI05_CHECKPOINT_ID))
        assets_path = (
            Path(ROOT)
            / "policy"
            / "pi05"
            / "checkpoints"
            / train_config_name
            / model_name
            / checkpoint_id
            / "assets"
        )
        print(
            "RSB_PI05_CHECKPOINT "
            + json.dumps(
                {
                    "train_config_name": train_config_name,
                    "model_name": model_name,
                    "checkpoint_id": checkpoint_id,
                    "assets_path": str(assets_path),
                    "assets_exists": assets_path.exists(),
                },
                sort_keys=True,
            ),
            flush=True,
        )
        pi05_mod = importlib.import_module("pi05")
        self.model = pi05_mod.get_model(usr_args)
        self.pi0_step = int(usr_args.get("pi0_step", 50))

    @staticmethod
    def _policy_obs(observation: dict[str, Any], instruction: str) -> dict[str, Any]:
        import numpy as np

        raw = observation["raw"]
        img_front = np.transpose(raw["observation"]["head_camera"]["rgb"], (2, 0, 1))
        img_right = np.transpose(raw["observation"]["right_camera"]["rgb"], (2, 0, 1))
        img_left = np.transpose(raw["observation"]["left_camera"]["rgb"], (2, 0, 1))
        return {
            "state": raw["joint_action"]["vector"],
            "images": {
                "cam_high": img_front,
                "cam_left_wrist": img_left,
                "cam_right_wrist": img_right,
            },
            "prompt": instruction,
        }

    def _infer_one(self, instruction: str, observation: dict[str, Any]) -> list[list[float]]:
        return self.model.policy.infer(self._policy_obs(observation, instruction))["actions"][
            : self.pi0_step
        ].tolist()

    def infer_batch(
        self,
        env_keys: list[int],
        instructions: list[str],
        observations: list[dict[str, Any]],
    ) -> list[list[list[float]]]:
        return [
            self._infer_one(instruction, observation)
            for instruction, observation in zip(instructions, observations, strict=True)
        ]


def _run_pi05_payload_with_policy(payload: dict[str, Any], policy: Pi05BatchPolicy) -> dict[str, Any]:
    import asyncio
    import shutil
    import time

    from archetype.core.config import StorageConfig
    from bench.robosemantic.batched import run_batched_cell

    suite = RsbSuite(**payload["suite"])
    args, _ = _load_rsb_config(suite, int(payload.get("max_eval_steps", 0) or 0), payload)
    env = ModalRsbEnvBatch(suite, args, str(payload.get("instruction_type", "unseen")))

    async def _run() -> dict[str, Any]:
        from archetype import ArchetypeRuntime

        cell_idx = int(payload["cell_idx"])
        local_results = Path("/tmp/rsb_archetype") / payload["run_id"] / f"cell{cell_idx}"
        volume_results = Path(RESULTS_DIR) / payload["run_id"] / "canonical" / f"cell{cell_idx}"
        if local_results.exists():
            shutil.rmtree(local_results)
        local_results.mkdir(parents=True, exist_ok=True)
        storage = StorageConfig(uri=str(local_results), namespace=CANONICAL_NS)
        async with ArchetypeRuntime() as runtime:
            started = time.perf_counter()
            summary = await run_batched_cell(
                runtime=runtime,
                suite=suite,
                run_name=str(payload.get("run_name", "baseline")),
                seeds=[int(seed) for seed in payload["seeds"]],
                env=env,
                policy=policy,
                max_steps=int(payload["max_steps"]),
                storage=storage,
                ledger_interval=int(payload.get("ledger_interval", 25)),
            )
            wall_s = time.perf_counter() - started
            if volume_results.exists():
                shutil.rmtree(volume_results)
            volume_results.parent.mkdir(parents=True, exist_ok=True)
            shutil.copytree(local_results, volume_results)
            summary.pop("world", None)
            summary.update(
                {
                    "policy_name": "pi05",
                    "ckpt_setting": payload["ckpt_setting"],
                    "cell_idx": cell_idx,
                    "shard_idx": cell_idx,
                    "episode_start": int(payload["episode_start"]),
                    "wall_s": round(wall_s, 1),
                    "results_path": str(volume_results),
                }
            )
            return summary

    out = asyncio.run(_run())
    manifest = Path(RESULTS_DIR) / payload["run_id"] / "batched_cells.jsonl"
    manifest.parent.mkdir(parents=True, exist_ok=True)
    with manifest.open("a", encoding="utf-8") as f:
        f.write(json.dumps({k: v for k, v in out.items() if k != "world"}, sort_keys=True) + "\n")
    results_volume.commit()
    print("RSB_BATCH_CELL " + json.dumps(out, sort_keys=True), flush=True)
    return out


@app.function(
    image=batched_image,
    gpu="L40S",
    volumes={
        RESULTS_DIR: results_volume,
        MODEL_CACHE_DIR: model_cache_volume,
        ASSET_CACHE_DIR: asset_cache_volume,
        RSB_DATA_VOLUME_DIR: data_volume,
        GSM8K_DATA_VOLUME_DIR: gsm8k_data_volume,
        MMLUQA2_DATA_VOLUME_DIR: mmluqa2_data_volume,
        f"{ROOT}/policy/pi05/checkpoints": pi05_checkpoints_volume,
    },
    timeout=24 * 3600,
    secrets=[hf_secret],
    enable_memory_snapshot=True,
)
def run_batched_pi05_cell(payload: dict[str, Any]) -> dict[str, Any]:
    """Run one batched RSB pi0.5 cell."""
    import asyncio
    import copy
    import faulthandler
    import importlib
    import os
    import shutil
    import subprocess
    import sys
    import time
    from pathlib import Path

    import numpy as np
    import yaml

    from archetype.core.config import StorageConfig
    from bench.robosemantic.batched import run_batched_cell

    faulthandler.enable()

    def stage(name: str) -> None:
        print(f"RSB_BATCH_STAGE {name}", flush=True)

    os.chdir(ROOT)
    sys.path.insert(0, ROOT)
    sys.path.insert(0, f"{ROOT}/policy")
    sys.path.insert(0, f"{ROOT}/description/utils")

    def link_volume_dir(target_relative: str, source_dir: str) -> None:
        target = Path(ROOT) / target_relative
        source = Path(source_dir)
        source.mkdir(parents=True, exist_ok=True)
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.is_symlink():
            target.unlink()
        elif target.exists():
            shutil.rmtree(target)
        target.symlink_to(source, target_is_directory=True)

    def with_modal_lock(lock_name: str, fn):
        lock_path = Path(MODEL_CACHE_DIR) / f".{lock_name}.lock"
        owns_lock = False
        for _ in range(360):
            try:
                lock_path.mkdir()
                owns_lock = True
                break
            except FileExistsError:
                time.sleep(5)
        else:
            raise RuntimeError(f"Timed out waiting for Modal lock {lock_name}")
        try:
            return fn()
        finally:
            if owns_lock:
                shutil.rmtree(lock_path, ignore_errors=True)

    def ensure_rsb_assets() -> None:
        from huggingface_hub import snapshot_download

        cache_dir = Path(ASSET_CACHE_DIR)
        cache_dir.mkdir(parents=True, exist_ok=True)
        ready_path = cache_dir / ".robosemantic-assets-ready"
        lock_path = cache_dir / ".robosemantic-assets.lock"
        asset_dirs = ("background_texture", "embodiments", "objects")
        zip_names = tuple(f"{name}.zip" for name in asset_dirs)

        def cache_is_ready() -> bool:
            return ready_path.exists() and all((cache_dir / name).exists() for name in asset_dirs)

        owns_lock = False
        if not cache_is_ready():
            for _ in range(720):
                try:
                    lock_path.mkdir()
                    owns_lock = True
                    break
                except FileExistsError:
                    if cache_is_ready():
                        break
                    time.sleep(5)
            else:
                raise RuntimeError("Timed out waiting for RSB asset cache bootstrap")

        if owns_lock:
            try:
                if not cache_is_ready():
                    snapshot_download(
                        repo_id="TianxingChen/RoboTwin2.0",
                        allow_patterns=list(zip_names),
                        local_dir=str(cache_dir),
                        repo_type="dataset",
                        resume_download=True,
                    )
                    for zip_name, dir_name in zip(zip_names, asset_dirs, strict=True):
                        if not (cache_dir / dir_name).exists():
                            subprocess.run(
                                ["unzip", "-q", "-o", str(cache_dir / zip_name)],
                                cwd=cache_dir,
                                check=True,
                            )
                    ready_path.write_text("ok\n", encoding="utf-8")
                    asset_cache_volume.commit()
            finally:
                shutil.rmtree(lock_path, ignore_errors=True)

        for name in asset_dirs:
            target = Path(ROOT) / "assets" / name
            source = cache_dir / name
            if target.is_symlink():
                target.unlink()
            if not target.exists():
                target.symlink_to(source, target_is_directory=True)

        subprocess.run([sys.executable, "./script/update_embodiment_config_path.py"], check=True)

    def patch_pi05_openpi() -> None:
        try:
            import pytest  # noqa: F401
        except ImportError:
            subprocess.run(["uv", "pip", "install", "--system", "pytest"], check=True)

        policy_config_path = Path(ROOT) / "policy" / "pi05" / "src" / "openpi" / "policies" / "policy_config.py"
        text = policy_config_path.read_text(encoding="utf-8")
        if "data_config.asset_id = robotwin_repo_id" in text:
            text = text.replace("import logging\n", "import dataclasses\nimport logging\n")
            text = text.replace(
                "            data_config.asset_id = robotwin_repo_id\n",
                "            data_config = dataclasses.replace(data_config, asset_id=robotwin_repo_id)\n",
            )
            policy_config_path.write_text(text, encoding="utf-8")

    def ensure_pi05_checkpoint_layout(policy_overrides: dict[str, Any]) -> None:
        train_config_name = str(policy_overrides.get("train_config_name", DEFAULT_PI05_TRAIN_CONFIG))
        model_name = str(policy_overrides.get("model_name", DEFAULT_PI05_MODEL_NAME))
        checkpoint_root = Path(ROOT) / "policy" / "pi05" / "checkpoints"
        legacy_path = checkpoint_root / LEGACY_PI05_CHECKPOINT_LAYOUT_CONFIG / model_name
        target_path = checkpoint_root / train_config_name / model_name
        if (
            train_config_name != LEGACY_PI05_CHECKPOINT_LAYOUT_CONFIG
            and legacy_path.exists()
            and not target_path.exists()
        ):
            target_path.parent.mkdir(parents=True, exist_ok=True)
            target_path.symlink_to(legacy_path, target_is_directory=True)

    def load_rsb_config(suite: RsbSuite, max_eval_steps: int) -> tuple[dict[str, Any], dict[str, Any]]:
        eval_policy_mod = importlib.import_module("script.eval_policy")
        with open(f"{ROOT}/task_config/{suite.eval_config}.yml", encoding="utf-8") as f:
            args = yaml.safe_load(f)
        if max_eval_steps > 0:
            step_limit_path = Path(ROOT) / "task_config" / "_eval_step_limit.yml"
            step_limits = yaml.safe_load(step_limit_path.read_text(encoding="utf-8"))
            original_limit = int(step_limits.get(suite.task_name, 1000))
            step_limits[suite.task_name] = min(original_limit, max_eval_steps)
            step_limit_path.write_text(yaml.safe_dump(step_limits, sort_keys=False), encoding="utf-8")
        args.update(
            {
                "task_name": suite.task_name,
                "task_config": suite.eval_config,
                "ckpt_setting": payload["ckpt_setting"],
                "policy_name": "pi05",
                "expert_check": False,
                "eval_mode": True,
                "eval_metadata_log": True,
                "eval_video_log": False,
                "eval_save_dir": Path(RESULTS_DIR) / payload["run_id"] / suite.task_name / f"cell{payload['cell_idx']}",
            }
        )
        args["eval_save_dir"].mkdir(parents=True, exist_ok=True)

        embodiment_type = args.get("embodiment")
        with open(f"{ROOT}/task_config/_embodiment_config.yml", encoding="utf-8") as f:
            embodiment_types = yaml.safe_load(f)
        with open(f"{ROOT}/task_config/_camera_config.yml", encoding="utf-8") as f:
            camera_config = yaml.safe_load(f)

        def get_embodiment_file(embodiment: str) -> str:
            robot_file = embodiment_types[embodiment]["file_path"]
            if robot_file is None:
                raise RuntimeError(f"No embodiment file configured for {embodiment}")
            return robot_file

        head_camera_type = args["camera"]["head_camera_type"]
        args["head_camera_h"] = camera_config[head_camera_type]["h"]
        args["head_camera_w"] = camera_config[head_camera_type]["w"]
        if len(embodiment_type) == 1:
            args["left_robot_file"] = get_embodiment_file(embodiment_type[0])
            args["right_robot_file"] = get_embodiment_file(embodiment_type[0])
            args["dual_arm_embodied"] = True
        elif len(embodiment_type) == 3:
            args["left_robot_file"] = get_embodiment_file(embodiment_type[0])
            args["right_robot_file"] = get_embodiment_file(embodiment_type[1])
            args["embodiment_dis"] = embodiment_type[2]
            args["dual_arm_embodied"] = False
        else:
            raise RuntimeError("embodiment items should be 1 or 3")
        args["left_embodiment_config"] = eval_policy_mod.get_embodiment_config(args["left_robot_file"])
        args["right_embodiment_config"] = eval_policy_mod.get_embodiment_config(args["right_robot_file"])

        usr_args = dict(payload["policy_overrides"])
        usr_args.update(
            {
                "task_name": suite.task_name,
                "task_config": suite.eval_config,
                "ckpt_setting": payload["ckpt_setting"],
                "policy_name": "pi05",
                "seed": int(payload.get("policy_seed", 0)),
                "instruction_type": payload.get("instruction_type", "unseen"),
                "expert_check": False,
                "left_arm_dim": len(args["left_embodiment_config"]["arm_joints_name"][0]),
                "right_arm_dim": len(args["right_embodiment_config"]["arm_joints_name"][1]),
            }
        )
        return args, usr_args

    class ModalRsbEnvBatch:
        def __init__(self, suite: RsbSuite, args: dict[str, Any], instruction_type: str) -> None:
            self.suite = suite
            self.args = args
            self.instruction_type = instruction_type
            self.envs: dict[int, Any] = {}
            self.eval_policy_mod = importlib.import_module("script.eval_policy")
            self.generate_episode_descriptions = importlib.import_module(
                "generate_episode_instructions"
            ).generate_episode_descriptions

        def reset_batch(self, env_keys: list[int], seeds: list[int]) -> list[dict[str, Any]]:
            out = []
            for episode_idx, (env_key, seed) in enumerate(zip(env_keys, seeds, strict=True)):
                task_env = self.eval_policy_mod.class_decorator(self.suite.task_name)
                task_args = copy.deepcopy(self.args)
                task_env.setup_demo(now_ep_num=episode_idx, seed=int(seed), is_test=True, **task_args)
                if getattr(task_env, "step_lim", None) is None:
                    raise RuntimeError(
                        "RSB eval setup did not load a step limit. "
                        f"task={self.suite.task_name} config={self.suite.eval_config} "
                        "expected eval_mode=True to load task_config/_eval_step_limit.yml"
                    )
                if hasattr(task_env, "set_episode_info"):
                    episode_info = task_env.set_episode_info()
                else:
                    episode_info = task_env.info
                descriptions = self.generate_episode_descriptions(
                    self.suite.task_name,
                    [episode_info["info"]],
                    len(seeds),
                )
                instruction = str(np.random.choice(descriptions[0][self.instruction_type]))
                task_env.set_instruction(instruction=instruction)
                observation = task_env.get_obs()
                self.envs[int(env_key)] = task_env
                out.append(
                    {
                        "instruction": instruction,
                        "raw": observation,
                        "state": observation["joint_action"]["vector"],
                        "episode_info": episode_info["info"],
                    }
                )
            return out

        def step_batch(
            self,
            env_keys: list[int],
            actions: list[list[float]],
        ) -> list[dict[str, Any]]:
            out = []
            for env_key, action in zip(env_keys, actions, strict=True):
                task_env = self.envs[int(env_key)]
                task_env.take_action(action)
                observation = task_env.get_obs()
                success = bool(task_env.eval_success)
                step_lim = getattr(task_env, "step_lim", None)
                done = success or (
                    step_lim is not None and int(task_env.take_action_cnt) >= int(step_lim)
                )
                grasp_success = bool(getattr(task_env, "any_block_grasp_success", False))
                if done:
                    task_env.close_env()
                out.append(
                    {
                        "raw": observation,
                        "state": observation["joint_action"]["vector"],
                        "done": done,
                        "success": success,
                        "grasp_success": grasp_success,
                        "episode_info": task_env.get_eval_metadata()
                        if hasattr(task_env, "get_eval_metadata")
                        else {},
                    }
                )
            return out

    class Pi05BatchPolicy:
        def __init__(self, usr_args: dict[str, Any]) -> None:
            pi05_mod = importlib.import_module("pi05")
            self.model = pi05_mod.get_model(usr_args)
            self.pi0_step = int(usr_args.get("pi0_step", 50))

        @staticmethod
        def _policy_obs(observation: dict[str, Any], instruction: str) -> dict[str, Any]:
            raw = observation["raw"]
            img_front = np.transpose(raw["observation"]["head_camera"]["rgb"], (2, 0, 1))
            img_right = np.transpose(raw["observation"]["right_camera"]["rgb"], (2, 0, 1))
            img_left = np.transpose(raw["observation"]["left_camera"]["rgb"], (2, 0, 1))
            return {
                "state": raw["joint_action"]["vector"],
                "images": {
                    "cam_high": img_front,
                    "cam_left_wrist": img_left,
                    "cam_right_wrist": img_right,
                },
                "prompt": instruction,
            }

        def _infer_one(self, instruction: str, observation: dict[str, Any]) -> list[list[float]]:
            return self.model.policy.infer(self._policy_obs(observation, instruction))["actions"][
                : self.pi0_step
            ].tolist()

        def infer_batch(
            self,
            env_keys: list[int],
            instructions: list[str],
            observations: list[dict[str, Any]],
        ) -> list[list[list[float]]]:
            # The warm model is shared across the whole cell. This method keeps
            # the API batch-shaped even if OpenPI falls back to per-example
            # transforms internally.
            return [
                self._infer_one(instruction, observation)
                for instruction, observation in zip(instructions, observations, strict=True)
            ]

    stage("setup")
    link_volume_dir("data", RSB_DATA_VOLUME_DIR)
    link_volume_dir("gsm8k/data", GSM8K_DATA_VOLUME_DIR)
    link_volume_dir("mmluqa2/data", MMLUQA2_DATA_VOLUME_DIR)
    ensure_rsb_assets()
    patch_pi05_openpi()

    suite = RsbSuite(**payload["suite"])
    policy_overrides = dict(payload["policy_overrides"])
    ensure_pi05_checkpoint_layout(policy_overrides)
    args, usr_args = load_rsb_config(suite, int(payload.get("max_eval_steps", 0) or 0))

    stage("load-model")
    serialize_model_load = bool(policy_overrides.get("serialize_model_load", True))
    if serialize_model_load:
        stage("load-model-wait-lock")
        policy = with_modal_lock("pi05-model-load", lambda: Pi05BatchPolicy(usr_args))
    else:
        policy = Pi05BatchPolicy(usr_args)
    stage("model-ready")
    env = ModalRsbEnvBatch(suite, args, str(payload.get("instruction_type", "unseen")))

    async def _run() -> dict[str, Any]:
        from archetype import ArchetypeRuntime

        cell_idx = int(payload["cell_idx"])
        local_results = Path("/tmp/rsb_archetype") / payload["run_id"] / f"cell{cell_idx}"
        volume_results = Path(RESULTS_DIR) / payload["run_id"] / "canonical" / f"cell{cell_idx}"
        if local_results.exists():
            shutil.rmtree(local_results)
        local_results.mkdir(parents=True, exist_ok=True)
        storage = StorageConfig(uri=str(local_results), namespace=CANONICAL_NS)
        async with ArchetypeRuntime() as runtime:
            started = time.perf_counter()
            summary = await run_batched_cell(
                runtime=runtime,
                suite=suite,
                run_name=str(payload.get("run_name", "baseline")),
                seeds=[int(seed) for seed in payload["seeds"]],
                env=env,
                policy=policy,
                max_steps=int(payload["max_steps"]),
                storage=storage,
                ledger_interval=int(payload.get("ledger_interval", 25)),
            )
            wall_s = time.perf_counter() - started
            if volume_results.exists():
                shutil.rmtree(volume_results)
            volume_results.parent.mkdir(parents=True, exist_ok=True)
            shutil.copytree(local_results, volume_results)
            summary.pop("world", None)
            summary.update(
                {
                    "policy_name": "pi05",
                    "ckpt_setting": payload["ckpt_setting"],
                    "cell_idx": cell_idx,
                    "shard_idx": cell_idx,
                    "episode_start": int(payload["episode_start"]),
                    "wall_s": round(wall_s, 1),
                    "results_path": str(volume_results),
                }
            )
            return summary

    out = asyncio.run(_run())
    manifest = Path(RESULTS_DIR) / payload["run_id"] / "batched_cells.jsonl"
    manifest.parent.mkdir(parents=True, exist_ok=True)
    with manifest.open("a", encoding="utf-8") as f:
        f.write(json.dumps({k: v for k, v in out.items() if k != "world"}, sort_keys=True) + "\n")
    results_volume.commit()
    print("RSB_BATCH_CELL " + json.dumps(out, sort_keys=True), flush=True)
    return out


@app.cls(
    image=batched_image,
    gpu="L40S",
    volumes={
        RESULTS_DIR: results_volume,
        MODEL_CACHE_DIR: model_cache_volume,
        ASSET_CACHE_DIR: asset_cache_volume,
        RSB_DATA_VOLUME_DIR: data_volume,
        GSM8K_DATA_VOLUME_DIR: gsm8k_data_volume,
        MMLUQA2_DATA_VOLUME_DIR: mmluqa2_data_volume,
        f"{ROOT}/policy/pi05/checkpoints": pi05_checkpoints_volume,
    },
    timeout=24 * 3600,
    secrets=[hf_secret],
    scaledown_window=900,
    max_containers=16,
)
class RsbPi05SuiteRunner:
    """Warm Modal worker for one RSB suite.

    The old function runner pays pi0.5 restore once per cell. This class pays
    restore once per warm container and, when deployed, lets Modal snapshot the
    post-restore process so new containers can fan out without reloading the
    checkpoint from scratch.
    """

    suite_json: str = modal.parameter(default="")
    ckpt_setting: str = modal.parameter(default="robotwin-pi05")
    policy_overrides_json: str = modal.parameter(default="")
    max_eval_steps: int = modal.parameter(default=0)
    policy_seed: int = modal.parameter(default=0)
    instruction_type: str = modal.parameter(default="unseen")

    @modal.enter()
    def load_policy(self) -> None:
        import faulthandler

        faulthandler.enable()
        _stage("class-enter-setup")
        _prepare_rsb_runtime()
        self.suite = RsbSuite(**json.loads(self.suite_json))
        self.policy_overrides = _parse_policy_overrides(self.policy_overrides_json)
        _ensure_pi05_checkpoint_layout(self.policy_overrides)
        policy_payload = {
            "suite": asdict(self.suite),
            "ckpt_setting": self.ckpt_setting,
            "run_id": "_snapshot",
            "cell_idx": 0,
            "policy_seed": int(self.policy_seed),
            "instruction_type": self.instruction_type,
            "policy_overrides": self.policy_overrides,
        }
        _, usr_args = _load_rsb_config(self.suite, int(self.max_eval_steps or 0), policy_payload)
        serialize_model_load = bool(self.policy_overrides.get("serialize_model_load", True))
        _stage("class-enter-load-model")
        if serialize_model_load:
            _stage("class-enter-load-model-wait-lock")
            self.policy = _with_modal_lock("pi05-model-load", lambda: Pi05BatchPolicy(usr_args))
        else:
            self.policy = Pi05BatchPolicy(usr_args)
        _stage("class-enter-model-ready")

    @modal.method()
    def run_cell(self, payload: dict[str, Any]) -> dict[str, Any]:
        _stage(f"class-run-cell-{payload['cell_idx']}")
        _prepare_rsb_runtime()
        suite = RsbSuite(**payload["suite"])
        if asdict(suite) != asdict(self.suite):
            raise RuntimeError(
                f"Suite runner mismatch: worker={asdict(self.suite)} payload={payload['suite']}"
            )
        return _run_pi05_payload_with_policy(payload, self.policy)


def _pi05_policy_overrides(policy_overrides_json: str) -> dict[str, Any]:
    policy_overrides = _parse_policy_overrides(policy_overrides_json)
    policy_overrides.setdefault("train_config_name", DEFAULT_PI05_TRAIN_CONFIG)
    policy_overrides.setdefault("model_name", DEFAULT_PI05_MODEL_NAME)
    policy_overrides.setdefault("checkpoint_id", DEFAULT_PI05_CHECKPOINT_ID)
    policy_overrides.setdefault("pi0_step", 50)
    policy_overrides.setdefault("serialize_model_load", True)
    return policy_overrides


def _build_batched_pi05_payloads(
    *,
    suites: str = "RSB-Math-4",
    ckpt_setting: str = "robotwin-pi05",
    policy_overrides_json: str = "",
    run_name: str = "baseline",
    run_id: str = "rsb-batched-smoke",
    episodes_per_suite: int = 4,
    batch_size: int = 4,
    seed: int = 0,
    max_steps: int = 1000,
    max_eval_steps: int = 0,
    ledger_interval: int = 25,
    instruction_type: str = "unseen",
    preflight_local_paths: bool = True,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    suite_names = [suite.strip() for suite in suites.split(",") if suite.strip()]
    policy_overrides = _pi05_policy_overrides(policy_overrides_json)

    jobs = build_shard_jobs(
        suite_names=suite_names,
        policy_name="pi05",
        ckpt_setting=ckpt_setting,
        run_id=run_id,
        episodes_per_suite=episodes_per_suite,
        shards_per_suite=1,
        seed=seed,
        policy_seed=0,
        instruction_type=instruction_type,
        policy_overrides=policy_overrides,
    )

    if preflight_local_paths:
        source_requirements = sorted(
            {
                "task_config/_camera_config.yml",
                "task_config/_embodiment_config.yml",
                "task_config/_eval_step_limit.yml",
                *(f"task_config/{job.suite.eval_config}.yml" for job in jobs),
            }
        )
        source_missing = sorted(
            {
                missing
                for missing in missing_local_requirements(
                    RSB_SOURCE_DIR or DEFAULT_RSB_SOURCE,
                    tuple(source_requirements),
                )
            }
        )
        if source_missing:
            rendered = "\n".join(f"  - {path}" for path in source_missing)
            raise RuntimeError(
                "RoboSemanticBench task config files are missing before Modal image build:\n"
                f"{rendered}"
            )

    payloads: list[dict[str, Any]] = []
    cell_idx = 0
    for job in jobs:
        seed_start = 100000 * (1 + int(job.seed))
        episode_start = 0
        for seed_cell in _split_seed_cells(
            seed_start=seed_start,
            episodes=job.episodes,
            batch_size=batch_size,
        ):
            if preflight_local_paths:
                data_requirements = tuple(
                    path
                    for path in local_requirements_for_job(
                        job,
                        expert_data_num=None,
                        checkpoint_num=None,
                    )
                    if not path.startswith("policy/")
                )
                missing = missing_local_requirements(RSB_SOURCE_DIR, data_requirements)
                if missing:
                    rendered = "\n".join(f"  - {path}" for path in missing)
                    raise RuntimeError(f"RoboSemanticBench prerequisites are missing locally:\n{rendered}")
            payloads.append(
                {
                    "suite": asdict(job.suite),
                    "ckpt_setting": ckpt_setting,
                    "run_id": run_id,
                    "run_name": run_name,
                    "cell_idx": cell_idx,
                    "episode_start": episode_start,
                    "seeds": seed_cell,
                    "max_steps": max_steps,
                    "max_eval_steps": max_eval_steps,
                    "ledger_interval": ledger_interval,
                    "instruction_type": instruction_type,
                    "policy_seed": job.policy_seed,
                    "policy_overrides": policy_overrides,
                }
            )
            cell_idx += 1
            episode_start += len(seed_cell)
    return policy_overrides, payloads


def _run_batched_pi05_payloads(
    *,
    payloads: list[dict[str, Any]],
    policy_overrides: dict[str, Any],
    ckpt_setting: str,
    max_eval_steps: int,
    instruction_type: str,
    warm_containers: int,
    run_id: str,
) -> dict[str, Any]:
    payloads_by_suite: dict[str, list[dict[str, Any]]] = {}
    for payload in payloads:
        suite_key = json.dumps(payload["suite"], sort_keys=True)
        payloads_by_suite.setdefault(suite_key, []).append(payload)

    summaries: list[dict[str, Any]] = []
    for suite_key, suite_payloads in payloads_by_suite.items():
        runner = RsbPi05SuiteRunner(
            suite_json=suite_key,
            ckpt_setting=ckpt_setting,
            policy_overrides_json=json.dumps(policy_overrides, sort_keys=True),
            max_eval_steps=max_eval_steps,
            policy_seed=0,
            instruction_type=instruction_type,
        )
        if warm_containers > 0:
            warm_payloads = []
            for idx in range(warm_containers):
                warm_payload = dict(suite_payloads[0])
                warm_payload.update(
                    {
                        "run_id": f"{run_id}-warm",
                        "cell_idx": idx,
                        "episode_start": idx,
                        "seeds": [900000 + idx],
                        "max_steps": 1,
                        "ledger_interval": 1,
                    }
                )
                warm_payloads.append(warm_payload)
            print(f"warming {warm_containers} {json.loads(suite_key)['name']} containers")
            list(runner.run_cell.map(warm_payloads, order_outputs=True))
        summaries.extend(runner.run_cell.map(suite_payloads, order_outputs=True))
    aggregate = aggregate_summaries(summaries)
    write_aggregate.remote(run_id, aggregate)
    return aggregate


def _print_batched_summary(run_id: str, aggregate: dict[str, Any]) -> None:
    aggregate_path = Path(RESULTS_DIR) / run_id / "aggregate.json"
    print("=== RoboSemanticBench batched summary ===")
    print(f"episodes: {aggregate['episodes']}")
    print(f"task_success_rate: {aggregate['task_success_rate']:.4f}")
    print(f"grasp_success_rate: {aggregate['grasp_success_rate']:.4f}")
    print(f"aggregate_path: {aggregate_path}")
    for suite_summary in aggregate["suites"]:
        nsg = suite_summary["normalized_semantic_grounding"]
        nsg_text = "undefined" if nsg is None else f"{nsg:.4f}"
        print(
            f"  {suite_summary['suite']}: TSR={suite_summary['task_success_rate']:.4f} "
            f"GSR={suite_summary['grasp_success_rate']:.4f} nSG={nsg_text} "
            f"episodes={suite_summary['episodes']}"
        )


@app.function(
    image=batched_image,
    volumes={RESULTS_DIR: results_volume},
    timeout=24 * 3600,
    secrets=[hf_secret],
)
def run_batched_pi05_job(
    suites: str = "RSB-Math-4",
    ckpt_setting: str = "robotwin-pi05",
    policy_overrides_json: str = "",
    run_name: str = "baseline",
    run_id: str = "rsb-batched-overnight",
    episodes_per_suite: int = 500,
    batch_size: int = 4,
    seed: int = 0,
    max_steps: int = 1000,
    max_eval_steps: int = 0,
    ledger_interval: int = 100,
    instruction_type: str = "unseen",
    warm_containers: int = 4,
) -> dict[str, Any]:
    """Modal-side orchestrator for overnight/detached RSB runs."""
    policy_overrides, payloads = _build_batched_pi05_payloads(
        suites=suites,
        ckpt_setting=ckpt_setting,
        policy_overrides_json=policy_overrides_json,
        run_name=run_name,
        run_id=run_id,
        episodes_per_suite=episodes_per_suite,
        batch_size=batch_size,
        seed=seed,
        max_steps=max_steps,
        max_eval_steps=max_eval_steps,
        ledger_interval=ledger_interval,
        instruction_type=instruction_type,
        preflight_local_paths=False,
    )
    aggregate = _run_batched_pi05_payloads(
        payloads=payloads,
        policy_overrides=policy_overrides,
        ckpt_setting=ckpt_setting,
        max_eval_steps=max_eval_steps,
        instruction_type=instruction_type,
        warm_containers=warm_containers,
        run_id=run_id,
    )
    _print_batched_summary(run_id, aggregate)
    results_volume.commit()
    return aggregate


@app.local_entrypoint()
def batched_pi05(
    suites: str = "RSB-Math-4",
    ckpt_setting: str = "robotwin-pi05",
    policy_overrides_json: str = "",
    run_name: str = "baseline",
    run_id: str = "rsb-batched-smoke",
    episodes_per_suite: int = 4,
    batch_size: int = 4,
    seed: int = 0,
    max_steps: int = 1000,
    max_eval_steps: int = 0,
    ledger_interval: int = 25,
    instruction_type: str = "unseen",
    warm_containers: int = 0,
):
    """Run RSB pi0.5 with Archetype entity batching."""
    policy_overrides, payloads = _build_batched_pi05_payloads(
        suites=suites,
        ckpt_setting=ckpt_setting,
        policy_overrides_json=policy_overrides_json,
        run_name=run_name,
        run_id=run_id,
        episodes_per_suite=episodes_per_suite,
        batch_size=batch_size,
        seed=seed,
        max_steps=max_steps,
        max_eval_steps=max_eval_steps,
        ledger_interval=ledger_interval,
        instruction_type=instruction_type,
        preflight_local_paths=True,
    )
    aggregate = _run_batched_pi05_payloads(
        payloads=payloads,
        policy_overrides=policy_overrides,
        ckpt_setting=ckpt_setting,
        max_eval_steps=max_eval_steps,
        instruction_type=instruction_type,
        warm_containers=warm_containers,
        run_id=run_id,
    )
    _print_batched_summary(run_id, aggregate)


@app.local_entrypoint()
def submit_batched_pi05(
    suites: str = "RSB-Math-4",
    ckpt_setting: str = "robotwin-pi05",
    policy_overrides_json: str = "",
    run_name: str = "baseline",
    run_id: str = "rsb-batched-overnight",
    episodes_per_suite: int = 500,
    batch_size: int = 4,
    seed: int = 0,
    max_steps: int = 1000,
    max_eval_steps: int = 0,
    ledger_interval: int = 100,
    instruction_type: str = "unseen",
    warm_containers: int = 4,
):
    """Submit an overnight-safe Modal-side RSB run and exit."""
    _build_batched_pi05_payloads(
        suites=suites,
        ckpt_setting=ckpt_setting,
        policy_overrides_json=policy_overrides_json,
        run_name=run_name,
        run_id=run_id,
        episodes_per_suite=episodes_per_suite,
        batch_size=batch_size,
        seed=seed,
        max_steps=max_steps,
        max_eval_steps=max_eval_steps,
        ledger_interval=ledger_interval,
        instruction_type=instruction_type,
        preflight_local_paths=True,
    )
    config = {
        "suites": suites,
        "ckpt_setting": ckpt_setting,
        "policy_overrides_json": policy_overrides_json,
        "run_name": run_name,
        "run_id": run_id,
        "episodes_per_suite": episodes_per_suite,
        "batch_size": batch_size,
        "seed": seed,
        "max_steps": max_steps,
        "max_eval_steps": max_eval_steps,
        "ledger_interval": ledger_interval,
        "instruction_type": instruction_type,
        "warm_containers": warm_containers,
    }
    try:
        deployed_job = modal.Function.from_name("archetype-robosemantic", "run_batched_pi05_job")
        call = deployed_job.spawn(**config)
    except Exception:
        call = run_batched_pi05_job.spawn(**config)
    call_id = getattr(call, "object_id", None) or getattr(call, "function_call_id", None) or str(call)
    print("=== RoboSemanticBench detached submission ===")
    print(f"run_id: {run_id}")
    print(f"function_call_id: {call_id}")
    print("logs: modal app logs archetype-robosemantic -f")
    print(f"aggregate_path: {Path(RESULTS_DIR) / run_id / 'aggregate.json'}")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Archetype-native batched RSB runner")
    parser.add_argument("--suites", default="RSB-Math-4")
    parser.add_argument("--ckpt-setting", default="robotwin-pi05")
    parser.add_argument("--policy-overrides-json", default="")
    parser.add_argument("--run-name", default="baseline")
    parser.add_argument("--run-id", default="rsb-batched-smoke")
    parser.add_argument("--episodes-per-suite", type=int, default=4)
    parser.add_argument("--batch-size", type=int, default=4)
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument("--max-steps", type=int, default=1000)
    parser.add_argument("--max-eval-steps", type=int, default=0)
    parser.add_argument("--ledger-interval", type=int, default=25)
    parser.add_argument("--instruction-type", default="unseen")
    parser.add_argument("--warm-containers", type=int, default=0)
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    batched_pi05(
        suites=args.suites,
        ckpt_setting=args.ckpt_setting,
        policy_overrides_json=args.policy_overrides_json,
        run_name=args.run_name,
        run_id=args.run_id,
        episodes_per_suite=args.episodes_per_suite,
        batch_size=args.batch_size,
        seed=args.seed,
        max_steps=args.max_steps,
        max_eval_steps=args.max_eval_steps,
        ledger_interval=args.ledger_interval,
        instruction_type=args.instruction_type,
        warm_containers=args.warm_containers,
    )
