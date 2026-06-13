# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Modal-native RoboSemanticBench runner.

RoboSemanticBench (RSB) is not a LIBERO prompt variant. It is a RoboTwin /
SAPIEN benchmark with Aloha-AgileX answer-block tasks and RSB metrics
(TSR, GSR, nSG). This runner keeps that boundary explicit:

- copy the RSB checkout into a dedicated Modal image;
- run one RSB suite/shard per Modal function invocation;
- aggregate Task Success Rate, Grasp Success Rate, and normalized Semantic
  Grounding without routing through the LIBERO eval stack.

The default local RSB checkout is the one cloned during this investigation.
Override it with ``RSB_SOURCE_DIR`` if needed.
"""

from __future__ import annotations

import argparse
import json
import os
from dataclasses import asdict
from pathlib import Path
from typing import Any

import modal

from bench.robosemantic.protocol import (
    RSB_SUITES,
    RsbShardJob,
    RsbSuite,
    aggregate_summaries,
    build_shard_jobs,
    curobo_install_command,
    local_requirements_for_job,
    missing_local_requirements,
    normalized_semantic_grounding,
    rsb_eval_requirements_install_command,
)

ROOT = "/rsb"
RESULTS_DIR = "/results"
MODEL_CACHE_DIR = "/models"
ASSET_CACHE_DIR = "/asset-cache"
RSB_DATA_VOLUME_DIR = "/volumes/rsb-data"
GSM8K_DATA_VOLUME_DIR = "/volumes/gsm8k-data"
MMLUQA2_DATA_VOLUME_DIR = "/volumes/mmluqa2-data"
DEFAULT_RSB_SOURCE = "/Users/darin/src/vendor/github.com/ZGC-EmbodyAI/RoboSemanticBench"
RSB_SOURCE_DIR = os.environ.get("RSB_SOURCE_DIR", DEFAULT_RSB_SOURCE)
ARCHETYPE_BENCH_DIR = Path(__file__).resolve().parents[1]
DEFAULT_SUITE_ARG = ",".join(suite.name for suite in RSB_SUITES)
DEFAULT_PI05_TRAIN_CONFIG = "pi05_aloha_full_base"
LEGACY_PI05_CHECKPOINT_LAYOUT_CONFIG = "pi05_base_aloha_lora"
DEFAULT_PI05_MODEL_NAME = "robotwin_pi05_aloha_agilex_randomized_5tasks_step20000"
DEFAULT_PI05_CHECKPOINT_ID = 20000


def hf_cache_env(model_cache_dir: str = MODEL_CACHE_DIR) -> dict[str, str]:
    """Hugging Face cache env pointed at a Modal volume."""
    return {
        "HF_HOME": f"{model_cache_dir}/huggingface",
        "HF_HUB_CACHE": f"{model_cache_dir}/huggingface/hub",
        "TRANSFORMERS_CACHE": f"{model_cache_dir}/huggingface/hub",
        "HF_XET_HIGH_PERFORMANCE": "1",
    }


image = (
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
        # RSB install applies two source patches needed by SAPIEN/MPLib.
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
    .add_local_dir(
        ARCHETYPE_BENCH_DIR,
        "/root/bench",
        copy=True,
        ignore=["**/__pycache__"],
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

app = modal.App("archetype-robosemantic", image=image)
hf_secret = modal.Secret.from_name("hf-token")
results_volume = modal.Volume.from_name("robosemantic-results", create_if_missing=True)
data_volume = modal.Volume.from_name("robosemantic-rsb-data", create_if_missing=True)
checkpoints_volume = modal.Volume.from_name("robosemantic-rsb-checkpoints", create_if_missing=True)
pi05_checkpoints_volume = modal.Volume.from_name(
    "robosemantic-rsb-pi05-checkpoints",
    create_if_missing=True,
)
model_cache_volume = modal.Volume.from_name("robosemantic-model-cache", create_if_missing=True)
asset_cache_volume = modal.Volume.from_name("robosemantic-rsb-assets", create_if_missing=True)
gsm8k_data_volume = modal.Volume.from_name("robosemantic-rsb-gsm8k-data", create_if_missing=True)
mmluqa2_data_volume = modal.Volume.from_name("robosemantic-rsb-mmluqa2-data", create_if_missing=True)


@app.function(
    gpu="L40S",
    volumes={
        RESULTS_DIR: results_volume,
        MODEL_CACHE_DIR: model_cache_volume,
        ASSET_CACHE_DIR: asset_cache_volume,
        RSB_DATA_VOLUME_DIR: data_volume,
        GSM8K_DATA_VOLUME_DIR: gsm8k_data_volume,
        MMLUQA2_DATA_VOLUME_DIR: mmluqa2_data_volume,
        f"{ROOT}/policy/DP/checkpoints": checkpoints_volume,
        f"{ROOT}/policy/pi05/checkpoints": pi05_checkpoints_volume,
    },
    timeout=24 * 3600,
    secrets=[hf_secret],
    enable_memory_snapshot=True,
)
def run_shard(job_payload: dict[str, Any]) -> dict[str, Any]:
    """Run one RSB suite shard in Modal."""
    import faulthandler
    import importlib
    import json
    import shutil
    import subprocess
    import sys
    import time
    from collections import defaultdict
    from pathlib import Path

    import yaml

    faulthandler.enable()

    def stage(name: str) -> None:
        print(f"RSB_STAGE {name}", flush=True)

    timings: dict[str, float] = defaultdict(float)
    timing_counts: dict[str, int] = defaultdict(int)

    def record_span(name: str, started: float) -> None:
        timings[name] += time.perf_counter() - started
        timing_counts[name] += 1

    def timed_call(name: str, fn, *args, **kwargs):
        span_started = time.perf_counter()
        try:
            return fn(*args, **kwargs)
        finally:
            record_span(name, span_started)

    def emit_timing(name: str, total_wall_s: float | None = None) -> None:
        payload: dict[str, Any] = {
            "name": name,
            "shard_idx": job_payload.get("shard_idx"),
            "episode_start": job_payload.get("episode_start"),
            "episodes": job_payload.get("episodes"),
            "spans_s": {key: round(value, 3) for key, value in sorted(timings.items())},
            "counts": dict(sorted(timing_counts.items())),
        }
        if total_wall_s is not None:
            payload["total_wall_s"] = round(total_wall_s, 3)
            if total_wall_s > 0:
                payload["eval_pct"] = {
                    key: round(value / total_wall_s * 100, 1)
                    for key, value in sorted(timings.items())
                    if key != "load_model"
                }
                if "load_model" in timings:
                    payload["cold_start_s"] = {"load_model": round(timings["load_model"], 3)}
        print("RSB_TIMING " + json.dumps(payload, sort_keys=True), flush=True)

    def with_modal_lock(lock_name: str, fn):
        lock_path = Path(MODEL_CACHE_DIR) / f".{lock_name}.lock"
        owns_lock = False
        for _ in range(720):
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

    stage("start")
    print(
        "RSB_SHARD "
        + json.dumps(
            {
                "run_id": job_payload.get("run_id"),
                "shard_idx": job_payload.get("shard_idx"),
                "episodes": job_payload.get("episodes"),
                "episode_start": job_payload.get("episode_start"),
                "policy_name": job_payload.get("policy_name"),
            },
            sort_keys=True,
        ),
        flush=True,
    )
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

    link_volume_dir("data", RSB_DATA_VOLUME_DIR)
    link_volume_dir("gsm8k/data", GSM8K_DATA_VOLUME_DIR)
    link_volume_dir("mmluqa2/data", MMLUQA2_DATA_VOLUME_DIR)
    stage("linked-volumes")

    def ensure_rsb_assets() -> None:
        """Populate the RSB asset volume once and link it into the checkout."""
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

        subprocess.run(
            [sys.executable, "./script/update_embodiment_config_path.py"],
            cwd=ROOT,
            check=True,
        )

    ensure_rsb_assets()
    stage("assets-ready")

    eval_policy_path = Path(ROOT) / "script" / "eval_policy.py"
    eval_policy_text = eval_policy_path.read_text(encoding="utf-8")
    if 'print("error occurs during eval setup !")' in eval_policy_text:
        eval_policy_text = eval_policy_text.replace(
            '            print("error occurs during eval setup !")\n'
            "            continue\n",
            '            print("error occurs during eval setup !", flush=True)\n'
            "            traceback.print_exc()\n"
            "            raise\n",
        )
        eval_policy_path.write_text(eval_policy_text, encoding="utf-8")

    if job_payload["policy_name"] == "pi05":
        try:
            import pytest  # noqa: F401
        except ImportError:
            stage("install-pi05-runtime-deps")
            subprocess.run(["uv", "pip", "install", "--system", "pytest"], check=True)
        policy_config_path = Path(ROOT) / "policy" / "pi05" / "src" / "openpi" / "policies" / "policy_config.py"
        policy_config_text = policy_config_path.read_text(encoding="utf-8")
        if "data_config.asset_id = robotwin_repo_id" in policy_config_text:
            policy_config_text = policy_config_text.replace(
                "import logging\n",
                "import dataclasses\nimport logging\n",
            )
            policy_config_text = policy_config_text.replace(
                "            data_config.asset_id = robotwin_repo_id\n",
                "            data_config = dataclasses.replace(data_config, asset_id=robotwin_repo_id)\n",
            )
            policy_config_path.write_text(policy_config_text, encoding="utf-8")
        pi_model_path = Path(ROOT) / "policy" / "pi05" / "pi_model.py"
        pi_model_text = pi_model_path.read_text(encoding="utf-8")
        if 'checkpoint_root = os.environ.get("PI05_CHECKPOINT_ROOT", "policy/pi05/checkpoints")' not in pi_model_text:
            pi_model_text = pi_model_text.replace(
                '        specified_path = f"policy/pi05/checkpoints/{self.train_config_name}/{self.model_name}/{self.checkpoint_id}/assets/"\n',
                '        checkpoint_root = os.environ.get("PI05_CHECKPOINT_ROOT", "policy/pi05/checkpoints")\n'
                '        specified_path = f"{checkpoint_root}/{self.train_config_name}/{self.model_name}/{self.checkpoint_id}/assets/"\n',
            )
            pi_model_text = pi_model_text.replace(
                '            f"policy/pi05/checkpoints/{self.train_config_name}/{self.model_name}/{self.checkpoint_id}",\n',
                '            f"{checkpoint_root}/{self.train_config_name}/{self.model_name}/{self.checkpoint_id}",\n',
            )
            pi_model_path.write_text(pi_model_text, encoding="utf-8")
    base_task_path = Path(ROOT) / "envs" / "_base_task.py"
    base_task_text = base_task_path.read_text(encoding="utf-8")
    noisy_step_print = (
        '        print(f"step: \\033[92m{self.take_action_cnt} / {self.step_lim}\\033[0m", end="\\r")\n'
    )
    if noisy_step_print in base_task_text:
        base_task_text = base_task_text.replace(
            noisy_step_print,
            '        if getattr(self, "eval_step_log", False):\n'
            '            print(f"step: \\033[92m{self.take_action_cnt} / {self.step_lim}\\033[0m", end="\\r")\n',
        )
        base_task_path.write_text(base_task_text, encoding="utf-8")
    stage("runtime-deps-ready")

    job = RsbShardJob(
        suite=RsbSuite(**job_payload["suite"]),
        policy_name=job_payload["policy_name"],
        ckpt_setting=job_payload["ckpt_setting"],
        run_id=job_payload["run_id"],
        shard_idx=int(job_payload["shard_idx"]),
        episode_start=int(job_payload.get("episode_start", 0)),
        episodes=int(job_payload["episodes"]),
        seed=int(job_payload["seed"]),
        policy_seed=int(job_payload.get("policy_seed", 0)),
        commit_every_episodes=int(job_payload.get("commit_every_episodes", 1)),
        instruction_type=job_payload.get("instruction_type", "unseen"),
        policy_overrides=dict(job_payload.get("policy_overrides", {})),
    )
    if job.policy_name == "pi05":
        train_config_name = str(job.policy_overrides.get("train_config_name", DEFAULT_PI05_TRAIN_CONFIG))
        model_name = str(job.policy_overrides.get("model_name", DEFAULT_PI05_MODEL_NAME))
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
        if bool(job.policy_overrides.get("stage_checkpoint_local", True)):
            local_checkpoint_root = Path("/tmp/rsb-pi05-checkpoints")
            local_model_path = local_checkpoint_root / train_config_name / model_name
            local_checkpoint_path = local_model_path / str(job.policy_overrides.get("checkpoint_id", DEFAULT_PI05_CHECKPOINT_ID))
            source_model_path = (checkpoint_root / train_config_name / model_name).resolve()
            source_checkpoint_path = source_model_path / str(job.policy_overrides.get("checkpoint_id", DEFAULT_PI05_CHECKPOINT_ID))
            if not source_checkpoint_path.exists():
                raise RuntimeError(f"pi0.5 checkpoint is missing before local staging: {source_checkpoint_path}")
            if not local_checkpoint_path.exists():
                stage("stage-pi05-checkpoint-local")
                local_model_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.copytree(source_model_path, local_model_path, symlinks=False, dirs_exist_ok=True)
            os.environ["PI05_CHECKPOINT_ROOT"] = str(local_checkpoint_root)
            stage("pi05-checkpoint-local-ready")
    expert_data_num = job.policy_overrides.get("expert_data_num")
    checkpoint_num = job.policy_overrides.get("checkpoint_num")
    missing = missing_local_requirements(
        ROOT,
        local_requirements_for_job(
            job,
            expert_data_num=int(expert_data_num) if expert_data_num is not None else None,
            checkpoint_num=int(checkpoint_num) if checkpoint_num is not None else None,
        ),
    )
    if missing:
        rendered = "\n".join(f"  - {path}" for path in missing)
        raise RuntimeError(f"RoboSemanticBench prerequisites are missing in Modal:\n{rendered}")
    stage("requirements-ready")

    stage("import-eval-policy")
    eval_policy_mod = importlib.import_module("script.eval_policy")
    stage("load-policy-decorator")
    get_model = eval_policy_mod.eval_function_decorator(job.policy_name, "get_model")
    stage("load-task-class")
    task_env = eval_policy_mod.class_decorator(job.suite.task_name)

    def wrap_task_method(method_name: str, span_name: str) -> None:
        original = getattr(task_env, method_name, None)
        if original is None:
            return

        def wrapped_method(*method_args, **method_kwargs):
            return timed_call(span_name, original, *method_args, **method_kwargs)

        setattr(task_env, method_name, wrapped_method)

    for method_name, span_name in (
        ("setup_scene", "setup_scene"),
        ("create_table_and_wall", "setup_table_wall"),
        ("load_robot", "setup_load_robot"),
        ("load_camera", "setup_load_camera"),
        ("move_to_homestate", "setup_move_to_homestate"),
        ("together_open_gripper", "setup_open_gripper"),
        ("load_actors", "setup_load_actors"),
        ("get_cluttered_table", "setup_cluttered_table"),
        ("check_stable", "setup_check_stable"),
    ):
        wrap_task_method(method_name, span_name)

    with open(f"{ROOT}/policy/{job.policy_name}/deploy_policy.yml", encoding="utf-8") as f:
        usr_args = yaml.safe_load(f)
    expert_check = bool(job.policy_overrides.get("expert_check", False))
    usr_args.update(job.policy_overrides)
    usr_args.update(
        {
            "task_name": job.suite.task_name,
            "task_config": job.suite.eval_config,
            "ckpt_setting": job.ckpt_setting,
            "policy_name": job.policy_name,
            # RSB policy loaders use this as part of the checkpoint path.
            "seed": job.policy_seed,
            "instruction_type": job.instruction_type,
            "expert_check": expert_check,
            "eval_metadata_log": True,
        }
    )

    with open(f"{ROOT}/task_config/{job.suite.eval_config}.yml", encoding="utf-8") as f:
        args = yaml.safe_load(f)
    max_eval_steps = int(job.policy_overrides.get("max_eval_steps", 0) or 0)
    if max_eval_steps > 0:
        step_limit_path = Path(ROOT) / "task_config" / "_eval_step_limit.yml"
        step_limits = yaml.safe_load(step_limit_path.read_text(encoding="utf-8"))
        original_limit = int(step_limits.get(job.suite.task_name, 1000))
        step_limits[job.suite.task_name] = min(original_limit, max_eval_steps)
        step_limit_path.write_text(yaml.safe_dump(step_limits, sort_keys=False), encoding="utf-8")
    args.update(
        {
            "task_name": job.suite.task_name,
            "task_config": job.suite.eval_config,
            "ckpt_setting": job.ckpt_setting,
            "policy_name": job.policy_name,
            "expert_check": expert_check,
            "eval_metadata_log": True,
        }
    )

    # This mirrors script.eval_policy.main setup without its hard-coded
    # test_num=500, so Modal can shard the benchmark natively.
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
    usr_args["left_arm_dim"] = len(args["left_embodiment_config"]["arm_joints_name"][0])
    usr_args["right_arm_dim"] = len(args["right_embodiment_config"]["arm_joints_name"][1])
    stage("configs-ready")

    save_dir = Path(RESULTS_DIR) / job.run_id / job.suite.task_name / f"shard{job.shard_idx}"
    if save_dir.exists() and any(path.name != "episode_metadata" for path in save_dir.iterdir()):
        raise RuntimeError(
            "Refusing to reuse non-empty RSB result path "
            f"{save_dir}. Choose a fresh --run-id or remove the old shard."
        )
    save_dir.mkdir(parents=True, exist_ok=True)
    metadata_dir = save_dir / "episode_metadata"
    metadata_dir.mkdir(parents=True, exist_ok=True)
    args["eval_save_dir"] = save_dir
    args["eval_metadata_save_dir"] = metadata_dir
    args["eval_video_log"] = False

    original_setup_demo = task_env.setup_demo

    def setup_demo_with_episode_offset(*setup_args, **setup_kwargs):
        if "now_ep_num" in setup_kwargs:
            setup_kwargs["now_ep_num"] = int(setup_kwargs["now_ep_num"]) + job.episode_start
        return timed_call("setup_demo", original_setup_demo, *setup_args, **setup_kwargs)

    task_env.setup_demo = setup_demo_with_episode_offset

    original_get_obs = task_env.get_obs

    def get_obs_with_timing(*obs_args, **obs_kwargs):
        return timed_call("get_obs", original_get_obs, *obs_args, **obs_kwargs)

    task_env.get_obs = get_obs_with_timing

    original_take_action = task_env.take_action

    def take_action_with_timing(*action_args, **action_kwargs):
        return timed_call("take_action", original_take_action, *action_args, **action_kwargs)

    task_env.take_action = take_action_with_timing

    if job.commit_every_episodes:
        original_close_env = task_env.close_env
        last_committed_episode = -1

        def close_env_with_volume_commit(*close_args, **close_kwargs):
            nonlocal last_committed_episode
            result = timed_call("close_env", original_close_env, *close_args, **close_kwargs)
            current_episode = int(getattr(task_env, "test_num", -1))
            metadata_path = metadata_dir / f"episode{current_episode}.json"
            should_commit = (
                current_episode >= 0
                and current_episode != last_committed_episode
                and metadata_path.exists()
                and (current_episode + 1) % job.commit_every_episodes == 0
            )
            if should_commit:
                results_volume.commit()
                last_committed_episode = current_episode
            return result

        task_env.close_env = close_env_with_volume_commit

    stage("load-model")
    serialize_model_load = bool(job.policy_overrides.get("serialize_model_load", job.policy_name == "pi05"))
    if serialize_model_load:
        stage("load-model-wait-lock")
        model = with_modal_lock(
            f"{job.policy_name}-model-load",
            lambda: timed_call("load_model", get_model, usr_args),
        )
    else:
        model = timed_call("load_model", get_model, usr_args)
    stage("model-ready")
    if hasattr(model, "policy") and hasattr(model.policy, "infer"):
        original_policy_infer = model.policy.infer

        def infer_with_timing(*infer_args, **infer_kwargs):
            return timed_call("policy_infer", original_policy_infer, *infer_args, **infer_kwargs)

        model.policy.infer = infer_with_timing
    st_seed = 100000 * (1 + job.seed)
    started = time.perf_counter()
    stage("eval-start")
    final_seed, successes, grasps = eval_policy_mod.eval_policy(
        job.suite.task_name,
        task_env,
        args,
        model,
        st_seed,
        test_num=job.episodes,
        video_size=None,
        instruction_type=job.instruction_type,
    )
    stage("eval-done")
    wall_s = time.perf_counter() - started
    emit_timing("eval", wall_s)

    tsr = successes / job.episodes
    gsr = grasps / job.episodes
    result_text = (
        f"Task Success Rate: {tsr}\n"
        f"Grasp Success Rate: {gsr}\n"
        f"Episode Start: {job.episode_start}\n"
        f"Start Seed: {st_seed}\n"
        f"Final Seed: {final_seed}\n"
    )
    result_path = save_dir / "_result.txt"
    result_path.write_text(result_text, encoding="utf-8")
    (save_dir / "job.json").write_text(json.dumps(asdict(job), indent=2), encoding="utf-8")
    results_volume.commit()
    model_cache_volume.commit()

    return {
        "suite": job.suite.name,
        "task_name": job.suite.task_name,
        "task_config": job.suite.eval_config,
        "choices": job.suite.choices,
        "policy_name": job.policy_name,
        "ckpt_setting": job.ckpt_setting,
        "shard_idx": job.shard_idx,
        "episode_start": job.episode_start,
        "episodes": job.episodes,
        "successes": successes,
        "grasp_successes": grasps,
        "task_success_rate": tsr,
        "grasp_success_rate": gsr,
        "normalized_semantic_grounding": normalized_semantic_grounding(
            tsr=tsr,
            gsr=gsr,
            choices=job.suite.choices,
        ),
        "wall_s": round(wall_s, 1),
        "results_path": str(save_dir),
    }


@app.function(volumes={RESULTS_DIR: results_volume}, timeout=300)
def write_aggregate(run_id: str, aggregate: dict[str, Any]) -> str:
    """Persist aggregate RSB metrics in the Modal results volume."""
    from pathlib import Path

    aggregate_path = Path(RESULTS_DIR) / run_id / "aggregate.json"
    aggregate_path.parent.mkdir(parents=True, exist_ok=True)
    aggregate_path.write_text(json.dumps(aggregate, indent=2), encoding="utf-8")
    results_volume.commit()
    return str(aggregate_path)


def _parse_policy_overrides(policy_overrides_json: str) -> dict[str, Any]:
    if not policy_overrides_json:
        return {}
    parsed = json.loads(policy_overrides_json)
    if not isinstance(parsed, dict):
        raise ValueError("--policy-overrides-json must decode to a JSON object")
    return parsed


@app.local_entrypoint()
def main(
    suites: str = DEFAULT_SUITE_ARG,
    policy_name: str = "DP",
    ckpt_setting: str = "default",
    expert_data_num: int = 50,
    checkpoint_num: int = 600,
    policy_seed: int = 0,
    policy_overrides_json: str = "",
    expert_check: bool = False,
    max_eval_steps: int = 0,
    run_id: str = "rsb-smoke",
    episodes_per_suite: int = 500,
    shards_per_suite: int = 1,
    seed: int = 0,
    commit_every_episodes: int = 1,
    instruction_type: str = "unseen",
):
    """Run RSB suite shards in parallel on Modal."""
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
        policy_overrides.setdefault("serialize_model_load", True)
    policy_overrides.setdefault("expert_check", expert_check)
    if max_eval_steps > 0:
        policy_overrides.setdefault("max_eval_steps", max_eval_steps)

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
            for missing in missing_local_requirements(RSB_SOURCE_DIR, tuple(source_requirements))
        }
    )
    if source_missing:
        rendered = "\n".join(f"  - {path}" for path in source_missing)
        raise RuntimeError(
            "RoboSemanticBench task config files are missing before Modal image build:\n"
            f"{rendered}"
        )
    summaries = list(run_shard.map([asdict(job) for job in jobs], order_outputs=True))
    aggregate = aggregate_summaries(summaries)
    aggregate_path = write_aggregate.remote(run_id, aggregate)
    print("=== RoboSemanticBench summary ===")
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
    parser = argparse.ArgumentParser(description="RoboSemanticBench Modal runner")
    parser.add_argument("--suites", default=DEFAULT_SUITE_ARG)
    parser.add_argument("--policy-name", default="DP")
    parser.add_argument("--ckpt-setting", default="default")
    parser.add_argument("--expert-data-num", type=int, default=50)
    parser.add_argument("--checkpoint-num", type=int, default=600)
    parser.add_argument("--policy-seed", type=int, default=0)
    parser.add_argument("--policy-overrides-json", default="")
    parser.add_argument("--expert-check", action="store_true")
    parser.add_argument("--max-eval-steps", type=int, default=0)
    parser.add_argument("--run-id", default="rsb-local")
    parser.add_argument("--episodes-per-suite", type=int, default=500)
    parser.add_argument("--shards-per-suite", type=int, default=1)
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument("--commit-every-episodes", type=int, default=1)
    parser.add_argument("--instruction-type", default="unseen")
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
        expert_check=args.expert_check,
        max_eval_steps=args.max_eval_steps,
        run_id=args.run_id,
        episodes_per_suite=args.episodes_per_suite,
        shards_per_suite=args.shards_per_suite,
        seed=args.seed,
        commit_every_episodes=args.commit_every_episodes,
        instruction_type=args.instruction_type,
    )
