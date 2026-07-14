# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Modernized LIBERO image — Archetype + LIBERO in ONE Python 3.12 env.

The whole premise of ``modal_worker.py`` was that LIBERO "can never live in the
Archetype process" because of its dependency pins. That premise is laziness,
not law. This image rebuilds LIBERO on a modern stack so it runs **in the same
Python 3.12 interpreter as Archetype**, as a normal in-process dependency:

- Python 3.12 (not 3.8-3.10). **This is the actual win** — not new robot libs.
- modern ``torch`` (no ``<2.6`` pin) — the only reason for that pin was
  ``torch.load``'s ``weights_only`` flip, patched in ``in_process.py``.
- ``robosuite==1.4.1`` kept on purpose: robosuite 1.5 removed ``SingleArmEnv``
  and ``load_controller_config``, so LIBERO @ this SHA fails at *import* on 1.5.
  1.4.1 has no py3.12 exclusion; we float its transitive pins (LIBERO's exact
  numpy/numba/scipy/opencv have no cp312 wheels) and keep ``numpy<2``.
- LIBERO cloned + editable-installed (``--no-deps``) at a pinned SHA, then
  Archetype installed on top so ``import archetype`` and ``import libero`` share
  one interpreter.

The **env** runs in-process: ``InProcessLiberoEnvClient`` drives
``OffScreenRenderEnv`` directly and the existing ``_EnvStepper`` ``@daft.cls``
steps it statefully — no env ``.remote()``, no env container split, no
env-state-loss. Modal is used for what genuinely needs it: a Linux+EGL host and
a GPU.

SCOPE (do not overclaim): "in-process" applies to the ENV. The VLA **policy**
(``VlaJepaPolicyClient``) is still a cross-app Modal ``.remote()`` RPC to the
separate ``vla_jepa_worker`` app — so the policy half is NOT in-process yet, and
the frames it reads must cross a Modal Volume between containers (see
``optimize_task``). Migrating the policy in-process (or formally accepting the
RPC) is an open architecture decision; until then this image runs the env
locally and the policy remotely.

The pin matrix below is research-verified against openpi / OpenVLA / LeRobot
(all three pin robosuite 1.4.1). The one residual unknown is robosuite-1.4.1
*on py3.12 specifically* (the ecosystem stays on 3.8-3.10) — expected to be
numpy<2 deprecation noise, not a wall. The smoke entrypoint confirms it:

    modal run bench/libero/image.py                # builds image, in-process smoke
    modal run bench/libero/image.py::eval_task      # batched eval on real LIBERO
    modal run bench/libero/image.py::optimize_task  # instruction optimization on the VLA
"""

# NOTE: keep annotations real (no `from __future__ import annotations`) — Modal
# introspects some signatures at build/registration time.
from pathlib import Path

import modal

# Pinned 2026-06-12: git ls-remote LIBERO HEAD.
_LIBERO_SHA = "8f1084e3132a39270c3a13ebe37270a43ece2a01"
_REPO_MOUNT = "/opt/archetype"


def _local_repo_root() -> str:
    """Checkout root for ``add_local_dir`` during image build.

    Modal re-imports this module on the worker as ``/root/image.py`` (not
    ``bench/libero/image.py``), so ``Path(__file__).parents[2]`` crash-loops.
    Walk up for ``pyproject.toml`` locally; fall back to the image mount.
    """
    for ancestor in Path(__file__).resolve().parents:
        if (ancestor / "pyproject.toml").is_file():
            return str(ancestor)
    return _REPO_MOUNT


# Exclude local runtime state from the image snapshot. ``add_local_dir(copy=True)``
# hashes every file; live LanceDB/audit writes under ``archetype_data/`` change
# mid-build and Modal aborts with "modified during build process" (crash loop).
_LOCAL_DIR_IGNORE = [
    "**/.git",
    "**/__pycache__",
    "**/.venv",
    "**/.pytest_cache",
    "**/.ruff_cache",
    "**/.mypy_cache",
    "**/archetype_data",
    "**/archetype_data/**",
    "**/data",
    "**/data/**",
    "**/*.lance",
    "**/*.lance/**",
    "**/mutants",
    "**/target",
    "**/node_modules",
    "**/.context",
    "**/bench-results.json",
    "**/eval-results.json",
]

FRAMES_VOLUME_NAME = "libero-frames"
FRAMES_MOUNT = "/frames"
frames_volume = modal.Volume.from_name(FRAMES_VOLUME_NAME, create_if_missing=True)

image = (
    modal.Image.debian_slim(python_version="3.12")
    .apt_install(
        "git",
        "libegl1",
        "libgl1",
        "libglew-dev",
        "libosmesa6-dev",
        "libglfw3",
        "patchelf",
        "libglib2.0-0",
        "libsm6",
        "libxext6",
        "libxrender1",
    )
    .pip_install(
        # VERIFIED MATRIX (compat research, cross-checked vs openpi/OpenVLA/LeRobot
        # which ALL pin robosuite 1.4.1). The win is Python *3.12*, not new robot
        # libs: robosuite 1.5 REMOVED SingleArmEnv and load_controller_config, so
        # LIBERO @ 8f1084e fails at import on 1.5. Keep robosuite 1.4.1 — it has no
        # py3.12 exclusion — and FLOAT its transitive pins upward (LIBERO's exact
        # numpy 1.22 / numba 0.53 / scipy 1.10 / opencv 4.6 have no cp312 wheels).
        "torch>=2.6",  # env layer is torch-agnostic; init-state pickles patched in in_process.py
        "robosuite==1.4.1",
        "mujoco==3.2.3",  # openpi-verified; avoid 3.1.1 (robosuite domain-randomization note)
        "bddl==1.0.1",  # LIBERO-vendored API; bddl 3.x (BEHAVIOR-1K) is NOT a drop-in
        "numpy>=1.26,<2",  # cp312 wheels AND keeps np.float/np.bool8 aliases robosuite 1.4.1 uses
        "numba>=0.59",  # 0.53.1 has no cp312 wheel
        "scipy>=1.11",  # 1.10.1 is <3.12-only
        "opencv-python-headless>=4.8",  # 4.6.0.66 has no cp312 wheel; headless for servers
        # LIBERO's env package imports ``gym`` at module load (``envs/venv.py``) even
        # though rollout goes through robosuite directly — omitting it breaks import.
        "gym==0.25.2",
        # Non-env-critical, modern versions are fine. (robomimic OMITTED: training-only,
        # no cp312 wheel.)
        "future",
        "matplotlib",
        "cloudpickle",
        "hydra-core",
        "einops",
        "easydict",
        "imageio[ffmpeg]",
    )
    .env(
        {
            "MUJOCO_GL": "egl",
            "PYOPENGL_PLATFORM": "egl",
            # LIBERO's repo root must be importable: the outer ``libero/`` dir has no
            # ``__init__.py``, so pip 25's legacy editable hook alone is unreliable.
            # ``bench/`` is repo-root code, not part of the archetype wheel.
            "PYTHONPATH": "/opt/LIBERO:/opt/archetype",
        }
    )
    .run_commands(
        f"git clone https://github.com/Lifelong-Robot-Learning/LIBERO.git /opt/LIBERO"
        f" && git -C /opt/LIBERO checkout {_LIBERO_SHA}",
        # install_requires=[] so -e won't pull py3.8 pins; compat mode keeps the
        # legacy setup.py develop layout working on pip 25 / Python 3.12.
        "pip install --no-deps -e /opt/LIBERO --config-settings editable_mode=compat",
        # Bake ~/.libero/config.yaml so runtime imports never block on stdin.
        "echo N | python -c 'import libero.libero'",
        # Confirm the env stack imports before we pay for a GPU smoke.
        "python -c 'from libero.libero.envs import OffScreenRenderEnv'",
    )
    # Archetype itself, installed into the SAME interpreter as LIBERO.
    .add_local_dir(_local_repo_root(), _REPO_MOUNT, copy=True, ignore=_LOCAL_DIR_IGNORE)
    .run_commands(
        "pip install -e /opt/archetype",
        # Fail the image build if bench imports are broken (not a runtime crash loop).
        "python -c 'from bench.libero.in_process import InProcessLiberoEnvClient'",
    )
)

app = modal.App("archetype-libero", image=image)

# ---------------------------------------------------------------------------
# Colocated image: VLA-JEPA in the SAME py3.12 container as the env.
# ---------------------------------------------------------------------------
# The VLA-JEPA worker pinned py3.10 / torch 2.5 / flash-attn-cp310, but its own
# requirements say otherwise: torchvision==0.21.0 (-> torch 2.6), numpy==1.26.4,
# transformers==4.57.0, no python_requires — the same torch/numpy the LIBERO
# image already has. So we install the *inference slice* on top of the LIBERO
# image (floating versions to coexist with Archetype, like the LIBERO deps),
# and the env + model live in one interpreter. No Modal .remote(), no frames
# Volume. (Training-only deps — wandb, tensorboard, deepspeed, fastparquet, the
# pydantic/pyarrow pins that would clobber Archetype — are deliberately omitted;
# `-e --no-deps` keeps the editable install from dragging them back in.)
_VLA_JEPA_REPO = "https://github.com/ginwind/VLA-JEPA.git"
_VLA_JEPA_SHA = "ec8c70f6e155e2377bbd4d787004c14179c00c7c"
_VLA_CKPT_DIR = "/ckpts"
vla_ckpt_volume = modal.Volume.from_name("vla-jepa-ckpts", create_if_missing=True)

colocated_image = (
    image.pip_install(
        # Hold numpy at the version BOTH stacks want (LIBERO/numba need <2.x, and
        # VLA-JEPA's own requirements pin numpy==1.26.4). Without this the VLA
        # layer floats numpy to 2.5 and robosuite's numba import dies
        # ("Numba needs NumPy 2.4 or less") — caught by vla_import_smoke.
        "numpy>=1.26,<2",
        "torchvision==0.21.0",  # pins torch 2.6.0 (matches the LIBERO image's torch>=2.6)
        "transformers==4.57.0",
        "accelerate==1.5.2",
        "tiktoken",
        "transformers_stream_generator==0.0.4",
        "qwen-vl-utils",
        "timm",
        "einops",
        "diffusers",
        "omegaconf",
        "safetensors",
        "eva-decord==0.6.1",  # decord 0.6.0 has no cp312 wheel; eva-decord is the cp312 fork
        "av==12.3.0",
        "albumentations==1.4.18",
        # upstream websocket model server + client (not in requirements.txt)
        "websockets",
        "msgpack",
        "msgpack-numpy",
        "websocket-client==1.8.0",
        "huggingface_hub",
    )
    .run_commands(
        f"git clone {_VLA_JEPA_REPO} /opt/VLA-JEPA"
        f" && git -C /opt/VLA-JEPA checkout {_VLA_JEPA_SHA}",
        # --no-deps: we installed the inference slice above; don't let the full
        # requirements.txt downgrade Archetype's pydantic/pyarrow/etc.
        "pip install --no-deps -e /opt/VLA-JEPA",
    )
    # flash-attn: the released VLA-JEPA config hardcodes attn_implementation=
    # "flash_attention_2" (transformers raises if flash_attn is absent — sdpa env
    # is not honored). Install the prebuilt wheel matching THIS stack (cu12 /
    # torch 2.6 / cp312 / cxx11abiFALSE), the torch-2.6 analogue of the worker's
    # proven torch-2.5/cp310 wheel. Prebuilt = no compile, fast layer.
    .pip_install(
        "https://github.com/Dao-AILab/flash-attention/releases/download/"
        "v2.7.4.post1/flash_attn-2.7.4.post1+cu12torch2.6cxx11abiFALSE-cp312-cp312-linux_x86_64.whl"
    )
    .env(
        {
            "PYTHONPATH": "/opt/LIBERO:/opt/archetype:/opt/VLA-JEPA",
            "HF_HOME": f"{_VLA_CKPT_DIR}/hf-cache",
        }
    )
)


def _import_ok(module: str, attr: str) -> str:
    import importlib  # noqa: PLC0415

    getattr(importlib.import_module(module), attr)
    return "ok"


@app.function(image=colocated_image, timeout=1800)
def vla_import_smoke() -> dict:
    """Cheap (CPU) colocation check: does VLA-JEPA import alongside Archetype +
    LIBERO in ONE py3.12 interpreter? Validates the dep slice coexists before we
    pay for a GPU model load. No weights, no inference.

    Every result is a plain string: a remote exception that references a torch
    object cannot be deserialized on a torch-less local machine, so we never let
    one propagate — we catch each import and report its last traceback line.
    """
    import importlib  # noqa: PLC0415
    import traceback  # noqa: PLC0415

    def _ver(name: str) -> str:
        return str(getattr(importlib.import_module(name), "__version__", "ok"))

    targets = [
        ("torch", lambda: _ver("torch")),
        ("torchvision", lambda: _ver("torchvision")),
        ("transformers", lambda: _ver("transformers")),
        ("timm", lambda: _ver("timm")),
        ("diffusers", lambda: _ver("diffusers")),
        ("numpy", lambda: _ver("numpy")),
        ("archetype", lambda: _ver("archetype")),
        ("libero.envs", lambda: _import_ok("libero.libero.envs", "OffScreenRenderEnv")),
        (
            "vla_server_client",
            lambda: _import_ok(
                "deployment.model_server.tools.websocket_policy_client",
                "WebsocketClientPolicy",
            ),
        ),
        (
            "in_process_policy",
            lambda: _import_ok("bench.libero.in_process_policy", "InProcessVlaJepaPolicy"),
        ),
    ]
    results: dict[str, str] = {}
    ok = True
    for name, fn in targets:
        try:
            results[name] = fn()
        except Exception:  # noqa: BLE001 — report, never raise (torch-deserialize hazard)
            ok = False
            results[name] = "FAIL: " + traceback.format_exc().splitlines()[-1]
    print(f"VLA_IMPORT_SMOKE coexist={ok}")
    for name, res in results.items():
        print(f"  {name}: {res}")
    return {"coexist": ok, "results": results}


@app.function(gpu="A10G", timeout=1800, volumes={FRAMES_MOUNT: frames_volume})
def libero_smoke(suite: str = "libero_spatial", task_id: int = 0, steps: int = 5) -> dict:
    """Prove modernized LIBERO loads, resets, and steps in-process on 3.12.

    Reset one env and take a few zero actions; return proprio so the caller can
    see real numbers. This is the verification that the dep upgrade actually
    works — no Archetype world needed yet, just the in-process env client.
    """
    from bench.libero.in_process import InProcessLiberoEnvClient  # noqa: PLC0415

    client = InProcessLiberoEnvClient(suite=suite, task_id=task_id)
    obs0 = client.reset(env_id=0, seed=0)
    zero = [0.0] * 7
    last = obs0
    for _ in range(steps):
        (last,) = client.step([0], [zero])
    return {
        "instruction": client.task_language(),
        "reset_eef_pos": obs0["eef_pos"],
        "final_eef_pos": last["eef_pos"],
        "final_success": last.get("success", False),
    }


@app.function(gpu="A10G", timeout=3600, volumes={FRAMES_MOUNT: frames_volume})
def eval_task(
    suite: str = "libero_spatial",
    task_id: int = 0,
    trials: int = 5,
    max_steps: int = 520,
) -> dict:
    """Batched control-plane eval plumbing smoke on real LIBERO.

    HONEST SCOPE: ``policy_client=None`` below, so NO policy acts — every trial
    takes zero actions and ``success_rate`` will be ~0. This entrypoint proves
    the in-process env + ServiceContainer + ledger grading run end-to-end on the
    GPU container; it is NOT a policy result and must not be reported as one.
    For a real (policy-driven) number use ``optimize_task`` (which wires
    ``VlaJepaPolicyClient``) once the frames-volume blocker is resolved, or wire
    the policy here. Returns the graded report from the ledger.
    """
    import asyncio  # noqa: PLC0415

    from archetype.app.container import ServiceContainer  # noqa: PLC0415
    from archetype.core.config import StorageConfig  # noqa: PLC0415
    from bench.libero.eval_run import run_task_eval  # noqa: PLC0415
    from bench.libero.in_process import InProcessLiberoEnvClient  # noqa: PLC0415

    async def _run() -> dict:
        container = ServiceContainer()
        try:
            storage = StorageConfig(uri="/tmp/libero-eval-store", namespace=f"eval_{suite}")
            env = InProcessLiberoEnvClient(suite=suite, task_id=task_id, with_frames=False)
            report = await run_task_eval(
                world_service=container.world_service,
                simulation_service=container.simulation_service,
                eval_service=container.eval_service,
                env_client=env,
                policy_client=None,  # TODO: wire the in-process VLA-JEPA policy
                suite=suite,
                task_id=task_id,
                trials=trials,
                max_steps=max_steps,
                storage=storage,
            )
            return {
                "policy": "none (env-only plumbing smoke; success_rate is NOT a policy result)",
                "suite": report.suite,
                "task_id": report.task_id,
                "instruction": report.instruction,
                "success_rate": report.success_rate,
                "mean_length": report.mean_length,
                "world_id": report.world_id,
                "run_id": report.run_id,
            }
        finally:
            await container.shutdown()

    return asyncio.run(_run())


@app.function(
    image=colocated_image, gpu="L40S", timeout=7200, volumes={_VLA_CKPT_DIR: vla_ckpt_volume}
)
def optimize_task(
    suite: str = "libero_spatial",
    task_id: int = 0,
    seeds_per_variant: int = 3,
    max_steps: int = 520,
    rounds: int = 4,
) -> dict:
    """The headline experiment: optimize the instruction for the VLA on real
    LIBERO, hill-climbing ledger-graded success-rate. Each round spawns the
    candidate instructions x ``seeds_per_variant`` trials in ONE batched world,
    grades success per variant from the persisted ``ManipStatus``, and adopts the
    best. Returns the full optimization trace and the winning instruction.

    COLOCATED: env + VLA-JEPA policy run in this one py3.12 container
    (``InProcessVlaJepaPolicy``, a localhost model server) — no Modal ``.remote()``
    and no frames Volume; the frames-visibility blocker is gone. ONE blocker
    remains for a *publishable* number (docs/planning/paper-readiness-dod.md):
    the token-toggle perturbation's reachable optimum is the base instruction, so
    it cannot exhibit positive lift — a real run needs a paraphrase/LLM strategy
    (roadmap H5). Until that lands, run this for plumbing, not for a cited number.
    """
    import asyncio  # noqa: PLC0415

    from archetype.app.container import ServiceContainer  # noqa: PLC0415
    from archetype.core.config import StorageConfig  # noqa: PLC0415
    from bench.libero.in_process import InProcessLiberoEnvClient  # noqa: PLC0415
    from bench.libero.in_process_policy import InProcessVlaJepaPolicy  # noqa: PLC0415
    from bench.libero.instruction_sweep import (  # noqa: PLC0415
        TemplatePerturbation,
        optimize_instruction,
        run_instruction_sweep,
    )

    local_frames = "/tmp/libero-opt-frames"

    async def _run() -> dict:
        container = ServiceContainer()
        try:
            storage = StorageConfig(uri="/tmp/libero-opt-store", namespace=f"opt_{suite}")
            # Colocated: env writes frames to a local dir; the in-process policy
            # reads them from the same local dir (no Volume, no commit).
            env = InProcessLiberoEnvClient(
                suite=suite, task_id=task_id, with_frames=True, frames_dir=local_frames
            )
            policy = InProcessVlaJepaPolicy(ckpt_dir=_VLA_CKPT_DIR, frames_dir=local_frames)
            base_instruction = env.task_language()

            async def evaluate(instructions: list[str]) -> dict[str, float]:
                report = await run_instruction_sweep(
                    world_service=container.world_service,
                    simulation_service=container.simulation_service,
                    eval_service=container.eval_service,
                    env_client=env,
                    policy_client=policy,
                    suite=suite,
                    task_id=task_id,
                    variants=instructions,
                    seeds_per_variant=seeds_per_variant,
                    max_steps=max_steps,
                    storage=storage,
                    with_frames=True,
                )
                return report.scores

            # PLACEHOLDER strategy (blocker 2): toggling the instruction's OWN
            # words can only delete from base, so its reachable optimum IS base —
            # it cannot exhibit positive lift. A real run must inject candidate
            # words / paraphrases the base lacks (roadmap H5: an LLM paraphraser).
            # Kept here only to make the wiring explicit, not to produce a number.
            strategy = TemplatePerturbation(vocabulary=base_instruction.split())
            result = await optimize_instruction(
                evaluate=evaluate,
                base=base_instruction,
                strategy=strategy,
                rounds=rounds,
                neighbors=len(base_instruction.split()),
            )
            return {
                "suite": suite,
                "task_id": task_id,
                "base_instruction": base_instruction,
                "best_instruction": result.best_instruction,
                "base_success_rate": result.trace[0].best_success_rate,
                "best_success_rate": result.best_success_rate,
                "trace": [
                    {
                        "round": r.round,
                        "best_instruction": r.best_instruction,
                        "best_success_rate": r.best_success_rate,
                        "evaluated": r.evaluated,
                    }
                    for r in result.trace
                ],
            }
        finally:
            await container.shutdown()

    return asyncio.run(_run())


@app.function(
    image=colocated_image, gpu="L40S", timeout=3600, volumes={_VLA_CKPT_DIR: vla_ckpt_volume}
)
def vla_smoke() -> dict:
    """Colocation proof: launch the VLA-JEPA model server IN THIS container and
    run one inference on a synthetic frame — no Modal ``.remote()``, no frames
    Volume. If this returns a 7-dim action, the policy is in-process."""
    import os  # noqa: PLC0415

    import cv2  # noqa: PLC0415
    import numpy as np  # noqa: PLC0415

    from bench.libero.in_process_policy import InProcessVlaJepaPolicy  # noqa: PLC0415

    frames_dir = "/tmp/vla-smoke-frames"
    os.makedirs(os.path.join(frames_dir, "s", "0"), exist_ok=True)
    gray = np.full((128, 128, 3), 127, dtype=np.uint8)
    for cam in ("agentview", "wrist"):
        cv2.imwrite(os.path.join(frames_dir, "s", "0", f"reset-{cam}.png"), gray)
    obs = {
        "eef_pos": [0.0, 0.0, 1.0],
        "eef_quat": [0.0, 0.0, 0.0, 1.0],
        "gripper_qpos": [0.0, 0.0],
        "agentview_ref": "s/0/reset-agentview.png",
        "wrist_ref": "s/0/reset-wrist.png",
    }
    policy = InProcessVlaJepaPolicy(ckpt_dir=_VLA_CKPT_DIR, frames_dir=frames_dir)
    (action,) = policy.act([0], ["pick up the black bowl and place it on the plate"], [obs])
    assert len(action) == 7, f"expected 7-dim action, got {len(action)}"
    print(f"VLA_SMOKE ok — in-process action (no Modal RPC): {action}")
    return {
        "rpc": "none (in-process localhost server)",
        "action_dim": len(action),
        "action": action,
    }


@app.function(
    image=colocated_image, gpu="L40S", timeout=7200, volumes={_VLA_CKPT_DIR: vla_ckpt_volume}
)
def colocated_eval_task(
    suite: str = "libero_spatial",
    task_id: int = 0,
    trials: int = 10,
    max_steps: int = 520,
    gpu: str = "L40S",
) -> dict:
    """Real LIBERO eval with env + VLA-JEPA policy COLOCATED in one container,
    FULLY PROFILED — the first genuine policy-driven LIBERO success-rate plus a
    cost-performance breakdown (model-load, per-inference latency, per-trial and
    per-control-step wall-clock, trials/GPU-hour).

    No Modal ``.remote()`` and no frames Volume: ``InProcessLiberoEnvClient``
    writes frames to a local dir and ``InProcessVlaJepaPolicy`` (a localhost
    model server) reads them from the same local dir. (``max_steps`` includes the
    reset tick → ``max_steps - 1`` control steps.)
    """
    import asyncio  # noqa: PLC0415
    import time  # noqa: PLC0415

    from archetype.app.container import ServiceContainer  # noqa: PLC0415
    from archetype.core.config import StorageConfig  # noqa: PLC0415
    from bench.libero.eval_run import run_task_eval  # noqa: PLC0415
    from bench.libero.in_process import InProcessLiberoEnvClient  # noqa: PLC0415
    from bench.libero.in_process_policy import InProcessVlaJepaPolicy  # noqa: PLC0415

    local_frames = "/tmp/libero-coloc-frames"

    async def _run() -> dict:
        container = ServiceContainer()
        try:
            storage = StorageConfig(uri="/tmp/libero-coloc-store", namespace=f"coloc_{suite}")
            env = InProcessLiberoEnvClient(
                suite=suite, task_id=task_id, with_frames=True, frames_dir=local_frames
            )
            policy = InProcessVlaJepaPolicy(ckpt_dir=_VLA_CKPT_DIR, frames_dir=local_frames)

            wall_start = time.monotonic()
            report = await run_task_eval(
                world_service=container.world_service,
                simulation_service=container.simulation_service,
                eval_service=container.eval_service,
                env_client=env,
                policy_client=policy,
                suite=suite,
                task_id=task_id,
                trials=trials,
                max_steps=max_steps,
                storage=storage,
                with_frames=True,
            )
            total_wall = time.monotonic() - wall_start

            # Cost-performance breakdown. Model-load is a one-time amortizable cost;
            # report steady-state separately so trials/GPU-hour is meaningful.
            load_s = policy.startup_seconds
            steady_s = max(total_wall - load_s, 1e-9)
            control_steps = sum(t.episode_length for t in report.trials)
            inf_n = policy.infer_count
            successes = sum(1 for t in report.trials if t.success)
            profile = {
                "model_load_s": round(load_s, 2),
                "total_wall_s": round(total_wall, 2),
                "steady_state_s": round(steady_s, 2),
                "control_steps_total": control_steps,
                "inference_calls": inf_n,
                "inference_total_s": round(policy.infer_seconds, 2),
                "inference_mean_ms": round(policy.infer_seconds / inf_n * 1000, 1)
                if inf_n
                else 0.0,
                "inference_fraction_of_steady": round(policy.infer_seconds / steady_s, 3),
                "wall_per_trial_s": round(steady_s / trials, 2) if trials else 0.0,
                "wall_per_control_step_ms": round(steady_s / control_steps * 1000, 1)
                if control_steps
                else 0.0,
                "trials_per_gpu_hour": round(3600.0 / (steady_s / trials), 1)
                if trials and steady_s
                else 0.0,
                "gpu_seconds_per_trial": round(steady_s / trials, 2) if trials else 0.0,
            }
            result = {
                "policy": "InProcessVlaJepaPolicy (colocated, no Modal RPC)",
                "gpu": gpu,
                "suite": report.suite,
                "task_id": report.task_id,
                "instruction": report.instruction,
                "trials": trials,
                "max_steps": max_steps,
                "successes": successes,
                "success_rate": report.success_rate,
                "mean_length": round(report.mean_length, 1),
                "per_trial": [
                    {"env_key": t.env_key, "success": t.success, "length": t.episode_length}
                    for t in report.trials
                ],
                "world_id": report.world_id,
                "run_id": report.run_id,
                "profile": profile,
            }
            print("COLOCATED_EVAL result:")
            print(f"  instruction: {report.instruction}")
            print(f"  success_rate: {report.success_rate}  ({successes}/{trials})")
            print(f"  mean_length: {result['mean_length']}")
            print(f"  profile: {profile}")
            return result
        finally:
            await container.shutdown()

    return asyncio.run(_run())


@app.local_entrypoint()
def main(suite: str = "libero_spatial", task_id: int = 0):
    result = libero_smoke.remote(suite=suite, task_id=task_id)
    print("in-process LIBERO smoke (Python 3.12, Archetype + LIBERO one env):")
    for k, v in result.items():
        print(f"  {k}: {v}")
