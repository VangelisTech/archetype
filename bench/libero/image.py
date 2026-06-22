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

The env then runs **in-process**: ``InProcessLiberoEnvClient`` drives
``OffScreenRenderEnv`` directly and the existing ``_EnvStepper`` ``@daft.cls``
steps it statefully — no ``.remote()``, no container split, no env-state-loss.
Modal is used only for what genuinely needs it: a Linux+EGL host and a GPU.

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
    """Full batched control-plane eval on real LIBERO, entirely in-process.

    Archetype's ServiceContainer, the LIBERO env, and (when wired) the policy
    all run in this one Python 3.12 interpreter on the GPU container. Returns
    the graded report — the trustworthy numbers, derived from the ledger.
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


@app.function(gpu="A10G", timeout=7200, volumes={FRAMES_MOUNT: frames_volume})
def optimize_task(
    suite: str = "libero_spatial",
    task_id: int = 0,
    seeds_per_variant: int = 3,
    max_steps: int = 520,
    rounds: int = 4,
) -> dict:
    """The headline experiment: optimize the instruction for the VLA, on real
    LIBERO, entirely in-process — prompt optimization lifting a VLA's success
    rate, the same way it lifts a coding agent's.

    Hill-climbs the task's natural-language instruction (perturbing its own
    words) against ledger-graded success-rate: each round spawns the candidate
    instructions x ``seeds_per_variant`` trials in ONE batched world, grades
    success per variant from the persisted ``ManipStatus``, and adopts the best.
    Returns the full optimization trace and the winning instruction — the paper
    numbers, derived from data, not computed in a script.
    """
    import asyncio  # noqa: PLC0415

    from archetype.app.container import ServiceContainer  # noqa: PLC0415
    from archetype.core.config import StorageConfig  # noqa: PLC0415
    from bench.libero.clients import VlaJepaPolicyClient  # noqa: PLC0415
    from bench.libero.in_process import InProcessLiberoEnvClient  # noqa: PLC0415
    from bench.libero.instruction_sweep import (  # noqa: PLC0415
        TemplatePerturbation,
        optimize_instruction,
        run_instruction_sweep,
    )

    async def _run() -> dict:
        container = ServiceContainer()
        try:
            storage = StorageConfig(uri="/tmp/libero-opt-store", namespace=f"opt_{suite}")
            env = InProcessLiberoEnvClient(suite=suite, task_id=task_id, with_frames=True)
            policy = VlaJepaPolicyClient(suite=suite, task_id=task_id)
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

            # Perturb the instruction's own words: dropping/re-adding the verb or
            # object tests which tokens the VLA actually depends on.
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


@app.local_entrypoint()
def main(suite: str = "libero_spatial", task_id: int = 0):
    result = libero_smoke.remote(suite=suite, task_id=task_id)
    print("in-process LIBERO smoke (Python 3.12, Archetype + LIBERO one env):")
    for k, v in result.items():
        print(f"  {k}: {v}")
