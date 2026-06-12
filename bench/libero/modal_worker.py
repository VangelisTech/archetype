# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""LIBERO env worker on Modal — the out-of-process half of EnvClient.

Why a separate process at all: upstream LIBERO pins Python 3.8-3.10-era
dependencies (and Linux + EGL for offscreen rendering), which cannot
coexist with Archetype's Python 3.12 process on any machine. The worker
therefore runs in its own container with its own interpreter; the
Archetype harness talks to it through the EnvClient protocol
(`archetype.experiments.manipulation`).

Why Modal specifically: nothing in the protocol requires it — any Linux
box or Docker image exposing reset/step works. Modal buys ephemeral
Linux+EGL containers without owning a box, and Stage 4's LIBERO sweep
(4 suites x 10 tasks x 50 rollouts) fans out across containers with one
`.map()`.

Setup (one time):
    uv tool install modal && modal setup

Smoke test (builds the image on first run, ~5-10 min):
    modal run bench/libero/modal_worker.py

Deploy for use from the Archetype harness:
    modal deploy bench/libero/modal_worker.py
    # then in the harness process:
    #   client = ModalEnvClient("libero_spatial")
    #   processor = EnvStepProcessor(client)

STATUS: scaffold — follows the standard OpenVLA-style LIBERO eval setup
(py3.10, LIBERO from git, MUJOCO_GL=egl). Unverified until the first
`modal run`; expect to iterate on image pins once.
"""

from __future__ import annotations

from typing import Any

import modal

# The OpenVLA-style LIBERO environment: python 3.10 with relaxed pins
# rather than upstream's frozen 3.8.13 lockfile.
image = (
    modal.Image.debian_slim(python_version="3.10")
    .apt_install(
        "git",
        "libegl1",
        "libgl1",
        "libglew-dev",
        "libosmesa6-dev",
        "libglfw3",
        "patchelf",
    )
    .pip_install(
        "torch>=2.2",
        "robosuite==1.4.1",
        "bddl==1.0.1",
        "easydict",
        "imageio[ffmpeg]",
        "opencv-python-headless",
    )
    .pip_install("libero @ git+https://github.com/Lifelong-Robot-Learning/LIBERO.git")
    .env({"MUJOCO_GL": "egl", "PYOPENGL_PLATFORM": "egl"})
)

app = modal.App("archetype-libero-env", image=image)


@app.cls(cpu=4, memory=8192, timeout=1800, scaledown_window=300)
class LiberoEnvBatch:
    """A batch of LIBERO envs for one task suite, keyed by env_key.

    Method signatures mirror archetype.experiments.manipulation.EnvClient
    so the harness-side adapter is a thin .remote() passthrough.
    """

    suite: str = modal.parameter(default="libero_spatial")
    task_id: int = modal.parameter(default=0)
    camera_size: int = modal.parameter(default=128)

    @modal.enter()
    def load_suite(self):
        from libero.libero import benchmark

        self._envs: dict[int, Any] = {}
        self._suite = benchmark.get_benchmark_dict()[self.suite]()
        self._task = self._suite.get_task(self.task_id)
        self._init_states = self._suite.get_task_init_states(self.task_id)

    def _make_env(self):
        import os

        from libero.libero import get_libero_path
        from libero.libero.envs import OffScreenRenderEnv

        bddl = os.path.join(
            get_libero_path("bddl_files"),
            self._task.problem_folder,
            self._task.bddl_file,
        )
        return OffScreenRenderEnv(
            bddl_file_name=bddl,
            camera_heights=self.camera_size,
            camera_widths=self.camera_size,
        )

    @staticmethod
    def _proprio(obs: dict) -> dict[str, Any]:
        return {
            "eef_pos": [float(v) for v in obs["robot0_eef_pos"]],
            "eef_quat": [float(v) for v in obs["robot0_eef_quat"]],
            "gripper": float(obs["robot0_gripper_qpos"][0]),
        }

    @modal.method()
    def reset(self, env_id: int, seed: int) -> dict[str, Any]:
        env = self._envs.get(env_id)
        if env is None:
            env = self._make_env()
            self._envs[env_id] = env
        env.seed(seed)
        env.reset()
        obs = env.set_init_state(self._init_states[seed % len(self._init_states)])
        return self._proprio(obs)

    @modal.method()
    def step(self, env_ids: list[int], actions: list[list[float]]) -> list[dict[str, Any]]:
        results = []
        for env_id, action in zip(env_ids, actions, strict=True):
            env = self._envs[env_id]
            obs, reward, done, info = env.step(action)
            success = bool(env.check_success())
            result = self._proprio(obs)
            result.update(
                {"reward": float(reward), "done": bool(done) or success, "success": success}
            )
            results.append(result)
        return results

    @modal.method()
    def task_language(self) -> str:
        return str(self._task.language)


class ModalEnvClient:
    """Harness-side EnvClient adapter over a deployed LiberoEnvBatch.

    Import this from the Archetype (py3.12) process; it only needs the
    `modal` client package, not LIBERO.
    """

    def __init__(self, suite: str = "libero_spatial", task_id: int = 0):
        cls = modal.Cls.from_name("archetype-libero-env", "LiberoEnvBatch")
        self._worker = cls(suite=suite, task_id=task_id)

    def reset(self, env_id: int, seed: int) -> dict[str, Any]:
        return self._worker.reset.remote(env_id, seed)

    def step(self, env_ids: list[int], actions: list[list[float]]) -> list[dict[str, Any]]:
        return self._worker.step.remote(env_ids, actions)


@app.local_entrypoint()
def smoke(suite: str = "libero_spatial", task_id: int = 0, steps: int = 5):
    """Reset one env and take a few zero actions; print what comes back."""
    worker = LiberoEnvBatch(suite=suite, task_id=task_id)
    print("task:", worker.task_language.remote())
    obs = worker.reset.remote(env_id=0, seed=0)
    print("reset obs:", obs)
    zero = [0.0] * 7
    for step_idx in range(steps):
        (result,) = worker.step.remote([0], [zero])
        print(
            f"step {step_idx}: eef_pos={result['eef_pos']} "
            f"reward={result['reward']} done={result['done']} success={result['success']}"
        )
    print("smoke OK")
