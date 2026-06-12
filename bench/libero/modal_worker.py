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

STATUS: verified 2026-06-11 — `modal run` smoke passes end to end:
libero_spatial task 0 loads, resets with real proprio obs, and steps
under EGL offscreen rendering. (Benign EGL teardown noise appears at
interpreter exit from robosuite's context __del__; harmless.)
"""

# NOTE: no `from __future__ import annotations` here — modal.parameter()
# validates real annotation objects, and PEP 563 string annotations break it.
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
        # LIBERO's requirements pull full opencv-python (not headless),
        # which links against GUI-adjacent system libs even offscreen.
        "libglib2.0-0",
        "libsm6",
        "libxext6",
        "libxrender1",
    )
    .pip_install(
        # <2.6: LIBERO torch.load()s init-state numpy pickles, which the
        # weights_only=True default introduced in torch 2.6 rejects.
        "torch>=2.2,<2.6",
        "robosuite==1.4.1",
        "bddl==1.0.1",
        # The env-path slice of LIBERO's requirements.txt (we skip the
        # training stack: robomimic, transformers, wandb, thop).
        "future",
        "matplotlib",
        "cloudpickle",
        "gym==0.25.2",
        "hydra-core",
        "einops",
        "easydict",
        "imageio[ffmpeg]",
        "opencv-python-headless",
    )
    # LIBERO's top-level package dir has no __init__.py, so find_packages()
    # produces an empty wheel from a plain `pip install git+...`. The repo's
    # own recipe — clone + editable install — is the only one that works.
    .run_commands(
        "git clone --depth 1 https://github.com/Lifelong-Robot-Learning/LIBERO.git /opt/LIBERO",
        "pip install -e /opt/LIBERO",
        # First import interactively prompts for a dataset folder; answer
        # 'N' (use defaults) once at build time so ~/.libero/config.yaml is
        # baked into the image and runtime imports never block on stdin.
        "echo N | python -c 'import libero.libero'",
    )
    .env({"MUJOCO_GL": "egl", "PYOPENGL_PLATFORM": "egl"})
)

app = modal.App("archetype-libero-env", image=image)


# max_containers=1: env instances live in container memory keyed by env_key;
# a second container would silently fork that state. One container per
# parameter set until envs move to a shared store.
@app.cls(cpu=4, memory=8192, timeout=1800, scaledown_window=300, max_containers=1)
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
    def _proprio(obs: dict, with_frames: bool = False) -> dict[str, Any]:
        out = {
            "eef_pos": [float(v) for v in obs["robot0_eef_pos"]],
            "eef_quat": [float(v) for v in obs["robot0_eef_quat"]],
            "gripper": float(obs["robot0_gripper_qpos"][0]),
            # Both gripper joint values: the VLA state vector wants the full
            # robot0_gripper_qpos, not just the first finger.
            "gripper_qpos": [float(v) for v in obs["robot0_gripper_qpos"]],
        }
        if with_frames:
            import base64

            import cv2

            def encode(rgb) -> str:
                ok, buf = cv2.imencode(".png", cv2.cvtColor(rgb, cv2.COLOR_RGB2BGR))
                if not ok:
                    raise RuntimeError("png encode failed")
                return base64.b64encode(buf.tobytes()).decode()

            # NOTE: LIBERO renders frames upside down; upstream eval code
            # rotates 180 degrees before the policy sees them. We ship raw
            # pixels and leave orientation to the policy client.
            out["agentview_png"] = encode(obs["agentview_image"])
            out["wrist_png"] = encode(obs["robot0_eye_in_hand_image"])
        return out

    @modal.method()
    def reset(self, env_id: int, seed: int, with_frames: bool = False) -> dict[str, Any]:
        env = self._envs.get(env_id)
        if env is None:
            env = self._make_env()
            self._envs[env_id] = env
        env.seed(seed)
        env.reset()
        obs = env.set_init_state(self._init_states[seed % len(self._init_states)])
        return self._proprio(obs, with_frames)

    @modal.method()
    def step(
        self,
        env_ids: list[int],
        actions: list[list[float]],
        with_frames: bool = False,
    ) -> list[dict[str, Any]]:
        results = []
        for env_id, action in zip(env_ids, actions, strict=True):
            env = self._envs[env_id]
            obs, reward, done, info = env.step(action)
            success = bool(env.check_success())
            result = self._proprio(obs, with_frames)
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

    Pickles as just (suite, task_id) and reconnects lazily: Daft batch
    UDFs serialize their constructor args, and a live Modal handle is not
    serializable.
    """

    def __init__(self, suite: str = "libero_spatial", task_id: int = 0, with_frames: bool = False):
        self._suite = suite
        self._task_id = task_id
        self._with_frames = with_frames
        self._worker = None

    def _get_worker(self):
        if self._worker is None:
            cls = modal.Cls.from_name("archetype-libero-env", "LiberoEnvBatch")
            self._worker = cls(suite=self._suite, task_id=self._task_id)
        return self._worker

    def __getstate__(self) -> dict[str, Any]:
        return {
            "suite": self._suite,
            "task_id": self._task_id,
            "with_frames": self._with_frames,
        }

    def __setstate__(self, state: dict[str, Any]) -> None:
        self._suite = state["suite"]
        self._task_id = state["task_id"]
        self._with_frames = state["with_frames"]
        self._worker = None

    def reset(self, env_id: int, seed: int) -> dict[str, Any]:
        return self._get_worker().reset.remote(env_id, seed, with_frames=self._with_frames)

    def step(self, env_ids: list[int], actions: list[list[float]]) -> list[dict[str, Any]]:
        return self._get_worker().step.remote(env_ids, actions, with_frames=self._with_frames)


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
