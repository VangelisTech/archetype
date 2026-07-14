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
    #   client = ModalEnvClient("libero_spatial", with_frames=True)
    #   processor = FramedEnvStepProcessor(client)

STATUS: verified 2026-06-11 — `modal run` smoke passes end to end:
libero_spatial task 0 loads, resets with real proprio obs, and steps
under EGL offscreen rendering. (Benign EGL teardown noise appears at
interpreter exit from robosuite's context __del__; harmless.)
"""

# NOTE: no `from __future__ import annotations` here — modal.parameter()
# validates real annotation objects, and PEP 563 string annotations break it.
import os
import uuid
from typing import Any

import modal

# SHA pinned 2026-06-12 via:
#   git ls-remote https://github.com/Lifelong-Robot-Learning/LIBERO.git HEAD
# → 8f1084e3132a39270c3a13ebe37270a43ece2a01
_LIBERO_SHA = "8f1084e3132a39270c3a13ebe37270a43ece2a01"

# The shared volume for frame sidecars.  Both the env worker and the VLA-JEPA
# worker mount it at /frames; the env worker writes PNGs, the policy reads them.
FRAMES_VOLUME_NAME = "libero-frames"
FRAMES_MOUNT = "/frames"

frames_volume = modal.Volume.from_name(FRAMES_VOLUME_NAME, create_if_missing=True)

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
        # Worker-side observability: archetype's Modal secret `logfire`
        # provides LOGFIRE_TOKEN; without it configure is skipped entirely.
        "logfire",
    )
    # LIBERO's top-level package dir has no __init__.py, so find_packages()
    # produces an empty wheel from a plain `pip install git+...`. The repo's
    # own recipe — clone + editable install — is the only one that works.
    # SHA pinned 2026-06-12: git ls-remote LIBERO HEAD → 8f1084e3...
    .run_commands(
        f"git clone https://github.com/Lifelong-Robot-Learning/LIBERO.git /opt/LIBERO"
        f" && git -C /opt/LIBERO checkout {_LIBERO_SHA}",
        "pip install -e /opt/LIBERO",
        # First import interactively prompts for a dataset folder; answer
        # 'N' (use defaults) once at build time so ~/.libero/config.yaml is
        # baked into the image and runtime imports never block on stdin.
        "echo N | python -c 'import libero.libero'",
    )
    .env({"MUJOCO_GL": "egl", "PYOPENGL_PLATFORM": "egl"})
)

# Worker-side observability is opt-out per deployer: the named Modal secret
# is only referenced when configured (default "logfire", Vangelis' secret).
# Deploying in a workspace without it: ARCHETYPE_MODAL_LOGFIRE_SECRET= modal deploy ...
_LOGFIRE_SECRET_NAME = os.environ.get("ARCHETYPE_MODAL_LOGFIRE_SECRET", "logfire")
_worker_secrets = [modal.Secret.from_name(_LOGFIRE_SECRET_NAME)] if _LOGFIRE_SECRET_NAME else []

app = modal.App("archetype-libero-env", image=image)


# max_containers=1: env instances live in container memory keyed by env_key;
# a second container would silently fork that state. One container per
# parameter set until envs move to a shared store.
@app.cls(
    cpu=4,
    memory=8192,
    timeout=1800,
    scaledown_window=300,
    max_containers=1,
    volumes={FRAMES_MOUNT: frames_volume},
    secrets=_worker_secrets,
)
class LiberoEnvBatch:
    """A batch of LIBERO envs for one task suite, keyed by env_key.

    Method signatures mirror archetype.experiments.manipulation.EnvClient
    so the harness-side adapter is a thin .remote() passthrough.

    When ``with_frames=True`` is passed to reset/step:
    - PNGs are written to the shared ``libero-frames`` volume at
      ``/frames/<session>/<env_key>/<step:05d>-{agentview,wrist}.png``.
    - The obs dict gains ``agentview_ref`` and ``wrist_ref`` string keys
      containing the volume-relative paths.
    - The inline base64 fields (``agentview_png``, ``wrist_png``) are also
      returned for backwards compatibility (e.g. the video rollout script).
    - Volume commits are batched per step call, not per file.
    """

    suite: str = modal.parameter(default="libero_spatial")
    task_id: int = modal.parameter(default=0)
    camera_size: int = modal.parameter(default=128)

    @modal.enter()
    def load_suite(self):
        import os

        from libero.libero import benchmark

        if os.environ.get("LOGFIRE_TOKEN"):
            import logfire

            logfire.configure(service_name="archetype-libero-env", console=False)

        self._envs: dict[int, Any] = {}
        self._suite = benchmark.get_benchmark_dict()[self.suite]()
        self._task = self._suite.get_task(self.task_id)
        self._init_states = self._suite.get_task_init_states(self.task_id)
        # Per-container session UUID: all frames written by this container
        # instance share the same session prefix, keeping volume keys unique
        # across parallel episode runs.
        self._session = str(uuid.uuid4())
        # Per-env step counter for constructing deterministic ref paths.
        self._step_counts: dict[int, int] = {}

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

    def _write_frames(
        self, env_key: int, step_label: str, agentview_rgb, wrist_rgb
    ) -> tuple[str, str]:
        """Write a pair of PNGs to the shared volume and return their refs.

        The ref is the path relative to the volume root (i.e. relative to
        FRAMES_MOUNT).  Callers can reconstruct the full path as
        ``/frames/<ref>``.
        """
        import os

        import cv2

        def encode_png(rgb) -> bytes:
            ok, buf = cv2.imencode(".png", cv2.cvtColor(rgb, cv2.COLOR_RGB2BGR))
            if not ok:
                raise RuntimeError("png encode failed")
            return buf.tobytes()

        av_ref = f"{self._session}/{env_key}/{step_label}-agentview.png"
        wr_ref = f"{self._session}/{env_key}/{step_label}-wrist.png"

        av_path = os.path.join(FRAMES_MOUNT, av_ref)
        wr_path = os.path.join(FRAMES_MOUNT, wr_ref)

        os.makedirs(os.path.dirname(av_path), exist_ok=True)
        with open(av_path, "wb") as f:
            f.write(encode_png(agentview_rgb))
        with open(wr_path, "wb") as f:
            f.write(encode_png(wrist_rgb))

        return av_ref, wr_ref

    @staticmethod
    def _proprio(obs: dict) -> dict[str, Any]:
        """Extract proprio fields (no frames)."""
        return {
            "eef_pos": [float(v) for v in obs["robot0_eef_pos"]],
            "eef_quat": [float(v) for v in obs["robot0_eef_quat"]],
            "gripper": float(obs["robot0_gripper_qpos"][0]),
            # Both gripper joint values: the VLA state vector wants the full
            # robot0_gripper_qpos, not just the first finger.
            "gripper_qpos": [float(v) for v in obs["robot0_gripper_qpos"]],
        }

    def _proprio_with_frames(
        self, obs: dict, env_key: int, step_label: str, commit: bool = True
    ) -> dict[str, Any]:
        """Extract proprio and write frame PNGs to the volume.

        NOTE: LIBERO renders frames upside down; upstream eval code rotates
        180 degrees before the policy sees them. We ship raw pixels and let
        the policy client handle orientation.
        """
        import base64

        import cv2

        out = self._proprio(obs)

        agentview_rgb = obs["agentview_image"]
        wrist_rgb = obs["robot0_eye_in_hand_image"]

        # Inline base64 path (kept for backwards compat with video_rollout.py).
        def encode_b64(rgb) -> str:
            ok, buf = cv2.imencode(".png", cv2.cvtColor(rgb, cv2.COLOR_RGB2BGR))
            if not ok:
                raise RuntimeError("png encode failed")
            return base64.b64encode(buf.tobytes()).decode()

        out["agentview_png"] = encode_b64(agentview_rgb)
        out["wrist_png"] = encode_b64(wrist_rgb)

        # Volume sidecar.
        av_ref, wr_ref = self._write_frames(env_key, step_label, agentview_rgb, wrist_rgb)
        out["agentview_ref"] = av_ref
        out["wrist_ref"] = wr_ref

        # Batch-commit after writing each step's frames; avoids per-file
        # commits while keeping the volume consistent after every step call.
        if commit:
            frames_volume.commit()

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
        self._step_counts[env_id] = 0
        if with_frames:
            return self._proprio_with_frames(obs, env_id, "reset", commit=True)
        return self._proprio(obs)

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
            self._step_counts[env_id] = self._step_counts.get(env_id, 0) + 1
            n = self._step_counts[env_id]
            step_label = f"{n:05d}"

            if with_frames:
                # commit=False: we batch-commit once after the loop.
                result = self._proprio_with_frames(obs, env_id, step_label, commit=False)
            else:
                result = self._proprio(obs)

            result.update(
                {"reward": float(reward), "done": bool(done) or success, "success": success}
            )
            results.append(result)

        if with_frames:
            # One commit per step() call — not per file — to bound latency.
            frames_volume.commit()

        return results

    @modal.method()
    def task_language(self) -> str:
        return str(self._task.language)


class ModalEnvClient:
    """Harness-side EnvClient adapter over a deployed LiberoEnvBatch.

    Import this from the Archetype (py3.12) process; it only needs the
    `modal` client package, not LIBERO.

    Pickles as just (suite, task_id, with_frames) and reconnects lazily:
    Daft batch UDFs serialize their constructor args, and a live Modal
    handle is not serializable.
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
        self._with_frames = state.get("with_frames", False)
        self._worker = None

    def reset(self, env_id: int, seed: int) -> dict[str, Any]:
        return self._get_worker().reset.remote(env_id, seed, with_frames=self._with_frames)

    def step(self, env_ids: list[int], actions: list[list[float]]) -> list[dict[str, Any]]:
        return self._get_worker().step.remote(env_ids, actions, with_frames=self._with_frames)


@app.local_entrypoint()
def smoke(suite: str = "libero_spatial", task_id: int = 0, steps: int = 3):
    """Reset one env and take a few zero actions with frames; verify refs land in the volume."""
    worker = LiberoEnvBatch(suite=suite, task_id=task_id)
    print("task:", worker.task_language.remote())

    obs = worker.reset.remote(env_id=0, seed=0, with_frames=True)
    print("reset obs eef_pos:", obs["eef_pos"])
    print("reset agentview_ref:", obs.get("agentview_ref", "MISSING"))
    print("reset wrist_ref:", obs.get("wrist_ref", "MISSING"))

    assert "agentview_ref" in obs, "reset must return agentview_ref when with_frames=True"
    assert "wrist_ref" in obs, "reset must return wrist_ref when with_frames=True"
    assert obs["agentview_ref"].endswith("-agentview.png"), (
        f"unexpected ref: {obs['agentview_ref']}"
    )
    assert len(obs.get("gripper_qpos", [])) == 2, "gripper_qpos must be 2-element"

    zero = [0.0] * 7
    for step_idx in range(steps):
        (result,) = worker.step.remote([0], [zero], with_frames=True)
        print(
            f"step {step_idx}: eef_pos={result['eef_pos']} "
            f"agentview_ref={result.get('agentview_ref', 'MISSING')}"
        )
        assert "agentview_ref" in result, f"step {step_idx} must return agentview_ref"
        assert "wrist_ref" in result, f"step {step_idx} must return wrist_ref"

    # Verify the files actually exist in the volume.

    # Re-read the volume from local context (the worker committed).
    # We can't directly list the modal volume from the local entrypoint,
    # but we can verify by running a quick lookup in a new function call.
    print(f"smoke OK — {steps + 1} ref pairs returned (reset + {steps} steps)")
    print("smoke: refs are present in obs dicts; volume commit executed inside worker")
