# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""In-process LIBERO env client — same interpreter as Archetype, no RPC.

This is the whole point: LIBERO is a `robosuite`/MuJoCo simulator, and there
is no reason it must live behind a Modal `.remote()` boundary. The only thing
that ever forced that boundary was upstream LIBERO's lazy dependency pins
(Python 3.8-3.10, `torch<2.6`, `robosuite==1.4.1`). On a modern image where
LIBERO is installed *alongside* Archetype in one Python 3.12 environment (see
``bench/libero/image.py``), the env runs in-process and the existing
``_EnvStepper`` ``@daft.cls`` drives it statefully — exactly like the Stage-1
MuJoCo boundary, exactly like ``ScriptedReachEnv`` does today.

``InProcessLiberoEnvClient`` mirrors ``LiberoEnvBatch`` (modal_worker.py) field
for field, minus the Modal wrapper: env instances live in a plain dict keyed by
``env_key``; ``reset``/``step`` call robosuite directly. Drop it into
``EnvStepProcessor(InProcessLiberoEnvClient(...))`` and the whole eval flow runs
in one interpreter — no container split, no env-state-loss across containers,
no nested ``.remote()`` lifecycle.

Rendering still needs a Linux host with EGL (or osmesa) — that is the one real,
non-laziness constraint. ``image.py`` provides it. On macOS, MuJoCo's native
context works for local smoke tests at smaller scale.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

from archetype.experiments.manipulation import EnvClient, EnvClientSpec


def _patch_torch_load_for_libero() -> None:
    """Make ``torch.load`` accept LIBERO's init-state pickles on torch>=2.6.

    torch 2.6 flipped ``torch.load``'s default to ``weights_only=True``, which
    rejects the numpy object pickles LIBERO stores its task init-states as. That
    one default is the *entire* reason the worker pinned ``torch<2.6`` — not a
    real constraint, a single unpatched call. We restore the old behavior once,
    at import, so modern torch works unchanged. (LIBERO's init-states are part
    of the trusted benchmark image, not untrusted input.)
    """
    try:
        import torch  # noqa: PLC0415
    except ImportError:
        return
    if getattr(torch.load, "_archetype_patched", False):
        return
    _orig = torch.load

    def _load(*args: Any, **kwargs: Any) -> Any:
        kwargs.setdefault("weights_only", False)
        return _orig(*args, **kwargs)

    _load._archetype_patched = True  # type: ignore[attr-defined]
    torch.load = _load  # type: ignore[assignment]


class InProcessLiberoEnvClient:
    """``EnvClient`` that runs LIBERO ``OffScreenRenderEnv`` in-process.

    One client owns a batch of envs for a single ``(suite, task_id)``, keyed by
    ``env_key`` (the trial index / entity id), so a control-plane world with N
    trial entities batch-steps N envs in one ``step`` call — no per-trial world,
    no orphaned trajectories.

    ``with_frames`` writes agentview+wrist PNGs to ``frames_dir`` and returns
    volume-relative refs (the local mirror of the Modal frames volume), so the
    framed step processor and a VLA policy work identically in-process.
    """

    def __init__(
        self,
        suite: str = "libero_spatial",
        task_id: int = 0,
        camera_size: int = 128,
        with_frames: bool = False,
        frames_dir: str = "/tmp/archetype-libero-frames",
    ) -> None:
        self._suite_name = suite
        self._task_id = task_id
        self._camera_size = camera_size
        self._with_frames = with_frames
        self._frames_dir = frames_dir
        # Lazy: built on first reset so importing this module never requires
        # LIBERO/robosuite to be installed (the framework stays import-light).
        self._suite: Any = None
        self._task: Any = None
        self._init_states: Any = None
        self._envs: dict[int, Any] = {}
        self._step_counts: dict[int, int] = {}
        self._session = f"{suite}-t{task_id}"

    def _ensure_suite(self) -> None:
        if self._suite is not None:
            return
        _patch_torch_load_for_libero()
        from libero.libero import benchmark  # noqa: PLC0415

        self._suite = benchmark.get_benchmark_dict()[self._suite_name]()
        self._task = self._suite.get_task(self._task_id)
        self._init_states = self._suite.get_task_init_states(self._task_id)

    def _make_env(self) -> Any:
        from libero.libero import get_libero_path  # noqa: PLC0415
        from libero.libero.envs import OffScreenRenderEnv  # noqa: PLC0415

        bddl = os.path.join(
            get_libero_path("bddl_files"),
            self._task.problem_folder,
            self._task.bddl_file,
        )
        return OffScreenRenderEnv(
            bddl_file_name=bddl,
            camera_heights=self._camera_size,
            camera_widths=self._camera_size,
        )

    def task_language(self) -> str:
        """The task's natural-language instruction (wired onto ``ManipTask``)."""
        self._ensure_suite()
        return str(self._task.language)

    @staticmethod
    def _proprio(obs: dict) -> dict[str, Any]:
        return {
            "eef_pos": [float(v) for v in obs["robot0_eef_pos"]],
            "eef_quat": [float(v) for v in obs["robot0_eef_quat"]],
            "gripper": float(obs["robot0_gripper_qpos"][0]),
            "gripper_qpos": [float(v) for v in obs["robot0_gripper_qpos"]],
        }

    def _write_frames(self, env_key: int, label: str, obs: dict) -> dict[str, str]:
        import cv2  # noqa: PLC0415

        rel_dir = os.path.join(self._session, str(env_key))
        abs_dir = os.path.join(self._frames_dir, rel_dir)
        os.makedirs(abs_dir, exist_ok=True)
        refs: dict[str, str] = {}
        for cam, key in (("agentview", "agentview_image"), ("wrist", "robot0_eye_in_hand_image")):
            rel = os.path.join(rel_dir, f"{label}-{cam}.png")
            ok, buf = cv2.imencode(".png", cv2.cvtColor(obs[key], cv2.COLOR_RGB2BGR))
            if not ok:
                raise RuntimeError("png encode failed")
            with open(os.path.join(self._frames_dir, rel), "wb") as f:
                f.write(buf.tobytes())
            refs[f"{cam}_ref"] = rel
        return refs

    def reset(self, env_id: int, seed: int) -> dict[str, Any]:
        self._ensure_suite()
        env = self._envs.get(env_id)
        if env is None:
            env = self._make_env()
            self._envs[env_id] = env
        env.seed(seed)
        env.reset()
        obs = env.set_init_state(self._init_states[seed % len(self._init_states)])
        self._step_counts[env_id] = 0
        out = self._proprio(obs)
        if self._with_frames:
            out.update(self._write_frames(env_id, "reset", obs))
        return out

    def step(self, env_ids: list[int], actions: list[list[float]]) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        for env_id, action in zip(env_ids, actions, strict=True):
            env = self._envs[env_id]
            obs, reward, done, _info = env.step(action)
            success = bool(env.check_success())
            self._step_counts[env_id] = self._step_counts.get(env_id, 0) + 1
            out = self._proprio(obs)
            out.update({"reward": float(reward), "done": bool(done) or success, "success": success})
            if self._with_frames:
                out.update(self._write_frames(env_id, f"{self._step_counts[env_id]:05d}", obs))
            results.append(out)
        return results


@dataclass
class InProcessLiberoEnvSpec(EnvClientSpec):
    """Resources spec that builds an in-process LIBERO env (no Modal).

    Register under ``EnvClientSpec`` exactly like ``LiberoEnvSpec``; the only
    difference is ``build()`` returns the in-process client, so the env runs in
    the same interpreter as the rest of the eval.
    """

    suite: str = "libero_spatial"
    task_id: int = 0
    camera_size: int = 128
    with_frames: bool = False
    frames_dir: str = "/tmp/archetype-libero-frames"

    def build(self) -> EnvClient:
        return InProcessLiberoEnvClient(
            suite=self.suite,
            task_id=self.task_id,
            camera_size=self.camera_size,
            with_frames=self.with_frames,
            frames_dir=self.frames_dir,
        )
