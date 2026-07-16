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

``InProcessLiberoEnvClient`` mirrors the retired ``LiberoEnvBatch`` (the deleted
``modal_worker.py`` — git history) field for field, minus the Modal wrapper:
env instances live in a plain dict keyed by
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
import queue
import threading
from collections.abc import Callable
from concurrent.futures import Future
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

    _load._archetype_patched = True  # type: ignore[attr-defined]  # ty: ignore[unresolved-attribute]
    torch.load = _load  # type: ignore[assignment]


# Process-global live-env registry, keyed by (suite, task_id, camera_size). The
# heavyweight, non-picklable MuJoCo state lives HERE, not on the client instance —
# so the client is a picklable scalar stub that survives Daft's `@daft.cls`
# serialization, while the driver's reset() and the Daft worker's step() (same
# process for the local executor) share one set of live envs by env_key. This is
# the in-process analogue of the out-of-process LiberoEnvBatch keeping env state
# in container memory: the env never crosses the serialization boundary.
_ENV_POOLS: dict[tuple[str, int, int], _EnvPool] = {}

# Every MuJoCo call (env creation, reset, step) is marshalled onto ONE
# persistent thread. OpenGL/EGL offscreen contexts are THREAD-BOUND: physics
# survives cross-thread calls but rendering silently returns garbage. The bug
# this pins down was found 2026-07-16 at tensor level: reset() ran on the
# driver thread (clean frames) while step() ran on Daft UDF worker threads —
# every post-reset frame was EGL noise, the policy went blind after its first
# chunk, and success was 0 while proprio stayed perfectly correct. Upstream's
# single-threaded loop in the same image scored 30/30 (upstream_probe.py);
# with this affinity our loop scores 3/3 on the same task + init states.
# A daemon thread (not ThreadPoolExecutor) so container exit is not held
# hostage by a live worker.
class _EnvThread:
    """One persistent daemon thread that owns every MuJoCo/EGL call."""

    def __init__(self) -> None:
        self._queue: queue.Queue = queue.Queue()
        self._thread = threading.Thread(target=self._loop, name="libero-env", daemon=True)
        self._thread.start()

    def _loop(self) -> None:
        while True:
            fn, args, kwargs, fut = self._queue.get()
            if not fut.set_running_or_notify_cancel():
                continue
            try:
                fut.set_result(fn(*args, **kwargs))
            except BaseException as exc:  # noqa: BLE001 — relayed to the caller
                fut.set_exception(exc)

    def submit(self, fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Future:
        fut: Future = Future()
        self._queue.put((fn, args, kwargs, fut))
        return fut


_ENV_THREAD = _EnvThread()


def _on_env_thread(fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    if threading.current_thread().name == "libero-env":
        return fn(*args, **kwargs)  # already on the env thread (nested call)
    return _ENV_THREAD.submit(fn, *args, **kwargs).result()


def _frame_rel_dir(session: str, env_key: int, episode: int) -> str:
    """Relative frame directory for one episode of one env.

    Unique per (client session, env_key, reset ordinal): a later episode or
    a later sweep round can never overwrite PNGs that earlier ledger rows
    reference.
    """
    return os.path.join(session, str(env_key), f"ep{episode:04d}")


class _EnvPool:
    """Live LIBERO state for one (suite, task_id): the benchmark suite, the task,
    its init-states, and the per-env_key ``OffScreenRenderEnv`` instances."""

    def __init__(self) -> None:
        self.suite: Any = None
        self.task: Any = None
        self.init_states: Any = None
        self.envs: dict[int, Any] = {}
        self.step_counts: dict[int, int] = {}
        # Per-env_key episode ordinal, bumped on every reset. Frame paths
        # include it so a later episode can never overwrite PNGs that earlier
        # ledger rows reference — frame artifacts stay append-only like the
        # rows that point at them.
        self.episode_counts: dict[int, int] = {}


class InProcessLiberoEnvClient:
    """``EnvClient`` that runs LIBERO ``OffScreenRenderEnv`` in-process.

    A control-plane world with N trial entities batch-steps N envs in one
    ``step`` call — no per-trial world, no orphaned trajectories. The live envs
    are keyed by ``env_key`` in a process-global pool (``_ENV_POOLS``); this
    client instance holds only picklable scalars, so it serializes cleanly into
    the Daft ``@daft.cls`` UDF while the actual MuJoCo state stays put in the one
    interpreter the eval runs in.

    ``with_frames`` writes agentview+wrist PNGs to ``frames_dir`` and returns
    volume-relative refs, so the framed step processor and a VLA policy work
    identically in-process.
    """

    def __init__(
        self,
        suite: str = "libero_spatial",
        task_id: int = 0,
        # 256 matches the published VLA-JEPA eval (render 256, policy resizes to
        # 224). The first colocated execution (2026-07-15) ran the old default of
        # 128 — upscaling 128→224 starves the model of detail and scored 0/3.
        camera_size: int = 256,
        with_frames: bool = False,
        frames_dir: str = "/tmp/archetype-libero-frames",
        # Upstream's eval takes 10 zero-action steps after reset before the
        # policy ever sees an observation (num_steps_wait=10): LIBERO drops
        # objects at reset and the first frames are mid-fall physics. These
        # settle steps run inside reset(), so tick 0 on the ledger is the
        # settled initial condition and they never count as control steps.
        settle_steps: int = 10,
        # Upstream seeds the ENV with a constant (their eval_libero.py:
        # "IMPORTANT: seed seems to affect object positions even when using
        # fixed initial state", default seed=7) and varies episodes ONLY via
        # init_states[episode_idx]. The per-trial seed passed to reset()
        # selects the init state; this constant seeds the env itself.
        env_seed: int = 7,
    ) -> None:
        self._suite_name = suite
        self._task_id = task_id
        self._camera_size = camera_size
        self._with_frames = with_frames
        self._frames_dir = frames_dir
        self._settle_steps = settle_steps
        self._env_seed = env_seed
        # Unique per client construction (computed before @daft.cls pickling,
        # so driver and worker copies agree): a fresh process or a fresh
        # client can never collide with frame refs persisted by an earlier
        # run into the same frames_dir.
        self._session = f"{suite}-t{task_id}-{os.urandom(4).hex()}"

    @property
    def _pool(self) -> _EnvPool:
        key = (self._suite_name, self._task_id, self._camera_size)
        pool = _ENV_POOLS.get(key)
        if pool is None:
            pool = _EnvPool()
            _ENV_POOLS[key] = pool
        return pool

    def _ensure_suite(self) -> None:
        pool = self._pool
        if pool.suite is not None:
            return
        _patch_torch_load_for_libero()
        from libero.libero import benchmark  # noqa: PLC0415

        pool.suite = benchmark.get_benchmark_dict()[self._suite_name]()
        pool.task = pool.suite.get_task(self._task_id)
        pool.init_states = pool.suite.get_task_init_states(self._task_id)

    def _make_env(self) -> Any:
        from libero.libero import get_libero_path  # noqa: PLC0415
        from libero.libero.envs import OffScreenRenderEnv  # noqa: PLC0415

        task = self._pool.task
        bddl = os.path.join(
            get_libero_path("bddl_files"),
            task.problem_folder,
            task.bddl_file,
        )
        return OffScreenRenderEnv(
            bddl_file_name=bddl,
            camera_heights=self._camera_size,
            camera_widths=self._camera_size,
        )

    def task_language(self) -> str:
        """The task's natural-language instruction (wired onto ``ManipTask``)."""
        self._ensure_suite()
        return str(self._pool.task.language)

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

        episode = self._pool.episode_counts.get(env_key, 0)
        rel_dir = _frame_rel_dir(self._session, env_key, episode)
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
        """Reset one env (on the dedicated env thread — see ``_ENV_THREAD``)."""
        return _on_env_thread(self._reset_impl, env_id, seed)

    def _reset_impl(self, env_id: int, seed: int) -> dict[str, Any]:
        self._ensure_suite()
        pool = self._pool
        env = pool.envs.get(env_id)
        if env is None:
            env = self._make_env()
            pool.envs[env_id] = env
        env.seed(self._env_seed)
        env.reset()
        obs = env.set_init_state(pool.init_states[seed % len(pool.init_states)])
        # Settle action = zeros + gripper open (-1), exactly what the proven
        # closed-loop demo used (video_rollout.py, git history).
        for _ in range(self._settle_steps):
            obs, _reward, _done, _info = env.step([0.0] * 6 + [-1.0])
        pool.step_counts[env_id] = 0
        pool.episode_counts[env_id] = pool.episode_counts.get(env_id, 0) + 1
        out = self._proprio(obs)
        if self._with_frames:
            out.update(self._write_frames(env_id, "reset", obs))
        return out

    def step(self, env_ids: list[int], actions: list[list[float]]) -> list[dict[str, Any]]:
        """Step envs (on the dedicated env thread — see ``_ENV_THREAD``)."""
        return _on_env_thread(self._step_impl, env_ids, actions)

    def _step_impl(self, env_ids: list[int], actions: list[list[float]]) -> list[dict[str, Any]]:
        pool = self._pool
        results: list[dict[str, Any]] = []
        for env_id, action in zip(env_ids, actions, strict=True):
            env = pool.envs[env_id]
            obs, reward, done, _info = env.step(action)
            success = bool(env.check_success())
            pool.step_counts[env_id] = pool.step_counts.get(env_id, 0) + 1
            out = self._proprio(obs)
            out.update({"reward": float(reward), "done": bool(done) or success, "success": success})
            if self._with_frames:
                out.update(self._write_frames(env_id, f"{pool.step_counts[env_id]:05d}", obs))
            results.append(out)
        return results


@dataclass
class InProcessLiberoEnvSpec(EnvClientSpec):
    """Resources spec that builds an in-process LIBERO env (no Modal).

    Register under ``EnvClientSpec``; ``build()`` returns the in-process client,
    so the env runs in the same interpreter as the rest of the eval. (The old
    ``LiberoEnvSpec``, which built the retired Modal RPC client, was deleted
    2026-07-15.)
    """

    suite: str = "libero_spatial"
    task_id: int = 0
    camera_size: int = 256
    with_frames: bool = False
    frames_dir: str = "/tmp/archetype-libero-frames"
    settle_steps: int = 10
    env_seed: int = 7

    def build(self) -> EnvClient:
        return InProcessLiberoEnvClient(
            suite=self.suite,
            task_id=self.task_id,
            camera_size=self.camera_size,
            with_frames=self.with_frames,
            frames_dir=self.frames_dir,
            settle_steps=self.settle_steps,
            env_seed=self.env_seed,
        )
