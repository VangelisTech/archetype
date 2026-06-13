# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""LIBERO-Plus env worker on Modal — the LANGUAGE-perturbation half of EnvClient.

LIBERO-Plus (github.com/sylvestf/LIBERO-plus, arXiv 2510.13626) is a drop-in
robustness *superset* of LIBERO on the **same robosuite/MuJoCo base** (not
SAPIEN): "replace the originally installed LIBERO repository with our repository
without modifying your code." It adds seven controlled perturbation dimensions;
this worker exposes the **Language Instructions** dimension, where the scene,
physics, and goal predicate are byte-identical to base LIBERO and only the
``(:language ...)`` string is rewritten.

Why a SEPARATE Modal app/class (not an edit to ``modal_worker.py``): the
``archetype-libero-env`` app is already deployed and serving live probes; an
in-place image change would rebuild and disturb them. This worker is fully
additive — new app ``archetype-libero-plus-env``, new class
``LiberoPlusEnvBatch`` — so the standard-LIBERO worker is untouched.

Method signatures mirror ``archetype.experiments.manipulation.EnvClient`` exactly
(reset / step / task_language), identical to ``LiberoEnvBatch``, so the harness
side reuses the proven batched driver, the VLA-JEPA policy worker, and the
baseline sweep without modification. The only behavioural difference is that
``task_language()`` returns the *perturbed* instruction (read from the
``_language_N.bddl`` variant) instead of the canonical one.

How a Language task is selected (ports ~30 lines from LIBERO-Plus's
``libero_plus_init.py`` selector idea, adapted to the repo's actual API):

  - ``task_classification.json`` (baked into the LIBERO-Plus repo at
    ``libero/libero/benchmark/task_classification.json``) is a dict keyed by the
    four standard suites, each a list of ``{id, name, category, difficulty_level}``.
  - The 1-based ``id`` is exactly the (id-1) 0-based index into
    ``libero_task_map[suite]`` — verified against the suite map — which is the
    same index the LIBERO-Plus ``Benchmark`` exposes via ``get_task(i)`` /
    ``get_task_bddl_file_path(i)`` / ``get_task_init_states(i)`` at the default
    task order (identity).
  - So ``get_ids_by_category(suite, "Language Instructions")`` yields the absolute
    benchmark indices of the language variants. The worker's ``task_id`` parameter
    is the *ordinal within that filtered list* (0 = first language variant), kept
    small and stable so the baseline sweep can fan out over a handful of them.

Path resolution is delegated entirely to LIBERO-Plus: the language ``name`` carries
a ``_view_0_0_100_0_0_initstate_0`` suffix, and LIBERO-Plus's ``ControlEnv``
strips it internally (split on ``_view_`` → base ``.bddl`` + view/initstate
params), so we just hand it ``get_task_bddl_file_path(i)`` unchanged. Init states
reuse the base scene via ``get_task_init_states(i)`` (strips ``_language_``).

NO ``assets.zip`` is needed for the language dimension — that 6.4GB download is
only for the visual perturbation dimensions (camera/light/background/noise).

Setup (one time):
    uv tool install modal && modal setup

Smoke test (builds the image on first run, ~5-10 min):
    modal run bench/libero/libero_plus_worker.py

Deploy for use from the Archetype harness:
    modal deploy bench/libero/libero_plus_worker.py
    # then in the harness process:
    #   client = LiberoPlusEnvClient("libero_spatial", task_id=0, with_frames=True)
"""

# NOTE: no `from __future__ import annotations` here — modal.parameter()
# validates real annotation objects, and PEP 563 string annotations break it.
import uuid
from typing import Any

import modal

# SHA pinned 2026-06-13 via:
#   gh api repos/sylvestf/LIBERO-plus/commits/HEAD --jq .sha
# → 4976dc30028e805ff8094b55501d532c48fec182
_LIBERO_PLUS_SHA = "4976dc30028e805ff8094b55501d532c48fec182"

# The perturbation dimension this worker serves. LIBERO-Plus labels the
# language-rewrite variants exactly this way in task_classification.json.
LANGUAGE_CATEGORY = "Language Instructions"

# Reuse the SAME frames volume as the standard LIBERO worker: frame sidecars are
# just PNGs keyed by a per-container session UUID, so the two workers cannot
# collide, and the VLA-JEPA policy reads from one mount regardless of which env
# produced them.
FRAMES_VOLUME_NAME = "libero-frames"
FRAMES_MOUNT = "/frames"

frames_volume = modal.Volume.from_name(FRAMES_VOLUME_NAME, create_if_missing=True)

# Same py3.10 + relaxed-pins recipe as the standard worker, but cloning
# LIBERO-Plus instead of upstream LIBERO. Extra apt libs are LIBERO-Plus
# specific: libexpat1 (mujoco xml parsing on some bases), libfontconfig1-dev +
# libmagickwand-dev (the `wand`/ImageMagick dep in extra_requirements.txt used
# by the visual-perturbation tooling; harmless for the language dim but part of
# the documented install).
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
        # opencv-python (non-headless) links GUI-adjacent libs even offscreen.
        "libglib2.0-0",
        "libsm6",
        "libxext6",
        "libxrender1",
        # LIBERO-Plus specific (per its README apt list / extra_requirements).
        "libexpat1",
        "libfontconfig1-dev",
        "libmagickwand-dev",
    )
    .pip_install(
        # <2.6: LIBERO torch.load()s init-state numpy pickles, which the
        # weights_only=True default introduced in torch 2.6 rejects.
        "torch>=2.2,<2.6",
        "robosuite==1.4.1",
        "bddl==1.0.1",
        # The env-path slice of LIBERO-Plus's requirements (we skip the training
        # stack: robomimic, transformers, wandb, thop).
        "future",
        "matplotlib",
        "cloudpickle",
        "gym==0.25.2",
        "hydra-core",
        "einops",
        "easydict",
        "imageio[ffmpeg]",
        "opencv-python-headless",
        # extra_requirements.txt: wand (ImageMagick bindings) + scikit-image.
        "wand",
        "scikit-image",
    )
    # LIBERO-Plus's top-level package dir has no __init__.py either, so the same
    # clone + editable-install recipe the upstream repo uses is required.
    .run_commands(
        f"git clone https://github.com/sylvestf/LIBERO-plus.git /opt/LIBERO-plus"
        f" && git -C /opt/LIBERO-plus checkout {_LIBERO_PLUS_SHA}",
        "pip install -e /opt/LIBERO-plus",
        # First import interactively prompts for a dataset folder; answer 'N'
        # (use defaults) once at build time so ~/.libero/config.yaml is baked in
        # and runtime imports never block on stdin.
        "echo N | python -c 'import libero.libero'",
    )
    .env({"MUJOCO_GL": "egl", "PYOPENGL_PLATFORM": "egl"})
)

app = modal.App("archetype-libero-plus-env", image=image)


def _language_indices(suite: str) -> list[int]:
    """Absolute benchmark indices of the LANGUAGE-perturbation variants.

    Ports the LIBERO-Plus category selector: read task_classification.json,
    filter to ``category == "Language Instructions"`` for ``suite``, and return
    the (id-1) 0-based indices in file order (sorted by id for determinism).
    These indices are valid for ``Benchmark.get_task(i)`` etc. at the default
    task order (identity), which is what we use.
    """
    import json
    import os

    from libero.libero import get_libero_path

    # task_classification.json ships next to the benchmark package.
    bench_root = get_libero_path("benchmark_root")
    tc_path = os.path.join(bench_root, "benchmark", "task_classification.json")
    with open(tc_path) as f:
        classification = json.load(f)

    items = classification.get(suite, [])
    lang = [it for it in items if it.get("category") == LANGUAGE_CATEGORY]
    lang.sort(key=lambda it: int(it["id"]))
    return [int(it["id"]) - 1 for it in lang]


# Same concurrency model as LiberoEnvBatch: env instances live in container
# memory keyed by env_key, so each (suite, task_id) parameter set must own a
# stable container for the full episode. max_containers caps the TOTAL container
# count across parameter sets; 16 lets a small language sweep hold one dedicated
# container per task simultaneously.
@app.cls(
    cpu=4,
    memory=8192,
    timeout=1800,
    scaledown_window=300,
    max_containers=16,
    volumes={FRAMES_MOUNT: frames_volume},
)
class LiberoPlusEnvBatch:
    """A batch of LIBERO-Plus LANGUAGE-perturbation envs, keyed by env_key.

    ``task_id`` is the *ordinal within the suite's Language Instructions list*
    (0 = first language variant), NOT the absolute benchmark index. The worker
    resolves it to the absolute index via the category selector so callers stay
    small and stable.

    Method signatures and frame-sidecar behaviour are identical to
    ``LiberoEnvBatch`` (see ``modal_worker.py``); only the task selection and the
    perturbed ``task_language()`` differ.
    """

    suite: str = modal.parameter(default="libero_spatial")
    # Ordinal within the Language Instructions list (0-based), not absolute index.
    task_id: int = modal.parameter(default=0)
    camera_size: int = modal.parameter(default=128)

    @modal.enter()
    def load_suite(self):
        from libero.libero import benchmark

        self._envs: dict[int, Any] = {}
        self._suite = benchmark.get_benchmark_dict()[self.suite]()

        # Resolve the language ordinal → absolute benchmark index.
        lang_idx = _language_indices(self.suite)
        if not lang_idx:
            raise RuntimeError(f"no Language Instructions tasks for suite {self.suite!r}")
        if not (0 <= self.task_id < len(lang_idx)):
            raise IndexError(
                f"language task_id {self.task_id} out of range "
                f"(suite {self.suite!r} has {len(lang_idx)} language variants)"
            )
        self._abs_index = lang_idx[self.task_id]
        self._task = self._suite.get_task(self._abs_index)
        self._init_states = self._suite.get_task_init_states(self._abs_index)

        # Per-container session UUID for unique frame keys across parallel runs.
        self._session = str(uuid.uuid4())
        self._step_counts: dict[int, int] = {}

    def _make_env(self):
        from libero.libero.envs import OffScreenRenderEnv

        # LIBERO-Plus's ControlEnv strips the `_view_..._initstate_` suffix
        # internally, so the absolute-index bddl path is handed over unchanged.
        bddl = self._suite.get_task_bddl_file_path(self._abs_index)
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
        FRAMES_MOUNT). Callers reconstruct the full path as ``/frames/<ref>``.
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
            "gripper_qpos": [float(v) for v in obs["robot0_gripper_qpos"]],
        }

    def _proprio_with_frames(
        self, obs: dict, env_key: int, step_label: str, commit: bool = True
    ) -> dict[str, Any]:
        """Extract proprio and write frame PNGs to the volume.

        NOTE: LIBERO renders frames upside down; the policy client handles
        orientation. We ship raw pixels.
        """
        import base64

        import cv2

        out = self._proprio(obs)

        agentview_rgb = obs["agentview_image"]
        wrist_rgb = obs["robot0_eye_in_hand_image"]

        def encode_b64(rgb) -> str:
            ok, buf = cv2.imencode(".png", cv2.cvtColor(rgb, cv2.COLOR_RGB2BGR))
            if not ok:
                raise RuntimeError("png encode failed")
            return base64.b64encode(buf.tobytes()).decode()

        out["agentview_png"] = encode_b64(agentview_rgb)
        out["wrist_png"] = encode_b64(wrist_rgb)

        av_ref, wr_ref = self._write_frames(env_key, step_label, agentview_rgb, wrist_rgb)
        out["agentview_ref"] = av_ref
        out["wrist_ref"] = wr_ref

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
                result = self._proprio_with_frames(obs, env_id, step_label, commit=False)
            else:
                result = self._proprio(obs)

            result.update(
                {"reward": float(reward), "done": bool(done) or success, "success": success}
            )
            results.append(result)

        if with_frames:
            frames_volume.commit()

        return results

    @modal.method()
    def task_language(self) -> str:
        """The *perturbed* LIBERO-Plus language instruction for this variant."""
        return str(self._task.language)

    @modal.method()
    def info(self) -> dict[str, Any]:
        """Diagnostics: how the language ordinal resolved + the instruction.

        Useful for the smoke test / sweep manifests: confirms the absolute
        benchmark index, the number of language variants available, and the
        perturbed string actually being served.
        """
        return {
            "suite": self.suite,
            "language_task_id": self.task_id,
            "abs_index": self._abs_index,
            "num_language_variants": len(_language_indices(self.suite)),
            "task_name": str(self._task.name),
            "language": str(self._task.language),
            "n_init_states": int(len(self._init_states)),
        }


class LiberoPlusEnvClient:
    """Harness-side EnvClient adapter over a deployed ``LiberoPlusEnvBatch``.

    Import this from the Archetype (py3.12) process; it only needs the ``modal``
    client package, not LIBERO. Drop-in compatible with ``ModalEnvClient``: same
    ``reset`` / ``step`` / ``task_language`` surface, so the eval driver and
    baseline sweep accept it without change. ``task_id`` is the language ordinal.

    Pickles as ``(suite, task_id, with_frames)`` and reconnects lazily, because
    Daft batch UDFs serialize constructor args and a live Modal handle is not
    serializable.
    """

    def __init__(self, suite: str = "libero_spatial", task_id: int = 0, with_frames: bool = False):
        self._suite = suite
        self._task_id = task_id
        self._with_frames = with_frames
        self._worker = None

    def _get_worker(self):
        if self._worker is None:
            cls = modal.Cls.from_name("archetype-libero-plus-env", "LiberoPlusEnvBatch")
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

    def task_language(self) -> str:
        """The perturbed LIBERO-Plus instruction for this (suite, language ordinal).

        Threaded into ``ManipTask.instruction`` by the eval driver so the policy
        conditions on the *perturbed* string — the honest LIBERO-Plus baseline.
        """
        return str(self._get_worker().task_language.remote())


@app.local_entrypoint()
def smoke(suite: str = "libero_spatial", task_id: int = 0, steps: int = 3):
    """Load one LANGUAGE variant, confirm the perturbed instruction, step a few times."""
    worker = LiberoPlusEnvBatch(suite=suite, task_id=task_id)

    meta = worker.info.remote()
    print("=== LIBERO-Plus LANGUAGE smoke ===")
    print("suite:", meta["suite"])
    print("language_task_id (ordinal):", meta["language_task_id"])
    print("abs_index:", meta["abs_index"])
    print("num_language_variants:", meta["num_language_variants"])
    print("task_name:", meta["task_name"])
    print("PERTURBED instruction:", repr(meta["language"]))
    print("n_init_states:", meta["n_init_states"])

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
            f"agentview_ref={result.get('agentview_ref', 'MISSING')} "
            f"success={result.get('success')}"
        )
        assert "agentview_ref" in result, f"step {step_idx} must return agentview_ref"
        assert "wrist_ref" in result, f"step {step_idx} must return wrist_ref"

    print(f"smoke OK — perturbed instruction served, {steps + 1} ref pairs returned")
