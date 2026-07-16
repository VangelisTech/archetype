# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""In-process VLA-JEPA policy — same container as the env, no Modal RPC.

The `.remote()` boundary in ``VlaJepaPolicyClient`` only existed to bridge two
incompatible interpreters: the env (py3.12) and the VLA-JEPA worker (py3.10 /
torch 2.5 / flash-attn-cp310). But VLA-JEPA's own pins say otherwise —
``torchvision==0.21.0`` → torch 2.6, ``numpy==1.26.4``, ``transformers==4.57.0``,
no ``python_requires`` — the same torch and numpy the modernized LIBERO image
already uses. So the model runs in the **same py3.12 container** as the env.

This client loads VLA-JEPA's upstream ``baseframework`` directly and calls
``predict_action`` in the same interpreter. The deleted websocket wrapper did
only three things: convert numpy images to PIL, call that method, and serialize
the returned dict. Those transformations now happen here without a subprocess,
socket, port, or polling loop. The consequences of colocation:

- no Modal ``.remote()`` or localhost transport — ``act()`` calls PyTorch;
- no frames Volume — the env writes PNGs to the local filesystem and this policy
  reads them from the same local path (the cross-container commit/visibility bug
  the out-of-process design had simply cannot occur);
- one GPU container holds env + policy + ledger.

Preprocessing and un-normalization are identical to the worker's ``infer_refs``
(rotate 180 + resize 224; state ``(1,1,8)``; gripper binarized via
``dataset_statistics.json``); the 8-dim state, gripper sign, and chunk-buffer
cadence are the same as ``VlaJepaPolicyClient`` (whose static helpers we reuse).
"""

from __future__ import annotations

import json
import os
import threading
import time
from dataclasses import dataclass
from typing import Any

from bench.libero.clients import VlaJepaPolicyClient

_CHUNK_LEN = 7


@dataclass(frozen=True)
class _ModelConfig:
    """Load-time settings that define one resident PyTorch model."""

    ckpt_dir: str
    use_bf16: bool
    use_sdpa: bool


_MODEL_LOCK = threading.Lock()
_PROFILE_LOCK = threading.Lock()
_MODEL: Any = None
_MODEL_CONFIG: _ModelConfig | None = None
_STARTUP_SECONDS = 0.0
_INFER_SECONDS = 0.0
_INFER_COUNT = 0


class InProcessVlaJepaPolicy:
    """``PolicyClient`` that runs VLA-JEPA directly in this interpreter.

    The upstream model object lives in a process-global cache so Daft's
    serialized ``@daft.cls`` policy copies and successive task worlds reuse one
    GPU load. The cache identity contains every load-time option; a different
    precision, attention backend, or checkpoint replaces the resident model
    instead of silently reusing stale configuration. There is no model server
    or network client. Per-``env_key`` chunk buffers mirror
    ``VlaJepaPolicyClient``; ``reset()`` drops them at sweep boundaries.
    """

    def __init__(
        self,
        ckpt_dir: str = "/ckpts",
        use_bf16: bool = True,
        frames_dir: str = "/frames",
        unnorm_key: str = "franka",
        # The released config hardcodes attn_implementation=flash_attention_2.
        # use_sdpa patches it to torch's built-in SDPA — the discriminator for
        # flash-attn-wheel numerics (the wheel is the torch-2.6/cp312 analogue
        # of the proven torch-2.5/cp310 one, not the proven wheel itself).
        use_sdpa: bool = False,
    ) -> None:
        self._ckpt_dir = ckpt_dir
        self._use_bf16 = use_bf16
        self._frames_dir = frames_dir
        self._unnorm_key = unnorm_key
        self._use_sdpa = use_sdpa
        self._buffers: dict[int, list[list[float]]] = {}
        self._reset_profile()

    def __getstate__(self) -> dict[str, Any]:
        # Picklable scalars only; chunk buffers must not replay across the Daft
        # boundary. The live torch model is process-global, not instance state.
        state = self.__dict__.copy()
        state["_buffers"] = {}
        return state

    @property
    def _model_config(self) -> _ModelConfig:
        return _ModelConfig(
            ckpt_dir=self._ckpt_dir,
            use_bf16=self._use_bf16,
            use_sdpa=self._use_sdpa,
        )

    @staticmethod
    def _reset_profile() -> None:
        global _INFER_COUNT, _INFER_SECONDS, _STARTUP_SECONDS
        with _PROFILE_LOCK:
            _STARTUP_SECONDS = 0.0
            _INFER_SECONDS = 0.0
            _INFER_COUNT = 0

    # Profiling counters are process-global scalars so the driver sees what the
    # Daft policy copy accumulated in this one-interpreter benchmark.
    @property
    def startup_seconds(self) -> float:
        with _PROFILE_LOCK:
            return _STARTUP_SECONDS

    @property
    def infer_seconds(self) -> float:
        with _PROFILE_LOCK:
            return _INFER_SECONDS

    @property
    def infer_count(self) -> int:
        with _PROFILE_LOCK:
            return _INFER_COUNT

    # --- direct model lifecycle (same interpreter, no transport) ----------

    def _ensure_model(self) -> Any:
        """Return the resident upstream model, loading this config if needed."""
        global _MODEL, _MODEL_CONFIG, _STARTUP_SECONDS
        requested = self._model_config
        with _MODEL_LOCK:
            if _MODEL is not None and _MODEL_CONFIG == requested:
                return _MODEL

            # Drop an incompatible warm-container model before loading another
            # one. The overall cache identity is (checkpoint, bf16, sdpa), so a
            # discriminator run cannot be silently attributed to stale flags.
            _MODEL = None
            _MODEL_CONFIG = None
            started = time.monotonic()
            model = self._load_model()
            _MODEL = model
            _MODEL_CONFIG = requested
            with _PROFILE_LOCK:
                _STARTUP_SECONDS = time.monotonic() - started
            return model

    def _load_model(self) -> Any:
        """Load upstream ``baseframework`` exactly as its old wrapper did."""
        import torch  # noqa: PLC0415
        from starVLA.model.framework.base_framework import baseframework  # noqa: PLC0415

        self._ensure_checkpoint()
        self._patch_base_model_paths()
        ckpt_path = f"{self._ckpt_dir}/LIBERO/checkpoints/VLA-JEPA-LIBERO.pt"
        model = baseframework.from_pretrained(ckpt_path)
        if self._use_bf16:
            model = model.to(torch.bfloat16)
        return model.to(torch.device("cuda:0")).eval()

    def _ensure_checkpoint(self) -> None:
        """Idempotently fetch the released run directory (config + stats +
        checkpoint) into ``ckpt_dir`` if absent. snapshot_download skips files
        already present, so a pre-populated ``vla-jepa-ckpts`` volume is a no-op.
        Drop any previously-patched config first so patching always sees pristine
        upstream copies."""
        from pathlib import Path  # noqa: PLC0415

        from huggingface_hub import snapshot_download  # noqa: PLC0415

        for cfg in Path(f"{self._ckpt_dir}/LIBERO").glob("config.*"):
            cfg.unlink()
        snapshot_download(
            repo_id="ginwind/VLA-JEPA",
            allow_patterns=["LIBERO/**"],
            local_dir=self._ckpt_dir,
        )

    def _patch_base_model_paths(self) -> None:
        """Rewrite the released config's absolute base-model paths to HF repo ids
        (same patch the worker applies) so transformers downloads Qwen3-VL /
        V-JEPA2 into the cache instead of the author's local disk."""
        import re  # noqa: PLC0415
        from pathlib import Path  # noqa: PLC0415

        replacements = {
            "Qwen3-VL-2B-Instruct": "Qwen/Qwen3-VL-2B-Instruct",
            "vjepa2-vitl-fpc64-256": "facebook/vjepa2-vitl-fpc64-256",
        }
        for cfg in Path(f"{self._ckpt_dir}/LIBERO").glob("config.*"):
            text = cfg.read_text()
            patched = text
            for marker, repo_id in replacements.items():
                patched = re.sub(rf"/home/[\w./-]*{re.escape(marker)}[\w./-]*", repo_id, patched)
            if self._use_sdpa:
                patched = patched.replace("flash_attention_2", "sdpa")
            if patched != text:
                cfg.write_text(patched)

    # --- frame preprocessing (identical to worker.infer_refs) -------------

    def _load_and_preprocess_ref(self, ref: str) -> Any:
        import cv2  # noqa: PLC0415
        import numpy as np  # noqa: PLC0415

        full_path = os.path.join(self._frames_dir, ref)
        bgr = cv2.imread(full_path, cv2.IMREAD_COLOR)
        if bgr is None:
            raise FileNotFoundError(f"frame not found: {full_path!r}")
        rgb = cv2.cvtColor(bgr, cv2.COLOR_BGR2RGB)
        rgb = np.ascontiguousarray(rgb[::-1, ::-1])  # LIBERO frames are upside down
        return cv2.resize(rgb, (224, 224), interpolation=cv2.INTER_AREA)

    def _unnormalize(self, normalized: Any) -> list[list[float]]:
        import numpy as np  # noqa: PLC0415

        with open(f"{self._ckpt_dir}/LIBERO/dataset_statistics.json") as f:
            norm_stats = json.load(f)
        key = self._unnorm_key
        if key not in norm_stats:
            (key,) = norm_stats.keys()
        stats = norm_stats[key]["action"]
        low = np.asarray(stats["min"])
        high = np.asarray(stats["max"])
        mask = np.asarray(stats.get("mask", np.ones_like(low, dtype=bool)))
        normalized[:, 6] = np.where(normalized[:, 6] < 0.5, 0, 1)  # gripper binarize
        actions = np.where(mask, 0.5 * (normalized + 1) * (high - low) + low, normalized)
        return [[float(v) for v in row] for row in actions]

    def _predict_normalized(
        self,
        instruction: str,
        agentview: Any,
        wrist: Any,
        state: list[float],
    ) -> Any:
        """Call upstream ``predict_action`` directly with its exact payload."""
        global _INFER_COUNT, _INFER_SECONDS

        import numpy as np  # noqa: PLC0415
        from PIL import Image  # noqa: PLC0415

        model = self._ensure_model()
        started = time.monotonic()
        response = model.predict_action(
            # The deleted websocket wrapper's only input transform was
            # ``Image.fromarray`` for these uint8 RGB arrays.
            batch_images=[[Image.fromarray(agentview), Image.fromarray(wrist)]],
            instructions=[instruction],
            unnorm_key=self._unnorm_key,
            do_sample=False,
            use_ddim=True,
            num_ddim_steps=10,
            state=[np.asarray(state, dtype=np.float32)[None, :]],
        )
        elapsed = time.monotonic() - started
        with _PROFILE_LOCK:
            _INFER_SECONDS += elapsed
            _INFER_COUNT += 1
        return np.clip(np.asarray(response["normalized_actions"])[0], -1, 1)

    def _infer_chunk(self, instruction: str, obs: dict[str, Any]) -> list[list[float]]:
        """One direct inference -> un-normalized robosuite action chunk."""

        agentview = self._load_and_preprocess_ref(obs["agentview_ref"])
        wrist = self._load_and_preprocess_ref(obs["wrist_ref"])
        state = VlaJepaPolicyClient._build_state(obs)
        normalized = self._predict_normalized(instruction, agentview, wrist, state)
        chunk = self._unnormalize(normalized)
        # model gripper {0,1} -> robosuite {-1 open, +1 close}, same as the client.
        actions = [VlaJepaPolicyClient._convert_gripper(a) for a in chunk]
        infer_count = self.infer_count
        if infer_count <= 3:  # first-chunk evidence in the run log
            print(
                f"VLA_INFER#{infer_count} refs=({obs['agentview_ref']}, {obs['wrist_ref']}) "
                f"state={[round(v, 4) for v in state]} a0={[round(v, 4) for v in actions[0]]}"
            )
        if infer_count == 1:
            # Thumbnail of the EXACT preprocessed model input (post rotate+resize),
            # so a run log can prove what the model saw. ~3KB base64 JPEG.
            import base64  # noqa: PLC0415

            import cv2  # noqa: PLC0415

            thumb = cv2.resize(agentview, (64, 64), interpolation=cv2.INTER_AREA)
            ok, jpg = cv2.imencode(".jpg", cv2.cvtColor(thumb, cv2.COLOR_RGB2BGR))
            if ok:
                print(f"VLA_INPUT_THUMB_B64 {base64.b64encode(jpg.tobytes()).decode()}")
        return actions

    # --- PolicyClient protocol --------------------------------------------

    def reset(self) -> None:
        """Drop per-env chunk buffers; the direct model stays resident."""
        self._buffers = {}

    def act(
        self,
        env_keys: list[int],
        instructions: list[str],
        observations: list[dict[str, Any]],
    ) -> list[list[float]]:
        actions: list[list[float]] = []
        for env_key, instruction, obs in zip(env_keys, instructions, observations, strict=True):
            buf = self._buffers.get(env_key, [])
            if not buf:
                buf = self._infer_chunk(instruction, obs)
            action = buf.pop(0)
            self._buffers[env_key] = buf
            actions.append(action)
        return actions
