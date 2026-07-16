# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""In-process VLA-JEPA policy — same container as the env, no Modal RPC.

The `.remote()` boundary in ``VlaJepaPolicyClient`` only existed to bridge two
incompatible interpreters: the env (py3.12) and the VLA-JEPA worker (py3.10 /
torch 2.5 / flash-attn-cp310). But VLA-JEPA's own pins say otherwise —
``torchvision==0.21.0`` → torch 2.6, ``numpy==1.26.4``, ``transformers==4.57.0``,
no ``python_requires`` — the same torch and numpy the modernized LIBERO image
already uses. So the model runs in the **same py3.12 container** as the env.

This client keeps full bit-compatibility with the published LIBERO eval by
reusing VLA-JEPA's own websocket model server (``server_policy.py``) — but as a
**localhost** subprocess in this container, not a cross-app Modal class. The
consequences of colocation:

- no Modal ``.remote()`` — ``act()`` calls a localhost websocket, in-process;
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
import subprocess
import time
from typing import Any

from bench.libero.clients import VlaJepaPolicyClient

_CHUNK_LEN = 7


class _ServerReg:
    """Process-global live state for one localhost model server (keyed by port):
    the subprocess, the websocket client, and the profiling counters. Lives in a
    module global, NOT on the policy instance, so the instance stays a picklable
    scalar stub across Daft's ``@daft.cls`` boundary while the server (and the
    counters the worker accumulates) are shared in the one interpreter."""

    def __init__(self) -> None:
        self.server: Any = None
        self.client: Any = None
        self.startup_seconds: float = 0.0
        self.infer_seconds: float = 0.0
        self.infer_count: int = 0


_POLICY_SERVERS: dict[int, _ServerReg] = {}


class InProcessVlaJepaPolicy:
    """``PolicyClient`` that runs the VLA-JEPA model server in this container.

    Lazily launches ``server_policy.py`` as a localhost subprocess on first use
    and proxies over a localhost websocket — the same server the upstream
    ``eval_libero.sh`` starts, so the action un-normalization and chunking match
    their published numbers. The subprocess + websocket live in a process-global
    registry (``_POLICY_SERVERS``) so this instance is a picklable scalar stub
    that survives the Daft worker boundary; the model is built once and reused.
    Per-``env_key`` chunk buffers mirror ``VlaJepaPolicyClient``; ``reset()``
    drops them at sweep boundaries.
    """

    def __init__(
        self,
        ckpt_dir: str = "/ckpts",
        vla_repo: str = "/opt/VLA-JEPA",
        port: int = 15084,
        use_bf16: bool = True,
        frames_dir: str = "/frames",
        unnorm_key: str = "franka",
        startup_timeout: float = 900.0,
        # The released config hardcodes attn_implementation=flash_attention_2.
        # use_sdpa patches it to torch's built-in SDPA — the discriminator for
        # flash-attn-wheel numerics (the wheel is the torch-2.6/cp312 analogue
        # of the proven torch-2.5/cp310 one, not the proven wheel itself).
        use_sdpa: bool = False,
    ) -> None:
        self._ckpt_dir = ckpt_dir
        self._vla_repo = vla_repo
        self._port = port
        self._use_bf16 = use_bf16
        self._frames_dir = frames_dir
        self._unnorm_key = unnorm_key
        self._startup_timeout = startup_timeout
        self._use_sdpa = use_sdpa
        self._buffers: dict[int, list[list[float]]] = {}

    def __getstate__(self) -> dict[str, Any]:
        # Picklable scalars only; chunk buffers must not replay across the
        # boundary (the live server/client live in the process-global registry).
        state = self.__dict__.copy()
        state["_buffers"] = {}
        return state

    @property
    def _reg(self) -> _ServerReg:
        reg = _POLICY_SERVERS.get(self._port)
        if reg is None:
            reg = _ServerReg()
            _POLICY_SERVERS[self._port] = reg
        return reg

    # Profiling counters read from the process-global registry, so the driver
    # sees what the Daft worker accumulated (same interpreter).
    @property
    def startup_seconds(self) -> float:
        return self._reg.startup_seconds

    @property
    def infer_seconds(self) -> float:
        return self._reg.infer_seconds

    @property
    def infer_count(self) -> int:
        return self._reg.infer_count

    # --- server lifecycle (localhost, in-container) -----------------------

    def _ensure_server(self) -> None:
        """Launch the local policy server once and connect the websocket client.

        Mirrors ``VlaJepaPolicy.start_server`` from the deleted
        ``vla_jepa_worker.py`` (git history) but in-process:
        the checkpoint is expected to already be present (image/volume), so this
        does no download — just patches the base-model paths, starts the server,
        and polls localhost until it accepts. Idempotent + process-global: a
        second caller (e.g. the Daft worker) reuses the already-running server.
        """
        reg = self._reg
        if reg.client is not None:
            return

        _start = time.monotonic()
        self._ensure_checkpoint()
        self._patch_base_model_paths()
        ckpt_path = f"{self._ckpt_dir}/LIBERO/checkpoints/VLA-JEPA-LIBERO.pt"
        cmd = [
            "python",
            f"{self._vla_repo}/deployment/model_server/server_policy.py",
            "--ckpt_path",
            ckpt_path,
            "--port",
            str(self._port),
            "--cuda",
            "0",
        ]
        if self._use_bf16:
            cmd.append("--use_bf16")
        reg.server = subprocess.Popen(cmd, cwd=self._vla_repo)  # noqa: S603

        from deployment.model_server.tools.websocket_policy_client import (  # noqa: PLC0415
            WebsocketClientPolicy,
        )

        deadline = time.monotonic() + self._startup_timeout
        last_err: Exception | None = None
        while time.monotonic() < deadline:
            if reg.server.poll() is not None:
                raise RuntimeError(f"server_policy.py exited early with {reg.server.returncode}")
            try:
                reg.client = WebsocketClientPolicy("127.0.0.1", self._port)
                reg.startup_seconds = time.monotonic() - _start
                return
            except Exception as exc:  # noqa: BLE001 - retry until deadline
                last_err = exc
                time.sleep(5.0)
        raise RuntimeError(f"local policy server never came up: {last_err}")

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

    def _infer_chunk(self, instruction: str, obs: dict[str, Any]) -> list[list[float]]:
        """One inference: local frames -> server -> un-normalized, gripper-mapped
        action chunk (robosuite convention)."""
        import numpy as np  # noqa: PLC0415

        agentview = self._load_and_preprocess_ref(obs["agentview_ref"])
        wrist = self._load_and_preprocess_ref(obs["wrist_ref"])
        state = VlaJepaPolicyClient._build_state(obs)
        payload = {
            "batch_images": [[agentview, wrist]],
            "instructions": [instruction],
            "unnorm_key": self._unnorm_key,
            "do_sample": False,
            "use_ddim": True,
            "num_ddim_steps": 10,
            "state": [np.asarray(state, dtype=np.float32)[None, :]],
        }
        reg = self._reg
        _t = time.monotonic()
        response = reg.client.infer(payload)
        reg.infer_seconds += time.monotonic() - _t
        reg.infer_count += 1
        normalized = np.clip(np.asarray(response["data"]["normalized_actions"])[0], -1, 1)
        chunk = self._unnormalize(normalized)
        # model gripper {0,1} -> robosuite {-1 open, +1 close}, same as the client.
        actions = [VlaJepaPolicyClient._convert_gripper(a) for a in chunk]
        if reg.infer_count <= 3:  # first-chunk evidence in the run log
            print(
                f"VLA_INFER#{reg.infer_count} refs=({obs['agentview_ref']}, {obs['wrist_ref']}) "
                f"state={[round(v, 4) for v in state]} a0={[round(v, 4) for v in actions[0]]}"
            )
        if reg.infer_count == 1:
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
        """Drop per-env chunk buffers (sweep boundary). Server stays up."""
        self._buffers = {}

    def act(
        self,
        env_keys: list[int],
        instructions: list[str],
        observations: list[dict[str, Any]],
    ) -> list[list[float]]:
        self._ensure_server()
        actions: list[list[float]] = []
        for env_key, instruction, obs in zip(env_keys, instructions, observations, strict=True):
            buf = self._buffers.get(env_key, [])
            if not buf:
                buf = self._infer_chunk(instruction, obs)
            action = buf.pop(0)
            self._buffers[env_key] = buf
            actions.append(action)
        return actions
