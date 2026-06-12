# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""VLA-JEPA policy worker on Modal — the GPU half of PolicyClient.

Wraps the upstream inference stack whole rather than reimplementing it:
the container launches VLA-JEPA's own websocket model server
(``deployment/model_server/server_policy.py``, the same process their
``eval_libero.sh`` starts) and ``infer`` proxies to it over localhost.
That keeps us bit-compatible with their published LIBERO evaluation —
same server, same action un-normalization, same chunking.

Checkpoint: ``VLA-JEPA-LIBERO.pt`` from https://huggingface.co/ginwind/VLA-JEPA,
cached in the ``vla-jepa-ckpts`` Modal volume on first start.

Setup:
    modal deploy bench/libero/vla_jepa_worker.py
    modal run bench/libero/vla_jepa_worker.py      # smoke: one inference

STATUS: verified 2026-06-11 — `modal run` smoke returns a real 7-step x
7-dim un-normalized action chunk from the released checkpoint on an
L40S. Payload contract confirmed against the live server: batch_images
(rotated 180 + resized 224), instructions, unnorm_key, state shaped
(1, 1, 8); response under data.normalized_actions.
"""

# NOTE: no `from __future__ import annotations` — modal.parameter()
# validates real annotation objects.
import base64
import subprocess
import time
from typing import Any

import modal

REPO = "https://github.com/ginwind/VLA-JEPA.git"
CKPT_REPO = "ginwind/VLA-JEPA"
CKPT_FILE = "LIBERO/checkpoints/VLA-JEPA-LIBERO.pt"
CKPT_DIR = "/ckpts"
SERVER_PORT = 15084

image = (
    modal.Image.from_registry("nvidia/cuda:12.4.1-devel-ubuntu22.04", add_python="3.10")
    .apt_install("git", "libgl1", "libglib2.0-0")
    .pip_install("torch==2.5.1", "packaging", "ninja", "wheel", "huggingface_hub")
    # Their deployment/ websocket server+client deps are not in requirements.txt.
    .pip_install("websockets", "msgpack", "msgpack-numpy")
    .run_commands(
        f"git clone --depth 1 {REPO} /opt/VLA-JEPA",
        "pip install -r /opt/VLA-JEPA/requirements.txt",
        "pip install -e /opt/VLA-JEPA",
    )
    # flash-attn from the release wheel that exactly matches
    # torch 2.5 / cu12 / cxx11abiFALSE / cp310 — pip's sdist path guesses
    # the ABI wrong and produces undefined C10 symbols at import.
    .pip_install(
        "https://github.com/Dao-AILab/flash-attention/releases/download/"
        "v2.7.4.post1/flash_attn-2.7.4.post1+cu12torch2.5cxx11abiFALSE-cp310-cp310-linux_x86_64.whl"
    )
    .env({"HF_HOME": f"{CKPT_DIR}/hf-cache", "PYTHONPATH": "/opt/VLA-JEPA"})
)

app = modal.App("archetype-vla-jepa", image=image)
ckpt_volume = modal.Volume.from_name("vla-jepa-ckpts", create_if_missing=True)


@app.cls(
    gpu="L40S",
    volumes={CKPT_DIR: ckpt_volume},
    timeout=3600,
    scaledown_window=600,
    max_containers=1,
)
class VlaJepaPolicy:
    use_bf16: int = modal.parameter(default=1)

    @modal.enter()
    def start_server(self):
        # from_pretrained expects the full run directory (config.yaml +
        # dataset_statistics.json beside checkpoints/), not just the .pt.
        # Drop previously-patched config files first so the snapshot always
        # starts from pristine upstream copies (patching must see originals).
        from pathlib import Path

        from huggingface_hub import snapshot_download

        for cfg in Path(f"{CKPT_DIR}/LIBERO").glob("config.*"):
            cfg.unlink()
        snapshot_download(repo_id=CKPT_REPO, allow_patterns=["LIBERO/**"], local_dir=CKPT_DIR)
        ckpt_path = f"{CKPT_DIR}/{CKPT_FILE}"
        self._patch_base_model_paths()
        ckpt_volume.commit()

        cmd = [
            "python",
            "/opt/VLA-JEPA/deployment/model_server/server_policy.py",
            "--ckpt_path",
            ckpt_path,
            "--port",
            str(SERVER_PORT),
            "--cuda",
            "0",
        ]
        if self.use_bf16:
            cmd.append("--use_bf16")
        self._server = subprocess.Popen(cmd, cwd="/opt/VLA-JEPA")

        from deployment.model_server.tools.websocket_policy_client import (
            WebsocketClientPolicy,
        )

        # Model load takes a while; poll until the websocket accepts.
        deadline = time.monotonic() + 900
        last_err = None
        while time.monotonic() < deadline:
            if self._server.poll() is not None:
                raise RuntimeError(f"server_policy.py exited early with {self._server.returncode}")
            try:
                self._client = WebsocketClientPolicy("127.0.0.1", SERVER_PORT)
                return
            except Exception as exc:  # noqa: BLE001 - retry until deadline
                last_err = exc
                time.sleep(5.0)
        raise RuntimeError(f"policy server never came up: {last_err}")

    @staticmethod
    def _patch_base_model_paths():
        """The released config.yaml/config.json reference the author's local
        disk for the Qwen3-VL and V-JEPA2 base models; rewrite those paths to
        HF repo ids so transformers downloads them into the volume cache."""
        from pathlib import Path

        replacements = {
            "Qwen3-VL-2B-Instruct": "Qwen/Qwen3-VL-2B-Instruct",
            "vjepa2-vitl-fpc64-256": "facebook/vjepa2-vitl-fpc64-256",
        }
        for cfg in Path(f"{CKPT_DIR}/LIBERO").glob("config.*"):
            text = cfg.read_text()
            patched = text
            for marker, repo_id in replacements.items():
                import re

                # Anchored to the author's /home/... prefix: idempotent, can
                # never re-match an already-substituted HF repo id.
                patched = re.sub(rf"/home/[\w./-]*{re.escape(marker)}[\w./-]*", repo_id, patched)
            if patched != text:
                cfg.write_text(patched)
                print(f"patched base-model paths in {cfg.name}")

    @modal.method()
    def infer(
        self,
        agentview_png: str,
        wrist_png: str,
        instruction: str,
        state: list[float],
        unnorm_key: str = "franka",
    ) -> list[list[float]]:
        """One policy inference -> an un-normalized action chunk.

        Payload and response shape follow upstream M1Inference
        (examples/LIBERO/model2libero_interface.py): the server returns
        normalized actions (B, chunk, D) in [-1, 1]; we un-normalize here
        with the checkpoint's dataset_statistics.json, gripper binarized,
        exactly as their LIBERO eval does.
        """
        import json

        import cv2
        import numpy as np

        def decode(b64: str) -> "np.ndarray":
            buf = np.frombuffer(base64.b64decode(b64), dtype=np.uint8)
            rgb = cv2.cvtColor(cv2.imdecode(buf, cv2.IMREAD_COLOR), cv2.COLOR_BGR2RGB)
            # Inputs are raw LIBERO frames; rotate 180 degrees to match the
            # model's train-time preprocessing (their eval does [::-1, ::-1]).
            rgb = np.ascontiguousarray(rgb[::-1, ::-1])
            return cv2.resize(rgb, (224, 224), interpolation=cv2.INTER_AREA)

        payload: dict[str, Any] = {
            "batch_images": [[decode(agentview_png), decode(wrist_png)]],
            "instructions": [instruction],
            "unnorm_key": unnorm_key,
            "do_sample": False,
            "use_ddim": True,
            "num_ddim_steps": 10,
            # Upstream wraps an already-batched (1, 8) state in a list, so the
            # server sees (1, 1, 8); state = eef_pos(3) + axis-angle(3) +
            # gripper_qpos(2).
            "state": [np.asarray(state, dtype=np.float32)[None, :]],
        }
        response = self._client.infer(payload)
        normalized = np.clip(np.asarray(response["data"]["normalized_actions"])[0], -1, 1)

        with open(f"{CKPT_DIR}/LIBERO/dataset_statistics.json") as f:
            norm_stats = json.load(f)
        if unnorm_key not in norm_stats:
            (unnorm_key,) = norm_stats.keys()
        stats = norm_stats[unnorm_key]["action"]
        low = np.asarray(stats["min"])
        high = np.asarray(stats["max"])
        mask = np.asarray(stats.get("mask", np.ones_like(low, dtype=bool)))

        normalized[:, 6] = np.where(normalized[:, 6] < 0.5, 0, 1)
        actions = np.where(mask, 0.5 * (normalized + 1) * (high - low) + low, normalized)
        return [[float(v) for v in row] for row in actions]


@app.local_entrypoint()
def smoke():
    """One inference against a synthetic frame; prints the action chunk."""
    import io

    try:
        from PIL import Image
    except ImportError as exc:  # pragma: no cover
        raise SystemExit("pip install pillow for the smoke entrypoint") from exc

    img = Image.new("RGB", (256, 256), color=(127, 127, 127))
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    png_b64 = base64.b64encode(buf.getvalue()).decode()

    policy = VlaJepaPolicy()
    chunk = policy.infer.remote(
        agentview_png=png_b64,
        wrist_png=png_b64,
        instruction="pick up the black bowl and place it on the plate",
        state=[0.0] * 8,
    )
    print(f"action chunk: {len(chunk)} steps x {len(chunk[0])} dims")
    print(f"first action: {chunk[0]}")
    print("vla-jepa smoke OK")
