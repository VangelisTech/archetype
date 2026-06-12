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

STATUS: scaffold pending first GPU run. The websocket payload keys below
follow examples/LIBERO/eval_libero.py + model2libero_interface.py
(M1Inference) and must be confirmed against the live server on the first
smoke; flash-attn build time is the other known unknown.
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
    .pip_install("torch>=2.4,<2.6", "packaging", "ninja", "wheel", "huggingface_hub")
    .run_commands(
        f"git clone --depth 1 {REPO} /opt/VLA-JEPA",
        "pip install -r /opt/VLA-JEPA/requirements.txt",
        "pip install -e /opt/VLA-JEPA",
    )
    # flash-attn last: it needs torch present at build time, and this layer
    # is the slow one — keep everything else cached above it.
    .run_commands("pip install flash-attn --no-build-isolation")
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
        from huggingface_hub import hf_hub_download

        ckpt_path = hf_hub_download(repo_id=CKPT_REPO, filename=CKPT_FILE, local_dir=CKPT_DIR)
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

    @modal.method()
    def infer(
        self,
        agentview_png: str,
        wrist_png: str,
        instruction: str,
        state: list[float],
    ) -> list[list[float]]:
        """One policy inference -> an action chunk (list of 7-dim actions).

        Payload keys mirror examples/LIBERO/eval_libero.py; confirm on
        first live smoke.
        """
        import cv2
        import numpy as np

        def decode(b64: str) -> "np.ndarray":
            buf = np.frombuffer(base64.b64decode(b64), dtype=np.uint8)
            return cv2.cvtColor(cv2.imdecode(buf, cv2.IMREAD_COLOR), cv2.COLOR_BGR2RGB)

        payload: dict[str, Any] = {
            "observation/image": decode(agentview_png),
            "observation/wrist_image": decode(wrist_png),
            "observation/state": np.asarray(state, dtype=np.float32),
            "prompt": instruction,
        }
        result = self._client.infer(payload)
        actions = np.asarray(result["actions"])
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
