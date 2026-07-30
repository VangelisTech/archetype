# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0
"""Disposable one-GPU OpenAI-compatible endpoint for Daft saturation proofs.

Deploy with MODAL_BENCH_API_KEY set, point the vectorized agent harness at the
printed URL, then stop the app. The fixed single replica makes throughput
changes attributable to vLLM continuous batching on one GPU, not autoscaling.
"""

from __future__ import annotations

import json
import os
import subprocess
import time
import urllib.request

import modal

APP_NAME = "archetype-daft-gemma4-saturation"
MODEL = "google/gemma-4-E4B-it"
MODEL_REVISION = "ee0ef6023621cff504d758262d4e04895a5af4a2"
SERVED_MODEL = "gemma-4-e4b"
PUBLIC_PORT = 8000
VLLM_PORT = 8001

image = (
    modal.Image.from_registry(
        "nvidia/cuda:12.9.0-devel-ubuntu22.04",
        add_python="3.12",
    )
    .entrypoint([])
    .uv_pip_install("fastapi==0.128.8", "httpx==0.28.1", "vllm==0.21.0")
    .add_local_file(
        "experiments/modal_vllm_proxy.py",
        "/root/modal_vllm_proxy.py",
        copy=True,
    )
    .env(
        {
            "HF_XET_HIGH_PERFORMANCE": "1",
            "VLLM_LOG_STATS_INTERVAL": "1",
        }
    )
)
hf_cache = modal.Volume.from_name(
    "throughput-bench-hf-cache",
    create_if_missing=False,
)
compile_cache = modal.Volume.from_name(
    "throughput-bench-pronto-cache",
    create_if_missing=False,
)
api_secret = (
    modal.Secret.from_dict({"VLLM_API_KEY": os.environ["MODAL_BENCH_API_KEY"]})
    if modal.is_local()
    else modal.Secret.from_dict({})
)
app = modal.App(APP_NAME)


@app.function(
    image=image,
    gpu="H100",
    min_containers=0,
    max_containers=1,
    scaledown_window=300,
    startup_timeout=900,
    timeout=3600,
    secrets=[modal.Secret.from_name("hf-token"), api_secret],
    volumes={
        "/root/.cache/huggingface": hf_cache,
        "/root/.cache/vllm": compile_cache,
    },
)
@modal.concurrent(max_inputs=132, target_inputs=128)
@modal.web_server(PUBLIC_PORT, startup_timeout=900)
def serve() -> None:
    vllm_command = [
        "vllm",
        "serve",
        MODEL,
        "--revision",
        MODEL_REVISION,
        "--served-model-name",
        SERVED_MODEL,
        "--host",
        "127.0.0.1",
        "--port",
        str(VLLM_PORT),
        "--async-scheduling",
        "--no-enforce-eager",
        "--max-model-len",
        "8192",
        "--gpu-memory-utilization",
        "0.92",
        "--limit-mm-per-prompt",
        json.dumps({"image": 0, "video": 0, "audio": 0}),
        "--default-chat-template-kwargs",
        json.dumps({"enable_thinking": False}),
    ]
    engine = subprocess.Popen(vllm_command)
    deadline = time.monotonic() + 840
    while time.monotonic() < deadline:
        if engine.poll() is not None:
            raise RuntimeError(f"vLLM exited during startup: {engine.returncode}")
        try:
            urllib.request.urlopen(
                f"http://127.0.0.1:{VLLM_PORT}/health",
                timeout=1,
            )
            break
        except OSError:
            time.sleep(1)
    else:
        raise RuntimeError("vLLM did not become ready")
    subprocess.Popen(["nvidia-smi", "dmon", "-s", "pucm", "-d", "1"])
    subprocess.Popen(
        [
            "uvicorn",
            "modal_vllm_proxy:app",
            "--host",
            "0.0.0.0",
            "--port",
            str(PUBLIC_PORT),
        ],
        cwd="/root",
    )
