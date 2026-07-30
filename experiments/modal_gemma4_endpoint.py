# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0
"""Disposable OpenAI endpoint for real Daft coding missions.

The default is Gemma 4 E4B on one L4. Environment overrides can select a larger
coding model and the smallest GPU that fits it. The endpoint exposes at most
one replica plus protected request-aligned GPU metrics. Deployment requires a
named ``hf-token`` Modal secret and local ``MODAL_BENCH_API_KEY``.
"""

from __future__ import annotations

import json
import os
import subprocess
import time
import urllib.request

import modal

APP_NAME = "archetype-daft-gemma4-saturation"
MODEL = os.getenv("MODAL_BENCH_MODEL", "google/gemma-4-E4B-it")
MODEL_REVISION = os.getenv(
    "MODAL_BENCH_REVISION",
    "ee0ef6023621cff504d758262d4e04895a5af4a2",
)
SERVED_MODEL = os.getenv("MODAL_BENCH_SERVED_MODEL", "gemma-4-e4b")
GPU = os.getenv("MODAL_BENCH_GPU", "L4")
MAX_MODEL_LEN = os.getenv("MODAL_BENCH_MAX_MODEL_LEN", "16384")
GPU_MEMORY_UTILIZATION = os.getenv("MODAL_BENCH_GPU_MEMORY_UTILIZATION", "0.96")
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
            "MODAL_BENCH_GPU_MEMORY_UTILIZATION": GPU_MEMORY_UTILIZATION,
            "MODAL_BENCH_GPU": GPU,
            "MODAL_BENCH_MAX_MODEL_LEN": MAX_MODEL_LEN,
            "MODAL_BENCH_MODEL": MODEL,
            "MODAL_BENCH_REVISION": MODEL_REVISION,
            "MODAL_BENCH_SERVED_MODEL": SERVED_MODEL,
            "VLLM_LOG_STATS_INTERVAL": "1",
        }
    )
)
hf_cache = modal.Volume.from_name(
    "throughput-bench-hf-cache",
    create_if_missing=True,
)
compile_cache = modal.Volume.from_name(
    "throughput-bench-pronto-cache",
    create_if_missing=True,
)
api_secret = (
    modal.Secret.from_dict({"VLLM_API_KEY": os.environ["MODAL_BENCH_API_KEY"]})
    if modal.is_local()
    else modal.Secret.from_dict({})
)
app = modal.App(APP_NAME)


@app.function(
    image=image,
    gpu=GPU,
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
@modal.concurrent(max_inputs=36, target_inputs=32)
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
        MAX_MODEL_LEN,
        "--max-num-seqs",
        "32",
        "--gpu-memory-utilization",
        GPU_MEMORY_UTILIZATION,
        "--limit-mm-per-prompt",
        json.dumps({"image": 0, "video": 0, "audio": 0}),
    ]
    if MODEL.startswith("Qwen/"):
        vllm_command.extend(["--language-model-only", "--reasoning-parser", "qwen3"])
    else:
        vllm_command.extend(
            [
                "--default-chat-template-kwargs",
                json.dumps({"enable_thinking": False}),
            ]
        )
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
