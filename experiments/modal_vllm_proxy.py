# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0
"""Authenticated vLLM proxy with request-aligned GPU sampling."""

from __future__ import annotations

import hmac
import json
import os
import subprocess
import threading
import time
from collections import deque
from contextlib import asynccontextmanager
from typing import Any

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import Response

UPSTREAM = "http://127.0.0.1:8001"
ALLOWED = {
    "/gpu/info",
    "/gpu/mark",
    "/gpu/samples",
    "/health",
    "/metrics",
    "/v1/chat/completions",
    "/v1/models",
}
samples: deque[dict[str, float | int]] = deque(maxlen=36_000)
sample_lock = threading.Lock()
active_requests = 0
sampler: subprocess.Popen[str] | None = None


def gpu_info() -> dict[str, str]:
    completed = subprocess.run(
        [
            "nvidia-smi",
            "--query-gpu=name,uuid,memory.total,driver_version",
            "--format=csv,noheader,nounits",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    name, uuid, memory_mib, driver = completed.stdout.strip().split(",", 3)
    return {
        "gpu_name": name.strip(),
        "gpu_uuid": uuid.strip(),
        "gpu_memory_total_mib": memory_mib.strip(),
        "driver_version": driver.strip(),
        "modal_gpu_request": os.environ["MODAL_BENCH_GPU"],
        "model": os.environ["MODAL_BENCH_MODEL"],
        "model_revision": os.environ["MODAL_BENCH_REVISION"],
        "served_model": os.environ["MODAL_BENCH_SERVED_MODEL"],
    }


def sample_gpu() -> None:
    global sampler
    sampler = subprocess.Popen(
        [
            "nvidia-smi",
            "--query-gpu=utilization.gpu,utilization.memory,power.draw,memory.used",
            "--format=csv,noheader,nounits",
            "--loop-ms=200",
        ],
        stdout=subprocess.PIPE,
        text=True,
    )
    assert sampler.stdout is not None
    for line in sampler.stdout:
        try:
            sm, memory, power, memory_used = (float(value.strip()) for value in line.split(","))
        except ValueError:
            continue
        with sample_lock:
            samples.append(
                {
                    "time_ns": time.time_ns(),
                    "active_requests": active_requests,
                    "sm": sm,
                    "memory": memory,
                    "power_w": power,
                    "memory_mib": memory_used,
                }
            )


@asynccontextmanager
async def lifespan(_app: FastAPI) -> Any:
    thread = threading.Thread(target=sample_gpu, daemon=True)
    thread.start()
    try:
        yield
    finally:
        if sampler is not None:
            sampler.terminate()
        await client.aclose()


app = FastAPI(
    openapi_url=None,
    docs_url=None,
    redoc_url=None,
    lifespan=lifespan,
)
client = httpx.AsyncClient(
    timeout=360,
    limits=httpx.Limits(max_connections=256, max_keepalive_connections=256),
)


@app.api_route("/{path:path}", methods=["GET", "POST"])
async def proxy(path: str, request: Request) -> Response:
    global active_requests
    route = f"/{path}"
    expected = f"Bearer {os.environ['VLLM_API_KEY']}"
    if not hmac.compare_digest(request.headers.get("authorization", ""), expected):
        raise HTTPException(status_code=401, detail="unauthorized")
    if route not in ALLOWED:
        raise HTTPException(status_code=404, detail="route not exposed")
    if route == "/gpu/info":
        return Response(json.dumps(gpu_info()), media_type="application/json")
    if route == "/gpu/mark":
        return Response(
            json.dumps({"time_ns": time.time_ns()}),
            media_type="application/json",
        )
    if route == "/gpu/samples":
        since = int(request.query_params.get("since_ns", "0"))
        until = int(request.query_params.get("until_ns", str(2**63 - 1)))
        with sample_lock:
            selected = [sample for sample in samples if since <= sample["time_ns"] <= until]
        return Response(json.dumps(selected), media_type="application/json")
    is_chat = route == "/v1/chat/completions"
    if is_chat:
        with sample_lock:
            active_requests += 1
    try:
        upstream = await client.request(
            request.method,
            UPSTREAM + route,
            content=await request.body(),
            params=request.query_params,
            headers={
                "accept": request.headers.get("accept", "*/*"),
                "authorization": expected,
                "content-type": request.headers.get(
                    "content-type",
                    "application/json",
                ),
            },
        )
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=502, detail="upstream unavailable") from exc
    finally:
        if is_chat:
            with sample_lock:
                active_requests -= 1
    return Response(
        content=upstream.content,
        status_code=upstream.status_code,
        headers={"content-type": upstream.headers.get("content-type", "text/plain")},
    )
