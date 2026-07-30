# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0
"""Bearer-authenticated allowlist proxy for the disposable vLLM endpoint."""

from __future__ import annotations

import hmac
import os

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import Response

UPSTREAM = "http://127.0.0.1:8001"
ALLOWED = {
    "/health",
    "/metrics",
    "/v1/chat/completions",
    "/v1/models",
}
app = FastAPI(openapi_url=None, docs_url=None, redoc_url=None)
client = httpx.AsyncClient(
    timeout=180,
    limits=httpx.Limits(max_connections=256, max_keepalive_connections=256),
)


@app.api_route("/{path:path}", methods=["GET", "POST"])
async def proxy(path: str, request: Request) -> Response:
    route = f"/{path}"
    expected = f"Bearer {os.environ['VLLM_API_KEY']}"
    if not hmac.compare_digest(request.headers.get("authorization", ""), expected):
        raise HTTPException(status_code=401, detail="unauthorized")
    if route not in ALLOWED:
        raise HTTPException(status_code=404, detail="route not exposed")
    try:
        upstream = await client.request(
            request.method,
            UPSTREAM + route,
            content=await request.body(),
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
    return Response(
        content=upstream.content,
        status_code=upstream.status_code,
        headers={"content-type": upstream.headers.get("content-type", "text/plain")},
    )
