# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0
"""Thin HTTP client for the version 0.7 shared operation contract."""

from __future__ import annotations

import json
import os
from pathlib import Path

import httpx
import typer
from archetype_native import wire

app = typer.Typer(name="archetype", help="Native ECS worlds, immutable programs and complete cuts")
world_app = typer.Typer(help="Explicit world lifecycle and complete-cut history")
app.add_typer(world_app, name="world")


def _send(raw: bytes, *, url: str | None = None, token: str | None = None) -> None:
    try:
        decoded = wire.Request.decode(raw)
    except (ValueError, TypeError, KeyError, UnicodeError):
        typer.echo("Invalid shared operation request", err=True)
        raise typer.Exit(2) from None
    credential = token or os.environ.get("ARCHETYPE_TOKEN", "")
    if not 24 <= len(credential) <= 4096 or any(c.isspace() for c in credential):
        typer.echo("Configure a real ARCHETYPE_TOKEN credential", err=True)
        raise typer.Exit(2)
    base = (url or os.environ.get("ARCHETYPE_URL", "http://127.0.0.1:8000")).rstrip("/")
    try:
        with httpx.Client(timeout=30) as client:
            with client.stream(
                "POST",
                base + "/invoke",
                content=raw,
                headers={
                    "Authorization": "Bearer " + credential,
                    "Content-Type": "application/json",
                },
            ) as result:
                response = bytearray()
                for chunk in result.iter_bytes():
                    if len(response) + len(chunk) > wire.MAX_RESPONSE_BYTES:
                        raise ValueError("Response bound")
                    response.extend(chunk)
                value = wire.decode_response(bytes(response), decoded)
                if value["ok"] and result.status_code != 200:
                    raise ValueError("Invalid success status")
    except httpx.HTTPError:
        typer.echo("Transport failed; dispatched outcome is unknown", err=True)
        raise typer.Exit(1) from None
    except (ValueError, TypeError, KeyError, UnicodeError, RecursionError):
        typer.echo("Invalid server response; dispatched outcome is unknown", err=True)
        raise typer.Exit(1) from None
    typer.echo(json.dumps(value, ensure_ascii=True, allow_nan=False))
    if not value["ok"]:
        raise typer.Exit(1)


@app.command()
def invoke(request: Path, url: str | None = None, token: str | None = None):
    """Send one exact operation document (including create, compose, admit or fork)."""
    try:
        with request.open("rb") as stream:
            raw = stream.read(wire.MAX_REQUEST_BYTES + 1)
    except OSError:
        typer.echo("Cannot read request document", err=True)
        raise typer.Exit(2) from None
    _send(raw, url=url, token=token)


def _world(operation: str, resource: str, arguments: dict, url, token):
    _send(
        wire.encode_request(
            {"version": 1, "resource": resource, "operation": operation, "arguments": arguments}
        ),
        url=url,
        token=token,
    )


@world_app.command()
def status(resource: str, url: str | None = None, token: str | None = None):
    _world("status", resource, {}, url, token)


@world_app.command()
def start(resource: str, url: str | None = None, token: str | None = None):
    _world("start", resource, {}, url, token)


@world_app.command()
def stop(resource: str, url: str | None = None, token: str | None = None):
    _world("stop", resource, {}, url, token)


@world_app.command()
def history(
    resource: str,
    offset: int = 0,
    limit: int = 32,
    url: str | None = None,
    token: str | None = None,
):
    _world("history", resource, {"offset": str(offset), "limit": str(limit)}, url, token)


@app.command()
def serve(host: str = "127.0.0.1", port: int = 8000):
    """Start the configured authenticated HTTP/MCP host. No developer-role credentials."""
    import uvicorn

    uvicorn.run("archetype.api.app:create_app", factory=True, host=host, port=port)


if __name__ == "__main__":
    app()
