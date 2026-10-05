# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0
"""Owned HTTP/MCP host over the same native runtime and closed operations."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from archetype_native.ingress import PrincipalVerifier

from archetype.api.config import ServerConfig
from archetype.api.principals import PrincipalDirectory
from archetype.wiring import artifact_workflow, build_runtime_resources


def create_app(*, config: ServerConfig | None = None, verifier: PrincipalVerifier | None = None):
    """Operator factory. Binds no socket; requires explicit principal/resource grants."""
    from archetype_native.ingress import Ingress
    from archetype_transports import create_app as transport_app
    from starlette.applications import Starlette
    from starlette.responses import Response
    from starlette.routing import Mount

    selected = ServerConfig.from_env() if config is None else config
    verifier = PrincipalDirectory.from_env() if verifier is None else verifier
    if verifier.configured is not True or not callable(getattr(verifier, "authenticate", None)):
        raise ValueError(
            "Configure a real principal verifier or ARCHETYPE_PRINCIPALS_PATH directory"
        )
    runtime = build_runtime_resources(selected)
    inner: Any = None
    cleanup: asyncio.Task[None] | None = None
    owned_ingress: Any = None

    async def proxy(scope, receive, send):
        if inner is None:
            await Response("Server unavailable", status_code=503)(scope, receive, send)
            return
        await inner(scope, receive, send)

    async def close_owned():
        if owned_ingress is not None:
            await owned_ingress.drain()
        await runtime.shutdown()

    async def shutdown():
        """Operator cleanup retry; retains the same native owner and never reopens."""
        nonlocal cleanup
        if cleanup is None or (cleanup.done() and cleanup.exception() is not None):
            cleanup = asyncio.create_task(close_owned())
        cancelled = None
        while not cleanup.done():
            try:
                await asyncio.shield(cleanup)
            except asyncio.CancelledError as error:
                cancelled = error
        cleanup.result()
        if cancelled is not None:
            raise cancelled

    @asynccontextmanager
    async def lifespan(_app):
        nonlocal inner, owned_ingress
        await runtime.__aenter__()
        try:
            host = await runtime._activate()
            ingress = Ingress(
                host,
                verifier=verifier,
                resources=selected.resources,
                grants=selected.grants,
                artifact_workflow=artifact_workflow(host),
            )
            owned_ingress = ingress
            inner = transport_app(ingress)
            close_error = None
            async with inner.router.lifespan_context(inner):
                try:
                    yield
                finally:
                    try:
                        await shutdown()
                    except Exception as error:
                        close_error = error
            if close_error is not None:
                raise close_error
        finally:
            inner = None
            if cleanup is None:
                await shutdown()

    app = Starlette(routes=[Mount("/", app=proxy)], lifespan=lifespan)
    # The operator may retry failed cleanup without accessing or replacing Host.
    app.state.aclose = shutdown
    return app
