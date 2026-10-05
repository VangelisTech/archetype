"""Local static-credential HTTP/MCP adapters. Construction binds no socket."""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from archetype_native.ingress import Ingress
from archetype_native.wire import MAX_REQUEST_BYTES
from mcp.server.auth.middleware.auth_context import AuthContextMiddleware, get_access_token
from mcp.server.auth.middleware.bearer_auth import BearerAuthBackend, RequireAuthMiddleware
from mcp.server.auth.provider import AccessToken
from mcp.server.context import ServerRequestContext
from mcp.server.lowlevel import Server
from mcp.server.transport_security import (
    RequestBodyLimitMiddleware,
    TransportSecurityMiddleware,
    TransportSecuritySettings,
)
from mcp_types import CallToolRequestParams, CallToolResult, ListToolsResult, TextContent, Tool
from starlette._utils import get_route_path
from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.middleware.authentication import AuthenticationMiddleware
from starlette.requests import ClientDisconnect, Request
from starlette.responses import Response
from starlette.routing import Mount, Route
from starlette.types import ASGIApp, Message, Receive, Scope, Send

__all__ = ["create_app"]
MCP_BODY_LIMIT = 512 * 1024  # JSON-RPC envelope plus escaped 64-KiB contract string.
TOOL = Tool(
    name="simulation",
    description="Invoke one version-1 simulation operation on a configured resource. "
    "request_json is the exact shared ingress JSON document; credentials are HTTP headers only. "
    "Disconnect does not cancel native work or authorize input replay.",
    input_schema={
        "type": "object",
        "properties": {"request_json": {"type": "string", "maxLength": MAX_REQUEST_BYTES}},
        "required": ["request_json"],
        "additionalProperties": False,
    },
)
HTTP_STATUS = {
    "unauthenticated": 401,
    "forbidden": 403,
    "invalid_request": 400,
    "busy": 429,
    "unavailable": 503,
    "operation_failed": 500,
    "corrupt_data": 500,
    "resource_limit": 422,
    "unsupported_format": 422,
}


class _Verifier:
    def __init__(self, ingress: Ingress):
        self.ingress = ingress

    async def verify_token(self, token: str) -> AccessToken | None:
        try:
            principal = self.ingress.authenticate(token)
            # These are actual directory claims. No issuer, audience, fabricated
            # expiry, OAuth issuance or discovery metadata is implied.
            return AccessToken(
                token=token, client_id=principal.principal_id, scopes=sorted(principal.capabilities)
            )
        except Exception:
            return None


class _RequireHTTPAuth:
    def __init__(self, app: ASGIApp):
        self.app = app
        self.protected = RequireAuthMiddleware(app, required_scopes=[])

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        # SDK's guard expects HTTP scope. Lifespan is process orchestration.
        await (self.protected if scope["type"] == "http" else self.app)(scope, receive, send)


def _unique(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Duplicate JSON key")
        result[key] = value
    return result


def _invalid_constant(_value: str) -> None:
    raise ValueError("Non-JSON numeric constant")


class _StrictMCPBody:
    """Reject duplicate outer JSON keys before SDK parsing loses them."""

    def __init__(self, app: ASGIApp):
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http" or get_route_path(scope) != "/mcp" or scope["method"] != "POST":
            await self.app(scope, receive, send)
            return
        try:
            body = await Request(scope, receive).body()  # outer SDK limiter runs first
            json.loads(
                body.decode("utf-8"), object_pairs_hook=_unique, parse_constant=_invalid_constant
            )
        except ClientDisconnect:
            return
        except (ValueError, UnicodeError, RecursionError):
            await Response("Invalid JSON envelope", status_code=400)(scope, receive, send)
            return
        delivered = False

        async def replay() -> Message:
            nonlocal delivered
            if not delivered:
                delivered = True
                return {"type": "http.request", "body": body, "more_body": False}
            return await receive()

        await self.app(scope, replay, send)


class _Limits:
    def __init__(self, app: ASGIApp):
        self.app = app
        self.http = RequestBodyLimitMiddleware(app, max_body_size=MAX_REQUEST_BYTES)
        self.mcp = RequestBodyLimitMiddleware(_StrictMCPBody(app), max_body_size=MCP_BODY_LIMIT)

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        selected = (
            self.app
            if scope["type"] != "http"
            else (self.mcp if get_route_path(scope) == "/mcp" else self.http)
        )
        await selected(scope, receive, send)


class _Headers:
    def __init__(self, app: ASGIApp, settings: TransportSecuritySettings):
        self.app = app
        self.security = TransportSecurityMiddleware(settings)

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] == "http":
            headers = scope.get("headers", [])
            for name in (b"authorization", b"host", b"content-length", b"content-type", b"origin"):
                if sum(key.lower() == name for key, _ in headers) > 1:
                    await Response("Duplicate request header", status_code=400)(
                        scope, receive, send
                    )
                    return
            if any(key.lower() == b"authorization" and len(value) > 4103 for key, value in headers):
                await Response("Invalid authorization", status_code=401)(scope, receive, send)
                return
            # No compressed-body ambiguity in this small local profile.
            if any(key.lower() == b"content-encoding" for key, _ in headers):
                await Response("Unsupported content encoding", status_code=415)(
                    scope, receive, send
                )
                return
            failure = await self.security.validate_request(
                Request(scope, receive), is_post=scope["method"] == "POST"
            )
            if failure is not None:
                await failure(scope, receive, send)
                return
        await self.app(scope, receive, send)


def create_app(ingress: Ingress) -> Starlette:
    """Build one local-only ASGI app borrowing the caller's ingress and Host.

    The caller enters this app's lifespan and remains the sole native lifetime
    owner: stop ingress, close Host off-loop, drain, then exit app/event loop.
    No binding, grant, credential, path, listener or native owner is created.
    """

    async def invoke(request: Request) -> Response:
        token = get_access_token()
        try:
            body = await request.body()
        except ClientDisconnect:
            return Response(status_code=400)
        result = await ingress.invoke("" if token is None else token.token, body)
        value = json.loads(result)
        status = 200 if value["ok"] else HTTP_STATUS[value["error"]["code"]]
        return Response(result, status_code=status, media_type="application/json")

    async def list_tools(_ctx: ServerRequestContext, _params: Any) -> ListToolsResult:
        return ListToolsResult(tools=[TOOL])

    async def call_tool(
        _ctx: ServerRequestContext, params: CallToolRequestParams
    ) -> CallToolResult:
        token = get_access_token()
        arguments = params.arguments
        # Only the SDK's named tool argument envelope is handled here. The
        # shared codec and ingress alone interpret or authorize operations.
        raw = b""
        if (
            params.name == TOOL.name
            and type(arguments) is dict
            and set(arguments) == {"request_json"}
            and type(arguments["request_json"]) is str
        ):
            try:
                raw = arguments["request_json"].encode("utf-8")
            except UnicodeError:
                pass
        result = await ingress.invoke("" if token is None else token.token, raw)
        value = json.loads(result)
        return CallToolResult(
            content=[TextContent(text=result.decode("utf-8"))],
            structured_content=value,
            is_error=not value["ok"],
        )

    server: Server = Server(
        "Archetype",
        version="0.7.0",
        on_list_tools=list_tools,
        on_call_tool=call_tool,
    )
    settings = TransportSecuritySettings(
        enable_dns_rebinding_protection=True,
        allowed_hosts=["127.0.0.1:*", "localhost:*", "[::1]:*", "127.0.0.1", "localhost", "[::1]"],
        allowed_origins=[
            "http://127.0.0.1:*",
            "http://localhost:*",
            "http://[::1]:*",
            "http://127.0.0.1",
            "http://localhost",
            "http://[::1]",
        ],
    )
    mcp_app = server.streamable_http_app(
        stateless_http=True,
        json_response=True,
        max_request_body_size=MCP_BODY_LIMIT,
        transport_security=settings,
    )

    @asynccontextmanager
    async def lifespan(_app: Starlette) -> AsyncIterator[None]:
        # A mounted SDK app's lifespan is not automatically entered. This owns
        # only SDK tasks; it never closes the shared ingress or native Host.
        async with server.session_manager.run():
            yield

    return Starlette(
        routes=[Route("/invoke", invoke, methods=["POST"]), Mount("/", app=mcp_app)],
        lifespan=lifespan,
        middleware=[
            Middleware(_Headers, settings=settings),
            Middleware(AuthenticationMiddleware, backend=BearerAuthBackend(_Verifier(ingress))),
            Middleware(AuthContextMiddleware),
            Middleware(_RequireHTTPAuth),
            Middleware(_Limits),
        ],
    )
