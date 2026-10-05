"""Actual SDK HTTP transport, authentication middleware and lifespan, in memory.

Synthetic credentials and the real PrincipalDirectory are reused from the
shared ingress tests. No listener, credential file or compiler is started.
"""

from __future__ import annotations

import asyncio
import json
import sys
import unittest
from contextlib import asynccontextmanager
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

import httpx2
from archetype_native.ingress import ContextResource, Grant, Ingress, ProgramResource
from archetype_native.wire import MAX_REQUEST_BYTES
from archetype_transports import MCP_BODY_LIMIT, create_app
from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client
from starlette.applications import Starlette
from starlette.routing import Mount

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO / "packages/archetype-native/tests"))
from test_ingress import (  # noqa: E402
    CAPS,
    OTHER,
    TOKEN,
    Backend,
    admit_args,
    directory,
    request,
    resource,
)


def headers(token=TOKEN):
    return {
        "authorization": "Bearer " + token,
        "content-type": "application/json",
        "accept": "application/json, text/event-stream",
    }


def rpc(raw, *, name="simulation", arguments=None):
    return {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/call",
        "params": {
            "name": name,
            "arguments": {"request_json": raw.decode()} if arguments is None else arguments,
        },
    }


@asynccontextmanager
async def local(ingress, *, token=TOKEN, prefix=""):
    """Enter SDK lifespan in the same task that exits it; no direct in-memory Client."""
    app = create_app(ingress)
    mounted = Starlette(routes=[Mount(prefix, app=app)]) if prefix else app
    async with app.router.lifespan_context(app):
        async with httpx2.AsyncClient(
            transport=httpx2.ASGITransport(app=mounted),
            base_url="http://127.0.0.1" + prefix,
            headers=headers(token),
        ) as http:
            yield http


@asynccontextmanager
async def session(http, *, read_timeout_seconds=5):
    # This serializes JSON-RPC over HTTP through the SDK auth/context middleware.
    async with streamable_http_client(
        str(http.base_url).rstrip("/") + "/mcp", http_client=http
    ) as (read, write):
        async with ClientSession(read, write, read_timeout_seconds=read_timeout_seconds) as client:
            await client.initialize()
            yield client


async def mcp(client, raw):
    result = await client.call_tool("simulation", {"request_json": raw.decode()})
    value = json.loads(result.content[0].text)
    assert result.structured_content == value
    assert result.is_error == (not value["ok"])
    return value


class TransportTests(unittest.IsolatedAsyncioTestCase):
    async def test_program_composition_all_refs_and_creation_have_http_mcp_parity(self):
        from test_logical_ingress import Backend as LogicalBackend
        from test_logical_ingress import composition, leaf

        names = ("pipeline", "first", "second")
        backend = LogicalBackend()
        ingress = Ingress(
            backend,
            verifier=directory(),
            resources=tuple(ProgramResource(n) for n in names),
            grants=tuple(Grant("agent", n, CAPS) for n in names if n != "second"),
        )

        class NoLookup:
            def __getitem__(self, key):
                raise AssertionError("Unauthorized program lookup")

        ingress._resources = NoLookup()
        raw = request(
            "program_compose",
            {"request_key": "compose", "description": "Pipeline", "composition": composition()},
            "pipeline",
        )
        async with local(ingress) as http, session(http) as client:
            response = await http.post("/invoke", content=raw)
            self.assertEqual(response.status_code, 403)
            self.assertEqual(await mcp(client, raw), response.json())
        self.assertEqual(backend.calls, [])
        ingress = Ingress(
            backend,
            verifier=directory(),
            resources=(ProgramResource("first"),),
            grants=(Grant("agent", "first", CAPS),),
        )
        raw = request(
            "program_create",
            {"request_key": "publish", "description": "First", "definition": leaf()},
            "first",
        )
        async with local(ingress) as http, session(http) as client:
            response = await http.post("/invoke", content=raw)
            self.assertEqual(response.status_code, 200)
            self.assertEqual(await mcp(client, raw), response.json())
            self.assertEqual(response.json()["value"]["phase"], "published")

    async def test_context_source_grants_have_http_mcp_parity(self):
        context = ContextResource("context", "alpha", "run_a", source_resource="alpha")
        ingress = Ingress(
            self.backend,
            verifier=directory(),
            resources=(resource(), context),
            grants=(Grant("agent", "context", CAPS),),
        )

        class NoLookup:
            def __getitem__(self, key):
                raise AssertionError("Unauthorized context lookup")

        ingress._resources = NoLookup()
        raw = request("publish_context", {"source_resource": "alpha"}, "context")
        async with local(ingress) as http, session(http) as client:
            response = await http.post("/invoke", content=raw)
            self.assertEqual(response.status_code, 403)
            self.assertEqual(await mcp(client, raw), response.json())
            self.assertEqual(
                response.json()["error"], {"code": "forbidden", "outcome": "not_dispatched"}
            )
        self.assertEqual(self.backend.calls, [])

    async def test_fork_source_destination_grants_have_http_mcp_parity(self):
        destination = replace(resource(), name="child", native_world=None, world="child")
        ingress = Ingress(
            self.backend,
            verifier=directory(),
            resources=(resource(), destination),
            grants=(Grant("agent", "child", CAPS),),
        )

        class NoLookup:
            def __getitem__(self, key):
                raise AssertionError("Unauthorized fork lookup")

        ingress._resources = NoLookup()
        raw = request(
            "fork",
            {
                "source_resource": "alpha",
                "receipt": {"world": "alpha", "run": "run_a", "tick": "1", "cut_id": "a" * 64},
                "request_key": "fork_one",
                "expected_generation": "0",
            },
            "child",
        )
        async with local(ingress) as http, session(http) as client:
            response = await http.post("/invoke", content=raw)
            self.assertEqual(response.status_code, 403)
            self.assertEqual(await mcp(client, raw), response.json())
            self.assertEqual(
                response.json()["error"], {"code": "forbidden", "outcome": "not_dispatched"}
            )
        self.assertEqual(self.backend.calls, [])

    def setUp(self):
        self.backend = Backend()
        self.ingress = self.make()

    def make(self, *, verifier=None, max_inflight=4):
        return Ingress(
            self.backend,
            verifier=verifier or directory(),
            resources=(resource(),),
            grants=(Grant("agent", "alpha", CAPS),),
            max_inflight=max_inflight,
        )

    async def test_actual_sdk_handshake_projection_parity_and_borrowed_lifetime(self):
        async with local(self.ingress) as http, session(http) as client:
            listed = await client.list_tools()
            self.assertEqual([tool.name for tool in listed.tools], ["simulation"])
            direct = json.loads(await self.ingress.invoke(TOKEN, request()))
            remote = await http.post("/invoke", content=request())
            self.assertEqual(remote.status_code, 200)
            self.assertEqual(remote.json(), direct)
            self.assertEqual(await mcp(client, request()), direct)
            changed = await http.post("/mcp", json=rpc(request()), headers=headers(OTHER))
            self.assertEqual(
                changed.json()["result"]["structuredContent"]["error"]["code"], "forbidden"
            )
            self.assertEqual(direct["value"]["revision"], "9007199254741109")
            self.assertNotIn("/private", remote.text)
            self.assertNotIn("secret", remote.text)
        # Exiting SDK lifespan neither shuts down nor replaces the borrowed ingress.
        self.assertTrue(json.loads(await self.ingress.invoke(TOKEN, request()))["ok"])
        self.assertEqual(len(self.backend.calls), 4)

    async def test_sdk_auth_rejects_missing_invalid_expired_revoked_before_dispatch(self):
        for verifier in (
            directory(),
            directory(revoked=True),
            directory(expires_at="2000-01-01T00:00:00Z"),
        ):
            ingress = self.make(verifier=verifier)
            async with local(ingress) as http:
                for token in ("", "admin", "x" * 32, TOKEN):
                    if token == TOKEN:
                        try:
                            verifier.authenticate(token)
                        except Exception:
                            pass
                        else:
                            continue
                    for path, raw in (
                        ("/invoke", request()),
                        ("/mcp", json.dumps(rpc(request())).encode()),
                    ):
                        response = await http.post(path, content=raw, headers=headers(token))
                        self.assertEqual(response.status_code, 401)
                        self.assertNotIn(token if token else "service-credential-", response.text)
                http.headers.pop("authorization")
                for path in ("/invoke", "/mcp"):
                    self.assertEqual((await http.post(path, content=b"{}")).status_code, 401)
        self.assertEqual(self.backend.calls, [])

    async def test_per_request_auth_context_and_cross_world_denied_before_lookup(self):
        class NoLookup:
            def __getitem__(self, key):
                raise AssertionError("Unauthorized native/resource lookup")

        self.ingress._resources = NoLookup()
        async with local(self.ingress) as http:
            for token, raw in ((OTHER, request()), (TOKEN, request(name="beta"))):
                response = await http.post("/invoke", content=raw, headers=headers(token))
                self.assertEqual(response.status_code, 403)
                expected = response.json()
                response = await http.post("/mcp", json=rpc(raw), headers=headers(token))
                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.json()["result"]["structuredContent"], expected)
        self.assertEqual(self.backend.calls, [])
        # Authenticated read principal still cannot acquire control via tool arguments.
        ingress = self.make(verifier=directory(frozenset({"simulation:read"})))
        async with local(ingress) as http, session(http) as client:
            self.assertEqual((await mcp(client, request("stop")))["error"]["code"], "forbidden")
        self.assertEqual(self.backend.calls, [])

    async def test_shared_codec_unknown_operations_and_exact_integer_rejection(self):
        invalid = [
            request("open"),
            request("history"),
            request("read"),
            request("step"),
            request(args={"actor": "admin"}),
            request().replace(b'"version": 1', b'"version": 1, "version": 1'),
            request().replace(b'"version": 1', b'"version": NaN'),
        ]
        invalid += [
            request("admit", admit_args(value))
            for value in (
                9007199254741109,
                True,
                None,
                1.5,
                "-0",
                "01",
                "1e3",
                "9223372036854775808",
                "-9223372036854775809",
            )
        ]
        async with local(self.ingress) as http, session(http) as client:
            for raw in invalid:
                response = await http.post("/invoke", content=raw)
                self.assertEqual(response.status_code, 400)
                self.assertEqual(await mcp(client, raw), response.json())
            for outer in (
                rpc(request(), name="raw"),
                rpc(request(), arguments={"request_json": request().decode(), "actor": "admin"}),
            ):
                result = (await http.post("/mcp", json=outer)).json()["result"]
                self.assertTrue(result["isError"])
                self.assertEqual(result["structuredContent"]["error"]["code"], "invalid_request")
            for path in ("/open", "/create", "/register", "/close", "/read", "/history"):
                self.assertEqual((await http.post(path, content=b"{}")).status_code, 404)
        self.assertEqual(self.backend.calls, [])

    async def test_body_and_header_caps_outer_duplicates_and_mount_prefix(self):
        async def chunks(size):
            yield b"x" * (size // 2)
            yield b"x" * (size - size // 2)

        for prefix in ("", "/preview"):
            async with local(self.ingress, prefix=prefix) as http:
                for path, cap in (("invoke", MAX_REQUEST_BYTES), ("mcp", MCP_BODY_LIMIT)):
                    response = await http.post(path, content=chunks(cap + 1))
                    self.assertEqual(response.status_code, 413)
                    self.assertNotIn("service-credential", response.text)
                duplicate = json.dumps(rpc(request())).replace('"id": 1', '"id": 1, "id": 2')
                self.assertEqual((await http.post("mcp", content=duplicate)).status_code, 400)
                for value in (float("nan"), float("inf"), -float("inf")):
                    outer = rpc(request())
                    outer["params"]["_meta"] = {"test": value}
                    self.assertEqual(
                        (await http.post("mcp", content=json.dumps(outer))).status_code, 400
                    )
                # A body between the two limits reaches the shared codec even
                # under a mount prefix; only the inner 64-KiB request is rejected.
                oversized_inner = request() + b" " * MAX_REQUEST_BYTES
                nested = await http.post("mcp", json=rpc(oversized_inner))
                self.assertEqual(nested.status_code, 200)
                self.assertEqual(
                    nested.json()["result"]["structuredContent"]["error"]["code"], "invalid_request"
                )
                for extra, status in (
                    ({"host": "attacker.example"}, 421),
                    ({"origin": "http://attacker.example"}, 403),
                    ({"content-encoding": "gzip"}, 415),
                    ({"authorization": "Bearer " + "x" * 4097}, 401),
                ):
                    self.assertEqual(
                        (await http.post("invoke", content=request(), headers=extra)).status_code,
                        status,
                    )
                for path in ("invoke", "mcp"):
                    duplicate_headers = [(key, value) for key, value in headers().items()]
                    duplicate_headers += [("authorization", "Bearer " + OTHER)]
                    self.assertEqual(
                        (
                            await http.post(path, content=b"{}", headers=duplicate_headers)
                        ).status_code,
                        400,
                    )
        self.assertEqual(self.backend.calls, [])

    async def test_disconnect_before_complete_body_never_dispatches(self):
        app = create_app(self.ingress)
        async with app.router.lifespan_context(app):
            for path in ("/invoke", "/mcp"):
                incoming = asyncio.Queue()
                incoming.put_nowait({"type": "http.request", "body": b'{"', "more_body": True})
                incoming.put_nowait({"type": "http.disconnect"})
                emitted = []

                async def send(message, output=emitted):
                    output.append(message)

                scope = {
                    "type": "http",
                    "asgi": {"version": "3.0"},
                    "method": "POST",
                    "scheme": "http",
                    "path": path,
                    "root_path": "",
                    "query_string": b"",
                    "server": ("127.0.0.1", 80),
                    "headers": [(b"host", b"127.0.0.1")]
                    + [(key.encode(), value.encode()) for key, value in headers().items()],
                }
                await asyncio.wait_for(app(scope, incoming.get, send), timeout=2)
                self.assertFalse(any(msg.get("status", 0) >= 500 for msg in emitted))
        self.assertEqual(self.backend.calls, [])

    async def test_error_redaction_and_unknown_outcome_parity(self):
        with patch.object(
            self.backend, "status", side_effect=RuntimeError("/private/path " + TOKEN)
        ):
            async with local(self.ingress) as http, session(http) as client:
                response = await http.post("/invoke", content=request())
                self.assertEqual(response.status_code, 500)
                self.assertEqual(
                    response.json()["error"], {"code": "operation_failed", "outcome": "unknown"}
                )
                self.assertEqual(await mcp(client, request()), response.json())
                self.assertNotIn(TOKEN, response.text)
                self.assertNotIn("/private", response.text)

    async def test_factual_code_parity_keeps_private_diagnostics(self):
        from archetype_native import NativeError

        for code, status in (
            ("resource_limit", 422),
            ("unsupported_format", 422),
            ("corrupt_data", 500),
            ("invalid_request", 400),
        ):
            with (
                self.subTest(code=code),
                patch.object(
                    self.backend,
                    "status",
                    side_effect=NativeError(
                        "operation", "/private/path " + TOKEN, "status", code=code
                    ),
                ),
            ):
                async with local(self.ingress) as http, session(http) as client:
                    response = await http.post("/invoke", content=request())
                    self.assertEqual(response.status_code, status)
                    self.assertEqual(response.json()["error"], {"code": code, "outcome": "unknown"})
                    self.assertEqual(await mcp(client, request()), response.json())
                    self.assertNotIn(TOKEN, response.text)
                    self.assertNotIn("/private", response.text)

    async def test_cancelled_http_waiter_retains_capacity_across_mcp_and_lifespan(self):
        self.backend.release.clear()
        self.ingress = self.make(max_inflight=1)
        try:
            async with local(self.ingress) as http, session(http) as client:
                task = asyncio.create_task(http.post("/invoke", content=request()))
                self.assertTrue(await asyncio.to_thread(self.backend.entered.wait, 2))
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await task
                self.assertEqual((await mcp(client, request()))["error"]["code"], "busy")
                self.assertEqual((await http.post("/invoke", content=request())).status_code, 429)
            self.assertEqual(len(self.ingress._pending), 1)
            self.assertEqual(len(self.backend.calls), 1)
        finally:
            self.backend.release.set()
            await self.ingress.drain()
        self.assertFalse(self.ingress._pending)

    async def test_mcp_transport_cancel_retains_native_work_and_capacity(self):
        self.backend.release.clear()
        self.ingress = self.make(max_inflight=1)
        try:
            async with local(self.ingress) as http:
                task = asyncio.create_task(http.post("/mcp", json=rpc(request())))
                self.assertTrue(await asyncio.to_thread(self.backend.entered.wait, 2))
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await task
                self.assertEqual((await http.post("/invoke", content=request())).status_code, 429)
            self.assertEqual(len(self.ingress._pending), 1)
            self.assertEqual(len(self.backend.calls), 1)
        finally:
            self.backend.release.set()
            await self.ingress.drain()


class NativeTransportTests(unittest.IsolatedAsyncioTestCase):
    async def test_retry_exact_admission_across_transports_real_iceberg(self):
        from test_binding import Fixture

        fixture = await asyncio.to_thread(Fixture)
        print(f"Transport existing ABI / real Iceberg / simulated driver: {fixture.root}")
        ingress = None
        try:
            binding = await asyncio.to_thread(
                fixture.world, "alpha", await asyncio.to_thread(fixture.program)
            )
            ingress = Ingress(
                fixture.host,
                verifier=directory(),
                resources=(resource(binding=binding),),
                grants=(Grant("agent", "alpha", CAPS),),
            )
            async with local(ingress) as http, session(http) as client:
                self.assertTrue((await mcp(client, request("start")))["ok"])
                await asyncio.to_thread(fixture.running, binding)
                state = (await http.post("/invoke", content=request())).json()["value"]
                args = admit_args()
                args.update(generation=state["generation"], revision=state["revision"])
                raw = request("admit", args)
                first = (await http.post("/invoke", content=raw)).json()
                self.assertTrue(first["ok"], first)
                boundary = first["value"]["boundary"]
                key = dict(
                    boundary,
                    generation=int(boundary["generation"]),
                    world_id=binding["scope"]["native_world"],
                )
                await asyncio.to_thread(fixture.frozen, key)
                retry = await mcp(client, raw)
                self.assertTrue(retry["ok"], retry)
                self.assertEqual(retry["value"]["boundary"], boundary)
                state_after = (await mcp(client, request()))["value"]
                self.assertEqual(int(state_after["revision"]), int(state["revision"]) + 1)
                published = await mcp(client, request("publish", {"boundary": boundary}))
                self.assertTrue(published["ok"], published)
                value = published["value"]
                confirm = {
                    "boundary": boundary,
                    "tick": value["receipt"]["tick"],
                    "expected_parent": value["parent"],
                }
                self.assertTrue(
                    (await http.post("/invoke", content=request("confirm", confirm))).json()["ok"]
                )
                receipt = dict(value["receipt"], tick=int(value["receipt"]["tick"]))
                rows = await asyncio.to_thread(fixture.host.read, receipt, "label")
                self.assertEqual(rows["rows"], [[9007199254741109, "héllo world"]])
                stale = await http.post("/invoke", content=raw)
                self.assertEqual(stale.status_code, 500)
                self.assertEqual(
                    stale.json()["error"], {"code": "operation_failed", "outcome": "unknown"}
                )
                observed = await mcp(
                    client,
                    request(
                        "admission_status",
                        {
                            "generation": boundary["generation"],
                            "admission_key": "first",
                        },
                    ),
                )
                self.assertEqual(observed["value"]["state"], "published")
                self.assertEqual(observed["value"]["boundary"], boundary)
        finally:
            if ingress is not None:
                ingress.stop_accepting()
            await asyncio.to_thread(fixture.close)
            if ingress is not None:
                await ingress.drain()


if __name__ == "__main__":
    unittest.main()
