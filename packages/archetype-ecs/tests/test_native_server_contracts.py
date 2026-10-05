# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0
"""Owned default server: real SDK/auth middleware, explicitly fake native owner."""

import asyncio
import threading
import unittest
from contextlib import asynccontextmanager
from unittest.mock import patch

import httpx2
from archetype_native.ingress import ContextResource, Grant
from test_ingress import TOKEN, directory, request

from archetype.api.app import create_app
from archetype.wiring import RuntimeBootstrapConfig


class Host:
    def __init__(self):
        self.closes = 0
        self.calls = 0

    def close(self):
        self.closes += 1

    def context(self, world, run):
        self.calls += 1
        return {
            "version": 1,
            "world": world,
            "run": run,
            "context_id": "a" * 64,
            "origin": {"kind": "artifact_collection"},
        }


def configuration():
    caps = frozenset({"artifacts:read"})
    return RuntimeBootstrapConfig(
        tuple(
            (key, value)
            for key, value in (
                ("library", "/library"),
                ("store", "/store"),
                ("registry", None),
                ("builds", None),
                ("driver", None),
            )
        ),
        (ContextResource("files", "files", "main"),),
        (Grant("agent", "files", caps),),
    )


class ServerContracts(unittest.IsolatedAsyncioTestCase):
    async def test_explicit_operator_verifier_retains_one_owner_and_rejects_unconfigured(self):
        host = Host()
        with patch("archetype.runtime.runtime.open_host", return_value=host) as opened:
            with self.assertRaises(ValueError):
                create_app(
                    config=configuration(), verifier=type("Verifier", (), {"configured": False})()
                )
            opened.assert_not_called()
            app = create_app(config=configuration(), verifier=directory())
            opened.assert_not_called()
            async with app.router.lifespan_context(app):
                async with httpx2.AsyncClient(
                    transport=httpx2.ASGITransport(app), base_url="http://localhost"
                ) as client:
                    response = await client.post(
                        "/invoke",
                        content=request("read_context", {}, "files"),
                        headers={
                            "Authorization": "Bearer " + TOKEN,
                            "Content-Type": "application/json",
                        },
                    )
                    self.assertEqual(response.status_code, 200)
            self.assertEqual((opened.call_count, host.closes), (1, 1))

    async def test_owned_host_exact_http_contract_auth_before_resolution_and_close(self):
        host = Host()
        with (
            patch("archetype.api.app.PrincipalDirectory.from_env", return_value=directory()),
            patch("archetype.runtime.runtime.open_host", return_value=host) as opened,
        ):
            app = create_app(config=configuration())
            opened.assert_not_called()
            async with app.router.lifespan_context(app):
                async with httpx2.AsyncClient(
                    transport=httpx2.ASGITransport(app), base_url="http://localhost"
                ) as client:
                    denied = await client.post(
                        "/invoke",
                        content=request("read_context", {}, "files"),
                        headers={"Content-Type": "application/json"},
                    )
                    self.assertEqual(denied.status_code, 401)
                    self.assertEqual(host.calls, 0)
                    response = await client.post(
                        "/invoke",
                        content=request("read_context", {}, "files"),
                        headers={
                            "Authorization": "Bearer " + TOKEN,
                            "Content-Type": "application/json",
                        },
                    )
                    self.assertEqual(response.status_code, 200, response.text)
                    self.assertEqual(response.json()["value"]["origin"], "artifact_collection")
                    self.assertNotIn("/store", response.text)
                    self.assertEqual(host.calls, 1)
                self.assertEqual(host.closes, 0)
            self.assertEqual((opened.call_count, host.closes), (1, 1))

    async def test_cancelled_lifespan_startup_drains_retained_open_before_owner_close(self):
        host, entered, release = Host(), threading.Event(), threading.Event()

        def opening(_configuration):
            entered.set()
            release.wait(10)
            return host

        with (
            patch("archetype.api.app.PrincipalDirectory.from_env", return_value=directory()),
            patch("archetype.runtime.runtime.open_host", opening),
        ):
            app = create_app(config=configuration())

            async def startup():
                async with app.router.lifespan_context(app):
                    self.fail("Cancelled startup reached serving")

            task = asyncio.create_task(startup())
            await asyncio.to_thread(entered.wait, 5)
            task.cancel()
            await asyncio.sleep(0)
            self.assertFalse(task.done())
            self.assertEqual(host.closes, 0)
            release.set()
            with self.assertRaises(asyncio.CancelledError):
                await task
            self.assertEqual(host.closes, 1)

    async def test_cancelled_shutdown_retains_sdk_lifespan_until_remote_drain_and_close(self):
        from archetype_transports import create_app as real_transport_app

        host, entered, release = Host(), threading.Event(), threading.Event()
        serving, exit_requested, sdk_closed = asyncio.Event(), asyncio.Event(), asyncio.Event()
        original_context = host.context

        def blocked_context(world, run):
            entered.set()
            release.wait(10)
            return original_context(world, run)

        host.context = blocked_context

        def observed_transport_app(ingress):
            inner = real_transport_app(ingress)
            original_lifespan = inner.router.lifespan_context

            @asynccontextmanager
            async def observed(app):
                async with original_lifespan(app):
                    try:
                        yield
                    finally:
                        sdk_closed.set()

            inner.router.lifespan_context = observed
            return inner

        with (
            patch("archetype.api.app.PrincipalDirectory.from_env", return_value=directory()),
            patch("archetype.runtime.runtime.open_host", return_value=host),
            patch("archetype_transports.create_app", observed_transport_app),
        ):
            app = create_app(config=configuration())

            async def lifetime():
                async with app.router.lifespan_context(app):
                    serving.set()
                    await exit_requested.wait()

            owner = asyncio.create_task(lifetime())
            await serving.wait()
            async with httpx2.AsyncClient(
                transport=httpx2.ASGITransport(app), base_url="http://localhost"
            ) as client:
                call = asyncio.create_task(
                    client.post(
                        "/invoke",
                        content=request("read_context", {}, "files"),
                        headers={
                            "Authorization": "Bearer " + TOKEN,
                            "Content-Type": "application/json",
                        },
                    )
                )
                await asyncio.to_thread(entered.wait, 5)
                exit_requested.set()
                await asyncio.sleep(0.05)
                owner.cancel()
                await asyncio.sleep(0.05)
                self.assertFalse(owner.done())
                self.assertFalse(sdk_closed.is_set())
                self.assertEqual(host.closes, 0)
                release.set()
                self.assertEqual((await call).status_code, 200)
                with self.assertRaises(asyncio.CancelledError):
                    await owner
            self.assertTrue(sdk_closed.is_set())
            self.assertEqual(host.closes, 1)

    async def test_operator_retries_failed_close_without_reopening_or_replaying(self):
        from archetype import RuntimeOperationError

        class RetryHost(Host):
            def close(self):
                self.closes += 1
                if self.closes == 1:
                    raise OSError("private close diagnostic")

        host = RetryHost()
        with (
            patch("archetype.api.app.PrincipalDirectory.from_env", return_value=directory()),
            patch("archetype.runtime.runtime.open_host", return_value=host) as opened,
        ):
            app = create_app(config=configuration())
            with self.assertRaises(RuntimeOperationError) as error:
                async with app.router.lifespan_context(app):
                    pass
            self.assertEqual(
                (error.exception.code, error.exception.outcome), ("native_close_failed", "unknown")
            )
            self.assertEqual((opened.call_count, host.closes, host.calls), (1, 1, 0))
            await app.state.aclose()
            await app.state.aclose()
            self.assertEqual((opened.call_count, host.closes, host.calls), (1, 2, 0))

    async def test_invalid_configuration_rejected_before_native_activation(self):
        from dataclasses import replace

        from archetype_native.ingress import LogicalResource

        from archetype.api import ServerConfig

        original = configuration()
        mutations = (
            {"native": list(original.native)},
            {"resources": list(original.resources)},
            {"grants": list(original.grants)},
            {"resources": (original.resources[0], original.resources[0])},
            {"grants": (Grant("agent", "absent", frozenset({"artifacts:read"})),)},
            {"resources": (ContextResource("files", "files", "main", "absent"),)},
            {
                "resources": (
                    original.resources[0],
                    LogicalResource("live", "files", "main", (), ()),
                )
            },
            {
                "native": tuple(
                    (key, "relative" if key == "store" else value) for key, value in original.native
                )
            },
        )
        with patch("archetype.runtime.runtime.open_host") as opened:
            for mutation in mutations:
                with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                    replace(original, **mutation)
            opened.assert_not_called()
        self.assertIs(type(original), ServerConfig)
