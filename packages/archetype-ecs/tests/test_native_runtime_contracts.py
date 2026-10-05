# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0
"""Deterministic ownership contracts for the native 0.7 facade."""

import asyncio
import os
import threading
import unittest
from unittest.mock import patch

from archetype_native import NativeError, ProtocolError

from archetype import ArchetypeRuntime, Change, RuntimeOperationError
from archetype.runtime.runtime import SyncArchetypeRuntime


class FakeHost:
    def __init__(self):
        self.closes = 0
        self.fail_close = False

    def close(self):
        self.closes += 1
        if self.fail_close:
            self.fail_close = False
            raise RuntimeError("retry close")


def result(_executor, request):
    if request.operation.name == "history":
        return {"receipts": [], "total": "0", "next_offset": None}
    return {"state": "running", "generation": "1", "revision": "1", "has_error": False}


def conflicting_fork(_executor, request):
    if request.operation.name == "fork":
        raise NativeError("binding", "/private/diagnostic", "fork", code="conflict")
    if request.operation.name == "history":
        return {
            "receipts": [
                {
                    "receipt": {"world": "source", "run": "main", "tick": "1", "cut_id": "a" * 64},
                    "parent": None,
                }
            ],
            "total": "1",
            "next_offset": None,
        }
    return result(_executor, request)


class OwnershipTests(unittest.IsolatedAsyncioTestCase):
    async def test_public_async_fork_preserves_typed_conflict(self):
        host = FakeHost()
        with (
            patch("archetype.runtime.runtime.open_host", return_value=host),
            patch("archetype.runtime.runtime._Executor.execute", conflicting_fork),
        ):
            async with ArchetypeRuntime(library="/library", store="/store") as runtime:
                source, destination = runtime.world("source"), runtime.world("destination")
                cut = (await source.history()).cuts[0]
                with self.assertRaises(RuntimeOperationError) as caught:
                    await destination.fork(source, cut, request_key="retained-fork")
                self.assertEqual(
                    (caught.exception.code, caught.exception.outcome), ("conflict", "unknown")
                )
                self.assertEqual(caught.exception.args, ("conflict",))
                self.assertIsNone(caught.exception.__cause__)
        self.assertEqual(host.closes, 1)

    async def test_concurrent_failed_activation_is_bounded_and_retryable(self):
        for failure, code in (
            (OSError("/private/loader"), "native_unavailable"),
            (ProtocolError("/private/abi"), "native_incompatible"),
        ):
            with self.subTest(code=code):
                host, entered, release = FakeHost(), threading.Event(), threading.Event()
                attempts = 0

                def opening(
                    _configuration, *, entered=entered, release=release, failure=failure, host=host
                ):
                    nonlocal attempts
                    attempts += 1
                    if attempts == 1:
                        entered.set()
                        if not release.wait(5):
                            raise AssertionError("Factory failure gate was not released")
                        raise failure
                    return host

                with (
                    patch("archetype.runtime.runtime.open_host", opening),
                    patch("archetype.runtime.runtime._Executor.execute", result),
                ):
                    runtime = ArchetypeRuntime(library="/library", store="/store")
                    first, second = runtime.world("first"), runtime.world("second")
                    tasks = [
                        asyncio.create_task(first.history()),
                        asyncio.create_task(second.history()),
                    ]
                    try:
                        self.assertTrue(await asyncio.to_thread(entered.wait, 5))
                        await asyncio.sleep(0)
                    finally:
                        release.set()
                    failures = await asyncio.gather(*tasks, return_exceptions=True)
                    self.assertEqual(attempts, 1)
                    for error in failures:
                        self.assertIsInstance(error, RuntimeOperationError)
                        self.assertEqual(
                            (error.code, error.outcome, error.args),
                            (code, "not_dispatched", (code,)),
                        )
                    self.assertEqual((await first.history()).total, 0)
                    self.assertEqual(attempts, 2)
                    await runtime.shutdown()
                    self.assertEqual(host.closes, 1)

    async def test_storage_only_ignores_live_environment_before_activation(self):
        with patch.dict(
            "os.environ",
            {
                "ARCHETYPE_REGISTRY": "/live/registry",
                "ARCHETYPE_BUILDS": "/live/builds",
                "ARCHETYPE_NATIVE_DRIVER": "/live/driver",
            },
        ):
            runtime = ArchetypeRuntime(storage_only=True)
            self.assertEqual(
                tuple(runtime._config[key] for key in ("registry_root", "build_root", "driver")),
                (None, None, None),
            )
            await runtime.shutdown()
        with self.assertRaises(ValueError):
            ArchetypeRuntime(storage_only=True, registry="/live")

    async def test_inert_context_singleflight_and_independent_world_shutdown(self):
        host = FakeHost()
        with (
            patch("archetype.runtime.runtime.open_host", return_value=host) as opened,
            patch("archetype.runtime.runtime._Executor.execute", result),
        ):
            async with ArchetypeRuntime(library="/library", store="/store") as runtime:
                first, second = runtime.world("first"), runtime.world("second")
                opened.assert_not_called()
                await asyncio.gather(first.status(), first.status(), second.status())
                self.assertEqual(opened.call_count, 1)
                await first.shutdown()
                with self.assertRaises(RuntimeError):
                    await first.status()
                self.assertEqual((await second.status()).state, "running")
                self.assertEqual(host.closes, 0)
            self.assertEqual(host.closes, 1)
            with self.assertRaises(RuntimeError):
                runtime.world("after")

    async def test_cancelled_waiter_retains_capacity_and_runtime_drain(self):
        host, entered, release = FakeHost(), threading.Event(), threading.Event()

        def held(executor, request):
            entered.set()
            release.wait(10)
            return result(executor, request)

        with (
            patch("archetype.runtime.runtime.open_host", return_value=host),
            patch("archetype.runtime.runtime._Executor.execute", held),
        ):
            runtime = ArchetypeRuntime(library="/library", store="/store", max_inflight=1)
            world = runtime.world("held")
            task = asyncio.create_task(world.history())
            await asyncio.to_thread(entered.wait, 5)
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
            with self.assertRaises(RuntimeOperationError) as caught:
                await world.history()
            self.assertEqual(
                (caught.exception.code, caught.exception.outcome), ("busy", "not_dispatched")
            )
            closing = asyncio.create_task(runtime.shutdown())
            await asyncio.sleep(0)
            self.assertEqual(host.closes, 0)
            closing.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await closing
            release.set()
            await runtime.shutdown()
            self.assertEqual(host.closes, 1)

    async def test_shutdown_retains_activation_even_when_waiter_is_cancelled(self):
        host, entered, release = FakeHost(), threading.Event(), threading.Event()

        def opening(_configuration):
            entered.set()
            release.wait(10)
            return host

        with (
            patch("archetype.runtime.runtime.open_host", opening),
            patch("archetype.runtime.runtime._Executor.execute", result),
        ):
            runtime = ArchetypeRuntime(library="/library", store="/store")
            task = asyncio.create_task(runtime.world("first").history())
            await asyncio.to_thread(entered.wait, 5)
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
            closing = asyncio.create_task(runtime.shutdown())
            await asyncio.sleep(0)
            self.assertFalse(closing.done())
            release.set()
            await closing
            self.assertEqual(host.closes, 1)

    async def test_failed_close_retry_uses_same_owner_and_closes_once_successfully(self):
        host = FakeHost()
        host.fail_close = True
        with (
            patch("archetype.runtime.runtime.open_host", return_value=host) as opened,
            patch("archetype.runtime.runtime._Executor.execute", result),
        ):
            runtime = ArchetypeRuntime(library="/library", store="/store")
            await runtime.world("first").history()
            with self.assertRaisesRegex(RuntimeOperationError, "native_close_failed"):
                await runtime.shutdown()
            with self.assertRaises(RuntimeError):
                await runtime.world("first").history()
            await asyncio.gather(runtime.shutdown(), runtime.shutdown())
            await runtime.shutdown()
            self.assertEqual((opened.call_count, host.closes), (1, 2))

    async def test_cancelled_direct_startup_activation_is_drained_before_close(self):
        host, entered, release = FakeHost(), threading.Event(), threading.Event()

        def opening(_configuration):
            entered.set()
            release.wait(10)
            return host

        with patch("archetype.runtime.runtime.open_host", opening):
            runtime = ArchetypeRuntime(library="/library", store="/store")
            await runtime.__aenter__()
            activation = asyncio.create_task(runtime._activate())
            await asyncio.to_thread(entered.wait, 5)
            activation.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await activation
            closing = asyncio.create_task(runtime.shutdown())
            await asyncio.sleep(0)
            self.assertFalse(closing.done())
            self.assertEqual(host.closes, 0)
            release.set()
            await closing
            self.assertEqual(host.closes, 1)
            self.assertTrue(runtime._closed)

    async def test_inert_world_close_does_not_activate_owner(self):
        with patch("archetype.runtime.runtime.open_host") as opened:
            async with ArchetypeRuntime() as runtime:
                await runtime.world("inert").shutdown()
            opened.assert_not_called()

    async def test_invalid_changes_and_scope_fail_before_native_open(self):
        with patch("archetype.runtime.runtime.open_host") as opened:
            async with ArchetypeRuntime() as runtime:
                world = runtime.world("inert")
                for value in (None, float("nan"), float("inf"), 2**63):
                    with self.assertRaises(ValueError):
                        Change("seed", (value,))
                with self.assertRaises(ValueError):
                    await world.admit(
                        (Change("seed", (1,)),),
                        generation=1,
                        revision=1,
                        admission_key="bad",
                        expected_head=None,
                    )
            opened.assert_not_called()

    async def test_loader_failure_is_bounded_and_retryable(self):
        with patch("archetype.runtime.runtime.open_host", side_effect=OSError("/private/path")):
            runtime = ArchetypeRuntime(library="/library", store="/store")
            for _ in range(2):
                with self.assertRaises(RuntimeOperationError) as caught:
                    await runtime.world("first").history()
                self.assertEqual(str(caught.exception), "native_unavailable")
                self.assertEqual(caught.exception.outcome, "not_dispatched")
            await runtime.shutdown()


class BlockingTests(unittest.TestCase):
    def test_public_sync_fork_preserves_typed_conflict(self):
        host = FakeHost()
        with (
            patch("archetype.runtime.runtime.open_host", return_value=host),
            patch("archetype.runtime.runtime._Executor.execute", conflicting_fork),
        ):
            with ArchetypeRuntime.sync(library="/library", store="/store") as runtime:
                source, destination = runtime.world("source"), runtime.world("destination")
                cut = source.history().cuts[0]
                with self.assertRaises(RuntimeOperationError) as caught:
                    destination.fork(source, cut, request_key="retained-fork")
                self.assertEqual(
                    (caught.exception.code, caught.exception.outcome), ("conflict", "unknown")
                )
                self.assertEqual(caught.exception.args, ("conflict",))
                self.assertIsNone(caught.exception.__cause__)
        self.assertEqual(host.closes, 1)

    def test_failed_close_retains_runner_and_same_host_for_retry(self):
        host = FakeHost()
        host.fail_close = True
        with (
            patch("archetype.runtime.runtime.open_host", return_value=host),
            patch("archetype.runtime.runtime._Executor.execute", result),
        ):
            runtime = ArchetypeRuntime.sync(library="/library", store="/store")
            runtime.__enter__()
            runtime.world("first").history()
            runner = runtime._runner
            with self.assertRaisesRegex(RuntimeOperationError, "native_close_failed"):
                runtime.shutdown()
            self.assertIs(runtime._runner, runner)
            runtime.shutdown()
            self.assertIsNone(runtime._runner)
            self.assertEqual(host.closes, 2)

    @unittest.skipUnless(hasattr(os, "fork"), "POSIX process ownership")
    def test_inherited_sync_runner_rejected_before_use_parent_remains_usable(self):
        host = FakeHost()
        with (
            patch("archetype.runtime.runtime.open_host", return_value=host),
            patch("archetype.runtime.runtime._Executor.execute", result),
        ):
            with ArchetypeRuntime.sync(library="/library", store="/store") as runtime:
                world = runtime.world("first")
                world.history()
                reader, writer = os.pipe()
                child = os.fork()
                if child == 0:
                    os.close(reader)
                    try:
                        with patch.object(
                            runtime._runner,
                            "run",
                            side_effect=AssertionError("inherited Runner touched"),
                        ):
                            try:
                                world.history()
                            except RuntimeError:
                                os.write(writer, b"rejected")
                            else:
                                os.write(writer, b"failed")
                    finally:
                        os._exit(0)
                os.close(writer)
                self.assertEqual(os.read(reader, 64), b"rejected")
                os.close(reader)
                self.assertEqual(os.waitpid(child, 0)[1], 0)
                self.assertEqual(world.history().total, 0)

    def test_minimal_surface_removes_legacy_live_operations(self):
        import archetype

        for name in ("AsyncWorld", "AsyncProcessor", "AsyncStore", "configure_session"):
            self.assertNotIn(name, archetype.__all__)
            with self.assertRaises(AttributeError):
                getattr(archetype, name)
        self.assertEqual(archetype.__version__, "0.7.0")
        self.assertFalse(hasattr(SyncArchetypeRuntime, "resources"))
