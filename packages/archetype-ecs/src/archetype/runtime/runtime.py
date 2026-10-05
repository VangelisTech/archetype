# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0
"""One lazy native process owner and its blocking facade."""

from __future__ import annotations

import asyncio
import os
from collections.abc import Callable, Coroutine
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from archetype_native import Host

from archetype_native import ConstructionCleanupError, NativeError, ProtocolError
from archetype_native import wire as w
from archetype_native.ingress import (
    ContextResource,
    LogicalResource,
    ProgramResource,
    Resource,
    _Executor,
    _validate_request,
)

from archetype.runtime._native import open_host
from archetype.runtime.contracts import ComponentProjection


class RuntimeOperationError(RuntimeError):
    """A bounded failure; dispatched failures never imply permission to replay."""

    def __init__(self, code: str, *, outcome: str):
        super().__init__(code)
        self.code, self.outcome = code, outcome


class ArchetypeRuntime:
    """Version 0.7 runtime over the existing DDlog WorldManager and CutStore.

    Construction and context entry are inert. The first operation checks the
    installed native ABI and contract before opening one process owner. Paths
    configure that private owner; they are never exposed by resource handles.
    Omit all three live paths for a storage-only reader.
    """

    def __init__(
        self,
        *,
        library: str | Path | None = None,
        store: str | Path | None = None,
        registry: str | Path | None = None,
        builds: str | Path | None = None,
        driver: str | Path | None = None,
        max_inflight: int = 4,
        storage_only: bool = False,
    ):
        if type(max_inflight) is not int or not 1 <= max_inflight <= 16:
            raise ValueError("max_inflight must be 1..16")
        if type(storage_only) is not bool or (
            storage_only and any(value is not None for value in (registry, builds, driver))
        ):
            raise ValueError("storage_only requires no explicit live paths")
        self._config = {
            "library": library or os.environ.get("ARCHETYPE_NATIVE_LIBRARY"),
            "store_root": store or os.environ.get("ARCHETYPE_STORE"),
            "registry_root": None
            if storage_only
            else registry or os.environ.get("ARCHETYPE_REGISTRY"),
            "build_root": None if storage_only else builds or os.environ.get("ARCHETYPE_BUILDS"),
            "driver": None if storage_only else driver or os.environ.get("ARCHETYPE_NATIVE_DRIVER"),
        }
        self._host: Host | None = None
        self._opening: asyncio.Task[Host] | None = None
        self._closing: asyncio.Task[None] | None = None
        self._resources: dict[str, Any] = {}
        self._pending: dict[asyncio.Task[Any], str] = {}
        self._world_closes: dict[str, asyncio.Task[None]] = {}
        self._active_worlds: set[str] = set()
        self._closed_worlds: set[str] = set()
        self._draining_worlds: set[str] = set()
        self._max_inflight = max_inflight
        self._accepting, self._closed = True, False
        self._loop: asyncio.AbstractEventLoop | None = None
        self._pid = os.getpid()

    def _process_owner(self) -> None:
        if os.getpid() != self._pid:
            raise RuntimeError("Runtime belongs to its creating process")

    def _owner(self) -> None:
        self._process_owner()
        loop = asyncio.get_running_loop()
        if self._loop is None:
            self._loop = loop
        elif self._loop is not loop:
            raise RuntimeError("Runtime belongs to another event loop")

    def _ensure_open(self) -> None:
        self._process_owner()
        if not self._accepting:
            raise RuntimeError("Runtime is draining or closed")

    async def __aenter__(self) -> ArchetypeRuntime:
        self._owner()
        self._ensure_open()
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.shutdown()

    def _configure(self, resource: Any) -> None:
        self._ensure_open()
        previous = self._resources.get(resource.name)
        if previous is not None and previous != resource:
            raise ValueError("Conflicting immutable resource declarations")
        self._resources[resource.name] = resource

    def program(self, name: str):
        from archetype.runtime.world import RuntimeProgram

        self._configure(ProgramResource(name))
        return RuntimeProgram(self, name)

    def world(
        self,
        name: str,
        *,
        run: str = "main",
        world: str | None = None,
        components: tuple[ComponentProjection, ...] = (),
        inputs: tuple[tuple[str, tuple[str, ...]], ...] = (),
    ):
        from archetype.runtime.world import RuntimeWorld

        self._configure(LogicalResource(name, world or name, run, components, inputs))
        return RuntimeWorld(self, name, world or name, run)

    def artifacts(self, name: str, *, run: str = "main", world: str | None = None, source=None):
        from archetype.runtime.world import RuntimeArtifacts

        if source is not None and (
            source._runtime is not self or (source.world, source.run) != (world or name, run)
        ):
            raise ValueError("Hosted artifact context requires its owned world source")
        self._configure(
            ContextResource(name, world or name, run, None if source is None else source.name)
        )
        return RuntimeArtifacts(self, name, world or name, run)

    async def _activate(self) -> Host:
        if self._host is not None:
            return self._host
        if self._opening is None:
            if self._config["library"] is None or self._config["store_root"] is None:
                raise ValueError("Configure ARCHETYPE_NATIVE_LIBRARY and ARCHETYPE_STORE")

            async def open_owner() -> Host:
                try:
                    host = await asyncio.to_thread(open_host, self._config)
                except ConstructionCleanupError as error:
                    self._host = error.host
                    self._accepting = False
                    raise RuntimeOperationError(
                        "native_cleanup_failed", outcome="not_dispatched"
                    ) from None
                except ProtocolError:
                    raise RuntimeOperationError(
                        "native_incompatible", outcome="not_dispatched"
                    ) from None
                except Exception:
                    raise RuntimeOperationError(
                        "native_unavailable", outcome="not_dispatched"
                    ) from None
                self._host = host
                return host

            self._opening = asyncio.create_task(open_owner())
            self._opening.add_done_callback(
                lambda done: None if done.cancelled() else done.exception()
            )
        try:
            return await asyncio.shield(self._opening)
        except Exception:
            if self._opening.done():
                self._opening = None
            raise

    async def _perform(self, request: w.Request) -> dict[str, Any]:
        host = await self._activate()
        try:
            from archetype.wiring import artifact_workflow

            result = await asyncio.to_thread(
                _Executor(host, self._resources, artifact_workflow(host)).execute, request
            )
            if not isinstance(request.operation, (w.Read, w.History)) and isinstance(
                self._resources[request.resource], (LogicalResource, Resource)
            ):
                self._active_worlds.add(request.resource)
            return result
        except NativeError as error:
            code = (
                error.code
                if error.code
                in {"resource_limit", "corrupt_data", "invalid_request", "unsupported_format"}
                else "operation_failed"
            )
            raise RuntimeOperationError(code, outcome="unknown") from None
        except Exception:
            raise RuntimeOperationError("operation_failed", outcome="unknown") from None

    async def _invoke(self, name: str, op: str, args: dict[str, Any]) -> dict[str, Any]:
        self._owner()
        self._ensure_open()
        if op != "read" and (name in self._closed_worlds or name in self._draining_worlds):
            raise RuntimeError("World is draining or closed")
        # Decode the exact shared transport contract before activating native ownership.
        request = w.Request.decode(
            w.encode_request({"version": 1, "resource": name, "operation": op, "arguments": args})
        )
        _validate_request(self._resources, request)
        return await self._submit(name, lambda: self._perform(request))

    async def _submit(self, name: str, operation: Callable[[], Coroutine[Any, Any, Any]]):
        self._owner()
        self._ensure_open()
        if len(self._pending) >= self._max_inflight:
            raise RuntimeOperationError("busy", outcome="not_dispatched")
        task = asyncio.create_task(operation())
        self._pending[task] = name

        def completed(done: asyncio.Task[Any]) -> None:
            self._pending.pop(done, None)
            if not done.cancelled():
                done.exception()

        task.add_done_callback(completed)
        return await asyncio.shield(task)

    async def _artifact_files(self, name: str, method: str, *arguments):
        async def execute():
            host = await self._activate()
            from archetype.wiring import artifact_workflow

            try:
                workflow = artifact_workflow(host)
                return await asyncio.to_thread(getattr(workflow, method), *arguments)
            except Exception:
                raise RuntimeOperationError("operation_failed", outcome="unknown") from None

        return await self._submit(name, execute)

    async def _shutdown_world(self, name: str) -> None:
        self._owner()
        if name in self._closed_worlds:
            return
        self._ensure_open()
        self._draining_worlds.add(name)
        existing = self._world_closes.get(name)
        if existing is None or existing.done():

            async def close_world() -> None:
                tasks = [t for t, resource in self._pending.items() if resource == name]
                if tasks:
                    await asyncio.wait(tasks)
                if name in self._active_worlds:
                    await self._perform(w.Request(name, w.Stop()))
                self._closed_worlds.add(name)

            existing = asyncio.create_task(close_world())
            self._world_closes[name] = existing
        await asyncio.shield(existing)

    async def shutdown(self) -> None:
        self._owner()
        if self._closed:
            return
        self._accepting = False
        if self._closing is None or self._closing.done():

            async def close_owner() -> None:
                while self._pending:
                    await asyncio.wait(tuple(self._pending))
                # Server startup activates directly, so its retained opening
                # must drain even when no public operation entered _pending.
                if self._opening is not None and not self._opening.done():
                    await asyncio.wait((self._opening,))
                closes = [t for t in self._world_closes.values() if not t.done()]
                if closes:
                    await asyncio.wait(closes)
                if self._host is not None:
                    try:
                        await asyncio.to_thread(self._host.close)
                    except Exception:
                        raise RuntimeOperationError(
                            "native_close_failed", outcome="unknown"
                        ) from None
                self._closed = True

            self._closing = asyncio.create_task(close_owner())
        await asyncio.shield(self._closing)

    @classmethod
    def sync(cls, **configuration: Any) -> SyncArchetypeRuntime:
        return SyncArchetypeRuntime(cls(**configuration))


class SyncArchetypeRuntime:
    """Blocking facade with one retained Runner, including failed-close retries."""

    def __init__(self, runtime: ArchetypeRuntime):
        self._runtime, self._runner = runtime, None

    def __enter__(self) -> SyncArchetypeRuntime:
        self._runtime._process_owner()
        if self._runner is not None:
            raise RuntimeError("Sync runtime is already entered")
        _outside_loop()
        self._runner = asyncio.Runner()
        try:
            self._runner.run(self._runtime.__aenter__())
        except BaseException:
            self._runner.close()
            self._runner = None
            raise
        return self

    def __exit__(self, *_: object) -> None:
        self.shutdown()

    def _dispatch(self, coroutine: Coroutine[Any, Any, Any]) -> Any:
        try:
            self._runtime._process_owner()
            _outside_loop()
            if self._runner is None:
                raise RuntimeError("Enter the sync runtime context first")
        except BaseException:
            coroutine.close()
            raise
        return self._runner.run(coroutine)

    def program(self, name: str):
        from archetype.runtime.world import SyncRuntimeProgram

        return SyncRuntimeProgram(self, self._runtime.program(name))

    def world(self, name: str, **declarations: Any):
        from archetype.runtime.world import SyncRuntimeWorld

        return SyncRuntimeWorld(self, self._runtime.world(name, **declarations))

    def artifacts(self, name: str, **scope: Any):
        from archetype.runtime.world import SyncRuntimeArtifacts

        return SyncRuntimeArtifacts(self, self._runtime.artifacts(name, **scope))

    def shutdown(self) -> None:
        if self._runner is None:
            if self._runtime._closed:
                return
            raise RuntimeError("Enter the sync runtime context first")
        self._dispatch(self._runtime.shutdown())
        self._runner.close()
        self._runner = None


def _outside_loop() -> None:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return
    raise RuntimeError("Use the async runtime inside an event loop")


def run_sync(coroutine: Coroutine[Any, Any, Any]) -> Any:
    try:
        _outside_loop()
    except BaseException:
        coroutine.close()
        raise
    return asyncio.run(coroutine)
