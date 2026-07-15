# Copyright 2025 Vangelis Technologies Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Process-level runtime and synchronous facade."""

from __future__ import annotations

import asyncio
import logging
import os
from pathlib import Path
from typing import Any
from weakref import WeakSet

from uuid_utils import UUID

from archetype import _obs
from archetype.app.auth.models import ActorCtx
from archetype.app.container import ServiceContainer
from archetype.app.models import WorldInfo
from archetype.core.config import CacheConfig, StorageConfig
from archetype.core.hooks import HookEvent
from archetype.runtime._actor import default_actor_ctx
from archetype.runtime._config import coerce_cache, coerce_storage
from archetype.runtime.world import RuntimeWorld, SyncRuntimeWorld, _RuntimeWorldState

_LOG_LEVELS = {
    "debug": logging.DEBUG,
    "info": logging.INFO,
    "warning": logging.WARNING,
    "error": logging.ERROR,
}


def _resolve_log_level(env: str | None = None) -> int | None:
    """Map ARCHETYPE_LOG (or an explicit override) to a stdlib level."""
    value = (env if env is not None else os.environ.get("ARCHETYPE_LOG", "")).strip().lower()
    return _LOG_LEVELS.get(value)


def _configure_archetype_logging(level: int) -> None:
    """Wire the ``archetype`` logger hierarchy at the script boundary.

    Every layer emits on module loggers and never configures handlers; the
    runtime is the application boundary, so the one user-facing flag lives
    here. Root logging is left untouched.
    """
    pkg_logger = logging.getLogger("archetype")
    pkg_logger.setLevel(level)
    if not pkg_logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("%(levelname).1s %(name)s: %(message)s"))
        pkg_logger.addHandler(handler)
        # This handler now owns archetype records. Without this, a host that
        # configured root logging (basicConfig, a Logfire root handler, ...)
        # would emit every record a second time through the root sinks.
        pkg_logger.propagate = False


class ArchetypeRuntime:
    """Own process-level services and create world handles.

    Use one runtime for a related set of worlds and close it with an async
    context manager. Calling `world()` only creates a handle; the world is
    activated on its first operation.

    Examples:
        >>> async with ArchetypeRuntime() as runtime:
        ...     world = runtime.world("experiment")
        ...     entity_id = await world.spawn()
        ...     result = await world.run(steps=10)
    """

    def __init__(self, *, actor_ctx: ActorCtx | None = None, log: str | None = None) -> None:
        """Initialize the runtime.

        Args:
            actor_ctx: Identity, roles, and quotas for operations. The default
                identity is suitable for local scripts.
            log: Package log level: `debug`, `info`, `warning`, or `error`.
                When omitted, `ARCHETYPE_LOG` is used and logging stays quiet
                if that variable is unset.
        """
        # One user-facing verbosity flag: ARCHETYPE_LOG=debug|info|warning|error
        # (or ArchetypeRuntime(log=...)). It wires the stdlib "archetype"
        # logger hierarchy, and at debug it also turns on console span output.
        # Quiet is the default: span walls interleave with script output and
        # drown the program's own voice — legibility of the script wins.
        level = _resolve_log_level(log)
        if level is not None:
            _configure_archetype_logging(level)

        # Tracing is vendor-neutral OpenTelemetry (see archetype._obs):
        # a host-registered provider is respected, LOGFIRE_*/OTEL_* env vars
        # select a backend, and with neither the API stays a no-op.
        _obs.configure_tracing(
            service_name="archetype-runtime",
            debug_console=level == logging.DEBUG,
        )

        self._container = ServiceContainer()
        self._actor_ctx = actor_ctx or default_actor_ctx()
        self._handles: WeakSet[RuntimeWorld] = WeakSet()
        self._closed = False

    async def __aenter__(self) -> ArchetypeRuntime:
        if self._closed:
            raise RuntimeError("ArchetypeRuntime cannot be reused after close")
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.shutdown()

    async def shutdown(self) -> None:
        """Close every world handle and release process-level resources.

        Repeated calls have no effect.
        """
        if self._closed:
            return
        self._closed = True

        errors: list[Exception] = []
        for handle in list(self._handles):
            try:
                await handle._shutdown_internal(from_runtime=True)
            except Exception as e:
                errors.append(e)

        try:
            await self._container.shutdown()
        except Exception as e:
            errors.append(e)

        if errors:
            raise RuntimeError(
                f"ArchetypeRuntime shutdown encountered {len(errors)} error(s): {errors[0]!r}"
            ) from errors[0]

    def world(
        self,
        name: str = "world",
        *,
        storage: str | Path | StorageConfig | None = None,
        cache: CacheConfig | None = None,
        processors: list | None = None,
        resources: list | None = None,
        hooks: list[tuple[type[HookEvent], Any]] | None = None,
    ) -> RuntimeWorld:
        """Create a lazy handle for a world.

        Args:
            name: Human-readable world name.
            storage: Storage location or explicit storage configuration.
            cache: Optional write-cache configuration.
            processors: Processors installed when the world is activated.
            resources: Resources installed when the world is activated.
            hooks: `(event type, handler)` pairs installed at activation.

        Returns:
            A handle that activates the world on its first operation.
        """
        if self._closed:
            raise RuntimeError("ArchetypeRuntime is closed")

        state = _RuntimeWorldState(
            runtime=self,
            name=name,
            storage_config=coerce_storage(storage),
            cache_config=coerce_cache(cache),
            init_processors=list(processors or []),
            init_resources=list(resources or []),
            init_hooks=list(hooks or []),
        )
        handle = RuntimeWorld(state=state, actor_ctx=self._actor_ctx)
        state.aliases.add(handle)
        self._handles.add(handle)
        return handle

    async def resume(
        self,
        world_id: str | UUID,
        *,
        storage: str | Path | StorageConfig | None = None,
        name: str = "resumed",
    ) -> RuntimeWorld:
        """Resume a durable world as the active writer.

        The resumed world restores its tick, entities, and fork lineage. Its
        component classes must already be imported. Processors, resources,
        and hooks are code rather than stored state, so reinstall them before
        stepping. Resuming also invalidates the previous writer; its next
        commit fails instead of overwriting the resumed world.

        Args:
            world_id: Durable identity of the world to resume.
            storage: Storage containing the world.
            name: Local name for the returned handle.
        """
        if self._closed:
            raise RuntimeError("ArchetypeRuntime is closed")
        gate = self._container.command_service
        info = await gate.resume_world(
            self._actor_ctx, coerce_storage(storage) or StorageConfig(), world_id
        )
        return self.attach(info.world_id, name=name)

    async def discover(self, storage: str | Path | StorageConfig | None = None) -> list[WorldInfo]:
        """List every world recorded for a storage identity.

        Discovery works without a live world and includes destroyed worlds,
        whose durable rows remain queryable.

        Args:
            storage: Storage whose durable world catalog should be listed.

        Returns:
            Durable descriptors for every world recorded in that storage.
        """
        if self._closed:
            raise RuntimeError("ArchetypeRuntime is closed")
        gate = self._container.command_service
        return await gate.discover_worlds(
            self._actor_ctx, coerce_storage(storage) or StorageConfig()
        )

    def attach(
        self,
        world_id: str | UUID,
        *,
        name: str = "attached",
        storage: str | Path | StorageConfig | None = None,
    ) -> RuntimeWorld:
        """Attach a non-owning handle to a live or durable world.

        With explicit storage, `info()` and `query()` can resolve a world that
        is not live in this process. The identity is validated on first use.
        Closing the handle does not destroy the world, although an explicit
        `RuntimeWorld.destroy()` still does.

        Args:
            world_id: Durable identity of the world to attach.
            name: Local name for the returned handle.
            storage: Storage containing a cold world. Omit it for a live world.
        """
        if self._closed:
            raise RuntimeError("ArchetypeRuntime is closed")

        state = _RuntimeWorldState(
            runtime=self,
            name=name,
            # None → the gate resolves the world's recorded storage on read;
            # explicit storage additionally enables cold reads.
            storage_config=coerce_storage(storage),
            cache_config=None,
            init_processors=[],
            init_resources=[],
            init_hooks=[],
            world_id=world_id,
            owns_world=False,
        )
        handle = RuntimeWorld(state=state, actor_ctx=self._actor_ctx)
        state.aliases.add(handle)
        self._handles.add(handle)
        return handle

    @classmethod
    def sync(
        cls, *, actor_ctx: ActorCtx | None = None, log: str | None = None
    ) -> SyncArchetypeRuntime:
        """Create the synchronous runtime facade."""
        return SyncArchetypeRuntime(actor_ctx=actor_ctx, log=log)

    def _register_handle(self, handle: RuntimeWorld) -> None:
        self._handles.add(handle)

    def _unregister_handle(self, handle: RuntimeWorld) -> None:
        self._handles.discard(handle)

    def _ensure_open(self) -> None:
        if self._closed:
            raise RuntimeError("ArchetypeRuntime is closed")


class SyncArchetypeRuntime:
    """Synchronous facade over `ArchetypeRuntime`.

    Use it as a context manager. New asynchronous applications should use
    `ArchetypeRuntime` directly.
    """

    def __init__(self, *, actor_ctx: ActorCtx | None = None, log: str | None = None) -> None:
        self._runtime = ArchetypeRuntime(actor_ctx=actor_ctx, log=log)
        self._runner: asyncio.Runner | None = None

    def __enter__(self) -> SyncArchetypeRuntime:
        self._runner = asyncio.Runner()
        self._runner.run(self._runtime.__aenter__())
        return self

    def __exit__(self, *exc_info: object) -> None:
        assert self._runner is not None
        try:
            self._runner.run(self._runtime.__aexit__(*exc_info))
        finally:
            self._runner.close()
            self._runner = None

    def _require_runner(self) -> asyncio.Runner:
        if self._runner is None:
            raise RuntimeError("SyncArchetypeRuntime is not active")
        return self._runner

    def _dispatch(self, coro) -> Any:
        """Run *coro* to completion from any thread.

        Sync autoresearch executes user callbacks in a worker thread while
        the runner's loop keeps running in the owning thread; sync handle
        methods called from those callbacks must schedule onto the running
        loop instead of re-entering Runner.run.
        """
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            pass
        else:
            coro.close()
            raise RuntimeError(
                "sync handle methods cannot be called from the event-loop thread; "
                "use async handles (ArchetypeRuntime) inside async callbacks"
            )
        runner = self._require_runner()
        loop = runner.get_loop()
        if loop.is_running():
            return asyncio.run_coroutine_threadsafe(coro, loop).result()
        return runner.run(coro)

    def world(
        self,
        name: str = "world",
        *,
        storage: str | Path | StorageConfig | None = None,
        cache: CacheConfig | None = None,
        processors: list | None = None,
        resources: list | None = None,
        hooks: list[tuple[type[HookEvent], Any]] | None = None,
    ) -> SyncRuntimeWorld:
        rw = self._runtime.world(
            name,
            storage=storage,
            cache=cache,
            processors=processors,
            resources=resources,
            hooks=hooks,
        )
        return SyncRuntimeWorld(rw, self)

    def discover(self, storage=None) -> list[WorldInfo]:
        """List durable worlds through the synchronous facade."""
        return self._dispatch(self._runtime.discover(storage))

    def attach(self, world_id, *, name: str = "attached", storage=None) -> SyncRuntimeWorld:
        """Attach a synchronous handle to a live or durable world."""
        return SyncRuntimeWorld(self._runtime.attach(world_id, name=name, storage=storage), self)

    def resume(self, world_id, *, storage=None, name: str = "resumed") -> SyncRuntimeWorld:
        """Resume a durable world as the active writer."""
        rw = self._dispatch(self._runtime.resume(world_id, storage=storage, name=name))
        return SyncRuntimeWorld(rw, self)


def run_sync(coro) -> Any:
    """Run one coroutine when no event loop is active in this thread."""
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)
    raise RuntimeError("run_sync() cannot be used from within a running event loop")
