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

"""ArchetypeRuntime — the script boundary.

Owns the ServiceContainer and default ActorCtx. Produces world handles
that route every operation through iCommandService.
"""

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
    """Process-level runtime. Owns the container and default identity."""

    def __init__(self, *, actor_ctx: ActorCtx | None = None, log: str | None = None) -> None:
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
        """Shut down all handles then the container. Idempotent."""
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
        """Create a world handle. The world is activated on first operation."""
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
        """Fenced mutable cold resume (issue #273): load a save state, live.

        Reconstructs a writable world from its durable rows and control
        catalog — tick, entities, and fork lineage restored — and fences out
        the previous writer, whose next commit fails closed. Requires the
        world's component classes to be imported (code is not rows).
        Processors, resources, and hooks are likewise not restored: reattach
        them on the returned handle before stepping if the simulation needs
        them.
        """
        if self._closed:
            raise RuntimeError("ArchetypeRuntime is closed")
        gate = self._container.command_service
        info = await gate.resume_world(
            self._actor_ctx, coerce_storage(storage) or StorageConfig(), world_id
        )
        return self.attach(info.world_id, name=name)

    async def discover(self, storage: str | Path | StorageConfig | None = None) -> list[WorldInfo]:
        """Every world ever recorded against a storage identity — works cold.

        Durable discovery (issue #272) from the recommended API: a fresh
        process pointed at existing storage sees each world's descriptor,
        including destroyed ones (their rows remain queryable). Pair with
        ``attach(world_id, storage=...)`` for cold reads or ``resume`` for a
        fenced writer.
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
        """Return a handle for an existing world — live, or cold via storage.

        The save-slot loader: episode worlds left behind by a rollout, an
        autoresearch lab world, or any world created outside this handle's
        scope can be queried and driven through the same gated surface. With
        an explicit ``storage``, the handle also reads worlds that are not
        live in this process (cold discovery): ``info()`` and ``query()``
        resolve through the durable catalog. The id is validated on first
        operation, and the handle does not own the world — shutting it down
        never destroys the world (``destroy()`` still can, explicitly).
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
        """Factory for the synchronous runtime facade."""
        return SyncArchetypeRuntime(actor_ctx=actor_ctx, log=log)

    def _register_handle(self, handle: RuntimeWorld) -> None:
        self._handles.add(handle)

    def _unregister_handle(self, handle: RuntimeWorld) -> None:
        self._handles.discard(handle)

    def _ensure_open(self) -> None:
        if self._closed:
            raise RuntimeError("ArchetypeRuntime is closed")


class SyncArchetypeRuntime:
    """Synchronous facade. Owns its own asyncio.Runner (R6, OQ6)."""

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
        """See ArchetypeRuntime.discover (durable discovery, works cold)."""
        return self._dispatch(self._runtime.discover(storage))

    def attach(self, world_id, *, name: str = "attached", storage=None) -> SyncRuntimeWorld:
        """Handle for an existing world. See ArchetypeRuntime.attach."""
        return SyncRuntimeWorld(self._runtime.attach(world_id, name=name, storage=storage), self)

    def resume(self, world_id, *, storage=None, name: str = "resumed") -> SyncRuntimeWorld:
        """See ArchetypeRuntime.resume (fenced mutable cold resume)."""
        rw = self._dispatch(self._runtime.resume(world_id, storage=storage, name=name))
        return SyncRuntimeWorld(rw, self)


def run_sync(coro) -> Any:
    """One-off escape hatch for running a coroutine synchronously."""
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)
    raise RuntimeError("run_sync() cannot be used from within a running event loop")
