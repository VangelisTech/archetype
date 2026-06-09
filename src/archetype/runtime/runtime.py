# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""ArchetypeRuntime — the script boundary.

Owns the ServiceContainer and default ActorCtx. Produces world handles
that route every operation through iCommandService.
"""

from __future__ import annotations

import asyncio
import itertools
from pathlib import Path
from typing import Any
from weakref import WeakValueDictionary

from archetype.app.auth.models import ActorCtx
from archetype.app.container import ServiceContainer
from archetype.core.config import CacheConfig, StorageConfig
from archetype.core.hooks import HookEvent
from archetype.runtime._actor import default_actor_ctx
from archetype.runtime._config import coerce_cache, coerce_storage
from archetype.runtime.world import RuntimeWorld, SyncRuntimeWorld, _RuntimeWorldState


class ArchetypeRuntime:
    """Process-level runtime. Owns the container and default identity."""

    def __init__(self, *, actor_ctx: ActorCtx | None = None) -> None:
        import logfire
        from logfire.exceptions import LogfireConfigError

        try:
            logfire.configure(service_name="archetype-runtime")
        except LogfireConfigError:
            # No Logfire credentials on this machine — degrade to
            # local-only instrumentation instead of refusing to start.
            logfire.configure(service_name="archetype-runtime", send_to_logfire=False)

        self._container = ServiceContainer()
        self._actor_ctx = actor_ctx or default_actor_ctx()
        # Insertion-ordered weak registry so shutdown can run LIFO (R5).
        self._handles: WeakValueDictionary[int, RuntimeWorld] = WeakValueDictionary()
        self._handle_seq = itertools.count()
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
        live = [self._handles[key] for key in sorted(self._handles.keys(), reverse=True)]
        for handle in live:
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
        self._register_handle(handle)
        return handle

    @classmethod
    def sync(cls, *, actor_ctx: ActorCtx | None = None) -> SyncArchetypeRuntime:
        """Factory for the synchronous runtime facade."""
        return SyncArchetypeRuntime(actor_ctx=actor_ctx)

    def _register_handle(self, handle: RuntimeWorld) -> None:
        self._handles[next(self._handle_seq)] = handle

    def _unregister_handle(self, handle: RuntimeWorld) -> None:
        for key, value in list(self._handles.items()):
            if value is handle:
                del self._handles[key]
                return

    def _ensure_open(self) -> None:
        if self._closed:
            raise RuntimeError("ArchetypeRuntime is closed")


class SyncArchetypeRuntime:
    """Synchronous facade. Owns its own asyncio.Runner (R6, OQ6)."""

    def __init__(self, *, actor_ctx: ActorCtx | None = None) -> None:
        self._runtime = ArchetypeRuntime(actor_ctx=actor_ctx)
        self._runner: asyncio.Runner | None = None

    def __enter__(self) -> SyncArchetypeRuntime:
        self._runner = asyncio.Runner()
        try:
            self._runner.run(self._runtime.__aenter__())
        except BaseException:
            self._runner.close()
            self._runner = None
            raise
        return self

    def __exit__(self, *exc_info: object) -> None:
        if self._runner is None:
            return
        try:
            self._runner.run(self._runtime.__aexit__(*exc_info))
        finally:
            self._runner.close()
            self._runner = None

    def shutdown(self) -> None:
        """Shut down all handles then the container. Idempotent (R6)."""
        if self._runner is None:
            return
        try:
            self._runner.run(self._runtime.shutdown())
        finally:
            self._runner.close()
            self._runner = None

    def _require_runner(self) -> asyncio.Runner:
        if self._runner is None:
            raise RuntimeError("SyncArchetypeRuntime is not active")
        return self._runner

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


def run_sync(coro) -> Any:
    """One-off escape hatch for running a coroutine synchronously."""
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)
    raise RuntimeError("run_sync() cannot be used from within a running event loop")
