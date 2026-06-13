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
from pathlib import Path
from typing import Any
from weakref import WeakSet

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
        import os

        import logfire

        # Never block on an interactive logfire setup prompt.  Send to logfire
        # only when the user has explicitly configured it via one of:
        #   - LOGFIRE_TOKEN / LOGFIRE_API_KEY environment variable
        #   - LOGFIRE_SEND_TO_LOGFIRE=true environment variable
        # Without explicit opt-in, degrade to local-only instrumentation so
        # ArchetypeRuntime() works in CI and offline environments without an
        # EOFError or a blocking interactive prompt.
        _has_token = bool(os.environ.get("LOGFIRE_TOKEN") or os.environ.get("LOGFIRE_API_KEY"))
        _send_env = os.environ.get("LOGFIRE_SEND_TO_LOGFIRE", "").lower()
        _send_explicit = _send_env in ("1", "true", "yes")
        _send_to_logfire = _has_token or _send_explicit

        logfire.configure(service_name="archetype-runtime", send_to_logfire=_send_to_logfire)

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

    @classmethod
    def sync(cls, *, actor_ctx: ActorCtx | None = None) -> SyncArchetypeRuntime:
        """Factory for the synchronous runtime facade."""
        return SyncArchetypeRuntime(actor_ctx=actor_ctx)

    def _register_handle(self, handle: RuntimeWorld) -> None:
        self._handles.add(handle)

    def _unregister_handle(self, handle: RuntimeWorld) -> None:
        self._handles.discard(handle)

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
