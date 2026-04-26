# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Archetype runtime — process lifecycle and session management."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any, Literal
from weakref import WeakSet

from uuid_utils import uuid7

from archetype.app.auth.models import ActorCtx
from archetype.app.container import ServiceContainer
from archetype.core.aio import AsyncProcessor
from archetype.core.config import CacheConfig, RunConfig, StorageConfig
from archetype.core.hooks import AsyncHookHandler, HookEvent
from archetype.runtime.world import RuntimeWorld, SyncRuntimeWorld, _RuntimeWorldState

_FireMode = Literal["blocking", "spawn"]


def _default_actor_ctx() -> ActorCtx:
    return ActorCtx(id=uuid7(), roles={"viewer", "operator", "maintainer"})


def _coerce_storage_config(storage: str | Path | StorageConfig | None) -> StorageConfig:
    if storage is None:
        return StorageConfig()
    if isinstance(storage, StorageConfig):
        return storage
    return StorageConfig(uri=str(storage))


class ArchetypeRuntime:
    """Explicit runtime owner for script-friendly worlds."""

    def __init__(
        self,
        *,
        registry_path: str | Path | None = None,
        actor_ctx: ActorCtx | None = None,
    ) -> None:
        self._container = ServiceContainer(registry_path=registry_path)
        self._actor_ctx = actor_ctx or _default_actor_ctx()
        self._handles: WeakSet[RuntimeWorld] = WeakSet()
        self._closed = False

    async def __aenter__(self) -> ArchetypeRuntime:
        self._ensure_open()
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.shutdown()

    @classmethod
    def sync(
        cls,
        *,
        registry_path: str | Path | None = None,
        actor_ctx: ActorCtx | None = None,
    ) -> SyncArchetypeRuntime:
        return SyncArchetypeRuntime(registry_path=registry_path, actor_ctx=actor_ctx)

    def world(
        self,
        name: str = "world",
        *,
        storage: str | Path | StorageConfig | None = None,
        cache: CacheConfig | None = None,
        processors: list[AsyncProcessor] | None = None,
        resources: list[Any] | None = None,
    ) -> RuntimeWorld:
        self._ensure_open()
        state = _RuntimeWorldState(
            runtime=self,
            name=name,
            storage=storage,
            cache=cache,
            processors=processors,
            resources=resources,
        )
        return state.bind(self._actor_ctx)

    async def shutdown(self) -> None:
        if self._closed:
            return
        self._closed = True
        for handle in list(self._handles):
            await handle._shutdown_internal(from_runtime=True)
        await self._container.shutdown()

    def _ensure_open(self) -> None:
        if self._closed:
            raise RuntimeError("ArchetypeRuntime is closed")

    def _register_handle(self, handle: RuntimeWorld) -> None:
        self._handles.add(handle)

    def _unregister_handle(self, handle: RuntimeWorld) -> None:
        self._handles.discard(handle)


class SyncArchetypeRuntime:
    """Synchronous wrapper around ``ArchetypeRuntime`` for scripts."""

    def __init__(
        self,
        *,
        registry_path: str | Path | None = None,
        actor_ctx: ActorCtx | None = None,
    ) -> None:
        self._runtime = ArchetypeRuntime(registry_path=registry_path, actor_ctx=actor_ctx)
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

    def run(self, awaitable) -> Any:
        return self._require_runner().run(awaitable)

    def world(
        self,
        name: str = "world",
        *,
        storage: str | Path | StorageConfig | None = None,
        cache: CacheConfig | None = None,
        processors: list[AsyncProcessor] | None = None,
        resources: list[Any] | None = None,
    ) -> SyncRuntimeWorld:
        return SyncRuntimeWorld(
            self._runtime.world(
                name,
                storage=storage,
                cache=cache,
                processors=processors,
                resources=resources,
            ),
            self,
        )


def run_sync(coro) -> Any:
    """Run an async coroutine synchronously."""
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)
    raise RuntimeError("run_sync() cannot be used from within a running event loop")
