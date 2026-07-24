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
from importlib import import_module
from pathlib import Path
from typing import TYPE_CHECKING, Any

from uuid_utils import UUID, uuid7

from archetype._logging import configure_host_observability
from archetype.artifacts.contracts import ArtifactStoreConfig
from archetype.core.config import CacheConfig, StorageConfig
from archetype.core.hooks import HookEvent
from archetype.physical_ai.models import (
    EvaluatePhysicalTask,
    SweepPhysicalInstructions,
)
from archetype.runtime._config import coerce_cache, coerce_storage
from archetype.runtime.world import (
    RuntimeWorld,
    SyncRuntimeWorld,
    _RuntimeWorldState,
)
from archetype.runtime_resources import RuntimeCloseState
from archetype.world.models import DiscoverWorlds, ResumeWorld, WorldInfo

if TYPE_CHECKING:
    from archetype.missions.contracts import AgentMissionConfig
    from archetype.physical_ai.contracts import (
        InstructionSweepConfig,
        InstructionSweepReport,
        PhysicalTaskEvalConfig,
        PhysicalTaskEvalReport,
    )
    from archetype.physical_ai.manipulation import EnvClient
    from archetype.physical_ai.policy import PolicyClient
    from archetype.runtime.missions import RuntimeMissions


def _bootstrap_config(*, artifact_store: ArtifactStoreConfig | None) -> Any:
    """Resolve the optional wiring module only when a runtime is constructed."""

    wiring = import_module("archetype.wiring")
    return wiring.RuntimeBootstrapConfig.from_env(
        artifact_store_config=artifact_store,
    )


def build_runtime_resources(config: Any) -> Any:
    """Late-bound construction seam shared with tests and downstream hosts."""

    return import_module("archetype.wiring").build_runtime_resources(config)


def _bind_world_state(resources: Any, state: _RuntimeWorldState) -> RuntimeWorld:
    """Strongly register one inert world handle with the process owner."""

    reservation = resources.reserve_owner(
        f"world:{uuid7()}",
        phase="world-handles",
    )
    handle = RuntimeWorld(state=state, reservation=reservation)
    reservation.bind(handle, close=handle._close_owned)
    return handle


class _RuntimeResourceHost:
    """Minimal host used by wiring-created workflows that need a world handle."""

    def __init__(self, resources: Any) -> None:
        self._resources = resources

    def _ensure_open(self) -> None:
        if (
            self._resources.close_state is not RuntimeCloseState.OPEN
            and not self._resources.operation_admitted()
        ):
            raise RuntimeError("Runtime resources are closed")

    def _bind_world_state(self, state: _RuntimeWorldState) -> RuntimeWorld:
        return _bind_world_state(self._resources, state)


def _runtime_world_for_resources(
    runtime_resources: Any,
    name: str = "world",
    *,
    storage: str | Path | StorageConfig | None = None,
    cache: CacheConfig | None = None,
    processors: list | None = None,
    resources: list | None = None,
    hooks: list[tuple[type[HookEvent], Any]] | None = None,
) -> RuntimeWorld:
    """Construct the narrow world surface used by wiring-owned workflows."""

    host = _RuntimeResourceHost(runtime_resources)
    state = _RuntimeWorldState(
        runtime=host,
        name=name,
        storage_config=coerce_storage(storage),
        cache_config=coerce_cache(cache),
        init_processors=list(processors or []),
        init_resources=list(resources or []),
        init_hooks=list(hooks or []),
    )
    return host._bind_world_state(state)


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

    def __init__(
        self,
        *,
        log: str | None = None,
        artifact_store: ArtifactStoreConfig | None = None,
    ) -> None:
        """Initialize the runtime.

        Args:
            log: Package log level: `debug`, `info`, `warning`, or `error`.
                When omitted, `ARCHETYPE_LOG` is used and logging stays quiet
                if that variable is unset.
            artifact_store: Optional content-addressed object-store bounds.
        """
        # This constructor is an explicit trusted-script process host. Quiet
        # remains the default; stdlib logging and vendor-neutral tracing are
        # configured together only here, never by an imported family module.
        configure_host_observability(
            service_name="archetype-runtime",
            log=log,
        )

        self._resources = build_runtime_resources(
            _bootstrap_config(artifact_store=artifact_store),
        )
        self._dispatcher = self._resources.dispatcher
        self._shutdown_lock = asyncio.Lock()
        self._shutdown_started = False
        self._closed = False

    async def __aenter__(self) -> ArchetypeRuntime:
        if self._shutdown_started or self._closed:
            raise RuntimeError("ArchetypeRuntime cannot be reused after close")
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.shutdown()

    async def shutdown(self) -> None:
        """Delegate retryable process teardown to the sole resource owner."""

        if self._resources.operation_admitted():
            raise RuntimeError("ArchetypeRuntime cannot close from an admitted operation")
        self._resources.ensure_close_allowed()
        async with self._shutdown_lock:
            if self._closed:
                return
            self._shutdown_started = True
            await self._resources.aclose()
            self._closed = True

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
        self._ensure_open()

        state = _RuntimeWorldState(
            runtime=self,
            name=name,
            storage_config=coerce_storage(storage),
            cache_config=coerce_cache(cache),
            init_processors=list(processors or []),
            init_resources=list(resources or []),
            init_hooks=list(hooks or []),
        )
        return self._bind_world_state(state)

    def missions(
        self,
        name: str = "agent-missions",
        *,
        config: AgentMissionConfig,
        storage: str | Path | StorageConfig | None = None,
    ) -> RuntimeMissions:
        """Create a lazy, batteries-included Agent Missions handle."""

        self._ensure_open()
        from archetype.runtime.missions import RuntimeMissions

        owner_id = f"mission:{uuid7()}"
        reservation = self._resources.reserve_owner(
            owner_id,
            phase="workflow-handles",
            closed_message="Agent Missions handle is closed",
        )
        handle = RuntimeMissions(
            self,
            name,
            config=config,
            storage=storage,
            owner_id=owner_id,
            reservation=reservation,
        )
        reservation.retain_anchor(handle)
        return handle

    async def evaluate_physical_task(
        self,
        config: PhysicalTaskEvalConfig,
        *,
        env_client: EnvClient,
        policy_client: PolicyClient | None = None,
    ) -> PhysicalTaskEvalReport:
        """Run and grade one batched physical task evaluation."""

        async with self._resources.admit_operation():
            self._ensure_open()
            return await self._dispatcher.apply(
                EvaluatePhysicalTask(
                    config=config,
                    env_client=env_client,
                    policy_client=policy_client,
                )
            )

    async def sweep_physical_instructions(
        self,
        config: InstructionSweepConfig,
        *,
        env_client: EnvClient,
        policy_client: PolicyClient,
    ) -> InstructionSweepReport:
        """Compare instruction variants on paired seeds in one world."""

        async with self._resources.admit_operation():
            self._ensure_open()
            return await self._dispatcher.apply(
                SweepPhysicalInstructions(
                    config=config,
                    env_client=env_client,
                    policy_client=policy_client,
                )
            )

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
        async with self._resources.admit_operation():
            self._ensure_open()
            info = await self._dispatcher.apply(
                ResumeWorld(
                    storage_config=coerce_storage(storage) or StorageConfig(),
                    world_id=world_id,
                )
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
        async with self._resources.admit_operation():
            self._ensure_open()
            return await self._dispatcher.apply(
                DiscoverWorlds(
                    storage_config=coerce_storage(storage) or StorageConfig(),
                )
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
        self._ensure_open()

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
        return self._bind_world_state(state)

    @classmethod
    def sync(
        cls,
        *,
        log: str | None = None,
        artifact_store: ArtifactStoreConfig | None = None,
    ) -> SyncArchetypeRuntime:
        """Create the synchronous runtime facade."""
        return SyncArchetypeRuntime(
            log=log,
            artifact_store=artifact_store,
        )

    def _bind_world_state(self, state: _RuntimeWorldState) -> RuntimeWorld:
        """Strongly register one inert world handle before returning it."""

        return _bind_world_state(self._resources, state)

    def _ensure_open(self) -> None:
        if (self._shutdown_started or self._closed) and not self._resources.operation_admitted():
            raise RuntimeError("ArchetypeRuntime is closed")


class SyncArchetypeRuntime:
    """Synchronous facade over `ArchetypeRuntime`.

    Use it as a context manager. New asynchronous applications should use
    `ArchetypeRuntime` directly.
    """

    def __init__(
        self,
        *,
        log: str | None = None,
        artifact_store: ArtifactStoreConfig | None = None,
    ) -> None:
        self._runtime = ArchetypeRuntime(
            log=log,
            artifact_store=artifact_store,
        )
        self._runner: asyncio.Runner | None = None

    def __enter__(self) -> SyncArchetypeRuntime:
        self._runner = asyncio.Runner()
        self._runner.run(self._runtime.__aenter__())
        return self

    def __exit__(self, *exc_info: object) -> None:
        del exc_info
        self.shutdown()

    def shutdown(self) -> None:
        """Retry process teardown and release the runner only after success."""

        runner = self._runner
        if runner is None:
            if self._runtime._closed:
                return
            raise RuntimeError("SyncArchetypeRuntime is not active")
        self._dispatch(self._runtime.shutdown())
        runner.close()
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

    def evaluate_physical_task(
        self,
        config: PhysicalTaskEvalConfig,
        *,
        env_client: EnvClient,
        policy_client: PolicyClient | None = None,
    ) -> PhysicalTaskEvalReport:
        """Synchronous physical task evaluation."""

        return self._dispatch(
            self._runtime.evaluate_physical_task(
                config,
                env_client=env_client,
                policy_client=policy_client,
            )
        )

    def sweep_physical_instructions(
        self,
        config: InstructionSweepConfig,
        *,
        env_client: EnvClient,
        policy_client: PolicyClient,
    ) -> InstructionSweepReport:
        """Synchronous paired-seed instruction sweep."""

        return self._dispatch(
            self._runtime.sweep_physical_instructions(
                config,
                env_client=env_client,
                policy_client=policy_client,
            )
        )

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
