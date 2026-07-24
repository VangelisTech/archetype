# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Exact-world cleanup capability contract."""

from __future__ import annotations

from typing import Any

from uuid_utils import UUID

from archetype.core.component import Component
from archetype.core.config import RunConfig
from archetype.world.interfaces import iWorldLifecycle, iWorldRegistry
from archetype.world.registry import WorldCleanupLease
from archetype.world.simulation import _step_locked


class WorldCleanup:
    """A non-ambient capability bound to one registry lease and world."""

    __slots__ = ("_lease", "_lifecycle", "_registry", "_world_id")

    def __init__(
        self,
        *,
        registry: iWorldRegistry,
        lifecycle: iWorldLifecycle,
        world_id: str | UUID,
        lease: WorldCleanupLease,
    ) -> None:
        exact_world_id = str(world_id)
        registry.validate_cleanup_lease(lease, world_id=exact_world_id)
        self._registry = registry
        self._lifecycle = lifecycle
        self._world_id = exact_world_id
        self._lease = lease

    @property
    def world_id(self) -> str:
        """Return the exact world identity bound to this capability."""

        return self._world_id

    def _validate(self) -> None:
        self._registry.validate_cleanup_lease(
            self._lease,
            world_id=self._world_id,
        )

    async def stage_teardown(self, components: list[Component]) -> int:
        """Stage one teardown-evidence entity in the bound world."""

        self._validate()
        async with self._registry.cleanup_operation(self._lease) as world:
            self._require_exact_world(world)
            return await world.create_entity(components)

    async def update_retained(
        self,
        entity_id: int,
        components: list[Component],
    ) -> None:
        """Update one retained-evidence entity in the bound world."""

        self._validate()
        async with self._registry.cleanup_operation(self._lease) as world:
            self._require_exact_world(world)
            await world.update_entity(entity_id, components)

    async def commit(
        self,
        run_config: RunConfig,
        **input_kwargs: Any,
    ) -> int:
        """Commit and reconcile one managed cleanup step in the bound world."""

        self._validate()
        async with self._registry.cleanup_operation(self._lease) as world:
            self._require_exact_world(world)
            return await _step_locked(
                self._registry,
                self._world_id,
                world,
                run_config,
                **input_kwargs,
            )

    async def finish(self) -> None:
        """Destroy and finish close for only the bound world."""

        self._validate()
        await self._lifecycle.destroy_world(
            self._world_id,
            lease=self._lease,
        )

    def _require_exact_world(self, world: object) -> None:
        if str(getattr(world, "world_id", "")) != self._world_id:
            raise ValueError("cleanup operation did not resolve its bound world")


__all__ = ["WorldCleanup"]
