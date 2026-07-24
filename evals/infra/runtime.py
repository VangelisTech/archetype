# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Repository-eval host for the canonical runtime composition transaction.

The self-harness occasionally needs storage, scheduler, or live-world evidence
that the supported runtime deliberately does not expose.  This helper still
constructs the real graph through :mod:`archetype.wiring`; it only gives evals
typed names for the exact objects already owned by that graph.
"""

from __future__ import annotations

import asyncio
from functools import partial
from pathlib import Path
from typing import cast

from archetype.commands.dispatch import CommandDispatcher
from archetype.commands.registry import OperationRegistry
from archetype.commands.scheduler import CommandScheduler
from archetype.core.component import Component
from archetype.core.config import StorageConfig
from archetype.runtime_resources import RuntimeResources
from archetype.storage.config import ControlCatalogConfig
from archetype.storage.service import StorageService
from archetype.wiring import RuntimeBootstrapConfig, build_runtime_resources
from archetype.world.models import ComponentTypeRef, ComponentValue
from archetype.world.registry import WorldRegistry


class EvalProcess:
    """One explicitly composed process graph plus eval-only evidence handles."""

    def __init__(
        self,
        *,
        control_catalog_config: ControlCatalogConfig | None = None,
        audit_storage_config: StorageConfig | None = None,
        storage_service: StorageService | None = None,
    ) -> None:
        control = control_catalog_config or ControlCatalogConfig.from_env()
        self._close_lock = asyncio.Lock()
        self._owns_storage = storage_service is None
        self._storage_closed = False
        self.storage = storage_service or StorageService(
            control_catalog_config=control,
        )
        config = RuntimeBootstrapConfig(
            control_catalog_config=control,
            storage_service=self.storage,
            audit_storage_config=audit_storage_config,
        )
        self.resources: RuntimeResources = build_runtime_resources(config)
        self.dispatcher = cast(CommandDispatcher, self.resources.dispatcher)
        self.registry = self.dispatcher._registry
        self.scheduler = cast(CommandScheduler, self.dispatcher._scheduler)
        self.worlds = _world_registry(self.registry)

    async def __aenter__(self) -> EvalProcess:
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        async with self._close_lock:
            await self.resources.aclose()
            if self._owns_storage and not self._storage_closed:
                await self.storage.shutdown()
                self._storage_closed = True


def isolated_eval_process(
    root: str | Path,
    *,
    audit_storage_config: StorageConfig | None = None,
) -> EvalProcess:
    """Build one process whose control catalogs are isolated under ``root``."""

    return EvalProcess(
        control_catalog_config=ControlCatalogConfig(
            catalog_dir=Path(root) / "catalogs",
        ),
        audit_storage_config=audit_storage_config,
    )


def component_values(
    components: list[Component] | tuple[Component, ...],
) -> tuple[ComponentValue, ...]:
    """Freeze live component inputs into one exact operation value."""

    return tuple(ComponentValue.from_component(component) for component in components)


def component_refs(
    component_types: list[type[Component]] | tuple[type[Component], ...],
) -> tuple[ComponentTypeRef, ...]:
    """Freeze component classes into exact schema-bound references."""

    return tuple(ComponentTypeRef.from_type(component_type) for component_type in component_types)


def _world_registry(registry: OperationRegistry) -> WorldRegistry:
    """Resolve the registry already bound to the canonical world handler."""

    handler = registry.resolve_name("get_world_info").handler
    if not isinstance(handler, partial) or not handler.args:
        raise RuntimeError("canonical get_world_info handler has no bound world registry")
    worlds = handler.args[0]
    if not isinstance(worlds, WorldRegistry):
        raise RuntimeError("canonical get_world_info handler is bound to an unexpected owner")
    return worlds


__all__ = [
    "EvalProcess",
    "component_refs",
    "component_values",
    "isolated_eval_process",
]
