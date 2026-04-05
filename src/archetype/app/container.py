# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Service Container

Wires all services together. Single point of construction.
"""

from __future__ import annotations

from pathlib import Path

from archetype.app.broker import CommandBroker
from archetype.app.command_service import CommandService
from archetype.app.query_service import QueryService
from archetype.app.registry import WorldRegistry
from archetype.app.simulation_service import SimulationService
from archetype.app.storage_service import StorageService
from archetype.app.world_service import WorldService


class ServiceContainer:
    """
    Wires all services together with correct dependency ordering.

    Usage:
        container = ServiceContainer()
        world = await container.world_service.create_world(config, storage_config)
        await container.simulation_service.step(world.world_id, run_config)

    Pass ``registry_path`` to enable persistent world discovery across
    processes — required for the CLI, where each command runs in a fresh
    process with a fresh container.
    """

    def __init__(self, registry_path: str | Path | None = None):
        # Infrastructure
        self.storage_service = StorageService()
        self.broker = CommandBroker()
        self.registry: WorldRegistry | None = (
            WorldRegistry(registry_path) if registry_path is not None else None
        )

        # Services (order matters — dependency chain)
        self.world_service = WorldService(
            self.storage_service, broker=self.broker, registry=self.registry
        )
        self.command_service = CommandService(self.broker, self.world_service)
        self.simulation_service = SimulationService(self.world_service, self.command_service)
        self.query_service = QueryService(self.world_service, broker=self.broker)

    async def shutdown(self) -> None:
        """Gracefully shut down all services."""
        await self.storage_service.shutdown()
