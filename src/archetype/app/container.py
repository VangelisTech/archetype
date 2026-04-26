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
from archetype.app.simulation_service import SimulationService
from archetype.app.storage_service import StorageService
from archetype.app.world_service import WorldOrchestrator, WorldRegistry


class ServiceContainer:
    """
    Wires services together with correct dependency ordering.

    Each service owns its internal composition (factories, context builders).
    The container only handles service-to-service wiring.

    Usage:
        container = ServiceContainer()
        world = await container.world_service.create_world(config, storage_config)
        await container.simulation_service.step(world.world_id, run_config)

    Pass ``registry_path`` to enable persistent world discovery across
    server restarts.
    """

    def __init__(self, registry_path: str | Path | None = None):
        # Infrastructure
        self.broker = CommandBroker()
        self.registry: WorldRegistry | None = (
            WorldRegistry(registry_path) if registry_path is not None else None
        )

        # Services
        self.storage_service = StorageService()
        self.world_service = WorldOrchestrator(
            self.storage_service,
            broker=self.broker,
            registry=self.registry,
        )
        self.command_service = CommandService(self.broker, self.world_service)
        self.simulation_service = SimulationService(self.world_service, self.command_service)
        self.query_service = QueryService(self.world_service, broker=self.broker)

    async def shutdown(self) -> None:
        """Gracefully shut down all services."""
        await self.broker.clear()
        await self.world_service.shutdown()
