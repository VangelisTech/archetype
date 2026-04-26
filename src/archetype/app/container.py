# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Service Container

Wires all services together. Single point of construction.
"""

from __future__ import annotations

from archetype.app.broker import CommandBroker
from archetype.app.command_service import CommandService
from archetype.app.query_service import QueryService
from archetype.app.simulation_service import SimulationService
from archetype.app.storage_service import StorageService
from archetype.app.world_service import WorldService


class ServiceContainer:
    """
    Wires services together with correct dependency ordering.

    Each service owns its internal composition.
    The container only handles service-to-service wiring.

    Usage:
        container = ServiceContainer()
        world = await container.world_service.create_world(config, storage_config)
        await container.simulation_service.step(world.world_id, run_config)
    """

    def __init__(self):
        # Infrastructure
        self.broker = CommandBroker()

        # Services
        self.storage_service = StorageService()
        self.world_service = WorldService(self.storage_service)
        self.command_service = CommandService(self.broker, self.world_service)
        self.simulation_service = SimulationService(self.world_service, self.command_service)
        self.query_service = QueryService(self.world_service, broker=self.broker)

    async def shutdown(self) -> None:
        """Gracefully shut down all services."""
        await self.broker.clear()
        await self.world_service.shutdown()
