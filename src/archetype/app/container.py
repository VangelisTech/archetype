# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Service Container

Wires all services together. Single point of construction.
"""

from archetype.app.broker import CommandBroker
from archetype.app.command_service import CommandService
from archetype.app.query_service import QueryService
from archetype.app.simulation_service import SimulationService
from archetype.app.storage_service import StorageService
from archetype.app.world_service import WorldService
from archetype.core.config import StorageConfig


class ServiceContainer:
    """
    Wires all services together with correct dependency ordering.

    Usage:
        container = ServiceContainer()
        world = await container.world_service.create_world(config)
        await container.simulation_service.step(world.world_id, run_config)
    """

    def __init__(self, default_storage_config: StorageConfig | None = None):
        # Infrastructure
        self.default_storage_config = default_storage_config or StorageConfig()
        self.storage_service = StorageService()
        self.broker = CommandBroker()

        # Services (order matters — dependency chain)
        self.world_service = WorldService(
            self.storage_service,
            broker=self.broker,
            default_storage_config=self.default_storage_config,
        )
        self.command_service = CommandService(self.broker, self.world_service)
        self.simulation_service = SimulationService(self.world_service, self.command_service)
        self.query_service = QueryService(self.world_service, broker=self.broker)

    async def shutdown(self) -> None:
        """Gracefully shut down all services."""
        await self.storage_service.shutdown()
