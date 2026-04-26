# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Service Container

Wires all services together. Single point of construction.
"""

from __future__ import annotations

from archetype.app.audit_log import AuditLog
from archetype.app.broker import CommandBroker
from archetype.app.command_service import CommandService
from archetype.app.mutation_service import MutationService
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

        # Leaf services
        self.storage_service = StorageService()

        # Storage-backed services
        self.world_service = WorldService(self.storage_service)
        self.audit_log = AuditLog(self.storage_service)
        self.query_service = QueryService(self.storage_service, self.audit_log)

        # Services that depend on WorldService
        self.mutation_service = MutationService(self.world_service)
        self.simulation_service = SimulationService(self.world_service)

        # The gate — depends on everything
        self.command_service = CommandService(
            mutations=self.mutation_service,
            worlds=self.world_service,
            simulation=self.simulation_service,
            queries=self.query_service,
            broker=self.broker,
            audit=self.audit_log,
        )
        self.simulation_service.set_command_drain(self.command_service.drain_and_apply)

    async def shutdown(self) -> None:
        """Gracefully shut down all services."""
        await self.audit_log.shutdown()
        await self.broker.clear()
        await self.world_service.shutdown()
