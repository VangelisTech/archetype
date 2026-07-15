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

"""
Service Container

Wires all services together. Single point of construction.
"""

from __future__ import annotations

from archetype.app.audit_log import AuditLog
from archetype.app.auth import reset_tick_counters
from archetype.app.autoresearch_service import AutoResearchService
from archetype.app.broker import CommandBroker
from archetype.app.command_service import CommandService
from archetype.app.eval_service import EvalService
from archetype.app.ingestion_service import IngestionService
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
        self.eval_service = EvalService(self.query_service)

        # Services that depend on WorldService
        self.mutation_service = MutationService(self.world_service)
        self.simulation_service = SimulationService(self.world_service)

        # AutoResearch — depends on WorldService + SimulationService
        self.autoresearch_service = AutoResearchService(self.world_service, self.simulation_service)

        # Ingestion — durable external facts (issue #274)
        self.ingestion_service = IngestionService(self.storage_service, self.world_service)

        # The gate — depends on everything
        self.command_service = CommandService(
            mutations=self.mutation_service,
            worlds=self.world_service,
            simulation=self.simulation_service,
            queries=self.query_service,
            broker=self.broker,
            audit=self.audit_log,
            ingestion=self.ingestion_service,
            autoresearch=self.autoresearch_service,
        )
        self.simulation_service.set_command_drain(self.command_service.drain_and_apply)
        # Per-tick RBAC quota resets at each tick boundary (bug B1).
        self.simulation_service.set_quota_reset(reset_tick_counters)

    async def shutdown(self) -> None:
        """Gracefully shut down all services."""
        await self.audit_log.shutdown()
        await self.broker.clear()
        await self.world_service.shutdown()
