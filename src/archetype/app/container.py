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
Service Container: Dependency Injection for Application Services
================================================================

Wires together all application services with proper dependency management.
This is the single entry point for accessing services.
"""

from archetype.app.broker import CommandBroker
from archetype.app.command_service import CommandService
from archetype.app.orchestrator import WorldOrchestrator
from archetype.app.simulation_service import SimulationService
from archetype.app.world_service import WorldService
from archetype.core.config import CacheConfig, StorageConfig


class ServiceContainer:
    """
    Dependency injection container for all application services.

    Manages the lifecycle of services and ensures proper dependency wiring.
    Services are lazily initialized on first access.

    Usage:
        container = ServiceContainer(storage_config, cache_config)

        # Access services
        sim = container.simulation_service
        cmd = container.command_service

        # Direct access to core components
        broker = container.broker
        orchestrator = container.orchestrator

        # Cleanup
        await container.shutdown()
    """

    def __init__(
        self,
        storage_config: StorageConfig,
        cache_config: CacheConfig | None = None,
    ):
        self.storage_config = storage_config
        self.cache_config = cache_config or CacheConfig()

        # Initialize core dependencies (singletons)
        self._orchestrator = WorldOrchestrator()
        self._broker = CommandBroker()

        # Services will be lazily initialized
        self._world_service: WorldService | None = None
        self._command_service: CommandService | None = None
        self._simulation_service: SimulationService | None = None

    @property
    def orchestrator(self) -> WorldOrchestrator:
        """Direct access to the orchestrator for advanced use cases."""
        return self._orchestrator

    @property
    def broker(self) -> CommandBroker:
        """Direct access to the command broker (universal simulation interface)."""
        return self._broker

    @property
    def world_service(self) -> WorldService:
        """Lazy initialization of world service."""
        if self._world_service is None:
            self._world_service = WorldService(orchestrator=self._orchestrator)
        return self._world_service

    @property
    def command_service(self) -> CommandService:
        """Lazy initialization of command service."""
        if self._command_service is None:
            self._command_service = CommandService(
                broker=self._broker,
                orchestrator=self._orchestrator,
            )
        return self._command_service

    @property
    def simulation_service(self) -> SimulationService:
        """Lazy initialization of simulation service."""
        if self._simulation_service is None:
            self._simulation_service = SimulationService(
                orchestrator=self._orchestrator,
                world_service=self.world_service,
                storage_config=self.storage_config,
                cache_config=self.cache_config,
            )
        return self._simulation_service

    async def shutdown(self):
        """Gracefully shutdown all services and resources."""
        await self._orchestrator.shutdown()
