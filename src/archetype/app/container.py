from typing import Optional

from archetype.app.simulation_service import SimulationService
from archetype.app.world_service import WorldService
from archetype.app.command_service import CommandService
from archetype.app.broker import AsyncCommandBroker
from archetype.app.auth.models import ActorCtx
from archetype.app.config import StorageConfig, CacheConfig
from archetype.core.orchestrator import WorldOrchestrator

from daft.catalog import Catalog
from daft.session import Session
from pyiceberg.catalog.sql import SqlCatalog


class ServiceContainer:
    """
    Dependency injection container for all application services.
    
    This container manages the lifecycle of services and ensures proper
    dependency wiring for the entire application.
    """
    
    def __init__(self, 
                 actor_ctx: ActorCtx,
                 storage_config: StorageConfig,
                 cache_config: Optional[CacheConfig] = None):
        self.actor_ctx = actor_ctx
        self.storage_config = storage_config
        self.cache_config = cache_config or CacheConfig()
        
        # Initialize the daft catalog
        if not storage_config.catalog:
            storage_config.catalog = Catalog.from_iceberg(
                SqlCatalog(
                    "default",
                    **{
                        "uri": f"sqlite:///{storage_config.uri}/catalog.db",
                        "warehouse": f"file://{storage_config.uri}",
                    },
                )
            )
        
        # Initialize core dependencies (singletons)
        self._orchestrator = WorldOrchestrator()
        self._broker = AsyncCommandBroker()
        
        # Services will be lazily initialized
        self._world_service: Optional[WorldService] = None
        self._command_service: Optional[CommandService] = None
        self._simulation_service: Optional[SimulationService] = None
    
    @property
    def orchestrator(self) -> WorldOrchestrator:
        """Direct access to the orchestrator for advanced use cases."""
        return self._orchestrator
    
    @property
    def broker(self) -> AsyncCommandBroker:
        """Direct access to the command broker."""
        return self._broker
    
    @property
    def world_service(self) -> WorldService:
        """Lazy initialization of world service."""
        if self._world_service is None:
            self._world_service = WorldService(
                orchestrator=self._orchestrator
            )
        return self._world_service
    
    @property
    def command_service(self) -> CommandService:
        """Lazy initialization of command service."""
        if self._command_service is None:
            self._command_service = CommandService(
                broker=self._broker,
                orchestrator=self._orchestrator
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
                cache_config=self.cache_config
            )
        return self._simulation_service
    
    async def shutdown(self):
        """Gracefully shutdown all services and resources."""
        await self._orchestrator.shutdown()
        # Add broker shutdown if it has one
        # await self._broker.shutdown()