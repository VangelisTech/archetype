"""
Dependency injection for FastAPI routes.
"""
from typing import Optional
from fastapi import Request
from archetype.app.broker import CommandBroker
from archetype.app.auth.models import ActorCtx
from archetype.app.container import ServiceContainer
from archetype.core.config import StorageConfig, CacheConfig
from archetype.core.orchestrator import WorldOrchestrator
import uuid_utils as uuid


class DependencyContainer:
    """
    Singleton container for application dependencies.
    """
    _instance: Optional['DependencyContainer'] = None
    
    def __init__(self, 
                 storage_uri: str = ".archetype_data",
                 namespace: str = "archetype"):
        self.storage_config = StorageConfig(uri=storage_uri, namespace=namespace)
        self.cache_config = CacheConfig()
        self.actor_ctx = self._get_default_actor_ctx()
        
        # Initialize the service container
        self.container = ServiceContainer(
            actor_ctx=self.actor_ctx,
            storage_config=self.storage_config,
            cache_config=self.cache_config
        )
        
        # Quick access to commonly used services
        self.broker = self.container.broker
        self.orchestrator = self.container.orchestrator
    
    @classmethod
    def get_instance(cls) -> 'DependencyContainer':
        """Get or create the singleton instance."""
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance
    
    def _get_default_actor_ctx(self) -> ActorCtx:
        """Create a default actor context for development."""
        return ActorCtx(id=uuid.uuid7(), roles={"admin"})


# FastAPI dependency functions
def get_broker() -> CommandBroker:
    """Get the command broker instance."""
    return DependencyContainer.get_instance().broker


def get_orchestrator() -> WorldOrchestrator:
    """Get the world orchestrator instance."""
    return DependencyContainer.get_instance().orchestrator


def get_actor_ctx(request: Request) -> ActorCtx:
    """Get the current actor context from request state (set by auth middleware)."""
    ctx = getattr(request.state, "actor_ctx", None)
    if ctx is None:
        # Fallback to container default in non-strict mode
        return DependencyContainer.get_instance().actor_ctx
    return ctx


def get_container() -> ServiceContainer:
    """Get the service container."""
    return DependencyContainer.get_instance().container