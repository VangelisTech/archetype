from typing import List, Dict, Optional, Type
from uuid_utils import UUID

from archetype.core.orchestrator import WorldOrchestrator
from archetype.app.auth.models import  ActorCtx
from archetype.app.models import Command
from archetype.core import Component
from archetype.app.broker import CommandBroker
from archetype.app.security.uc_wrappers import ActorCtxProvider
from archetype.integrations.unity.uc_client import UnityCatalogREST
from archetype.integrations.unity.uc_permissions import UCPermissions, OP_TO_PRIVILEGES
from archetype.core.config import StorageConfig

class CommandService:
    """
    Service for managing the command queue and broker.
    
    This service is responsible for:
    - Enqueueing commands from external sources (API, UI, etc.)
    - Managing command priorities and sequencing
    - Providing command history and audit trails
    
    Note: Actual command execution is handled by WorldService.
    """
    
    def __init__(self, broker: CommandBroker, orchestrator: WorldOrchestrator):
        self.broker = broker
        self.orchestrator = orchestrator
        self._storage_config: StorageConfig | None = None
        self._actor_provider: ActorCtxProvider | None = None

    def bind_context(self, storage_config: StorageConfig, actor_provider: ActorCtxProvider):
        self._storage_config = storage_config
        self._actor_provider = actor_provider
    
    # --- Command Queue Management ---
    
    async def enqueue_command(self, 
                              world_id: UUID,
                              op: str,
                              payload: Dict,
                              actor_ctx: ActorCtx,
                              priority: int = 0) -> UUID:
        """
        Enqueue a command for later processing.
        
        Returns:
            The command ID for tracking
        """
        # Validate world exists
        world = self.orchestrator.get_world(world_id)
        
        cmd = Command(
            actor_id=actor_ctx.id,
            tick=getattr(world, 'tick', 0),
            op=op,
            payload=payload,
            priority=priority
        )
        
        # UC permission check at enqueue time for write operations
        uc = getattr(self._storage_config, "uc_config", None)
        if self._storage_config and uc and getattr(uc, "enabled", False):
            required = OP_TO_PRIVILEGES.get(op)
            if required:
                actor = self._actor_provider() if self._actor_provider else actor_ctx
                principal = getattr(actor, "principal", None)
                token = getattr(actor, "uc_token", None) or ((getattr(uc, "token", "") or ""))
                uc_client = UnityCatalogREST(endpoint=uc.endpoint, token=token)
                perms = UCPermissions(uc_client)
                # Heuristic: infer table from op payload if components present
                if "components" in payload and payload["components"]:
                    from archetype.core import Archetype, Component
                    comps = [Component.from_dict(c) for c in payload["components"]]
                    sig = tuple(sorted((type(c) for c in comps), key=lambda t: t.__name__))
                    table_name = Archetype.get_name(sig)
                    full_name = f"{uc.catalog}.{uc.schema}.{table_name}"
                    # Strict behavior by default: if UC check raises, bubble up to caller
                    perms.ensure_allowed("table", full_name, required, principal=principal)

        await self.broker.enqueue(world_id, cmd, actor_ctx)
        return cmd.id
    
    async def dequeue_commands(self, 
                               world_id: UUID,
                               max_commands: Optional[int] = None) -> List[Command]:
        """
        Dequeue pending commands for a world.
        
        Args:
            world_id: World to get commands for
            max_commands: Maximum number of commands to dequeue
        
        Returns:
            List of commands ready for processing
        """
        return await self.broker.dequeue_batch(world_id, max_commands)
    
    # --- High-Level Command Creation Helpers ---
    # These create commands but don't execute them
    
    async def create_entity_command(self,
                                    world_id: UUID,
                                    components: List[Component],
                                    actor_ctx: ActorCtx) -> UUID:
        """Create a command to spawn an entity."""
        payload = {
            "components": [{"type": c.__class__.__name__, **c.model_dump()} for c in components]
        }
        return await self.enqueue_command(
            world_id, "create_entity", payload, actor_ctx
        )
    
    async def remove_entity_command(self,
                                    world_id: UUID,
                                    entity_id: int,
                                    actor_ctx: ActorCtx) -> UUID:
        """Create a command to remove an entity."""
        payload = {"entity_id": entity_id}
        return await self.enqueue_command(
            world_id, "delete_entity", payload, actor_ctx
        )
    
    async def add_components_command(self,
                                     world_id: UUID,
                                     entity_id: int,
                                     components: List[Component],
                                     actor_ctx: ActorCtx) -> UUID:
        """Create a command to add components to an entity."""
        payload = {
            "entity_id": entity_id,
            "components": [{"type": c.__class__.__name__, **c.model_dump()} for c in components]
        }
        return await self.enqueue_command(
            world_id, "add_component", payload, actor_ctx
        )
    
    async def remove_components_command(self,
                                        world_id: UUID,
                                        entity_id: int,
                                        component_types: List[Type[Component]],
                                        actor_ctx: ActorCtx) -> UUID:
        """Create a command to remove components from an entity."""
        payload = {
            "entity_id": entity_id,
            "component_types": [t.__name__ for t in component_types]
        }
        return await self.enqueue_command(
            world_id, "remove_component", payload, actor_ctx
        )
    
    # --- Command History and Audit ---
    
    async def get_command_history(self, 
                                  world_id: UUID,
                                  limit: int = 100) -> List[Command]:
        """Get historical commands for a world."""
        # This would query the broker's persistent storage
        return await self.broker.get_history(world_id, limit)
    
    async def get_pending_count(self, world_id: UUID) -> int:
        """Get count of pending commands for a world."""
        return await self.broker.get_pending_count(world_id)