from typing import List, Dict, Optional, Type
from uuid_utils import UUID

from archetype.core.orchestrator import WorldOrchestrator
from archetype.app.broker import AsyncCommandBroker
from archetype.app.auth.models import Command, ActorCtx
from archetype.core import Component
from archetype.core.aio import AsyncProcessor


class CommandService:
    """
    Service for managing the command queue and broker.
    
    This service is responsible for:
    - Enqueueing commands from external sources (API, UI, etc.)
    - Managing command priorities and sequencing
    - Providing command history and audit trails
    
    Note: Actual command execution is handled by WorldService.
    """
    
    def __init__(self, broker: AsyncCommandBroker, orchestrator: WorldOrchestrator):
        self.broker = broker
        self.orchestrator = orchestrator
    
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
            "components": [c.model_dump() for c in components]
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
            world_id, "remove_entity", payload, actor_ctx
        )
    
    async def add_components_command(self,
                                     world_id: UUID,
                                     entity_id: int,
                                     components: List[Component],
                                     actor_ctx: ActorCtx) -> UUID:
        """Create a command to add components to an entity."""
        payload = {
            "entity_id": entity_id,
            "components": [c.model_dump() for c in components]
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