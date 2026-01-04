# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
RayAgent: Actor-based agent abstraction
========================================

Each agent runs as a Ray actor with:
- Async execution
- Private state
- Access to vectorized services via Archetype core
- State synchronization with ECS DataFrames
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

try:
    import ray
except ImportError:
    ray = None

from archetype.core.component import Component


class RayAgent:
    """
    Base class for Ray actor-based agents.
    
    Each agent:
    - Runs as an independent Ray actor
    - Maintains local state
    - Can request vectorized services (inference, etc.)
    - Syncs state back to Archetype ECS for tracking
    
    Example:
        @ray.remote
        class DebateAgent(RayAgent):
            async def act(self, context):
                # Get batched inference from vectorized service
                response = await self.world.request_inference(
                    prompt=self.build_prompt(),
                    model="gpt-4o-mini"
                )
                
                # Update local state
                self.state["history"].append(response)
                
                # Sync to ECS for tracking
                await self.sync_state()
                
                return response
    """
    
    def __init__(
        self,
        agent_id: str,
        world_ref: Any = None,
        components: dict[str, Component] | None = None,
    ):
        """
        Initialize a Ray actor agent.
        
        Args:
            agent_id: Unique identifier for this agent
            world_ref: Ray actor reference to RayAgentWorld
            components: Initial component data for ECS state
        """
        self.agent_id = agent_id
        self.world_ref = world_ref
        self.components = components or {}
        self.state: dict[str, Any] = {}
        self._pending_requests: list[asyncio.Task] = []
        
    async def initialize(self) -> None:
        """
        Initialize the agent. Override to add custom initialization.
        Called after construction but before first act().
        """
        pass
    
    async def act(self, tick: int, context: dict[str, Any]) -> Any:
        """
        Main action method. Override this in subclasses.
        
        Args:
            tick: Current simulation tick
            context: Context dict from world (messages, shared state, etc.)
            
        Returns:
            Action result (can be any type)
        """
        raise NotImplementedError("Subclasses must implement act()")
    
    async def sync_state(self) -> None:
        """
        Synchronize local state back to Archetype ECS.
        This enables state tracking and querying via DataFrames.
        """
        if self.world_ref is None:
            return
            
        # Convert state to components for ECS storage
        state_data = {
            "agent_id": self.agent_id,
            "state_json": json.dumps(self.state),
            "components": {
                name: self._component_to_dict(comp)
                for name, comp in self.components.items()
            },
        }
        
        # Ray remote call to world
        if ray is not None:
            await self.world_ref.update_agent_state.remote(self.agent_id, state_data)
    
    async def request_service(
        self,
        service_name: str,
        request_data: dict[str, Any],
    ) -> Any:
        """
        Request a vectorized service from the world.
        Requests are batched automatically for efficiency.
        
        Args:
            service_name: Name of the service (e.g., "inference", "embedding")
            request_data: Request payload
            
        Returns:
            Service response
        """
        if self.world_ref is None:
            raise RuntimeError("No world reference - cannot request services")
        
        if ray is not None:
            return await self.world_ref.request_service.remote(
                service_name, self.agent_id, request_data
            )
        
        raise RuntimeError("Ray not available")
    
    def get_component(self, component_type: str) -> Component | None:
        """Get a component by type name."""
        return self.components.get(component_type)
    
    def set_component(self, component_type: str, component: Component) -> None:
        """Set or update a component."""
        self.components[component_type] = component
    
    def _component_to_dict(self, component: Component) -> dict[str, Any]:
        """Convert component to dict for serialization."""
        if hasattr(component, "__dataclass_fields__"):
            return {
                field: getattr(component, field)
                for field in component.__dataclass_fields__
            }
        return {}
    
    async def receive_message(self, sender_id: str, content: str, metadata: dict = None) -> None:
        """
        Receive a message from another agent.
        Override to handle incoming messages.
        
        Args:
            sender_id: ID of sending agent
            content: Message content
            metadata: Optional metadata
        """
        pass
    
    async def cleanup(self) -> None:
        """
        Cleanup before agent shutdown.
        Override to add custom cleanup logic.
        """
        # Cancel pending requests
        for task in self._pending_requests:
            if not task.done():
                task.cancel()
