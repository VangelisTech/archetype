# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
RayAgentWorld: Distributed agent world manager
==============================================

Manages Ray actor agents and coordinates with Archetype core for:
- State tracking via ECS DataFrames
- Vectorized service batching
- Agent lifecycle management
"""

from __future__ import annotations

import asyncio
from collections import defaultdict
from pathlib import Path
from typing import Any

try:
    import ray
except ImportError:
    ray = None

from archetype.app.orchestrator import WorldOrchestrator
from archetype.core.aio import AsyncWorld
from archetype.core.component import Component
from archetype.core.config import RunConfig, StorageBackend, StorageConfig, WorldConfig
from archetype.ray.services import ServiceBatcher, VectorizedService


class RayAgentWorld:
    """
    World manager for Ray actor-based agents.
    
    Coordinates between:
    - Ray actors (agents running independently)
    - Archetype core (vectorized services + state tracking)
    - Service batching (automatic request batching)
    
    Example:
        async with RayAgentWorld("debate_sim") as world:
            # Register vectorized services
            world.register_service("inference", InferenceService())
            
            # Create agents as Ray actors
            agents = [
                ray.remote(DebateAgent).remote(f"agent_{i}", world.get_ref())
                for i in range(4)
            ]
            
            # Initialize agents
            await asyncio.gather(*[a.initialize.remote() for a in agents])
            
            # Run simulation
            for tick in range(10):
                results = await world.step(tick, agents)
                
            # Query state from ECS
            states = world.query_agent_states()
    """
    
    def __init__(
        self,
        name: str,
        storage: str | Path = "./archetype_data",
        batch_window_ms: int = 50,
    ):
        """
        Initialize Ray agent world.
        
        Args:
            name: World name
            storage: Storage path for ECS state
            batch_window_ms: Time window for batching service requests
        """
        self.name = name
        self._storage_path = Path(storage)
        self._batch_window_ms = batch_window_ms
        
        # Archetype core components
        self._orchestrator: WorldOrchestrator | None = None
        self._ecs_world: AsyncWorld | None = None
        
        # Ray actor management
        self._agent_refs: dict[str, Any] = {}  # agent_id -> actor ref
        self._agent_states: dict[str, dict[str, Any]] = {}  # agent_id -> state
        
        # Service management
        self._services: dict[str, VectorizedService] = {}
        self._batchers: dict[str, ServiceBatcher] = {}
        
        # Request queue for batching
        self._pending_requests: dict[str, list[tuple[str, dict, asyncio.Future]]] = defaultdict(list)
        self._batch_task: asyncio.Task | None = None
        
    async def __aenter__(self) -> RayAgentWorld:
        """Async context manager entry."""
        # Initialize Archetype core
        self._orchestrator = WorldOrchestrator()
        
        storage_config = StorageConfig(
            uri=self._storage_path,
            namespace=f"{self.name}_ray_agents",
            backend=StorageBackend.ICEBERG,
        )
        
        self._ecs_world = await self._orchestrator.create_world(
            config=WorldConfig(name=self.name),
            storage_config=storage_config,
        )
        
        # Start batch processing
        self._batch_task = asyncio.create_task(self._batch_processor())
        
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Async context manager exit."""
        # Stop batch processing
        if self._batch_task:
            self._batch_task.cancel()
            try:
                await self._batch_task
            except asyncio.CancelledError:
                pass
        
        # Shutdown orchestrator
        if self._orchestrator:
            await self._orchestrator.shutdown()
    
    def get_ref(self) -> Any:
        """
        Get a reference to this world for passing to agents.
        If running in Ray, returns actor ref. Otherwise returns self.
        """
        if ray is not None and hasattr(self, "__ray_actor__"):
            return ray.get_runtime_context().current_actor
        return self
    
    def register_service(self, name: str, service: VectorizedService) -> None:
        """
        Register a vectorized service.
        
        Args:
            name: Service name (e.g., "inference", "embedding")
            service: Service implementation
        """
        self._services[name] = service
        self._batchers[name] = ServiceBatcher(service, batch_window_ms=self._batch_window_ms)
    
    async def request_service(
        self,
        service_name: str,
        agent_id: str,
        request_data: dict[str, Any],
    ) -> Any:
        """
        Request a service on behalf of an agent.
        Automatically batched with other concurrent requests.
        
        Args:
            service_name: Name of the service
            agent_id: Requesting agent ID
            request_data: Request payload
            
        Returns:
            Service response
        """
        if service_name not in self._batchers:
            raise ValueError(f"Unknown service: {service_name}")
        
        batcher = self._batchers[service_name]
        return await batcher.request(agent_id, request_data)
    
    async def update_agent_state(self, agent_id: str, state_data: dict[str, Any]) -> None:
        """
        Update agent state in ECS for tracking.
        Called by agents via sync_state().
        
        Args:
            agent_id: Agent ID
            state_data: State data including components
        """
        self._agent_states[agent_id] = state_data
        
        # Convert state to ECS components and store
        # This enables querying agent states via DataFrames
        if self._ecs_world:
            # Create/update entity in ECS
            # Implementation depends on how we map agent state to components
            pass
    
    async def step(self, tick: int, agents: list[Any], context: dict = None) -> list[Any]:
        """
        Execute one simulation step.
        
        Args:
            tick: Current tick number
            agents: List of agent Ray actor refs
            context: Optional context to pass to agents
            
        Returns:
            List of agent action results
        """
        # Build context for agents
        step_context = context or {}
        step_context["tick"] = tick
        step_context["agent_count"] = len(agents)
        
        # Call act() on all agents in parallel
        if ray is not None:
            results = await asyncio.gather(
                *[agent.act.remote(tick, step_context) for agent in agents],
                return_exceptions=True,
            )
        else:
            results = await asyncio.gather(
                *[agent.act(tick, step_context) for agent in agents],
                return_exceptions=True,
            )
        
        # Handle exceptions
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                print(f"Agent {i} failed at tick {tick}: {result}")
        
        return results
    
    async def broadcast_message(
        self,
        sender_id: str,
        content: str,
        metadata: dict = None,
        exclude: list[str] = None,
    ) -> None:
        """
        Broadcast a message from one agent to all others.
        
        Args:
            sender_id: Sending agent ID
            content: Message content
            metadata: Optional metadata
            exclude: Agent IDs to exclude from broadcast
        """
        exclude_set = set(exclude or [])
        
        # Send to all agents except excluded
        tasks = []
        for agent_id, agent_ref in self._agent_refs.items():
            if agent_id == sender_id or agent_id in exclude_set:
                continue
            
            if ray is not None:
                task = agent_ref.receive_message.remote(sender_id, content, metadata)
            else:
                task = agent_ref.receive_message(sender_id, content, metadata)
            tasks.append(task)
        
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
    
    def query_agent_states(self, filter_fn=None) -> list[dict[str, Any]]:
        """
        Query agent states from ECS.
        
        Args:
            filter_fn: Optional filter function
            
        Returns:
            List of agent state dicts
        """
        states = list(self._agent_states.values())
        if filter_fn:
            states = [s for s in states if filter_fn(s)]
        return states
    
    async def _batch_processor(self) -> None:
        """
        Background task that processes batched service requests.
        Runs continuously while world is active.
        """
        while True:
            try:
                # Process batches for all services
                for service_name, batcher in self._batchers.items():
                    await batcher.process_batch()
                
                # Small sleep to prevent busy-waiting
                await asyncio.sleep(0.01)
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"Error in batch processor: {e}")
                await asyncio.sleep(0.1)
    
    def get_metrics(self) -> dict[str, Any]:
        """
        Get world metrics.
        
        Returns:
            Dict of metrics (agent count, request counts, etc.)
        """
        return {
            "agent_count": len(self._agent_refs),
            "active_states": len(self._agent_states),
            "services": list(self._services.keys()),
            "batch_metrics": {
                name: batcher.get_metrics()
                for name, batcher in self._batchers.items()
            },
        }
