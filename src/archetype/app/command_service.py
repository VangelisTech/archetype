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
Command Service: API Surface for Command Broker
================================================

Service layer for managing commands through the broker.
External APIs (FastAPI, MCP) call this service to submit commands.

Auth is handled at the API layer before reaching this service.
"""

from uuid_utils import UUID

from archetype.app.broker import CommandBroker
from archetype.app.models import Command, CommandType
from archetype.app.orchestrator import WorldOrchestrator
from archetype.core import Component


class CommandService:
    """
    Service for managing the command queue and broker.

    Responsibilities:
    - Enqueueing commands from external sources (API, UI, agents)
    - Managing command priorities and sequencing
    - Providing command history and audit trails
    - High-level helpers for common operations

    Note: Actual command execution is handled by WorldService.
    """

    def __init__(self, broker: CommandBroker, orchestrator: WorldOrchestrator):
        self.broker = broker
        self.orchestrator = orchestrator

    # --- Command Queue Management ---

    async def enqueue_command(
        self,
        world_id: UUID | str,
        op: str,
        payload: dict,
        priority: int = 0,
        actor_id: UUID | None = None,
    ) -> UUID:
        """
        Enqueue a command for later processing.

        Args:
            world_id: Target world for the command
            op: Operation type (e.g., "create_entity", "run_rollout")
            payload: Command payload data
            priority: Command priority (lower = higher priority)
            actor_id: Optional actor ID for audit

        Returns:
            The command ID for tracking
        """
        # Validate world exists
        world = (
            self.orchestrator.get_world(world_id)
            if isinstance(world_id, UUID)
            else self.orchestrator.get_world_by_name(world_id)
        )

        cmd = Command(
            actor_id=actor_id,
            tick=getattr(world, "tick", 0),
            op=op,
            payload=payload,
            priority=priority,
        )

        await self.broker.enqueue(world_id, cmd)
        return cmd.id

    async def dequeue_commands(
        self,
        world_id: UUID | str,
        max_commands: int | None = None,
    ) -> list[Command]:
        """
        Dequeue pending commands for a world.

        Args:
            world_id: World to get commands for
            max_commands: Maximum number of commands to dequeue

        Returns:
            List of commands ready for processing
        """
        return await self.broker.dequeue_batch(world_id, max_commands)

    # --- Entity Command Helpers ---

    async def create_entity_command(
        self,
        world_id: UUID | str,
        components: list[Component],
        priority: int = 0,
        actor_id: UUID | None = None,
    ) -> UUID:
        """Create a command to spawn an entity."""
        payload = {
            "components": [{"type": c.__class__.__name__, **c.model_dump()} for c in components]
        }
        return await self.enqueue_command(world_id, "create_entity", payload, priority, actor_id)

    async def remove_entity_command(
        self,
        world_id: UUID | str,
        entity_id: int,
        priority: int = 0,
        actor_id: UUID | None = None,
    ) -> UUID:
        """Create a command to remove an entity."""
        payload = {"entity_id": entity_id}
        return await self.enqueue_command(world_id, "delete_entity", payload, priority, actor_id)

    async def add_components_command(
        self,
        world_id: UUID | str,
        entity_id: int,
        components: list[Component],
        priority: int = 0,
        actor_id: UUID | None = None,
    ) -> UUID:
        """Create a command to add components to an entity."""
        payload = {
            "entity_id": entity_id,
            "components": [{"type": c.__class__.__name__, **c.model_dump()} for c in components],
        }
        return await self.enqueue_command(world_id, "add_component", payload, priority, actor_id)

    async def remove_components_command(
        self,
        world_id: UUID | str,
        entity_id: int,
        component_types: list[type[Component]],
        priority: int = 0,
        actor_id: UUID | None = None,
    ) -> UUID:
        """Create a command to remove components from an entity."""
        payload = {"entity_id": entity_id, "component_types": [t.__name__ for t in component_types]}
        return await self.enqueue_command(world_id, "remove_component", payload, priority, actor_id)

    # --- Simulation Command Helpers (for recursive simulation) ---

    async def create_world_command(
        self,
        parent_world_id: UUID | str,
        child_name: str,
        initial_state: dict | None = None,
        priority: int = 0,
        actor_id: UUID | None = None,
    ) -> UUID:
        """
        Create a command to spawn a child simulation.

        Used by agents for mental simulation / MCTS.
        """
        payload = {
            "name": child_name,
            "initial_state": initial_state or {},
        }
        return await self.enqueue_command(
            parent_world_id, CommandType.CREATE_WORLD.value, payload, priority, actor_id
        )

    async def run_rollout_command(
        self,
        world_id: UUID | str,
        target_world: str,
        num_steps: int,
        priority: int = 0,
        actor_id: UUID | None = None,
    ) -> UUID:
        """
        Create a command to run a rollout in a (child) world.

        Used by agents for mental simulation / planning.
        """
        payload = {
            "target_world": target_world,
            "num_steps": num_steps,
        }
        return await self.enqueue_command(
            world_id, CommandType.RUN_ROLLOUT.value, payload, priority, actor_id
        )

    async def query_world_command(
        self,
        world_id: UUID | str,
        target_world: str,
        query: str,
        priority: int = 0,
        actor_id: UUID | None = None,
    ) -> UUID:
        """
        Create a command to query state from a (child) world.
        """
        payload = {
            "target_world": target_world,
            "query": query,
        }
        return await self.enqueue_command(
            world_id, CommandType.QUERY_WORLD.value, payload, priority, actor_id
        )

    # --- Command History and Audit ---

    async def get_command_history(
        self,
        world_id: UUID | str,
        limit: int = 100,
    ) -> list[Command]:
        """Get historical commands for a world."""
        return await self.broker.get_history(world_id, limit)

    async def get_pending_count(self, world_id: UUID | str) -> int:
        """Get count of pending commands for a world."""
        return await self.broker.get_pending_count(world_id)

    async def peek_commands(
        self,
        world_id: UUID | str,
        max_commands: int | None = None,
    ) -> list[Command]:
        """Peek at pending commands without removing them."""
        return await self.broker.peek(world_id, max_commands)
