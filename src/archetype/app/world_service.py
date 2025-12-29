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

from uuid_utils import UUID

from archetype.app.models import Command
from archetype.app.orchestrator import WorldOrchestrator
from archetype.core import Component


class WorldService:
    """
    Handles world-level operations and command processing.

    This service is responsible for:
    - Processing commands that mutate world state
    - Validating operations before applying them
    - Providing business logic for world operations
    """

    def __init__(self, orchestrator: WorldOrchestrator):
        self.orchestrator = orchestrator

    async def apply_commands(self, world_id: UUID, cmds: list[Command]) -> list[UUID]:
        """
        Processes a batch of commands for a specific world.

        Commands are separated into data mutations (entities/components) and
        system mutations (processors), then applied in order.
        """
        # Get the world we're operating on
        world = self.orchestrator.get_world(world_id)

        processed_cmd_ids = [c.id for c in cmds]

        # Separate state and behavior mutations
        data_cmds = [c for c in cmds if c.op not in {"add_processor", "remove_processor"}]
        proc_cmds = [c for c in cmds if c.op in {"add_processor", "remove_processor"}]

        # Apply data commands to populate spawn, despawn, and transmute caches
        for cmd in data_cmds:
            handler = getattr(self, f"_handle_{cmd.op}", None)
            if handler:
                await handler(world, cmd)

        # Apply processor commands directly (hot-swapping for next tick)
        for cmd in proc_cmds:
            handler = getattr(self, f"_handle_{cmd.op}", None)
            if handler:
                await handler(world, cmd)

        return processed_cmd_ids

    async def _handle_create_entity(self, world, cmd: Command) -> int:
        """Handle entity creation with validation."""
        components = [Component.from_dict(c) for c in cmd.payload["components"]]

        # Add any business validation here
        # e.g., check component limits, validate component data, etc.

        return await world.create_entity(components)

    async def _handle_delete_entity(self, world, cmd: Command):
        """Handle entity removal."""
        return await world.remove_entity(cmd.payload["entity_id"])

    async def _handle_add_component(self, world, cmd: Command):
        """Handle adding components to an existing entity."""
        entity_id = cmd.payload["entity_id"]
        components = [Component.from_dict(c) for c in cmd.payload["components"]]

        return await world.add_components(entity_id, components)

    async def _handle_remove_component(self, world, cmd: Command):
        """Handle removing components from an entity."""
        entity_id = cmd.payload["entity_id"]
        component_types = [
            Component.get_type_by_name(name) for name in cmd.payload["component_types"]
        ]

        return await world.remove_components(entity_id, component_types)

    async def _handle_add_processor(self, world, cmd: Command):
        """Handle adding a processor to the world's system."""
        # Note: This assumes the processor is already instantiated
        # In practice, you might need to deserialize or look it up
        processor = cmd.payload["processor"]
        await world.add_processor(processor)

    async def _handle_remove_processor(self, world, cmd: Command):
        """Handle removing a processor from the world's system."""
        processor_type = cmd.payload["processor"]
        await world.remove_processor(processor_type)

    # --- Additional business operations ---

    async def get_world_status(self, world_id: UUID) -> dict:
        """Get the current status of a world."""
        world = self.orchestrator.get_world(world_id)
        return {
            "world_id": world.world_id,
            "name": getattr(world, "name", None),
            "tick": getattr(world, "tick", 0),
            "entity_count": len(getattr(world, "_entity2sig", {})),
        }
