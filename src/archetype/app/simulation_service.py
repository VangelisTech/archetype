from archetype.core.command import Command
from archetype.core.aio.async_broker import AsyncCommandBroker
from archetype.core.orchestrator import WorldOrchestrator
from archetype.core.interfaces import Component, 




class SimulationService: 
    def __init__(self, *args, **kwargs):
        # 
        self.broker = AsyncCommandBroker(*args, **kwargs) 
        self.orchestrator = WorldOrchestrator(uri)

    async def create_entity(self, world_id: str, *components: Component):
        """Enqueues a command to create a new entity with the given components."""
        cmd = Command(
            op="create_entity",
            payload={"components": [c.model_dump() for c in components]},
            tick= self.tick
        )
        await self.broker.enqueue(cmd, self._get_default_actor_ctx())

    async def delete_entity(self, entity_id: int):
        """Enqueues a command to delete an entity."""
        cmd = Command(
            op="delete_entity",
            payload={"entity_id": entity_id},
            tick= self.tick
        )
        await self.broker.enqueue(cmd, self._get_default_actor_ctx())

    async def add_component(self, entity_id: int, *components: Component):
        """Enqueues a command to add components to an entity."""
        cmd = Command(
            op="add_component",
            payload={"entity_id": entity_id, "components": [c.model_dump() for c in components]},
            tick= self.tick
        )
        await self.broker.enqueue(cmd, self._get_default_actor_ctx())

    async def remove_component(self, entity_id: int, *component_types: Type[Component], ):
        """Enqueues a command to remove components from an entity."""
        cmd = Command(
            op="remove_component",
            payload={"entity_id": entity_id, "component_types": [t.__name__ for t in component_types]},
            tick= self.tick
        )
        await self.broker.enqueue(cmd, self._get_default_actor_ctx())

    async def add_processor(self, processor: iAsyncProcessor):
        """Enqueues a command to add a processor to the system."""
        cmd = Command(
            op="add_processor",
            payload={"processor": processor},
            tick= self.tick
        )
        await self.broker.enqueue(cmd, self._get_default_actor_ctx())

    async def remove_processor(self, processor: iAsyncProcessor):
        """Enqueues a command to remove a processor from the system."""
        cmd = Command(
            op="remove_processor",
            payload={"processor": processor},
            tick= self.tick
        )
        await self.broker.enqueue(cmd, self._get_default_actor_ctx())

    def _get_default_actor_ctx(self):
        # In a real scenario, this would come from an auth system.
        # For now, we create a default context.
        from archetype.core.auth import ActorCtx
        import uuid_utils as uuid
        return ActorCtx(id=uuid.uuid7(), roles={"admin"})
    
    def step_world(self, world_id: str, *args, **kwargs):
        """Steps the world forward by the given delta time."""
        cmd = Command(
            op="step_world",
            payload={"dt": dt},
            tick= self.tick
        )
        cmds = self.broker.dequeue_due(world_id)
        self.orchestrator._words[world_id].apply_commands(cmds)
        self.orchestrator.step_world(world_id, dt)

    
