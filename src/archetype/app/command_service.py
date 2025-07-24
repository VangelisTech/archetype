from archetype.core.orchestrator import WorldOrchestrator
from archetype.core.aio.async_broker import AsyncCommandBroker

class CommandService: 
    def __init__(self,broker: AsyncCommandBroker, orchestrator: WorldOrchestrator):
        self.broker = broker
        self.orchestrator = orchestrator

    def enqueue_cmd(self, cmd: Command, actor_ctx: ActorCtx):