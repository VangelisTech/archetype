from archetype.core.command import Command
from archetype.src.archetype.core.aio.async_broker import AsyncCommandBroker
from archetype.src.archetype.core.orchestrator import WorldOrchestrator
from archetype.core.interfaces import Component, 




class SimulationService: 
    def __init__(self, broker: AsyncCommandBroker, orchestrator: WorldOrchestrator):
        # 
        self.broker = broker
        self.orchestrator = orchestrator

    def create_simulation(self, config: SimConfig) -> str:
        "High Level Simulation Factory"
        pass

    def run_simulation(self, world_id: str, steps: int):
        pass




    
