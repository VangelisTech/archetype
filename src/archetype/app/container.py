from app.simulation_service import SimulationService
from app.world_service import WorldService
from app.command_service import CommandService

class ServiceContainer:
    def __init__(self, config: ArchetypeConfig):
        self.config = config
        self._orchestrator = None
        self._broker = None

    @property
    def simulation_service(self) -> SimulationService:
        return SimulationService(
            br
        )