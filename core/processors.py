from .base import BaseProcessor
from typing import Any, TYPE_CHECKING
from dependency_injector.wiring import inject, Provide
from dependency_injector import containers, providers

# Import interfaces
from .interfaces import ProcessorInterface, WorldInterface

# Import the CONTAINER INTERFACE
if TYPE_CHECKING:
    from .container_interface import CoreContainerInterface

class Processor(BaseProcessor, ProcessorInterface):
    """
    A processor that can be prioritized.
    """
    world: WorldInterface

    @inject
    def __init__(self):
        # Inject using INTERFACE container
        self.world: WorldInterface = Provide[CoreContainerInterface.world]
        
    def process(self, dt: float) -> None:
        """
        The core logic of the processor.

        Args:
            querier: Interface to read committed component state.
            updater: Interface to queue component updates.
            dt: Time delta for the current step.
            *args, **kwargs: Additional arguments passed from the world's process cycle.
        """
        raise NotImplementedError