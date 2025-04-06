from .base import Processor
from typing import Any, TYPE_CHECKING
import daft

# Conditionally import World for type checking only
if TYPE_CHECKING:
    from .world import World 

class TimedProcessor(Processor):
    """
    A processor that can be prioritized.
    """
    def __init__(self):
        self.world: World = None # Initialize world to None
        
    def process(self, dt: float, *inputs: Any) -> None:
        """
        The core logic of the processor.

        Args:
            querier: Interface to read committed component state.
            updater: Interface to queue component updates.
            dt: Time delta for the current step.
            *args, **kwargs: Additional arguments passed from the world's process cycle.
        """
        raise NotImplementedError
    