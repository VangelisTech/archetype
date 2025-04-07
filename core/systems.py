

# Python
from typing import Any, Dict, List, Type, Optional, Set, TYPE_CHECKING
from logging import Logger
from dependency_injector import containers, providers
from dependency_injector.wiring import inject, Provide


# Technologies
import daft

# Internal 
from .base import BaseSystem
# Import interfaces
from .interfaces import SystemInterface, ProcessorInterface, WorldInterface

# Import the CONTAINER INTERFACE
if TYPE_CHECKING:
    from .container_interface import CoreContainerInterface
    # Import concrete Processor for type checking if needed locally, but use interface elsewhere
    from .processors import Processor 


class SequentialSystem(BaseSystem, SystemInterface): # Implement interface
    """
    Executes processors sequentially.
    """
    processors: Set[ProcessorInterface] # Use interface type
    results: Dict[ProcessorInterface, daft.DataFrame] # Use interface type

    @inject
    def __init__(self):
        self.processors = set() 
        self.results = {}

    def add_processor(self, processor: ProcessorInterface) -> None: # Use interface type
        """Adds a processor instance."""
        
        # Check might need adjustment if comparing interface instances vs concrete types
        if processor in self.processors:
            # Consider logging based on processor.__class__.__name__ if needed
            Logger.warning(f"SequentialSystem: Replacing existing processor of type {processor.__class__.__name__}") 
        
        self.processors.add(processor)

    def remove_processor(self, processor: ProcessorInterface) -> None: # Use interface type
        """Removes all processors of a specific type."""
        if processor in self.processors:
            self.processors.remove(processor)

    def get_processor(self, processor: ProcessorInterface) -> Optional[ProcessorInterface]: # Use interface type
        """Gets the managed instance of a specific processor type."""
        # This linear search might need refinement depending on how processors are identified
        for p in self.processors:
            if isinstance(p, type(processor)): # Check instance type if passed an instance
                 return p
            # If passed a type, might need adjustment: if type(p) is processor:
        return None # Original implementation returned self.processors.get(processor) which doesn't exist on set

    def execute(self, *args, **kwargs) -> Dict[ProcessorInterface, daft.DataFrame]: # Use interface type
        # Simple sequential execution of processors in order of addition
        self.results = {}
        for processor in self.processors:
            # Assuming processor() calls the process method implicitly? Check Processor definition.
            # Or call explicitly: processor.process(*args, **kwargs)? Need ProcessorInterface definition check.
            # If Processor.__call__ is defined to call process, this is fine.
            # Let's assume Processor instances are callable and call their process method.
            
            # Capture results - Assuming process returns the DataFrame or None
            result_df = processor.process(*args, **kwargs) # Explicit call is clearer
            if result_df is not None: # Handle processors that might not return updates
                self.results[processor] = result_df
        
        return self.results

