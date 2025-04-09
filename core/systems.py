# Python
from typing import Any, Dict, List, Type, Optional, Set, TYPE_CHECKING
import logging # Use standard logging
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
    # from .processors import Processor # Likely not needed here anymore

# Setup logger
logger = logging.getLogger(__name__)

class SequentialSystem(BaseSystem, SystemInterface): # Implement interface
    """
    Executes processors sequentially following the preprocess -> process pattern.
    """
    processors: Set[ProcessorInterface] # Use interface type
    results: Dict[ProcessorInterface, daft.DataFrame] # Use interface type

    @inject
    def __init__(self):
        self.processors = set() 
        self.results = {}

    def add_processor(self, processor: ProcessorInterface) -> None: # Use interface type
        """Adds a processor instance."""
        if processor in self.processors:
            logger.warning(f"SequentialSystem: Replacing existing processor: {processor.__class__.__name__}") 
        self.processors.add(processor)

    def remove_processor(self, processor: ProcessorInterface) -> None: # Use interface type
        """Removes a specific processor instance."""
        # Note: This now removes a specific instance, not all of type.
        # To remove by type, the loop logic would be needed again.
        if processor in self.processors:
            self.processors.remove(processor)
        else:
             logger.warning(f"SequentialSystem: Attempted to remove processor not found: {processor.__class__.__name__}")

    def get_processor(self, processor_type: ProcessorInterface) -> Optional[ProcessorInterface]: # Accept type
        """Gets the first found managed instance of a specific processor type."""
        for p in self.processors:
            if isinstance(p, processor_type):
                 return p
        return None

    # Update execute signature to match BaseSystem/SystemInterface if needed
    # Pass dt explicitly, return type matches interface Optional[Dict...]
    def execute(self, dt: float, **kwargs) -> Optional[Dict[ProcessorInterface, daft.DataFrame]]: 
        """
        Executes processors sequentially: preprocess -> process.
        Collects non-None results from the process step.
        Assumes dt is passed as a positional or keyword argument.
        """
        self.results = {} # Reset results for this step
        # logger.debug(f"Executing system with processors: {[p.__class__.__name__ for p in self.processors]}")
        for processor in self.processors:
            # logger.debug(f"Preprocessing for {processor.__class__.__name__}")
            # Step 1: Preprocess (returns a DataFrame, possibly empty)
            try:
                state_df = processor.preprocess()
            except Exception as e:
                logger.error(f"Error during preprocess for {processor.__class__.__name__}: {e}", exc_info=True)
                continue # Skip this processor for the step
            
            # logger.debug(f"Processing for {processor.__class__.__name__} with state_df length: {len(state_df) if state_df is not None else 'None'}")
            # Step 2: Process (always called, operates on potentially empty DataFrame)
            try:
                result_df = processor.process(dt=dt, state_df=state_df) # Pass state_df and dt
            except Exception as e:
                logger.error(f"Error during process for {processor.__class__.__name__}: {e}", exc_info=True)
                continue # Skip this processor for the step

            # Step 3: Store result if not None (process might return None intentionally)
            if result_df is None:
                # Optional: Log that processor returned None, e.g., for debugging specific logic
                logger.debug(f"Processor {processor.__class__.__name__} returned None from process.")
            else:
                # Basic check: is it actually a DataFrame?
                if isinstance(result_df, daft.DataFrame):
                     self.results[processor] = result_df
                else: # Log warning if unexpected type returned
                    logger.warning(f"Processor {processor.__class__.__name__} process method did not return a Daft DataFrame or None, got: {type(result_df)}")
        
        return self.results # Return the collected results dictionary

