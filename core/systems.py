# Python
from typing import Any, Dict, List, Type, Optional, Set, TYPE_CHECKING, Tuple
import logging # Use standard logging
from dependency_injector import containers, providers
from dependency_injector.wiring import inject, Provide


# Technologies
import daft
from daft import col # Import col

# Internal 
from .base import BaseSystem, Component
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
    processors: Dict[ProcessorInterface, ProcessorInterface] # Use interface type
    results: Dict[ProcessorInterface, daft.DataFrame] # Use interface type

    @inject
    def __init__(self):
        self.processors = {} 
        self.results = {}

    def add_processor(self, processor: ProcessorInterface) -> None: # Use interface type
        """Adds a processor instance."""
        if processor in self.processors:
            logger.warning(f"SequentialSystem: Replacing existing processor: {processor.__class__.__name__}") 
        self.processors[processor] = processor

    def remove_processor(self, processor: ProcessorInterface) -> None: # Use interface type
        """Removes a specific processor instance."""
        # Note: This now removes a specific instance, not all of type.
        # To remove by type, the loop logic would be needed again.
        if processor in self.processors:
            self.processors.pop(processor)
        else:
             logger.warning(f"SequentialSystem: Attempted to remove processor not found: {processor.__class__.__name__}")

    def get_processor(self, processor_type: ProcessorInterface) -> Optional[ProcessorInterface]: # Accept type
        """Gets the first found managed instance of a specific processor type."""
        for p in self.processors:
            if isinstance(p, processor_type):
                 return p
        return None

    def _merge_processor_results(self, processor_results: Dict[ProcessorInterface, daft.DataFrame]) -> Optional[daft.DataFrame]:
        """Merges results from multiple processors into a single DataFrame."""
        merged_df: Optional[daft.DataFrame] = None
        processor_order: List[ProcessorInterface] = list(processor_results.keys()) 

        if not processor_results:
            return None

        logger.debug(f"Merging results from {len(processor_order)} processors.")
        for processor in processor_order:
            df = processor_results[processor]

            if df is None:
                logger.warning(f"Unexpected None result from {processor.__class__.__name__} passed to merge.")
                continue

            if "entity_id" not in df.column_names:
                 logger.warning(f"Processor {processor.__class__.__name__} result missing 'entity_id'. Skipping merge for this result.")
                 continue

            # Perform the merge/join
            if merged_df is None:
                merged_df = df
                logger.debug(f"Initialized merged_df with result from {processor.__class__.__name__}")
            else:
                logger.debug(f"Merging result from {processor.__class__.__name__} into merged_df")
                merged_df = merged_df.join(
                    df,
                    on="entity_id",
                    how="outer",
                )
                # TODO: Implement robust coalescing logic here if needed.
                logger.debug(f"Merged_df columns after join with {processor.__class__.__name__}: {merged_df.column_names}")

        logger.debug(f"Final merged_df columns: {merged_df.column_names if merged_df else 'None'}")
        return merged_df

    def _process_single_processor(self, processor: ProcessorInterface, dt: float) -> Optional[daft.DataFrame]:
        """Handles the preprocess, process, and result validation for a single processor."""
        processor_name = processor.__class__.__name__
        try:
            logger.debug(f"Preprocessing for {processor_name}")
            state_df = processor.preprocess()
        except Exception as e:
            logger.error(f"Error during preprocess for {processor_name}: {e}", exc_info=True)
            return None  # Indicate failure

        try:
            logger.debug(f"Processing for {processor_name} with state_df length: {len(state_df) if state_df is not None else 'None'}")
            result_df = processor.process(dt=dt, state_df=state_df)
        except Exception as e:
            logger.error(f"Error during process for {processor_name}: {e}", exc_info=True)
            return None # Indicate failure

        if result_df is None:
            logger.debug(f"Processor {processor_name} returned None from process.")
            return None # Intentionally returned None
        elif isinstance(result_df, daft.DataFrame):
            return result_df # Successful processing, return DataFrame
        else:
            # Raise error for incorrect type immediately
            raise ValueError(f"Processor {processor_name} process method did not return a Daft DataFrame or None, got: {type(result_df)}")

    def execute(self, dt: float, **kwargs) -> Optional[daft.DataFrame]:
        """
        Executes processors sequentially, merges results, and returns the merged state.
        Assumes dt is passed as a positional or keyword argument.
        """
        processor_results: Dict[ProcessorInterface, daft.DataFrame] = {}

        for processor in self.processors:
            result_df = self._process_single_processor(processor, dt)
            if result_df is not None:
                processor_results[processor] = result_df

        # Merge the collected results
        merged_df = self._merge_processor_results(processor_results)

        return merged_df


