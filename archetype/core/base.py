from typing import Any, Type, Optional, List, Dict
from abc import ABC, abstractmethod
import daft

# Note: World is defined in another module and should be imported or forward-referenced as needed.
from lancedb.pydantic import LanceModel


# -- Entity Base --- 
# Entity is just an uint64 managed in the component store 

# --- Component Base ---
class Component(LanceModel):
    pass

# --- Processor Base Class ---
class BaseProcessor(ABC):
    """
    Base class for systems that process entities and components.
    Follows a preprocess -> process pattern.
    """
    # Add attribute to hold the list of components
    _components_used: List[Type[Component]]

    @abstractmethod
    def _fetch_state(self, world: 'World', step: int) -> daft.DataFrame:
        """
        Fetches and prepares the initial data DataFrame for the process method.
        Should return an empty DataFrame with the correct schema if no data is relevant.
        """
        raise NotImplementedError

    @abstractmethod
    def process(self, state_df: daft.DataFrame) -> Optional[daft.DataFrame]:
        """
        The core transformation logic of the processor, operating on the DataFrame
        provided by preprocess.

        Args:
            state_df: The input DataFrame prepared by the preprocess method.

        Returns:
            Optional[daft.DataFrame]: DataFrame with updated data + keys (entity_id, etc.)
                                      or None if no updates should be committed.
        """
        raise NotImplementedError



class BaseSystem(ABC):
    """
    Abstract base class for orchestrating processor execution.
    Implementations can define sequential, parallel, or DAG-based execution.
    """
    @abstractmethod
    def execute(self, *args: Any, **kwargs: Any) -> Optional[Dict['BaseProcessor', daft.DataFrame]]:
        """
        Executes the managed processors.

        Args:
            *args, **kwargs: Additional arguments passed from the world's process cycle (e.g., dt).

        Returns:
            Optional[Dict[BaseProcessor, daft.DataFrame]]: A dictionary mapping processor
                                                               instances to their resulting
                                                               update DataFrames, or None/empty.
        """
        raise NotImplementedError

    @abstractmethod
    def add_processor(self, processor: BaseProcessor) -> None:
        """Adds a processor to be managed by this system."""
        raise NotImplementedError

    @abstractmethod
    def remove_processor(self, processor_type: Type[BaseProcessor]) -> None:
        """Removes all processors of a specific type."""
        raise NotImplementedError

    @abstractmethod
    def get_processor(self, processor_type: Type[BaseProcessor]) -> Optional[BaseProcessor]:
         """Gets the first found instance of a specific processor type."""
         return None