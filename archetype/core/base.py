from typing import Any, Type, Optional, Dict, Tuple
from abc import ABC, abstractmethod
import daft
from .interfaces import Component, iQuerier

# --- Processor Base Class ---
class BaseProcessor(ABC):
    """
    Base class for systems that process entities and components.
    Follows a preprocess -> process pattern.
    """
    # Add attribute to hold the list of components
    priority: int
    components: Tuple[Type[Component], ...]

    @abstractmethod
    def preprocess(self, querier: iQuerier, step: int) -> Dict[str, daft.DataFrame]:
        """
        Fetches and prepares the initial data DataFrames for the process method, keyed by archetype hash.
        Should return an empty dictionary if no data is relevant.
        """
        raise NotImplementedError

    @abstractmethod
    def process(self, state_df: daft.DataFrame) -> Optional[daft.DataFrame]:
        """
        The core transformation logic of the processor, operating on a DataFrame.
        If preprocess returns a Dict, the System is responsible for how this method gets its DataFrame.

        Args:
            state_df: The input DataFrame prepared by the preprocess method or provided by the system.

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
    def add_processor(self, processor: BaseProcessor) -> None:
        """Adds a processor to be managed by this system."""
        raise NotImplementedError

    @abstractmethod
    def remove_processor(self, processor_type: Type[BaseProcessor]) -> None:
        """Removes all processors of a specific type."""
        raise NotImplementedError
    
    @abstractmethod
    def execute(self, *args: Any, **kwargs: Any) -> Dict[str, daft.DataFrame]:
        """
        Executes the managed processors.

        Args:
            *args, **kwargs: Additional arguments passed from the world's process cycle (e.g., dt).

        Returns:
            Dict[str, daft.DataFrame]: A dictionary mapping archetype hashes to their resulting
                                       update DataFrames.
        """
        raise NotImplementedError