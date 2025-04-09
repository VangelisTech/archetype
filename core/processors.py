from .base import BaseProcessor, Component
from typing import Any, TYPE_CHECKING, List, Type, Optional
from abc import abstractmethod
from dependency_injector.wiring import inject, Provide
from dependency_injector import containers, providers
import daft
from daft import DataFrame

# Import interfaces
from .interfaces import ProcessorInterface, WorldInterface

# Import the CONTAINER INTERFACE
if TYPE_CHECKING:
    from .container_interface import CoreContainerInterface

class Processor(BaseProcessor, ProcessorInterface):
    """
    A concrete base processor implementing the preprocess/process pattern.
    Handles component fetching via `preprocess` based on `_components_used`.
    Subclasses must set `_components_used` and implement the core logic in `process`.
    """
    world: WorldInterface
    _components_used: List[Type[Component]]

    @inject
    def __init__(self,
                 # Inject using INTERFACE container
                 world: WorldInterface = Provide[CoreContainerInterface.world]
                ):
        self.world = world
        # Default to empty list; MUST be overridden or set by subclass __init__
        self._components_used = []

    # Concrete implementation of the getter
    def get_components_used(self) -> List[Type[Component]]:
        """Returns the list of component types this processor uses, set during init."""
        # Defensive check (optional but recommended)
        if not hasattr(self, '_components_used') or self._components_used is None:
             raise NotImplementedError(f"Processor {self.__class__.__name__} must set self._components_used in __init__")
        return self._components_used

    # Concrete implementation of default preprocess
    def preprocess(self) -> daft.DataFrame:
        """
        Default implementation: Fetches the latest state of components declared
        in `_components_used` from the world.
        Returns an empty DataFrame if no components are specified or no entities found.
        """
        components_to_fetch = self.get_components_used()
        if not components_to_fetch:
            # Return an empty DataFrame, ideally with a schema hint if possible,
            # but Daft usually handles concat/joins with truly empty frames.
            # We need at least entity_id for joins later? Check world.get_components return.
            # For now, assume world.get_components handles empty list query or returns None/empty.
            return daft.from_pylist([]) # Safest empty DF

        # Fetch components from the world (expects get_components to handle None/empty)
        state_df = self.world.get_components(*components_to_fetch)

        # Ensure a DataFrame is returned, even if empty
        if state_df is None:
             # This case might indicate an issue in world.get_components
             # Log warning? For now, return empty.
             return daft.from_pylist([])
        
        # Optimization: Check if DF is empty after query? Maybe not needed if Daft handles it.
        # if len(state_df) == 0:
        #     return state_df # Return the empty DF

        return state_df

    @abstractmethod # Keep process abstract as logic is domain-specific
    def process(self, dt: float, state_df: daft.DataFrame) -> Optional[daft.DataFrame]:
        """
        The core transformation logic of the processor. Subclasses must implement this.

        Args:
            dt: Time delta for the current step.
            state_df: The input DataFrame prepared by the preprocess method.
                      This DataFrame might be empty if no relevant entities were found.

        Returns:
            Optional[daft.DataFrame]: DataFrame with updated component data + keys,
                                      or None if no update should be committed.
        """
        raise NotImplementedError