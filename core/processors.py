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
        components = self.get_components_used()

        # Fetch lastest component state for steps <= step_number
        state_df = self.world.get_components(*components) # World inserts step number

        # Other Built-in State Fetching methods at World Facade include: 
        #
        # If your processor needs a specific step or multiple steps 
        # state_df = self.world.get_components_from_steps(*components, steps=steps)
        #
        # OR IF your processor needs a specific entity or multiple entities
        # state_df = self.world.get_component_for_entities(*components, steps=steps)

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

        Example:
        ```python
        class Position(Component):
            x: float = 0.0 # m
            y: float = 0.0 # m

        class Velocity(Component):
            vx: float = 0.0 # m/s
            vy: float = 0.0 # m/s

        class MovementProcessor(Processor): # Inherit from Processor
            def __init__(self):
                super().__init__() # Handles world injection

                # Declare components used 
                self._components_used: List[Type[Component]] = [Position, Velocity]

            # No need to implement preprocess since we want latest step state

            def process(self, dt: float, state_df: DataFrame) -> Optional[DataFrame]:

                # Calculate 2D Frictionless Kinematics
                update_df = state_df.with_columns(
                    {
                        "x": col("x") + col("vx") * dt,
                        "y": col("y") + col("vy") * dt,
                    }
                )

                return update_df
        
        ```
        """
        raise NotImplementedError