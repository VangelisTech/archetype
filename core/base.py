# -*- coding: utf-8 -*-
"""Defines the foundational types and abstract base classes for the ECS system."""

from typing import Any, Type, Optional
from abc import ABC, abstractmethod

from lancedb.pydantic import LanceModel


# --- Forward Declarations ---
# These help type hints work even if classes are defined later or in other files.
# Using strings for forward references within the same file is also common.
class QueryInterface: pass
class UpdateManager: pass
class World: pass

# --- Component Base ---
# Inherit from LanceModel to leverage Pydantic validation and LanceDB schema features
class Component(LanceModel):
    """Base class for all components. Inherits from LanceModel."""
    # Components should define their own fields.
    # We avoid adding a base entity_id here as it's managed by the store/dataframe.
    pass

# --- Processor Base Class ---
class Processor(ABC):
    """
    Base class for systems that process entities and components.
    Processors should read from the QueryInterface and write via the UpdateManager.
    """
    @abstractmethod
    def process(self, *inputs: Any) -> None:
        """
        The core logic of the processor.

        Args:
            *inputs: Additional arguments passed from the world's process cycle.
            
        """
        raise NotImplementedError

# --- System Base Class ---
class System(ABC):
    """
    Abstract base class for orchestrating processor execution.
    Implementations can define sequential, parallel, or DAG-based execution.
    """
    @abstractmethod
    def execute(self, dt: float, *args: Any, **kwargs: Any) -> None:
        """
        Executes the managed processors.

        Args:
            querier: Interface for processors to read committed state.
            updater: Interface for processors to queue updates.
            dt: Time delta for the current step.
            *args, **kwargs: Additional arguments passed from the world's process cycle.
        """
        raise NotImplementedError

    @abstractmethod
    def add_processor(self, processor: Processor) -> None:
        """Adds a processor to be managed by this system."""
        raise NotImplementedError

    @abstractmethod
    def remove_processor(self, processor_type: Type[Processor]) -> None:
        """Removes all processors of a specific type."""
        raise NotImplementedError

    @abstractmethod
    def get_processor(self, processor_type: Type[Processor]) -> Optional[Processor]:
         """Gets the first found instance of a specific processor type."""
         return None

