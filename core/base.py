# -*- coding: utf-8 -*-
"""Defines the foundational types and abstract base classes for the ECS system."""

from typing import Any, Type, Optional
from abc import ABC, abstractmethod

from lancedb.pydantic import LanceModel


# --- Component Base ---
# Inherit from LanceModel to leverage Pydantic validation and LanceDB schema features
class Component(LanceModel):
    pass

# --- Processor Base Class ---
class BaseProcessor(ABC):
    """
    Base class for systems that process entities and components.
    Processors should read from the QueryManager and write via the UpdateManager.
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
class BaseSystem(ABC):
    """
    Abstract base class for orchestrating processor execution.
    Implementations can define sequential, parallel, or DAG-based execution.
    """
    @abstractmethod
    def execute(self, *args: Any, **kwargs: Any) -> None:
        """
        Executes the managed processors.

        Args:
            *args, **kwargs: Additional arguments passed from the world's process cycle.
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

