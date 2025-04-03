# -*- coding: utf-8 -*-
"""Defines the foundational types and abstract base classes for the ECS system."""

from typing import Any, Type, Set, TypeVar, Optional, Dict
# from dataclasses import dataclass, field # No longer needed for Component base
from abc import ABC, abstractmethod
import time
from collections import defaultdict
import datetime
# Import daft early, even if only used by subclasses, to ensure it's available
import daft
# Import LanceModel and ensure pydantic is available

from lancedb.pydantic import LanceModel


# --- Forward Declarations ---
# These help type hints work even if classes are defined later or in other files.
# Using strings for forward references within the same file is also common.
class EcsQueryInterface: pass
class EcsUpdateManager: pass
class EcsWorld: pass

# --- Entity Type ---
class EntityType:
    """Defines a category of entities and the components they are allowed to have."""
    def __init__(self, name: str, allowed_components: Optional[Set[Type['Component']]] = None):
        self.name = name
        # If None, all components are allowed. If empty set, none are allowed initially.
        self.allowed_components: Optional[Set[Type['Component']]] = allowed_components

    def allows_component(self, component_type: Type['Component']) -> bool:
        """Checks if this entity type allows the given component type."""
        # None means wildcard (allow all)
        if self.allowed_components is None:
            return True
        # Ensure it's checked against the set
        if isinstance(self.allowed_components, set):
             return component_type in self.allowed_components
        return False # Should not happen if initialized correctly

    def add_allowed_component(self, component_type: Type['Component']) -> None:
        """Adds a component type to the set of allowed components."""
        if self.allowed_components is None:
            # Changed behavior: Convert wildcard to explicit set on first addition
            self.allowed_components = {component_type}
        elif isinstance(self.allowed_components, set):
            self.allowed_components.add(component_type)
        else:
             # Should not happen
             raise TypeError("allowed_components is not a valid set or None.")


    def __str__(self) -> str: return f"EntityType('{self.name}')"
    def __repr__(self) -> str: return self.__str__()

# --- Component Base ---
# Inherit from LanceModel to leverage Pydantic validation and LanceDB schema features
class Component(LanceModel):
    """Base class for all components. Inherits from LanceModel."""
    # Components should define their own fields.
    # We avoid adding a base entity_id here as it's managed by the store/dataframe.
    pass

_C = TypeVar('_C', bound=Component)

# --- Processor Base Class ---
class Processor(ABC):
    """
    Base class for systems that process entities and components.
    Processors should read from the QueryInterface and write via the UpdateManager.
    """
    priority: int = 0 # Execution priority (higher runs first in sequential systems)

    @abstractmethod
    def process(self, querier: 'EcsQueryInterface', updater: 'EcsUpdateManager', dt: float, *args: Any, **kwargs: Any) -> None:
        """
        The core logic of the processor.

        Args:
            querier: Interface to read committed component state.
            updater: Interface to queue component updates.
            dt: Time delta for the current step.
            *args, **kwargs: Additional arguments passed from the world's process cycle.
        """
        raise NotImplementedError

# --- System Base Class ---
class System(ABC):
    """
    Abstract base class for orchestrating processor execution.
    Implementations can define sequential, parallel, or DAG-based execution.
    """
    @abstractmethod
    def execute(self, querier: 'EcsQueryInterface', updater: 'EcsUpdateManager', dt: float, *args: Any, **kwargs: Any) -> None:
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
    def add_processor(self, processor: Processor, priority: Optional[int] = None) -> None:
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
