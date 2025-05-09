"""
Archetype: An Entity Component System (ECS) core library.
"""
# Make key classes available at the top-level package
from .core.base import Component, BaseProcessor, BaseSystem
from .core.store import ArchetypeStore
from .core.world import World # Added World as it's a key part of an ECS
from .core.components import (
    BaseComponent, # Assuming BaseComponent might be a common alias or base
    Position,      # Example common component
    Velocity,      # Example common component
    Metadata       # Example common component
) # Assuming you might have common components defined in core.components
from .core.processor import Processor # Assuming a base Processor might be in core.processor
from .core.query import Query # Assuming Query class from core.query


# Define __all__ for `from archetype import *` to control what's exported.
# This helps avoid polluting the namespace of users of the library.
__all__ = [
    # From core.base
    "Component",
    "BaseProcessor",
    "BaseSystem",
    # From core.store
    "ArchetypeStore",
    # From core.world
    "World",
    # From core.components (add actual common components here if they exist)
    "BaseComponent",
    "Position",
    "Velocity",
    "Metadata",
    # From core.processor
    "Processor",
    # From core.query
    "Query",
]

# You could also consider versioning here if desired, e.g.:
# __version__ = "0.1.0" 