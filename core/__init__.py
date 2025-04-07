"""
Core package for ECS implementation.
"""

from .base import Component
from .world import World
from .processors import Processor

__all__ = [
    "Component",
    "World", 
    "Processor",
]



def create_world():
    return World()
    