"""
Core package for ECS implementation.
"""

from .base import Component
from .world import World
from .processor import TimedProcessor

__all__ = [
    "Component",
    "World", 
    "TimedProcessor"
]




    