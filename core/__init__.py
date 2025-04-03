"""
Core package for ECS implementation.
"""

from .base import Component, EntityType
from .store import EcsComponentStore
from .managers import EcsQueryInterface, EcsUpdateManager

__all__ = ["Component", "EntityType", "EcsComponentStore", "EcsQueryInterface", "EcsUpdateManager"]


if __name__ == "__main__":
    from . import store
    store.__main__()

    