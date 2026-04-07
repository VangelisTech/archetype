"""Archetype DSL — ergonomic simulation API.

Usage::

    from archetype.dsl import World, behavior
    from archetype.core.component import Component
"""

from archetype.dsl.behavior import behavior
from archetype.dsl.proxy import AgentProxy
from archetype.dsl.world import World

__all__ = ["World", "behavior", "AgentProxy"]
