# Copyright 2025 Vangelis Technologies Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Resources: Type-safe dependency injection for world-level shared state.

Resources are not entity data—they're the scaffolding around it:
- Environment parameters (gravity, reward scale)
- Shared services (CommandBroker)
- Simulation context (recursion depth, budget)

In RL terms: MDP parameters, hyperparameters, shared infrastructure.
"""

from typing import TypeVar, cast

T = TypeVar("T")


class Resources:
    """
    Type-safe container for world-level shared state.

    Usage:
        world.resources.insert(SimConfig(gravity=9.8))
        world.resources.insert(broker)

        # In processor
        config = resources.require(SimConfig)
        broker = resources.get(CommandBroker)  # Returns None if not present
    """

    __slots__ = ("_store",)

    def __init__(self):
        self._store: dict[type, object] = {}

    def insert(self, resource: T) -> None:
        """Insert a resource, keyed by its type."""
        self._store[type(resource)] = resource

    def insert_as(self, resource: object, key_type: type) -> None:
        """Insert a resource keyed by an explicit type rather than ``type(resource)``.

        Useful in tests when a subclass should be looked up as its base type::

            world.resources.insert_as(FakeSpec(client), PolicyClientSpec)

        Production processors that call ``resources.require(PolicyClientSpec)``
        will then find ``FakeSpec(client)`` transparently.
        """
        self._store[key_type] = resource

    def get(self, resource_type: type[T]) -> T | None:
        """Get a resource, or None if not present."""
        # _store is keyed by type(resource), so the value is a resource_type instance.
        return cast("T | None", self._store.get(resource_type))

    def require(self, resource_type: type[T]) -> T:
        """Get a resource, raising KeyError if not present."""
        if resource_type not in self._store:
            raise KeyError(f"Resource {resource_type.__name__} not registered")
        return cast("T", self._store[resource_type])

    def remove(self, resource_type: type[T]) -> T | None:
        """Remove and return a resource, or None if not present."""
        return cast("T | None", self._store.pop(resource_type, None))

    def contains(self, resource_type: type[T]) -> bool:
        """Check if a resource type is registered."""
        return resource_type in self._store

    def __contains__(self, resource_type: type) -> bool:
        """Support 'in' operator: `SimConfig in resources`."""
        return resource_type in self._store

    def items(self):
        """Iterate over (type, instance) pairs."""
        return self._store.items()

    def __repr__(self) -> str:
        types = ", ".join(t.__name__ for t in self._store.keys())
        return f"Resources({types})"
