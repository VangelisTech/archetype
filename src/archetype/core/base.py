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

from typing import Any, Type, Optional, Dict, Tuple, List, Set
from abc import ABC, abstractmethod
from itertools import count

import daft
import ulid

from archetype.core.interfaces import Component, ArchetypeSignature
from archetype.core.command import Command
from archetype.core.auth import ActorCtx


class BaseProcessor(ABC):
    """
    Abstract base class for Processor implementations.
    """
    @abstractmethod
    def process(self, df: daft.DataFrame, *args, **kwargs) -> daft.DataFrame:
        """Process the DataFrame."""
        return df


class BaseSystem(ABC):
    """
    Abstract base class for orchestrating processor execution.
    Implementations can define sequential, parallel, or DAG-based execution.
    """

    @abstractmethod
    def add_processor(self, processor: BaseProcessor) -> None:
        """Adds a processor to be managed by this system."""
        raise NotImplementedError

    @abstractmethod
    def remove_processor(self, processor_type: Type[BaseProcessor]) -> None:
        """Removes all processors of a specific type."""
        raise NotImplementedError

    @abstractmethod
    def execute(self, df: daft.DataFrame, sig: ArchetypeSignature, *args: Any, **kwargs: Any) -> daft.DataFrame:
        """
        Executes the managed processors.

        Args:
            *args, **kwargs: Additional arguments passed from the world's process cycle (e.g., dt).

        Returns:
            A processed daft.DataFrame
        """
        raise NotImplementedError

class BaseWorld(ABC):
    """
    Abstract base class for World implementations with shared functionality.
    Contains common logic for entity management, spawn caching, and world state.
    """
    @abstractmethod
    def step(self, *args, **kwargs) -> None:
        raise NotImplementedError

    @property
    @abstractmethod
    def active_signatures(self) -> Set[Any]:
        raise NotImplementedError

    @abstractmethod
    def create_entity(self, *components: Component, step: Optional[int] = None) -> int:
        raise NotImplementedError

    @abstractmethod
    def delete_entity(self, entity_id: int, step: Optional[int] = None) -> None:
        raise NotImplementedError
    
    @abstractmethod
    def add_component(self, entity_id: int, component: Component, step: Optional[int] = None) -> None:
        """
        Adds a component to an entity.
        """
        raise NotImplementedError
    
    @abstractmethod
    def remove_component(self, entity_id: int, component_type: Type[Component], step: Optional[int] = None) -> None:
        """        Removes a component from an entity.
        """
        raise NotImplementedError
    
    @abstractmethod
    def add_processor(self, processor: BaseProcessor) -> None:
        """
        Adds a processor to the world.
        """
        raise NotImplementedError
    
    @abstractmethod
    def remove_processor(self, processor_type: Type[BaseProcessor]) -> None:
        """
        Removes all processors of a specific type from the world.
        """
        raise NotImplementedError
    
class BaseStore(ABC):
    """
    Abstract base class for Store implementations.
    Provides methods to manage entity data and archetypes.
    """
    @abstractmethod
    def ensure_table(self, sig: ArchetypeSignature) -> daft.Table:
        """
        Ensures that a table exists for the given signature.
        If it does not exist, it creates a new table with the appropriate schema.
        """
        raise NotImplementedError

    @abstractmethod
    def append(self, sig: ArchetypeSignature, df: daft.DataFrame, tick: int, world_id: str, run_id: str) -> None:
        """Append a DataFrame to the store."""
        raise NotImplementedError

    @abstractmethod
    def get_archetype_df(self, sig: ArchetypeSignature, tick: int, world_id: str, run_id: str) -> daft.DataFrame:
        """Get a DataFrame of entities matching the given signature."""
        raise NotImplementedError


class BaseQuerier(ABC):
    """
    Abstract base class for Querier implementations.
    Provides methods to query entities and their components.
    """
    @abstractmethod
    def get_archetype(self, sig: ArchetypeSignature, tick: int, world_id: str, run_id: str) -> daft.DataFrame:
        """
        Returns a DataFrame of entities matching the given signature.
        """
        raise NotImplementedError
    
    def get_archetype_for_entity(self, entity_id: int, sig: ArchetypeSignature) -> daft.DataFrame:
        """
        Returns a DataFrame of components for a specific entity.
        """
        raise NotImplementedError

class BaseUpdater(ABC):
    """
    Abstract base class for Updater implementations.
    """
    @abstractmethod
    def update(self, df: daft.DataFrame, sig: ArchetypeSignature, tick: int, world_id: str, run_id: str) -> None:
        raise NotImplementedError


class BaseCommandBroker(ABC): 
    """
    Abstract base class for Broker implementations.
    Brokers manage the communication between different components or systems.
    """
    @abstractmethod
    def enqueue(self, world_id: str, cmd: "Command", ctx: "ActorCtx") -> None:
        """Enqueue a message to be processed."""
        raise NotImplementedError
    
    @abstractmethod
    def enqueue_bulk(self, world_id: str, cmds: List["Command"]) -> None:
        """Enqueue multiple messages for processing."""
        raise NotImplementedError
    
    @abstractmethod
    def dequeue_due(self, *, world_id: str, tick: int, limit: int = 1_000) -> List["Command"]:
        """Dequeue a message for processing."""
        raise NotImplementedError
    
    @abstractmethod
    def ack(self, world_id: str, cmd_id: str) -> None:
        """Acknowledge that a command has been processed."""
        raise NotImplementedError