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

from typing import Any, Type, List, Set, Tuple
from abc import ABC, abstractmethod

import daft
from uuid_utils import UUID

from archetype.core.interfaces import ArchetypeSignature
from archetype.core.archetype import Component
from archetype.core.config import RunConfig

class BaseProcessor(ABC):
    """
    Abstract base class for Processor implementations.
    """
    priority: int = 10
    components: Tuple[Type[Component], ...] = ()

    @abstractmethod
    def process(self, df: daft.DataFrame, **input_kwargs) -> daft.DataFrame:
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
    def execute(self, df: daft.DataFrame, sig: ArchetypeSignature, **input_kwargs: Any) -> daft.DataFrame:
        """
        Executes the managed processors.

        Args:
            *args, **input_kwargs: Additional arguments passed from the world's process cycle (e.g., dt).

        Returns:
            A processed daft.DataFrame
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
    def get_archetype(self, sig: ArchetypeSignature, tick: int, world_id: str, run_config: RunConfig) -> daft.DataFrame:
        """
        Returns a DataFrame of entities matching the given signature.
        """
        raise NotImplementedError
    
    def query_archetype(self, entity_id: int, sig: ArchetypeSignature) -> daft.DataFrame:
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

class BaseWorld(ABC):
    """
    Abstract base class for World implementations with shared functionality.
    Contains common logic for entity management, spawn caching, and world state.
    """
    @abstractmethod
    async def step(self, run_config: RunConfig, **input_kwargs) -> Any:
        raise NotImplementedError

    @property
    @abstractmethod
    def active_signatures(self) -> Set[Any]:
        raise NotImplementedError

    @abstractmethod
    async def create_entity(self, components: List[Component]) -> int:
        raise NotImplementedError

    @abstractmethod
    async def remove_entity(self, entity_id: int) -> None:
        raise NotImplementedError
    
    @abstractmethod
    async def add_components(self, entity_id: int, components: List[Component]) -> None:
        raise NotImplementedError
    
    @abstractmethod
    async def remove_components(self, entity_id: int, component_types: List[Type[Component]]) -> None:
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
    
class BaseCommandBroker(ABC): 
    """
    Abstract base class for Broker implementations.
    Brokers manage the communication between different components or systems.
    """
    @abstractmethod
    def enqueue(self, world_id: str, cmd: Any, ctx: Any) -> None:
        """Enqueue a message to be processed."""
        raise NotImplementedError
    
    @abstractmethod
    def enqueue_bulk(self, world_id: str, cmds: List[Any]) -> None:
        """Enqueue multiple messages for processing."""
        raise NotImplementedError
    
    @abstractmethod
    def dequeue_due(self, *, world_id: str, tick: int, limit: int = 1_000) -> List[Any]:
        """Dequeue a message for processing."""
        raise NotImplementedError
    
    @abstractmethod
    def ack(self, world_id: str, cmd_id: str) -> None:
        """Acknowledge that a command has been processed."""
        raise NotImplementedError
    

