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



from archetype.core import Component, Archetype, ArchetypeSignature


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
    def execute(self, *args: Any, **kwargs: Any) -> Dict[str, daft.DataFrame]:
        """
        Executes the managed processors.

        Args:
            *args, **kwargs: Additional arguments passed from the world's process cycle (e.g., dt).

        Returns:
            Dict[str, daft.DataFrame]: A dictionary mapping archetype hashes to their resulting
                                       update DataFrames.
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

    @abstractmethod
    def get_active_signatures(self) -> Set[Any]:
        raise NotImplementedError

    @abstractmethod
    def spawn(self, *components: Component, step: Optional[int] = None) -> int:
        raise NotImplementedError

    @abstractmethod
    def despawn(self, entity_id: int, step: Optional[int] = None) -> None:
        raise NotImplementedError

    @abstractmethod
    def materialize_spawns(self) -> None:
        raise NotImplementedError

class BaseUpdater(ABC):
    """
    Abstract base class for Updater implementations.
    """
    @abstractmethod
    def update(self, df: daft.DataFrame, sig: ArchetypeSignature, step: int, world_id: str, run_id: str) -> None:
        raise NotImplementedError

    @abstractmethod
    def materialize_spawns(self, spawn_cache: Dict[ArchetypeSignature, List[Dict[str, Any]]], world_id: str, run_id: str) -> None:
        raise NotImplementedError

    @abstractmethod
    def despawn(self, entity_id: int, step: Optional[int] = None) -> None:
        raise NotImplementedError
