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

import time
from typing import List, Optional, Tuple, Set, Dict, Any
from itertools import count
import ulid

from daft import DataFrame

from archetype.core.interfaces import Component, Archetype, ArchetypeSignature, iSystem, iQuerier, iUpdater
from archetype.core.processor import Processor
from archetype.core.base import BaseWorld



from logging import getLogger

logger = getLogger(__name__)

class SyncWorld(BaseWorld):
    def __init__(self,
        querier: iQuerier,
        updater: iUpdater,
        system: iSystem,
        world_id: str | None = None,
        run_id: str | None = None,
        debug: bool = False,
    ):
        self.querier = querier
        self.updater = updater
        self.system = system

        # Set World Properties
        self.world_id = world_id or f"world_{str(ulid.ULID())}"
        self.run_id = run_id or f"run_{str(ulid.ULID())}"
        self.current_step = 0

        # Initialize internal caches
        self._spawn_cache: Dict[str, List[Dict[str, Any]]] = {}  # ArchetypeSignature -> List[row_dict]
        self._entity2sig: Dict[int, ArchetypeSignature] = {}  # entity_id -> ArchetypeSignature
        self._entity_counter = count(start=1)

    def step(self, *args, **kwargs):
        """
        Execute one simulation step with individual archetype processing.
        """
        # Materialize any pending spawns before the first step
        start = time.time()

        self.materialize_spawns()

        # Get all active signatures and process each archetype individually
        active_signatures = self.get_active_signatures()

        for sig in active_signatures:
            df = self.get_archetype(sig, self.current_step)
            processed_df = self.execute(df, sig, *args, **kwargs)
            self.updater.update(processed_df, sig, self.current_step, self.world_id, self.run_id)

        end = time.time()
        logger.info(f"Sync Step {self.current_step} done in {end-start:.3f}s")

        self.current_step += 1


    def get_active_signatures(self) -> Set[Any]:
        """Get all active archetype signatures. Must be implemented by subclasses."""
        return set(self._entity2sig.values())

    def _new_archetype_row(self,
        entity_id: int,
        step: int,
        components: List[Component],
        world_id: str,
        run_id: str) -> Dict[str, Any]:
        """
        Convert entity components to a row dictionary for archetype storage.

        Args:
            entity_id: The unique entity identifier
            step: The simulation step
            components: List of component instances
            world_id: The world identifier
            run_id: The run identifier

        Returns:
            Dict containing the row data for this entity
        """
        row_dict = {
            "world_id": world_id,
            "run_id": run_id,
            "entity_id": entity_id,
            "step": step,
            "is_active": True
        }

        for c in components:
            prefix = c.get_prefix()
            row_dict.update({prefix + key: value for key, value in c.model_dump().items()})

        return row_dict

    def spawn(self, *components: Component, step: Optional[int] = None) -> int:
        """Create a new entity with these components."""
        assert len(components) != 0, "Cannot create an entity with no components"

        # Get the entity id and signature
        entity_id = next(self._entity_counter)
        sig = Archetype.sig_from_components(components) # Just using the Archetype as a convenience class here
        self._entity2sig[entity_id] = sig

        # Create the row dict
        row_dict = self._new_archetype_row(entity_id, step or self.current_step, components, self.world_id, self.run_id)

        # Add row to the spawn cache
        if sig not in self._spawn_cache:
            self._spawn_cache[sig] = []
        self._spawn_cache[sig].append(row_dict)

        return entity_id

    # ---------------------------------------------------------------------
    # UpdateManager Facade
    # ---------------------------------------------------------------------

    def update(self, archetypes: List[Tuple[ArchetypeSignature, DataFrame]], step: int):
        self.updater.update(archetypes, step, self.world_id, self.run_id)

    def materialize_spawns(self) -> None:
        """Materialize any pending spawns."""
        if self._spawn_cache:
            self.updater.materialize_spawns(self._spawn_cache, self.world_id, self.run_id)
            self._spawn_cache.clear()

    def despawn(self, entity_id: int, step: Optional[int] = None) -> None:
        """Mark an entity dead (is_active=False)."""
        self.updater.remove_entity(entity_id, step or self.current_step, self.world_id, self.run_id)


    # ---------------------------------------------------------------------
    # QueryManager Facade
    # ---------------------------------------------------------------------

    def get_archetype(self, sig, step: Optional[int] = -1) -> List[Tuple[ArchetypeSignature, DataFrame]]:
        """Fetch the latest live state of these components at the given step."""
        return self.querier.get_archetype(sig, step, self.world_id, self.run_id)

    def archetype_for_entity(self, entity_id: int, step: int) -> DataFrame:
        """Get a archetype for an entity."""
        sig = self._entity2sig[entity_id]
        return self.querier.get_archetype_for_entity(entity_id, sig, step, self.world_id, self.run_id)



    # ---------------------------------------------------------------------
    # System Facade
    # ---------------------------------------------------------------------

    def add_processor(self, proc: Processor) -> None:
        """Install a Processor into the sequential system."""
        self.system.add_processor(proc)

    def remove_processor(self, proc: Processor) -> None:
        """Remove a Processor from the sequential system."""
        self.system.remove_processor(proc)

    def execute(self, df: DataFrame, sig: ArchetypeSignature, *args, **kwargs) -> DataFrame:
        """Execute the system for a single step."""
        return self.system.execute(df, sig, *args, **kwargs)
