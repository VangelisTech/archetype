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

from daft import DataFrame

from archetype.core.interfaces import Component, Archetype, ArchetypeSignature, iSystem, iQuerier, iUpdater
from archetype.core.processor import Processor
from archetype.core.base import BaseWorld
from archetype.core.command import EcsContext



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
        self.cmd_queue = EcsCommandQueue(world_id=world_id, run_id=run_id)

    def step(self, *args, **kwargs):
        """
        Execute one simulation tick with individual archetype processing.
        """
        # Materialize any pending spawns before the first tick
        start = time.time()
        


        # Process all entities for each archetype signature
        for sig in set(self._entity2sig.values()):
            cmd_df = self.materialize_cmds()
            last_tick_df = self.get_archetype(sig, [self.tick - 1]) # Wont execute on tick 0 (No Active Sigs)
            
            processed_df = self.execute(df, sig, self.context, *args, **kwargs)

            # Apply ECS Context Queued Changes 
            applicable_spawns = [c in self.context._spawn_queue for c in sig if ]

            self.updater.update(processed_df, sig, self.tick, self.world_id, self.run_id)

        end = time.time()
        logger.info(f"Sync tick {self.tick} done in {end-start:.3f}s")

        self.tick += 1

    def materialize_spawns(self, spawn_queue: List[Tuple[Component,...]]) -> int:
        """Create a new entity with these components."""
        
        spawns_pylist = [ 
            Archetype(components).to_pydict_row(
                entity_id=next(self._entity_counter),
                  tick=self.tick, 
                  world_id=self.world_id, 
                  run_id=self.run_id
                )
            for components in spawn_queue if Archetype(components) in sig
        ]
        spawns_df = DataFrame.from_pylist([row for row in spawns_pylist if row.keys() in])

        for components in spawn_queue:
            entity_id = next(self._entity_counter)
            archetype =  # Just using the Archetype as a convenience class here
            archetype 
            

        spawned_df = DataFrame.from_pylist(new_rows)
        self._entity2sig[entity_id] = sig

        return entity_id
    
    def _spawn(self, *components: Component) -> int:
        """Create a new entity with these components."""

        # Get the entity id and signature
        entity_id = next(self._entity_counter)
        sig = Archetype.sig_from_components(components) # Just using the Archetype as a convenience class here
        self._entity2sig[entity_id] = sig

        # Create the row dict
        row_dict = self._new_archetype_row(entity_id, tick or self.tick, components, self.world_id, self.run_id)

        # Add row to the spawn cache
        if sig not in self._spawn_cache:
            self._spawn_cache[sig] = []
        self._spawn_cache[sig].append(row_dict)
        

    def _despawn(self, entity_id: int) -> None:
        """Mark an entity dead (is_active=False)."""
        sig = self._entity2sig[entity_id]

  
    # ---------------------------------------------------------------------
    # UpdateManager Facade
    # ---------------------------------------------------------------------

    def update(self, archetypes: List[Tuple[ArchetypeSignature, DataFrame]], tick: int):
        self.updater.update(archetypes, tick, self.world_id, self.run_id)

    

    # ---------------------------------------------------------------------
    # QueryManager Facade
    # ---------------------------------------------------------------------

    def get_archetype(self, sig, tick: List[int]) -> List[Tuple[ArchetypeSignature, DataFrame]]:
        """Fetch the latest live state of these components at the given tick."""
        return self.querier.get_archetype(sig, tick, self.world_id, self.run_id)

    def archetype_for_entity(self, entity_id: int, tick: int) -> DataFrame:
        """Get a archetype for an entity."""
        sig = self._entity2sig[entity_id]
        return self.querier.get_archetype_for_entity(entity_id, sig, tick, self.world_id, self.run_id)

    def get_component(self, component_type: Type[Component]):
        


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
        """Execute the system for a single tick."""
        return self.system.execute(df, sig, *args, **kwargs)
