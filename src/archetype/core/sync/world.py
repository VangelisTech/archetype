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
from typing import List, Optional, Tuple, Set, Dict, Any, Type
from itertools import count

import daft
from daft import DataFrame, col
import pyarrow as pa
from uuid_utils import UUID
import uuid_utils as uuid

from archetype.core import ArchetypeSignature, Component, Archetype
from archetype.core.base import BaseWorld
from archetype.core.interfaces import iSystem, iQueryManager, iUpdateManager
from archetype.core.config import RunConfig
from archetype.core.sync.processor import SyncProcessor

from logging import getLogger

logger = getLogger(__name__)

class SyncWorld(BaseWorld):
    def __init__(self,
        name: str,
        world_id: UUID,
        querier: iQueryManager,
        updater: iUpdateManager,
        system: iSystem,
    ):
        """
        Initialize the synchronous world.
        """
        # World Properties
        self.name = name
        self.world_id = world_id
        self.run_id = None
        self.debug = False
        self.validate = False
        self.loggerplate = f"world={self.world_id}"  # Initialize loggerplate

        # Dependencies
        self.querier = querier
        self.updater = updater
        self.system = system

        # Internal State
        self.tick = 0
        self._entity_counter = count(start=1)
        self._entity2sig: Dict[int, ArchetypeSignature] = {}
        self._spawn_cache: Dict[ArchetypeSignature, List[Dict[str, Any]]] = {}
        self._despawn_cache: Dict[ArchetypeSignature, List[int]] = {}

    def run(self, run_config: RunConfig, **input_kwargs):
        """
        Runs the world for the given run configuration.
        """
        self.run_id = run_config.run_id
        self.debug = run_config.debug
        self.validate = run_config.enable_validation

        # Update Loggerplate
        self.loggerplate = f"world={self.world_id} run={self.run_id}"

        # Set Run Config for System
        self.system.validate = self.validate
        self.system.debug = self.debug

        for _ in range(run_config.num_steps):
            self.step(run_config, **input_kwargs)

    def step(self, run_config: RunConfig, **input_kwargs) -> List[Tuple[DataFrame, ArchetypeSignature]]:
        """
        Executes one full simulation tick.
        """
        start_time = time.time()

        all_sigs_this_tick = self.active_signatures
        
        results = []
        for sig in sorted(all_sigs_this_tick):
            result = self._run_archetype(sig, run_config, **input_kwargs)
            results.append(result)
        
        # Finalize tick
        self._clear_caches()
        self.tick += 1
        logger.info(f"{self.loggerplate} tick {self.tick-1} completed in {time.time() - start_time:.4f}s")

        return results

    def _run_archetype(self, sig: ArchetypeSignature, run_config: RunConfig, **input_kwargs) -> Tuple[DataFrame, ArchetypeSignature]:
        """
        Process a single archetype through the full pipeline.
        """
        # 1. Fetch previous state for all entities and their components
        df = self.query_archetype(sig, ticks=[self.tick - 1], run_config=run_config)

        # 2. Materialize Mutations (Spawns/Despawns)
        df = self._materialize_mutations(df, sig, run_config)

        # 3. Execute Processors for this archetype via system
        df = self.execute(df, sig, run_config, **input_kwargs)

        # 4. Update
        self.update(df, sig, run_config, self.tick)

        return df, sig

    # ---------------------------------------------------------------------
    #  Step Planning
    # ---------------------------------------------------------------------

    @property
    def active_signatures(self) -> Set[ArchetypeSignature]:
        """Get the union of all archetypes that need processing this tick."""
        active_sigs = set(self._entity2sig.values())
        spawned_sigs = set(self._spawn_cache.keys())
        return active_sigs | spawned_sigs

    def _materialize_mutations(self, df: DataFrame, sig: ArchetypeSignature, run_config: RunConfig):
        # Handle Despawns
        if self._despawn_cache.get(sig):
            # Grab despawn list of dicts and dedupe by most recent mutation command
            self._despawn_cache[sig] = list(dict.fromkeys(reversed(self._despawn_cache[sig])))
            entities_to_despawn = self._despawn_cache[sig]

            # Left Join is O(n+m), better than df['entity_id'].is_in(entities_to_despawn) -> O(n*m)
            mask_df = daft.from_pydict({
                "entity_id": entities_to_despawn,
                "is_active": [False]*len(entities_to_despawn)
            })
            
            df = df.join(mask_df, left_on="entity_id", right_on="entity_id", how="left", suffix="_right") \
                .with_column("is_active",
                    col("is_active_right").is_null().if_else(col("is_active"),col("is_active_right"))) \
                .select(*df.column_names)

        # Handle Spawns
        if self._spawn_cache.get(sig):
            # Grab spawn list of dicts
            rows = self._spawn_cache[sig]
            
            # Dedupe duplicate spawns, prioritizing "most recent cmd" for easy user overwrite
            rows = list({row["entity_id"]: row for row in reversed(rows)}.values())

            # Convert list of dicts to arrow table and eventually daft df
            pyarrow_schema = Archetype.get_archetype_schema(sig)
            arrow_table = pa.Table.from_pylist(rows, schema=pyarrow_schema)
            spawns_df = daft.from_arrow(arrow_table)
            df = df.concat(spawns_df)

        if run_config.debug:
            df.explain() # Introspection opportunity to understand how mutations impact performance.

        return df

    def _move_entity(self,
        entity_id: int,
        old_sig: ArchetypeSignature,
        new_sig: ArchetypeSignature,
        mutated_components: List[Component]
    ) -> Dict[str, Any]:
        """
        Returns a row dict that is valid for the NEW archetype.
        Any field that is NOT in `mutated_components` is read from the
        previous most-recent row in the OLD archetype.
        """

        # 1) fetch *only* the single entity from old archetype
        df = self.query_archetype(
            old_sig,
            ticks=[self.tick-1],            # all ticks to snapshot
            entity_ids=[entity_id],
            components=None
        )     # grab full row
        
        if df.collect().count()[0] == 0:
            return {}                # entity vanished, caller decides

        # 2) take latest tick row
        row_dict = df.sort(col("tick"), desc=True).limit(1).to_pylist()[0]

        # 3) overlay components that change with the new ones (skips for remove component with 0 member list)
        for c in mutated_components:
            row_dict.update(c.to_row_dict())

        # 4) stamp housekeeping columns for new location
        row_dict.update({
            "entity_id": entity_id,
            "tick":      self.tick,
            "world_id":  self.world_id,
            "run_id":    self.run_id,
            "is_active": True
        })
        return row_dict

    def _clear_caches(self):
        self._spawn_cache.clear()
        self._despawn_cache.clear()

    # ---------------------------------------------------------------------
    # World Mutation Commands
    # ---------------------------------------------------------------------

    def create_entity(self, components: List[Component]) -> int:
        entity_id = next(self._entity_counter)
        sig = Archetype.sig_from_components(components)
        self._entity2sig[entity_id] = sig

        row_dict = Archetype.to_row_dict(entity_id, self.tick, components, self.world_id, self.run_id)
        self._spawn_cache.setdefault(sig, []).append(row_dict)
        return entity_id

    def remove_entity(self, entity_id: int):
        sig = self._entity2sig.pop(entity_id, None)
        if sig:
            self._despawn_cache.setdefault(sig, []).append(entity_id)
        else:
            logger.warn(f"{self.loggerplate} Entity Removal Failed: No entity: {entity_id}")

    def add_components(self, entity_id: int, components: List[Component]) -> None:
        old_sig = self._entity2sig.get(entity_id)
        if not old_sig:
            return #TODO: add warning -> no existing archetype for entity blah

        new_sig = Archetype.add_components(old_sig, components)
        if new_sig == old_sig:
            return #TODO: add warning -> provided components already in archetype sig

        row = self._move_entity(entity_id, old_sig, new_sig, components)

        # 1) mark *old row* inactive
        self._despawn_cache.setdefault(old_sig, []).append(entity_id)

        # 2) row to *insert* under new signature
        self._spawn_cache.setdefault(new_sig, []).append(row)

        # 3) update bookkeeping – atomically
        self._entity2sig[entity_id] = new_sig

    def remove_components(self, entity_id: int, component_types: List[Type[Component]]) -> None:
        old_sig = self._entity2sig.get(entity_id)
        if old_sig is None:
            return

        new_sig = Archetype.remove_components(old_sig, component_types)
        if new_sig == old_sig:
            return

        row = self._move_entity(entity_id, old_sig, new_sig, [])  # remove ≡ keep remaining columns

        self._despawn_cache.setdefault(old_sig, []).append(entity_id)
        self._spawn_cache.setdefault(new_sig, []).append(row)
        self._entity2sig[entity_id] = new_sig

    def add_processor(self, processor: 'SyncProcessor'):
        self.system.add_processor(processor)

    def remove_processor(self, processor: Type['SyncProcessor']):
        self.system.remove_processor(processor)

    # ---------------------------------------------------------------------
    # Updater, Querier, System Facade methods
    # ---------------------------------------------------------------------
    
    def query_archetype(self, sig: ArchetypeSignature, ticks: List[int]=None, entity_ids: List[int]=None, components: List[Component] = None, run_config: RunConfig | None = None) -> DataFrame:
        """Get an archetype by signature and tick. Esper equivalent of get_component for Archetype"""
        return self.querier.query_archetype(
            sig,
            self.world_id,
            run_config.run_id if run_config else self.run_id,
            ticks or [self.tick],
            entity_ids,
            components,
            run_config or RunConfig()
        )
    
    def execute(self, df: DataFrame, sig: ArchetypeSignature, run_config: RunConfig | None = None, **input_kwargs) -> DataFrame:
        """
        Execute system processors.
        """
        return self.system.execute(df, sig, run_config, **input_kwargs)

    def update(self, df: DataFrame, sig: ArchetypeSignature, run_config: RunConfig, tick: Optional[int] = None) -> None:
        """Update the store with the given archetypes."""
        self.updater.update(df, sig, tick or self.tick, self.world_id, run_config.run_id)

