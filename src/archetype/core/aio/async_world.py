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

from itertools import count
from typing import List, Type, Optional, Dict, Any, Set
import asyncio
from uuid_utils import UUID  # noqa: F401 imported for type hints

import daft
from daft import DataFrame, col
import pyarrow as pa
# duplicate UUID import removed
from archetype.core import ArchetypeSignature, Component, Archetype
from archetype.core.config import WorldConfig, RunConfig
from archetype.core.interfaces import iAsyncWorld, iAsyncQueryManager, iAsyncUpdateManager, iAsyncSystem
from archetype.core.aio.async_processor import AsyncProcessor

from logging import getLogger

logger = getLogger(__name__)

class AsyncWorld(iAsyncWorld):
    def __init__(self,
        world_config: WorldConfig,
        querier: iAsyncQueryManager,
        updater: iAsyncUpdateManager,
        system: iAsyncSystem,
    ):
        """
        Initialize the fully parallel async world.
        """
        # World Properties
        self.name = world_config.name
        self.world_id = world_config.world_id

        # Dependencies
        self.querier = querier
        self.updater = updater 
        self.system = system

        # Internal State
        self.tick = 0
        self.run_id: Optional[str] = None
        self._entity_counter = count(start=1)
        self._entity2sig: Dict[int, ArchetypeSignature] = {}
        self._spawn_cache: Dict[ArchetypeSignature, List[Dict[str, Any]]] = {}
        self._despawn_cache: Dict[ArchetypeSignature, List[int]] = {}
        
        # Live snapshot of the most recent processed DataFrame per signature (current tick)
        self._live: Dict[ArchetypeSignature, DataFrame] = {}
        

    async def run(self, run_config: RunConfig, **input_kwargs) -> None:
        """
        Runs the world for the given run configuration.
        """

        # Track the current run identifier for default queries
        self.run_id = str(run_config.run_id)
        for _ in range(run_config.num_steps):
            await self.step(run_config, **input_kwargs)

    async def step(self, run_config: RunConfig, **input_kwargs) -> None:
        """
        Executes one full, parallel simulation tick.
        """
        sigs = sorted(self.active_signatures, key=Archetype.get_name)
        futures = [self._run_archetype(sig, run_config, **input_kwargs) for sig in sigs]
        results = await asyncio.gather(*futures, return_exceptions=True)
        errors = {sig: r for sig, r in zip(sigs, results) if isinstance(r, Exception)} # from asyncio.gather docs: The order of result values corresponds to the order of awaitables in aws. 

        if errors:
            raise RuntimeError("; ".join(f"{Archetype.get_name(sig)}: {e}" for sig, e in errors.items()))

        self._live = {sig: df.where(col("is_active")) for sig, df in zip(sigs, results)}
        
        self.tick += 1

        
    async def _run_archetype(self, sig: ArchetypeSignature, run_config: RunConfig, **input_kwargs) -> DataFrame:
        "Atomic sequence of a world step with a dedicated execution and materialization helper for future remote operators."
       
        # 1. Fetch previous state for all entities and their components
        if self.tick > 0 and run_config.prefer_live_reads and sig in self._live:
            df = self._live[sig]
        else:
            # Builds a clean df with the correct schema for the archetype if tick is 0
            df = await self.query_archetype(
                sig=sig,
                run_id=run_config.run_id,
                ticks=[self.tick - 1],
                entity_ids=None,
                components=None
            )

        # 2. Materialize Mutations (Spawns/Despawns)
        df = self.materialize_mutations(df, sig) 
        
        # 3. Execute Processors for this archetype via system 
        df = await self.execute(df, sig, **input_kwargs)

        # 4. Update (returns materialized df with tick/world/run/entity_id set)
        df_mat = await self.update(df, sig, run_config)

        return df_mat

    # ---------------------------------------------------------------------
    #  Step Planning
    # ---------------------------------------------------------------------

    @property
    def active_signatures(self) -> Set[ArchetypeSignature]:
        """Get the union of all archetypes that need processing this tick."""
        active_sigs = set(self._entity2sig.values())
        spawned_sigs = set(self._spawn_cache.keys())
        despawn_sigs = set(self._despawn_cache.keys())
        return active_sigs | spawned_sigs | despawn_sigs
    
    
    def materialize_mutations(self, df: DataFrame, sig: ArchetypeSignature) -> DataFrame:
        # Handle Despawns
        if self._despawn_cache.get(sig):
            entities_to_despawn = set(self._despawn_cache[sig])
            df = df.with_column("is_active", 
                col("entity_id").is_in(entities_to_despawn).if_else(False, col("is_active"))
            )
            # Clear Cache 
            self._despawn_cache[sig] = []


        # Handle Spawns
        if self._spawn_cache.get(sig):
            # Dedupe duplicate spawns, prioritizing "most recent cmd" 
            rows = list({row["entity_id"]: row for row in reversed(self._spawn_cache[sig])}.values())

            # Convert list of dicts to arrow table and eventually daft df
            pyarrow_schema = Archetype.get_archetype_schema(sig)
            arrow_table = pa.Table.from_pylist(rows, schema=pyarrow_schema)
            spawns_df = daft.from_arrow(arrow_table)

            df = df.concat(spawns_df)
            self._spawn_cache[sig] = []

        return df
    
    async def _move_entity(self,
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
        df = self._live.get(old_sig, daft.from_arrow(pa.Table.from_batches([], schema=Archetype.get_archetype_schema(old_sig))))
        df = df.where(col("entity_id") == entity_id) if df is not None else df
        
        # Materialize and check for emptiness using count_rows to avoid expression truthiness
        df_mat = df.collect()
        if df_mat.count_rows() == 0:
            return {}                # entity vanished, caller decides

        # 2) take latest tick row
        row_dict = df_mat.sort(col("tick"), desc=True).limit(1).to_pylist()[0]
    

        # 3) overlay components that change with the new ones (skips for remove component with 0 member list)
        for c in mutated_components:
            row_dict.update(c.to_row_dict())

        # 4) stamp housekeeping columns for new location
        row_dict.update({
            "entity_id": entity_id,
            "tick":      self.tick,
            "world_id":  str(self.world_id),
            # Placeholder; updater will stamp run_id during update
            "run_id":    "",
            "is_active": True
        })
        return row_dict
        
    def _clear_caches(self, sig: ArchetypeSignature):
        self._spawn_cache.pop(sig, None)
        self._despawn_cache.pop(sig, None)

    # ---------------------------------------------------------------------
    # World Mutation Commands
    # ---------------------------------------------------------------------

    async def create_entity(self, components: List[Component]) -> int:
        entity_id = next(self._entity_counter)
        sig = Archetype.sig_from_components(components)
        self._entity2sig[entity_id] = sig

        # Placeholder run_id; updater will stamp correct run_id on update
        row_dict = Archetype.to_row_dict(entity_id, self.tick, components, self.world_id, run_id="")
        self._spawn_cache.setdefault(sig, []).append(row_dict)
        return entity_id

    async def remove_entity(self, entity_id: int):
        sig = self._entity2sig.pop(entity_id, None)
        if sig:
            self._despawn_cache.setdefault(sig, []).append(entity_id)
        else:
            logger.warning(f"World {self.name} ({self.world_id}): Entity Removal Failed: No entity: {entity_id}")

    async def add_components(self, entity_id: int, components: List[Component]) -> None:

        old_sig = self._entity2sig.get(entity_id)
        if not old_sig: 
            return #TODO: add warning -> no existing archetype for entity blah

        new_sig = Archetype.add_components(old_sig, [type(c) for c in components])
        if new_sig == old_sig: 
            return #TODO: add warning -> provided components already in archetype sig

        row = await self._move_entity(entity_id, old_sig, new_sig, components)

        # 1) mark *old row* inactive
        self._despawn_cache.setdefault(old_sig, []).append(entity_id)

        # 2) row to *insert* under new signature
        self._spawn_cache.setdefault(new_sig, []).append(row)

        # 3) update bookkeeping – atomically
        self._entity2sig[entity_id] = new_sig
        
    
    async def remove_components(self, entity_id: int, component_types: List[Type[Component]]) -> None:
        old_sig = self._entity2sig.get(entity_id)
        if old_sig is None:
            return

        new_sig = Archetype.remove_components(old_sig, component_types)
        if new_sig == old_sig:
            return

        row = await self._move_entity(entity_id, old_sig, new_sig, [])  # remove ≡ keep remaining columns

        self._despawn_cache.setdefault(old_sig, []).append(entity_id)
        self._spawn_cache.setdefault(new_sig, []).append(row)
        self._entity2sig[entity_id] = new_sig
            

    async def add_processor(self, processor: 'AsyncProcessor'):
        await self.system.add_processor(processor)

    async def remove_processor(self, processor: Type['AsyncProcessor']):
        await self.system.remove_processor(processor)

    # ---------------------------------------------------------------------
    # Updater, Querier, System Facade methods
    # ---------------------------------------------------------------------
    async def query_archetype(
        self,
        sig: ArchetypeSignature,
        run_config_or_ticks=None,
        *,
        ticks: Optional[List[int]] = None,
        entity_ids: Optional[List[int]] = None,
        components: Optional[List[Component]] = None,
        run_id: Optional[str] = None,
        run_config: Optional[RunConfig] = None,
    ) -> DataFrame:
        """Facade Method to query an archetype table by signature.

        Defaults to the world's most recent run_id when not provided. Accepts an optional
        run_config for instrumentation; ignored by the base querier.
        """

        # Back-compat: tests sometimes pass RunConfig as the 2nd positional arg
        if run_config_or_ticks is not None and not isinstance(run_config_or_ticks, list):
            run_config = run_config_or_ticks  # type: ignore[assignment]
        elif isinstance(run_config_or_ticks, list) and ticks is None:
            ticks = run_config_or_ticks

        effective_run_id = run_id or (self.run_id and str(self.run_id)) or (run_config and str(run_config.run_id)) or ""
        # Prefer to pass run_config if the querier supports it (instrumented); otherwise omit.
        try:
            return await self.querier.query_archetype(
                sig=sig,
                world_id=self.world_id,
                run_id=effective_run_id,
                ticks=ticks or [self.tick],
                entity_ids=entity_ids,
                components=components,
                run_config=run_config,
            )  # type: ignore[call-arg]
        except TypeError:
            return await self.querier.query_archetype(
                sig=sig,
                world_id=self.world_id,
                run_id=effective_run_id,
                ticks=ticks or [self.tick],
                entity_ids=entity_ids,
                components=components,
            )
    
    async def get_components(
            self,
            components: List[Type[Component]],
            entity_ids: Optional[List[int]] = None,
        ) -> DataFrame:
        """
        Query all active entities that contain at least the provided component types,
        unioning rows across all matching archetype signatures.

        Returns a DataFrame projected to the provided components' schema.
        """
        required_types = set(components)

        # Build output schema directly from component types
        temp_sig = tuple(sorted(components, key=lambda t: t.__name__))
        schema = Archetype.get_archetype_schema(temp_sig)
        df = daft.from_arrow(pa.Table.from_batches([], schema=schema))
        matching_sigs = [sig for sig in self._live.keys() if required_types.issubset(set(sig))]

        # Project to requested schema and filter to active entities
        proj_cols = schema.names
        for sig in matching_sigs:
            if sig in self._live:
                df = df.concat(self._live[sig].where(col("is_active")).select(*proj_cols))
        
        if entity_ids:
            df = df.where(col("entity_id").is_in(entity_ids))

        return df


    
    async def execute(self, df: DataFrame, sig: ArchetypeSignature, **input_kwargs) -> DataFrame:
        """
        Execute system processors. 
        """
        return await self.system.execute(df, sig, **input_kwargs)

    async def update(self, df: DataFrame, sig: ArchetypeSignature, run_config: RunConfig, tick: Optional[int] = None) -> DataFrame:
        """Update the store with the given archetypes."""
        df = await self.updater.update(df, sig, tick or self.tick, self.world_id, run_config.run_id)
        return df
