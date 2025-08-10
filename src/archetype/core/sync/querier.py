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

from typing import Optional, List

from daft import DataFrame
from archetype.core.interfaces import iQueryManager, iStore, ArchetypeSignature
from archetype.core.interfaces import Component
from archetype.core.config import RunConfig
from archetype.core.archetype import Archetype

import logging

logger = logging.getLogger(__name__)

class QueryManager(iQueryManager):
    def __init__(self, store: iStore, debug: bool = False):
        self._store = store
        self._debug = debug
    
    def get_archetype(self, sig: ArchetypeSignature, world_id: str, run_id: str) -> DataFrame:
        """
        Get all archetypes that contain all of the specified component types for provided world_id and run_id. 
        """
        return self._store.get_archetype_df(sig, world_id, run_id)
    
    def query_archetype(
            self, 
            sig: ArchetypeSignature, 
            world_id: str, 
            ticks: Optional[List[int]] = None, 
            entity_ids: Optional[List[int]] = None,
            components: Optional[List['Component']] = None,
            run_config: RunConfig = None
        ) -> DataFrame:
        """
        Queries all active entities for the provided archetype signature, world_id, run_id. 
        Filters for ticks, entities, and components are provided. 

        Supports traditional ecs queries like: 
        - get_archetype_for_entity
        - get_archetype_for_tick
        - get_archetype_for_component
        - get_archetype_for_component_type
        - get_archetype_for_component_type_and_tick
        - get_archetype_for_component_type_and_tick_and_entity_id
        - get_archetype_for_component_type_and_tick_and_entity_id_and_component_type
        - get_component_for_entity
        - get_component_for_tick
        - get_component_for_component_type
        - get_component_for_component_type_and_tick
        - get_component_for_component_type_and_tick_and_entity_id
        - get_component_for_component_type_and_tick_and_entity_id_and_component_type
        
        """
        df =  self.get_archetype(sig, world_id, run_config.run_id)
        df = df.where(df["is_active"])

        # Filter to active entities with ticks
        if ticks:
            df = df.where(df["tick"].is_in(ticks))
        
        if entity_ids: 
            df = df.where(df["entity_id"].is_in(entity_ids))

        if components:
            a = Archetype(components)
            df = df.select(*a.schema.names())

        if run_config.debug:
            logger.info(f"Querying {Archetype.get_name(sig)} with {df.count_rows()} rows")
            df.explain()
            df.show()

        if run_config.enable_validation:
            self._validate(sig, df)

        return df
    
    def _validate(self, sig: ArchetypeSignature, df: DataFrame):
        # Query validation would entail ...
        raise NotImplementedError
