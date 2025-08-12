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
from archetype.core import ArchetypeSignature, Archetype, Component
from archetype.core.interfaces import iAsyncStore, iAsyncQueryManager

class AsyncQueryManager(iAsyncQueryManager):
    def __init__(self, store: iAsyncStore):
        self._store = store

    
    async def get_archetype(self, sig: ArchetypeSignature, world_id: str, run_id: str) -> DataFrame:
        """
        Get all archetypes that contain all of the specified component types for provided world_id and run_id. 
        """
        return await self._store.get_archetype_df(sig=sig, world_id=world_id, run_id=run_id)
    
    async def query_archetype(
            self, 
            sig: ArchetypeSignature, 
            world_id: str, 
            ticks: Optional[List[int]] = None, 
            entity_ids: Optional[List[int]] = None,
            components: Optional[List['Component']] = None,
            run_id: str = None
        ) -> DataFrame:
        """
        Queries all active entities for the provided archetype signature, world_id, run_id. 
        Filters for ticks, entities, and components are provided. 
        """
        df =  await self.get_archetype(sig=sig, world_id=world_id, run_id=run_id)
        df = df.where(df["is_active"])

        # Filter to active entities with ticks
        if ticks:
            df = df.where(df["tick"].is_in(ticks))
        
        if entity_ids: 
            df = df.where(df["entity_id"].is_in(entity_ids))

        if components:
            a = Archetype(components)
            # PyArrow Schema.names is a list property, not a callable
            df = df.select(*a.schema.names)

        return df
    
    async def _validate(self, sig: ArchetypeSignature, df: DataFrame):
        # No-op in baseline; validation lives in instrumentation layer
        return None

