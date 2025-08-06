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

from daft import DataFrame
from ..interfaces import iQueryManager, iStore, ArchetypeSignature

class QueryManager(iQueryManager):
    def __init__(self, store: iStore, debug: bool = False):
        self._store = store
        self._debug = debug
    
    def get_archetype(self, sig: ArchetypeSignature, tick: int, world_id: str, run_id: str) -> DataFrame:
        """
        Get all archetypes that contain all of the specified component types.
        """
        # TODO: add role/ctx checks for data access control
        return self._store.get_archetype_df(sig, tick, world_id, run_id)

    def get_archetype_for_entity(self, entity_id: int, sig: ArchetypeSignature, tick: int, world_id: str, run_id: str) -> DataFrame:
        """Get a component for an entity."""
        df = self.get_archetype(sig, tick, world_id=world_id, run_id=run_id)
        return df.where(df["entity_id"] == entity_id)
    
    def get_component_for_entity(self, component_type: type, sig: ArchetypeSignature,  tick: int, world_id: str, run_id: str) -> DataFrame:
        """
        Get a component by type, merging state from across all archetypes
        """
        raise NotImplementedError
