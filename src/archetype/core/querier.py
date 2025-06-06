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

# Python
from daft import col, DataFrame
from typing import List, Type, Tuple, Sequence

from .base import Component
from .interfaces import iQuerier, iStore
from .store import ArchetypeSignature

class QueryManager(iQuerier):
    def __init__(self, store: iStore):
        self._store = store

    def _filter_on_step_and_active(self,
        archetypes: List[Tuple[ArchetypeSignature, DataFrame]],
          step: int
        ) -> List[Tuple[ArchetypeSignature, DataFrame]]:

        return [
            (
                sig,
                df.where(col("step").is_in([step])).where(df["is_active"])
            )
            for sig, df in archetypes
        ]

    def get_archetypes(self, step: int, world_id: str, run_id: str) -> List[Tuple[ArchetypeSignature, DataFrame]]:
        """
        Get all archetypes that contain all of the specified component types.
        """
        active_archetypes =  self._store.get_archetypes(world_id, run_id)
        if step == -1:
            return active_archetypes

        return self._filter_on_step_and_active(active_archetypes, step-1)

    def archetype_for_entity(self,
        entity_id: int,
        component_types: Sequence[Type[Component]],
        step: int
        ) -> DataFrame:
        """Get a component for an entity."""
        df = self._store.get_archetype_for_entity(entity_id, *component_types)
        if step == -1:
            return df
        return self._filter_on_step_and_active(df, step)
