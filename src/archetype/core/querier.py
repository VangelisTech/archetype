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
from daft import DataFrame
from typing import Tuple

from .interfaces import iQuerier, iStore, ArchetypeSignature

class QueryManager(iQuerier):
    def __init__(self, store: iStore):
        self._store = store


    def get_archetype(self, sig: ArchetypeSignature, current_step: int, world_id: str, run_id: str) -> Tuple[ArchetypeSignature, DataFrame]:
        """
        Get archetype data for a specific signature and step.
        """
        return self._store.get_archetype(sig, current_step, world_id, run_id)

    def get_archetype_for_entity(self, entity_id: int, sig: ArchetypeSignature, step: int, world_id: str, run_id: str) -> DataFrame:
        """Get archetype data for a specific entity."""
        return self._store.get_archetype_for_entity(entity_id, sig, step, world_id, run_id)
