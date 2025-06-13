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

from typing import Tuple

from daft import DataFrame
from archetype.core.interfaces import ArchetypeSignature
from archetype.core.aio.async_interfaces import iAsyncStore, iAsyncQuerier

class AsyncQueryManager(iAsyncQuerier):
    def __init__(self, store: iAsyncStore):
        self._store = store

    async def get_archetype(self, sig: ArchetypeSignature, current_step: int, world_id: str, run_id: str) -> Tuple[ArchetypeSignature, DataFrame]:
        """
        Get all archetypes that contain all of the specified component types.
        """
        return await self._store.get_archetype(sig, current_step, world_id, run_id)

    async def get_archetype_for_entity(self, entity_id: int, sig: ArchetypeSignature, step: int, world_id: str, run_id: str) -> DataFrame:
        """Get a component for an entity."""
        return await self._store.get_archetype_for_entity(entity_id, sig, step, world_id, run_id)
