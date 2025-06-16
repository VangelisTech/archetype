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
    def __init__(self, store: iAsyncStore, debug: bool = False):
        self._store = store
        self._debug = debug

    async def get_archetype(self, sig: ArchetypeSignature, current_step: int, world_id: str, run_id: str) -> DataFrame:
        """
        Get all archetypes that contain all of the specified component types.
        """
        df = await self._store.get_archetype_df(sig, current_step-1, world_id, run_id)

        if self._debug:
            print(f"QueryManager: Getting archetype for {sig} at step {current_step} for world {world_id} and run {run_id}")
            df.show()

        return df
    async def get_archetype_for_entity(self, entity_id: int, sig: ArchetypeSignature, step: int, world_id: str, run_id: str) -> DataFrame:
        """Get a component for an entity."""
        df = await self.get_archetype(sig, current_step=step, world_id=world_id, run_id=run_id)

        return df.where(df["entity_id"] == entity_id)
