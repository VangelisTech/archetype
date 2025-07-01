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
from .interfaces import iQuerier, iStore, ArchetypeSignature

class QueryManager(iQuerier):
    def __init__(self, store: iStore, debug: bool = False):
        self._store = store
        self._debug = debug

    def get_archetype(self, sig: ArchetypeSignature, tick: int, world_id: str, run_id: str) -> DataFrame:
        """
        Get archetype data for a specific signature and tick.
        """
        df = self._store.get_archetype_df(sig)
        tick = max(0, tick - 1)

        # Filter by tick, world_id, and run_id
        df = df.where(df["world_id"] == world_id) \
               .where(df["run_id"] == run_id) \
               .where(df["tick"] == tick) \
               .where(df["is_active"])

        if self._debug:
            print(f"QueryManager: Getting archetype for {sig} at tick {tick} for world {world_id} and run {run_id}")
            df.show()

        return df

    def get_archetype_for_entity(self, entity_id: int, sig: ArchetypeSignature, tick: int, world_id: str, run_id: str) -> DataFrame:
        """Get archetype data for a specific entity."""
        df = self.get_archetype(sig, tick, world_id, run_id)

        return df.where(df["entity_id"] == entity_id)
