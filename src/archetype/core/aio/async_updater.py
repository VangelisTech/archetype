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




from daft import DataFrame, lit
from typing import List, Dict, Any
from logging import getLogger
from archetype.core.aio.async_interfaces import iAsyncUpdater, iAsyncStore
from archetype.core.interfaces import ArchetypeSignature, Archetype

logger = getLogger(__name__)


class AsyncUpdateManager(iAsyncUpdater):
    def __init__(self, store: iAsyncStore):
        self.store = store

    async def update(self, df: DataFrame, sig: ArchetypeSignature, step: int, world_id: str, run_id: str) -> None:
        df = df.with_columns({"step": lit(step), "world_id": lit(world_id), "run_id": lit(run_id)})
        try:
            await self.store.append(sig, df, step, world_id, run_id)
        except Exception as e:
            logger.error(f"Error updating table {Archetype.get_name(sig)}: {e}")

    async def materialize_spawns(self, spawn_cache: Dict[ArchetypeSignature, List[Dict[str, Any]]], world_id: str, run_id: str) -> None:
        await self.store.materialize_spawns(spawn_cache, world_id, run_id)

    async def remove_entity(self, entity_id: int, sig: ArchetypeSignature, step: int, world_id: str, run_id: str) -> None:
        await self.store.remove_entity(entity_id, sig, step, world_id, run_id)
