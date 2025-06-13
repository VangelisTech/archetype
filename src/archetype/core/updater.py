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
from .interfaces import iUpdater, iStore, ArchetypeSignature
from .store import sig2hash
logger = getLogger(__name__)

class UpdateManager(iUpdater):
    def __init__(self, store: iStore):
        self.store = store

    def update(self, df: DataFrame, sig: ArchetypeSignature, step: int, world_id: str, run_id: str) -> None:
        """
        Update the store with the given DataFrame.
        """

        # Ensure step, world_id, and run_id are set for this update
        df = df.with_columns({"step": lit(step), "world_id": lit(world_id), "run_id": lit(run_id)})

        try:
            self.store.append(sig, df, step, world_id, run_id)
        except Exception as e:
            logger.error(f"Error updating table {sig2hash(sig)}: {e}")

    def materialize_spawns(self, spawn_cache: Dict[ArchetypeSignature, List[Dict[str, Any]]], world_id: str, run_id: str) -> None:
        self.store.materialize_spawns(spawn_cache, world_id, run_id)

    # in an async version, we would need to use a semaphore to limit the number of concurrent updates heavily.
    # In fact if we wanted to scale writes, we would need to use Ray workers to parallelize.
    # Thankfully, since we are writing to distinct tables, we can just use a semaphore and couple it toa worker.
    # We just wont be able to take advantage of the multithreading that Daft already uses to max out IOPS if we are running locally.
