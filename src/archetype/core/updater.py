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
from typing import List, Tuple
from logging import getLogger
from .interfaces import iUpdater, iStore
from .store import ArchetypeSignature
from .store import sig2hash
logger = getLogger(__name__)

class UpdateManager(iUpdater):
    def __init__(self, store: iStore):
        self._store = store

    def _set_step(self,
        archetypes: List[Tuple[ArchetypeSignature, DataFrame]],
        step: int
        ) -> List[Tuple[ArchetypeSignature, DataFrame]]:

        return [
            (
                sig,
                df.with_column("step", lit(step))
            )
            for sig, df in archetypes
        ]

    def update(self, updates: List[Tuple[ArchetypeSignature, DataFrame]], step: int, world_id: str, run_id: str):
        updates = self._set_step(updates, step)
        for sig, df in updates:
            try:
                self._store.append(sig, df, step, world_id, run_id)
            except Exception as e:
                logger.error(f"Error updating table {sig2hash(sig)}: {e}")

    # in an async version, we would need to use a semaphore to limit the number of concurrent updates heavily.
    # In fact if we wanted to scale writes, we would need to use Ray workers to parallelize.
    # Thankfully, since we are writing to distinct tables, we can just use a semaphore and couple it toa worker.
    # We just wont be able to take advantage of the multithreading that Daft already uses to max out IOPS if we are running locally.
