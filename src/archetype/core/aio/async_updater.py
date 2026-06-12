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

import logging
import time

import daft
from daft import DataFrame, col, lit

from archetype.core.archetype import Archetype
from archetype.core.interfaces import ArchetypeSignature, iAsyncStore, iAsyncUpdateManager

logger = logging.getLogger(__name__)


class AsyncUpdateManager(iAsyncUpdateManager):
    def __init__(self, store: iAsyncStore, validate_flag: bool = False):
        self.store = store
        self.validate_flag = validate_flag

    async def update(
        self, df: DataFrame, sig: ArchetypeSignature, tick: int, world_id: str, run_id: str
    ) -> DataFrame:
        # A schemaless frame (no columns) cannot be stamped and carries
        # nothing to persist — a legitimate no-op, distinct from a failure.
        if not df.column_names:
            logger.info(f"Update skipped (empty schema): archetype={Archetype.get_name(sig)}")
            return df
        try:
            df = df.with_columns(
                {
                    # Use signed 32-bit to match expected schema in union paths
                    "tick": lit(tick).cast(daft.DataType.int32()),
                    "world_id": lit(str(world_id)),
                    "run_id": lit(str(run_id)),
                    "entity_id": col("entity_id").cast(daft.DataType.int32()),
                }
            )

            t0 = time.perf_counter()
            await self.store.append(sig, df)
            duration_ms = (time.perf_counter() - t0) * 1000
            logger.info(
                f"Append committed: archetype={Archetype.get_name(sig)} world_id={world_id} run_id={run_id} tick={tick} duration_ms={duration_ms:.3f}"
            )

        except Exception as e:
            # Persistence failure must be observable: a tick that did not
            # commit its rows did not happen. Swallowing here would let the
            # world advance past a hole in the ledger.
            logger.error(f"Error updating table {Archetype.get_name(sig)}: {e}")
            raise
        return df
