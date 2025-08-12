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

import daft
from daft import col, DataFrame, lit

from archetype.core.interfaces import iAsyncUpdateManager, iAsyncStore
from archetype.core import ArchetypeSignature, Archetype
import time
import logging

logger = logging.getLogger(__name__)


class AsyncUpdateManager(iAsyncUpdateManager):
    def __init__(self, store: iAsyncStore, validate_flag: bool = False):
        self.store = store
        self.validate_flag = validate_flag

    async def update(self, df: DataFrame, sig: ArchetypeSignature, tick: int, world_id: str, run_id: str) -> DataFrame:
        try:
            df.collect()  # Moment of Materialization
            row_count = df.count_rows()

            if row_count == 0 or not df.column_names:
                logger.info(
                    f"Append skipped: archetype={Archetype.get_name(sig)} world_id={world_id} run_id={run_id} tick={tick} rows={row_count}"
                )
                return df
            
            df = df.with_columns({
                "tick": lit(tick).cast(daft.DataType.uint32()),
                "world_id": lit(str(world_id)),
                "run_id": lit(str(run_id)),
                "entity_id": col("entity_id").cast(daft.DataType.uint32()),
            })

            t0 = time.perf_counter()
            await self.store.append(sig, df)
            duration_ms = (time.perf_counter() - t0) * 1000
            logger.info(
                f"Append committed: archetype={Archetype.get_name(sig)} world_id={world_id} run_id={run_id} tick={tick} rows={row_count} duration_ms={duration_ms:.3f}"
            )

        except Exception as e:
            logger.error(f"Error updating table {Archetype.get_name(sig)}: {e}")
        return df

