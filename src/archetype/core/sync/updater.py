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
from typing import List, Dict, Any
from logging import getLogger
from archetype.core.interfaces import iUpdateManager, iStore, ArchetypeSignature, Archetype

logger = getLogger(__name__)

class UpdateManager(iUpdateManager):
    def __init__(self, store: iStore):
        self.store = store

    def update(self, df: DataFrame, sig: ArchetypeSignature, tick: int, world_id: str, run_id: str) -> None:
        """
        Update the store with the given DataFrame.
        """
        df = df.with_columns({
            "tick": lit(tick).cast(daft.DataType.uint32()),
            "world_id": lit(world_id),
            "run_id": lit(run_id),
            "entity_id": col("entity_id").cast(daft.DataType.uint32()),
        })
        try:
            self.store.update(sig, df)
        except Exception as e:
            logger.error(f"Error updating table {Archetype.get_name(sig)}: {e}")

