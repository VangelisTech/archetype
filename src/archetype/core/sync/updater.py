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

from logging import getLogger

import daft
from daft import DataFrame, col, lit

from archetype.core.archetype import Archetype
from archetype.core.interfaces import ArchetypeSignature, iStore, iUpdateManager

logger = getLogger(__name__)


class UpdateManager(iUpdateManager):
    def __init__(self, store: iStore):
        self.store = store

    def update(
        self, df: DataFrame, sig: ArchetypeSignature, tick: int, world_id: str, run_id: str
    ) -> DataFrame:
        """
        Update the store with the given DataFrame.
        Returns the DataFrame with stamped metadata for live snapshots.
        """
        # A schemaless frame (no columns) cannot be stamped and carries
        # nothing to persist — a legitimate no-op, distinct from a failure.
        if not df.column_names:
            logger.info(f"Update skipped (empty schema): archetype={Archetype.get_name(sig)}")
            return df
        df = df.with_columns(
            {
                "tick": lit(tick).cast(daft.DataType.uint32()),
                "world_id": lit(str(world_id)),
                "run_id": lit(str(run_id)),
                "entity_id": col("entity_id").cast(daft.DataType.uint32()),
                # The sync stack runs uncoordinated: implicit epoch-0 commit
                # identity, everything visible (issue #273). The async stack
                # is the coordinated path.
                "commit_token": lit(""),
                "writer_epoch": lit(0).cast(daft.DataType.int64()),
            }
        )
        try:
            self.store.append(sig, df)
        except Exception as e:
            # Same contract as AsyncUpdateManager: persistence failure must be
            # observable, so a failed append raises instead of returning a
            # stamped-but-uncommitted frame.
            logger.error(f"Error updating table {Archetype.get_name(sig)}: {e}")
            raise
        return df
