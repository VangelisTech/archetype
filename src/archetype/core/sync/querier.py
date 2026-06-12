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

from daft import DataFrame

from archetype.core.archetype import Archetype
from archetype.core.config import RunConfig
from archetype.core.interfaces import ArchetypeSignature, Component, iQueryManager, iStore

logger = logging.getLogger(__name__)


class QueryManager(iQueryManager):
    def __init__(self, store: iStore, debug: bool = False):
        self._store = store
        self._debug = debug

    def get_archetype(self, sig: ArchetypeSignature, world_id: str, run_id: str) -> DataFrame:
        """
        Get all archetypes that contain all of the specified component types for provided world_id and run_id.
        """
        return self._store.get_archetype_df(sig, world_id, run_id)

    def query_archetype(
        self,
        sig: ArchetypeSignature,
        world_id: str,
        ticks: list[int] | None = None,
        entity_ids: list[int] | None = None,
        components: list[type["Component"]] | None = None,
        run_config: RunConfig | None = None,
        run_id: str | None = None,
    ) -> DataFrame:
        """
        Queries all active entities for the provided archetype signature, world_id, run_id.
        Filters for ticks, entities, and components are provided.

        ``run_id`` takes precedence over ``run_config.run_id`` when both are provided,
        so callers that pin a stable run_id at the world layer can pass it through
        without rebuilding a frozen RunConfig.
        """
        if run_id is not None:
            effective_run_id = run_id
        elif run_config is not None:
            # RunConfig.run_id may be a UUID; storage stamps run_id as str.
            effective_run_id = str(run_config.run_id)
        else:
            # Reads MUST be scoped by world_id and run_id (spec §137).
            raise ValueError(
                "query_archetype requires either run_id or run_config to scope the read"
            )
        df = self.get_archetype(sig, world_id, effective_run_id)
        df = df.where(df["is_active"])

        # Filter to active entities with ticks
        if ticks:
            df = df.where(df["tick"].is_in(ticks))

        if entity_ids:
            df = df.where(df["entity_id"].is_in(entity_ids))

        if components:
            df = df.select(*Archetype.projection_columns(components))

        if run_config and run_config.debug:
            logger.info(f"Querying {Archetype.get_name(sig)} with {df.count_rows()} rows")
            df.explain()
            df.show()

        return df

    def list_signatures(self) -> list[ArchetypeSignature]:
        """Delegate to the underlying store's signature registry."""
        return self._store.list_signatures()
