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

from archetype.core.archetype import Archetype
from archetype.core.component import Component
from archetype.core.interfaces import ArchetypeSignature, iAsyncQueryManager, iAsyncStore


class AsyncQueryManager(iAsyncQueryManager):
    def __init__(self, store: iAsyncStore):
        self._store = store

    async def get_archetype(self, sig: ArchetypeSignature, world_id: str, run_id: str) -> DataFrame:
        """
        Get all archetypes that contain all of the specified component types for provided world_id and run_id.
        """
        return await self._store.get_archetype_df(
            sig=sig, world_id=world_id, run_id=run_id, active_only=True
        )

    async def query_archetype(
        self,
        sig: ArchetypeSignature,
        world_id: str,
        ticks: list[int] | None = None,
        entity_ids: list[int] | None = None,
        components: list["Component"] | None = None,
        run_id: str = None,
    ) -> DataFrame:
        """
        Queries all active entities for the provided archetype signature, world_id, run_id.
        Filters for ticks, entities, and components are provided.
        """
        df = await self._store.get_archetype_df(
            sig=sig,
            world_id=world_id,
            run_id=run_id,
            ticks=ticks,
            entity_ids=entity_ids,
            active_only=True,
        )

        if components:
            a = Archetype(components)
            # PyArrow Schema.names is a list property, not a callable
            df = df.select(*a.schema.names)

        return df

    async def query_components(
        self,
        components: list[type[Component]],
        world_id: str,
        run_id: str,
        *,
        ticks: list[int] | None = None,
        entity_ids: list[int] | None = None,
    ) -> DataFrame:
        """Query all entities that contain the requested component types.

        Discovers matching archetype signatures, queries each, projects to
        the requested component columns, and unions the results.
        """
        import daft
        import pyarrow as pa

        required = set(components)

        # Build the output schema from the requested components
        output_sig = tuple(sorted(components, key=lambda t: t.__name__))
        schema = Archetype.get_archetype_schema(output_sig)
        proj_cols = schema.names

        # Find all sigs that contain the required types
        all_sigs = await self.list_signatures()
        matching = [sig for sig in all_sigs if required.issubset(set(sig))]

        # Start with empty DataFrame of the right schema
        result = daft.from_arrow(pa.Table.from_batches([], schema=schema))

        for sig in matching:
            df = await self._store.get_archetype_df(
                sig=sig,
                world_id=world_id,
                run_id=run_id,
                ticks=ticks,
                entity_ids=entity_ids,
                active_only=True,
            )
            result = result.concat(df.select(*proj_cols))

        return result

    async def list_signatures(self) -> list[ArchetypeSignature]:
        """Delegate to the underlying store's signature registry."""
        return await self._store.list_signatures()

    async def _validate(self, sig: ArchetypeSignature, df: DataFrame):
        # No-op in baseline; validation lives in instrumentation layer
        return None
