from __future__ import annotations

import logging
from typing import Optional, List

from daft import DataFrame

from archetype.core.aio.async_querier import AsyncQueryManager
from archetype.core.aio.async_interfaces import iAsyncStore
from archetype.core import ArchetypeSignature, Archetype, Component
from archetype.core.config import RunConfig
from .profiling_shim import zone
from .logging_shim import show_df, show_plan


logger = logging.getLogger(__name__)


class InstrumentedAsyncQueryManager(AsyncQueryManager):
    def __init__(self, store: iAsyncStore):
        super().__init__(store)

    async def query_archetype(
        self,
        sig: ArchetypeSignature,
        world_id: str,
        ticks: Optional[List[int]] = None,
        entity_ids: Optional[List[int]] = None,
        components: Optional[List[Component]] = None,
        run_id: str | None = None,
        run_config: RunConfig | None = None,
    ) -> DataFrame:  # type: ignore[override]
        run_config = run_config or RunConfig()
        with zone(f"querier.query_archetype[{Archetype.get_name(sig)}]"):
            df = await super().query_archetype(sig, world_id, ticks, entity_ids, components, run_id)

            if run_config.debug:
                try:
                    logger.info(
                        f"Querying {Archetype.get_name(sig)} with {df.count_rows()} rows"
                    )
                except Exception:
                    pass
                show_df(None, f"querier.query_archetype[{Archetype.get_name(sig)}]", df, run_config)
                show_plan(None, f"querier.query_archetype[{Archetype.get_name(sig)}]", df, run_config)

            return df


