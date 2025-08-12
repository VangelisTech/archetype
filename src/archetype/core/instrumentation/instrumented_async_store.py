from __future__ import annotations

import logging
import time
from daft import DataFrame

from archetype.core.aio.async_store import AsyncStore
from archetype.core import ArchetypeSignature, Archetype
from archetype.core.runtime.storage import StorageContext
from .profiling_shim import zone
from .logging_shim import log_event


logger = logging.getLogger(__name__)


class InstrumentedAsyncStore(AsyncStore):
    def __init__(self, context: StorageContext):
        super().__init__(context)
    
    async def get_archetype_df(self, sig: ArchetypeSignature, world_id: str, run_id: str) -> DataFrame:  # type: ignore[override]
        with zone(f"store.get_archetype_df[{Archetype.get_name(sig)}]"):
            t0 = time.perf_counter()
            df = await super().get_archetype_df(sig, world_id, run_id)
            duration_ms = (time.perf_counter() - t0) * 1000
            log_event(
                logging.DEBUG,
                "store_get_archetype",
                base={"world_id": world_id, "run_id": run_id},
                archetype=Archetype.get_name(sig),
                duration_ms=round(duration_ms, 3),
            )
            return df

    async def append(self, sig: ArchetypeSignature, df: DataFrame) -> None:  # type: ignore[override]
        with zone(f"store.append[{Archetype.get_name(sig)}]"):
            t0 = time.perf_counter()
            await super().append(sig, df)
            duration_ms = (time.perf_counter() - t0) * 1000
            log_event(
                logging.INFO,
                "store_append",
                archetype=Archetype.get_name(sig),
                rows=df.count_rows(),
                duration_ms=round(duration_ms, 3),
            )


