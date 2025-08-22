from __future__ import annotations

import logging
import time
from daft import DataFrame

from archetype.core.aio.async_store import AsyncStore
from archetype.core.interfaces import iAsyncStore
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


class InstrumentedStoreWrapper(iAsyncStore):
    """
    Delegating wrapper that preserves the selected backend/caching and injects
    instrumentation around i/o calls. Use when the concrete store may not be
    an AsyncStore subclass (e.g., cached store, LanceDB store).
    """
    def __init__(self, inner: iAsyncStore):
        self._inner = inner

    async def get_archetype_df(self, sig: ArchetypeSignature, world_id: str, run_id: str) -> DataFrame:  # type: ignore[override]
        with zone(f"store.get_archetype_df[{Archetype.get_name(sig)}]"):
            t0 = time.perf_counter()
            df = await self._inner.get_archetype_df(sig, world_id, run_id)
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
            await self._inner.append(sig, df)
            duration_ms = (time.perf_counter() - t0) * 1000
            log_event(
                logging.INFO,
                "store_append",
                archetype=Archetype.get_name(sig),
                rows=df.count_rows(),
                duration_ms=round(duration_ms, 3),
            )

    async def shutdown(self) -> None:
        return await self._inner.shutdown()

