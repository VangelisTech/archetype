from __future__ import annotations

import time
import logging

from daft import DataFrame

from archetype.core.aio.async_updater import AsyncUpdateManager
from archetype.core import ArchetypeSignature, Archetype
from archetype.core.config import RunConfig
from .profiling_shim import zone


logger = logging.getLogger(__name__)


class InstrumentedAsyncUpdateManager(AsyncUpdateManager):
    async def update(
        self,
        df: DataFrame,
        sig: ArchetypeSignature,
        tick: int,
        world_id: str,
        run_id: str,
    ) -> DataFrame:  # type: ignore[override]
        with zone(f"updater.update[{Archetype.get_name(sig)}]"):
            t0 = time.perf_counter()
            out = await super().update(df, sig, tick, world_id, run_id)
            duration_ms = (time.perf_counter() - t0) * 1000
            try:
                logger.info(
                    f"Update committed: archetype={Archetype.get_name(sig)} world_id={world_id} run_id={run_id} tick={tick} rows={out.count_rows()} duration_ms={duration_ms:.3f}"
                )
            except Exception:
                pass
            return out


