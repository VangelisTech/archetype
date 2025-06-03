


from daft import DataFrame, lit
from typing import List, Tuple, Dict, Any
from logging import getLogger
from archetype.core.aio.async_interfaces import iAsyncUpdater, iAsyncStore
from archetype.core.store import ArchetypeSignature, sig2hash

logger = getLogger(__name__)


class AsyncUpdateManager(iAsyncUpdater):
    def __init__(self, store: iAsyncStore):
        self.store = store

    def _set_step(self,
        archetypes: Tuple[ArchetypeSignature, DataFrame],
        step: int
        ) -> Tuple[ArchetypeSignature, DataFrame]:
        sig, df = archetypes
        return (sig, df.with_column("step", lit(step)))    

    async def update(self, archetypes: List[Tuple[ArchetypeSignature, DataFrame]], step: int, world_id: str, run_id: str) -> None:
        archetypes = self._set_step(archetypes, step)
        try: 
            sig, df = archetypes
            await self.store.append(sig, df, step, world_id, run_id)
        except Exception as e:
            logger.error(f"Error updating table {sig2hash(sig)}: {e}")

    async def materialize_spawns(self, spawn_cache: Dict[ArchetypeSignature, List[Dict[str, Any]]], world_id: str, run_id: str) -> None:
        await self.store.materialize_spawns(spawn_cache, world_id, run_id)



