from daft import DataFrame
from typing import Dict
from logging import getLogger
from .interfaces import iUpdater, iStore
logger = getLogger(__name__)

class UpdateManager(iUpdater):
    def __init__(self, store: iStore):
        self._store = store

    async def __call__(self, updates: Dict[str, DataFrame], step: int):
        await self.collect(updates, step)

    async def collect(self, updates: Dict[str, DataFrame], step: int):
        for tablename, df in updates.items():
            await self._store.upsert(tablename, df, step)


