from daft import DataFrame
from typing import Dict
from daft import col
from logging import getLogger
from .interfaces import iUpdater, iStore
logger = getLogger(__name__)

class UpdateManager(iUpdater):
    def __init__(self, store: iStore):
        self._store = store

    async def __call__(self, updates: Dict[str, DataFrame], step: int):
        await self.collect(updates, step)

    async def collect(self, updates: Dict[str, DataFrame], step: int):
        for sig_hash, df in updates.items():
            # Add the step column to the dataframe
            df = df.with_column("step", col("step").lit(step))

            # Get the signature from the hash
            sig = self._store._hash2sig[sig_hash]
            
            # Upsert the data into the table
            await self._store.upsert(sig, df.to_arrow())


