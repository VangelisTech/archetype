from daft import DataFrame
from .store import ArchetypeStore
from typing import Dict
from daft import col
from logging import getLogger

logger = getLogger(__name__)

class UpdateManager:
    def __init__(self, store: ArchetypeStore):
        self._store = store

    async def collect(self, updates: Dict[str, DataFrame], step: int):
        for sig_hash, df in updates.items():
            # Add the step column to the dataframe
            df = df.with_column("step", col("step").lit(step))

            # Get the signature from the hash
            sig = self._store._hash2sig[sig_hash]
            
            # Upsert the data into the table
            await self._store.upsert(sig, df.to_arrow())


