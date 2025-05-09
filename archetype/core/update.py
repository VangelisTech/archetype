from daft import DataFrame
from .store import ArchetypeStore
from .base import Component
from typing import Optional, Dict, Tuple, Type
import daft
from logging import getLogger

logger = getLogger(__name__)

class UpdateManager:
    def __init__(self, store: ArchetypeStore):
        self._store = store
        self.step_updates = {}

    def commit(self, sig: Tuple[Type[Component], ...], df: DataFrame, step: int):
        self._store.archetypes[sig] = df.collect()

    def collect(self, updated_archetypes: Dict[str, DataFrame], step: int):
        """Collects the updates for the current step."""

        for hash, df in updated_archetypes.items():
            sig = self._store._hash2sig[hash]
            before_df = self._store.archetypes[sig]

            

            new_df = before_df.concat(materialized_step_df)

            self._store.archetype_data_by_hash(hash)
            logger.debug(f"UpdateManager.collect: Delegated update for sig_hash {hash} to ArchetypeStore.")

    def reset(self):
        self.step_updates = {}

