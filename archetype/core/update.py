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
        logger.debug(f"UpdateManager.commit: Materializing and storing data for signature {sig} at step {step}.")
        self._store.archetypes[sig] = df.collect()

    def collect(self, updated_archetypes: Dict[str, DataFrame], step: int):
        """Collects the updates for the current step by concatenating with existing archetype data."""
        if not updated_archetypes:
            logger.debug(f"UpdateManager.collect (step {step}): No new archetypes to process.")
            return

        logger.debug(f"UpdateManager.collect (step {step}): Processing {len(updated_archetypes)} archetypes.")
        for sig_hash, current_step_lazy_df in updated_archetypes.items():
            sig = self._store._hash2sig.get(sig_hash)
            if not sig:
                logger.warning(f"UpdateManager.collect (step {step}): Unknown sig_hash {sig_hash}. Skipping.")
                continue
            
            if sig in self._store.archetypes:
                before_df_lazy = self._store.archetypes[sig]
                logger.debug(f"UpdateManager.collect (step {step}): Concatenating new data for existing archetype {sig}.")
                new_combined_lazy_df = before_df_lazy.concat(current_step_lazy_df)
            else:
                logger.debug(f"UpdateManager.collect (step {step}): New archetype {sig}. Using its data directly.")
                new_combined_lazy_df = current_step_lazy_df
            
            self._store.archetypes[sig] = new_combined_lazy_df
            logger.debug(f"UpdateManager.collect (step {step}): Updated store for archetype {sig} (hash: {sig_hash}).")

    def reset(self):
        logger.debug("UpdateManager.reset called.")
        self.step_updates = {}

