from daft import DataFrame
from typing import Dict
from logging import getLogger
from .interfaces import iUpdater, iStore
logger = getLogger(__name__)

class UpdateManager(iUpdater):
    def __init__(self, store: iStore):
        self._store = store

    def __call__(self, updates: Dict[str, DataFrame]):
        for tablename, df in updates.items():
            try: 
                self.update(tablename, df)
            except Exception as e:
                logger.error(f"Error updating table {tablename}: {e}")

    def update(self, tablename: str, df: DataFrame):
        self._store.append(tablename, df)


