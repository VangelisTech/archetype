from ..processor import Processor as SyncProcessor
from daft import DataFrame
import asyncio

class AsyncProcessor(SyncProcessor):
    async def process(self, df: DataFrame, semaphore: asyncio.Semaphore, *args, **kwargs) -> DataFrame:
        """
        Async version of process method. Override this in subclasses.
        """
        return df