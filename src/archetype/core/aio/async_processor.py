from ..processor import Processor as SyncProcessor
from daft import DataFrame
import asyncio
from typing import Type
from ..base import Component

def async_processor(*component_types: Type[Component], priority: int = 0):
    """
    Class decorator to assign the list of components an AsyncProcessor reads/writes.
    Similar to @processor but for async processors.
    """
    def wrap(cls: Type[AsyncProcessor]):
        cls.components = tuple(sorted(component_types, key=lambda t: t.__name__))
        cls.priority = priority
        return cls
    return wrap

class AsyncProcessor(SyncProcessor):
    async def process(self, df: DataFrame, semaphore: asyncio.Semaphore, *args, **kwargs) -> DataFrame:
        """
        Async version of process method. Override this in subclasses.
        """
        return df