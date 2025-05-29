from typing import Tuple, Type, AsyncGenerator
from daft import DataFrame, col, lit

from archetype.core.base import Component
from .aio_interfaces import iAsyncQuerier
from archetype.core.processor import BaseProcessor


class AsyncProcessor(BaseProcessor):
    
    priority: int = 0
    components: Tuple[Type[Component], ...] = None

    async def preprocess_stream(
        self, 
        querier: iAsyncQuerier, 
        step: int
    ) -> AsyncGenerator[Tuple[str, DataFrame], None]:
        """Queries the store and yields (archetype_name, DataFrame) tuples."""
        if not self.components:
            raise ValueError("AsyncProcessor must have components set. Use @async_processor decorator.")
        # The querier itself is now a stream producer
        async for archetype_name, df in querier(self.components, step=step):
            yield archetype_name, df
    
    async def process_stream(
        self, 
        archetype_name: str, 
        df_stream: DataFrame, # This is a single DataFrame for one archetype
        dt: float
    ) -> DataFrame:
        """Processes a single archetype DataFrame."""
        # Base implementation, to be overridden
        return df_stream 
    
    async def postprocess_stream(
        self, 
        df_processed: DataFrame, 
        step: int
    ) -> DataFrame:
        """Applies postprocessing, like updating the step."""
        # Ensure step column is updated correctly
        # This operation might need to be careful if df_processed is empty
        if len(df_processed) > 0: # Check if DataFrame is not empty before adding column
            return df_processed.with_column("step", lit(step))
        return df_processed
    

# Decorator for AsyncProcessors
def async_processor(*component_types: Type[Component], priority: int = 0):
    def wrap(cls: Type[AsyncProcessor]): # Ensure this type hint is correct
        if not issubclass(cls, AsyncProcessor):
            raise TypeError("Decorator @async_processor can only be used with AsyncProcessor subclasses.")
        cls.components = component_types
        cls.priority = priority
        return cls
    return wrap


@async_processor(priority=1) # Example usage
class ExampleAsyncProcessor(AsyncProcessor):
    async def process_stream(self, archetype_name: str, df_stream: DataFrame, dt: float) -> DataFrame:
        # Example: Add a dummy column
        if len(df_stream) > 0:
             return df_stream.with_column("processed_example", lit(True))
        return df_stream
