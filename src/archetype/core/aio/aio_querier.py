from typing import Sequence, Type, AsyncGenerator, Tuple, List
from daft import DataFrame, col

from archetype.core.base import Component
from archetype.core.interfaces import iAsyncQuerier, iAsyncStore


class AsyncQueryManager(iAsyncQuerier):
    def __init__(self, store: iAsyncStore):
        self._store = store

    async def __call__(self,
        component_types: Sequence[Type[Component]],
        step: int,
        ) -> AsyncGenerator[Tuple[str, DataFrame], None]:
        async for item in self.async_query(*component_types, step=[step]):
            yield item

    async def async_query(self,
        *component_types: Type[Component], 
        step: List[int],
        ) -> AsyncGenerator[Tuple[str, DataFrame], None]:
        """
        Streams (archetype_name, DataFrame) tuples that match the query.
        Filters are applied as data streams in.
        """
        # The semaphore from the store is used internally by async_get_df,
        # which is called by async_get_archetypes_stream.
        # No need to acquire semaphore here directly if store methods handle it.
        
        async for archetype_name, df in self._store.async_get_archetypes_stream(*component_types):
            # Apply step and activity filters as data streams in
            # Ensure 'is_active' column exists or handle appropriately
            # This assumes 'is_active' is a boolean column.
            # If 'step' is empty, this might return all steps or need specific handling.
            if not step: # If step list is empty, perhaps return all steps? Or raise error?
                # For now, let's assume it means no step filtering if empty
                filtered_df = df.where(col("is_active"))
            else:
                filtered_df = df.where(col("step").is_in(step)) \
                               .where(col("is_active"))
            
            # Only yield if the DataFrame is not empty after filtering.
            # Daft DataFrames don't have an `is_empty()` method directly.
            # We can check `len(df) == 0` after a `collect()`, but that defeats streaming.
            # A more Daft-idiomatic way is to let downstream consumers handle empty DataFrames
            # or perform a cheap check if possible. For now, we'll yield it.
            # If `filtered_df` could be None from a UDF or complex operation, check that.
            yield (archetype_name, filtered_df)
    
    async def component_for_entity(self, entity_id: int, component_type: Type[Component]) -> Component:
        """Get a component for an entity asynchronously."""
        # This remains challenging in a pure streaming model without collecting.
        # It implies querying all relevant archetypes, then filtering down.
        # For a specific entity, you might need a direct lookup if the store supports it,
        # or accept the cost of iterating/collecting for this specific operation.
        raise NotImplementedError("async_component_for_entity needs a specific strategy for streaming architecture")

