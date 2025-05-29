from daft import DataFrame, lit, col
from typing import Type, Tuple, Dict, List
from .base import Component, BaseProcessor
from .interfaces import iQuerier, iAsyncQuerier


def processor(*component_types: Type[Component], priority: int = 0):
    """
    Class decorator to assign the list of components a Processor reads/writes.
    It also injects the `__init__`, `_fetch_state`, and `process` methods into the class.
     
    """
    def wrap(cls: Type[BaseProcessor]):
        cls.components = component_types
        cls.priority = priority
        return cls
    return wrap


class Processor(BaseProcessor):
    priority: int = 0
    components: Tuple[Type[Component], ...] = None
    def preprocess(self, querier: iQuerier, step: int) -> Dict[str, DataFrame]:
        if not self.components:
            raise ValueError("Processor must have at least one component set at self.components. Use @processor(Component1, Component2) decorator")
        return querier.get_matching_archetypes(self.components, step=[step])

    def process(self, df: DataFrame, dt: float) -> DataFrame:
        "Processor method are allowed to query more than one step at a time, but must return a single step."
        return df
    
    def postprocess(self, df: DataFrame, step: int) -> DataFrame:
        "Processor method are allowed to query more than one step at a time, but must return a single step."
        return df.with_column("step", lit(step))

class InputProcessor(BaseProcessor):
    def __init__(self, entity_ids: List[int]):
        self.entity_ids = entity_ids

    def preprocess(self, querier: iQuerier, step: int) -> DataFrame:
        df = querier.archetype_for_entity(self.entity_ids, self.components, step=[step])
        return df
    
    def process(self, df: DataFrame, dt: float = None) -> DataFrame:
        return df
    
    def postprocess(self, df: DataFrame, step: int) -> DataFrame:
        return df


