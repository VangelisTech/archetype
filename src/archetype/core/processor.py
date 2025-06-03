from daft import DataFrame
from typing import Type, Tuple
from .base import Component, BaseProcessor

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

    def process(self, df: DataFrame, *args, **kwargs) -> DataFrame:
        """
        Processor method are provided the state of the archetype at the current step.
        Processors are not responsible for updating the step value. 
        """
        return df



