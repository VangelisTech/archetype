from daft import DataFrame
from typing import Type, Tuple
from .base import Component, BaseProcessor
from .interfaces import iQuerier

class Processor(BaseProcessor):
    components: Tuple[Type[Component], ...] = None
    priority: int = 0
    def preprocess(self, querier: iQuerier, step: int) -> DataFrame:
        if not self.components:
            raise ValueError("Processor must have at least one component set at self._components. Use @processor(Component1, Component2) decorator")
        return querier(self.components, step=step)

    def process(self, df: DataFrame) -> DataFrame:
        return df


def processor(priority: int = 0, *component_types: Type[Component]):
    """
    Class decorator to assign the list of components a Processor reads/writes.
    It also injects the `__init__`, `_fetch_state`, and `process` methods into the class.
    """
    def wrap(cls: Type[Processor]):
        cls.components = list(component_types)
        cls.priority = priority
        return cls
    return wrap

