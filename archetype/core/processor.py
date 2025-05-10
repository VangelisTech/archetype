import daft
from daft import DataFrame
from typing import Type, Tuple, TYPE_CHECKING, Optional, Union, List
from .base import Component, BaseProcessor

if TYPE_CHECKING:
    from .world import World

class Processor(BaseProcessor):

    components: Tuple[Type[Component], ...] = None
    priority: int = 0
    def preprocess(self, world: World, step: int) -> DataFrame:
        if not self.components:
            raise ValueError("Processor must have at least one component set at self._components. Use @processor(Component1, Component2) decorator")
        return world.query(self.components, step=step, entities=self.entities)
    

    def process(self, df: DataFrame) -> DataFrame | None:
        raise NotImplementedError("must implement in subclass")


def processor(*component_types: Type[Component], entities: Optional[Union[int, List[int]]] = None, priority: int = 0):
    """
    Class decorator to assign the list of components a Processor reads/writes.
    It also injects the `__init__`, `_fetch_state`, and `process` methods into the class.
    """
    def wrap(cls):
        
        cls.components = list(component_types)
        cls.entities = entities
        cls.priority = priority
        return cls
    return wrap

