from typing import Tuple
from ..core.base import Component
from ..core.processor import Processor

class Archetype: 
    def __init__(self, components: Tuple[Component, ...], processors: Tuple[Processor, ...]):
        self.components = components
        self.processors = processors

    def __str__(self):
        return f"Archetype(components={self.components}, processors={self.processors})"
