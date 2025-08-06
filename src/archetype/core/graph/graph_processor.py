from typing import List, Type, Callable, Dict, Any
from enum import Enum, auto
import daft

from archetype.core.base import BaseProcessor


class UpdateStage(Enum):
    """Defines the main execution stages for a simulation tick."""
    PreUpdate = auto()
    Update = auto()
    PostUpdate = auto()


class GraphProcessor(BaseProcessor):
    """The new base class for processors, holding scheduling metadata."""
    reads: List[Type] = []
    writes: List[Type] = []
    stage: UpdateStage = UpdateStage.Update
    run_condition: Callable[[Dict[str, Any]], bool] = lambda state: True
    is_eager: bool = False

    async def process(self, df: daft.DataFrame, **kwargs) -> daft.DataFrame:
        """The core logic of the processor."""
        raise NotImplementedError