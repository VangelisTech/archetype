from core.base import Processor
from core.systems import EcsQueryInterface, EcsUpdateManager

class TimedProcessor(Processor):
    """
    A processor that can be prioritized.
    """
    def __init__(self, world: World, *inputs: Any):
        super().__init__(world)
        

    def process(self, dt: float, *args: Any, **kwargs: Any) -> None:
        """
        The core logic of the processor.

        Args:
            querier: Interface to read committed component state.
            updater: Interface to queue component updates.
            dt: Time delta for the current step.
            *args, **kwargs: Additional arguments passed from the world's process cycle.
        """
        raise NotImplementedError
    
class UdfProcessor(Processor):
    """
    A processor that can be prioritized.
    """
    def __init__(self, world: World):
        self.world = world

    def process(self, *args: Any, **kwargs: Any) -> None:
        """
        The core logic of the processor.
        """
        raise NotImplementedError
    
    @daft.udf(return_type=daft.DataType.int64)
    def udf_process(self, *args: Any, **kwargs: Any) -> None:
        pass

class NodeProcessor(Processor):
    pass

class EdgeProcessor(Processor):
    """adds and removes and updates relationships between entities""" 
    pass


if __name__ == "__main__":
    from core.base import Component

    class Position(Component):
        x: float = 0.0
        y: float = 0.0
        z: float = 0.0

    class Velocity(Component):
        vx: float = 0.0
        vy: float = 0.0
        vz: float = 0.0

    class Acceleration(Component):
        ax: float = 0.0
        ay: float = 0.0
        az: float = 0.0
    # 