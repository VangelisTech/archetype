from core.base import Processor
from core.system import EcsQueryInterface, EcsUpdateManager

class TimedProcessor(Processor):
    """
    A processor that can be prioritized.
    """
    def __init__(self, querier: EcsQueryInterface, updater: EcsUpdateManager):
        

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
    def __init__(self, querier: EcsQueryInterface):
        self.querier = querier()

    def process(self, *args: Any, **kwargs: Any) -> None:
        """
        The core logic of the processor.
        """
        raise NotImplementedError
    
    @daft.udf(return_type=daft.DataType.int64)
    def udf_process(self, *args: Any, **kwargs: Any) -> None: