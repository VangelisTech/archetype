from typing import Protocol, runtime_checkable, List, Type, Optional, Dict, Any, Set
from .base import Component, BaseProcessor # Import base types
from daft import DataFrame

@runtime_checkable
class ComponentStoreInterface(Protocol):
    """Interface for Component Storage operations."""
    components: Dict[Type[Component], DataFrame] # Allow read access?
    _dead_entities: Set[int] # Make internal state accessible if needed by others like UpdateManager

    def register_component(self, component: Type[Component]) -> None: ...
    def get_component(self, component: Type[Component]) -> DataFrame: ...
    def update_component(self, update_df: DataFrame, component: Type[Component]) -> None: ...
    def add_component(self, entity_id: int, component: Component, step: int) -> None: ...
    def add_entity(self, components: Optional[List[Component]] = None, step: Optional[int] = -1) -> int: ...
    def remove_entity(self, entity_id: int) -> None: ...
    def remove_component_from_entity(self, entity_id: int, component: Type[Component]) -> None: ... # Method seems unused externally?
    def remove_entity_from_component(self, entity_id: int, component_type: Type[Component], step: int) -> None: ... # Renamed in World, check usage
    def remove_component(self, component: Type[Component]) -> None: ... # Used internally by store? Needs review.
    def get_column_names(self, component: Type[Component]) -> List[str]: ...

@runtime_checkable
class QueryManagerInterface(Protocol):
    """Interface for querying ECS state."""
    store: ComponentStoreInterface
    
    def get_component(self, component: Type[Component]) -> DataFrame: ...
    def get_combined_state_history(self, *components: Type[Component]) -> List[DataFrame]: ...
    def get_latest_active_state_from_step(self, *components: Type[Component], step: Optional[int] = None) -> DataFrame: ...
    def component_for_entity(self, entity_id: int, component: Type[Component]) -> Optional[Component]: ...

@runtime_checkable
class UpdateManagerInterface(Protocol):
    """Interface for managing component updates."""
    store: ComponentStoreInterface

    def commit(self, update_df: DataFrame, components: List[Type[Component]]) -> None: ...
    def collect(self, step: int) -> None: ...
    def remove_dead_entities(self) -> None: ...
    def update_dead_entities(self) -> None: ...
    def clear_caches(self) -> None: ...

@runtime_checkable
class WorldInterface(Protocol):
    """Interface for the main ECS World."""
    store: ComponentStoreInterface
    querier: QueryManagerInterface
    updater: UpdateManagerInterface
    system: SystemInterface
    current_step: int

    # Public Methods
    def step(self, dt: float) -> None: ...
    def add_entity(self, components: Optional[List[Component]] = None, step: Optional[int] = None) -> int: ...
    def remove_entity(self, entity_id: int) -> None: ...
    def add_component(self, entity_id: int, component_instance: Component) -> None: ...
    def remove_component(self, entity_id: int, component_type: Type[Component], immediate: bool = False) -> None: ...
    def get_components(self, *components: Type[Component]) -> DataFrame: ...
    def get_components_from_step(self, *components: Type[Component], step: int) -> DataFrame: ...
    def component_for_entity(self, entity_id: int, component: Type[Component]) -> Optional[Component]: ...
    def add_processor(self, processor: ProcessorInterface, priority: Optional[int] = None) -> None: ... # Use ProcessorInterface
    def remove_processor(self, processor_type: Type[ProcessorInterface]) -> None: ... # Use ProcessorInterface type
    def get_processor(self, processor_type: Type[ProcessorInterface]) -> Optional[ProcessorInterface]: ... # Use ProcessorInterface type


@runtime_checkable
class ProcessorInterface(BaseProcessor, Protocol): 
    """Interface for Processors"""
    world: WorldInterface 

    def process(self, dt: float) -> None: ... # From Processor base

@runtime_checkable
class SystemInterface(Protocol):
    """Interface for system orchestration."""
    
    def execute(self, *args: Any, **kwargs: Any) -> Optional[Dict[ProcessorInterface, DataFrame]]: ... # Return type from SequentialSystem
    def add_processor(self, processor: ProcessorInterface) -> None: ... # Use ProcessorInterface
    def remove_processor(self, processor: ProcessorInterface) -> None: ... # Use ProcessorInterface
    def get_processor(self, processor: ProcessorInterface) -> Optional[ProcessorInterface]: ... # Use ProcessorInterface