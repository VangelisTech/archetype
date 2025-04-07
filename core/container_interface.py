from dependency_injector import containers, providers
from .interfaces import ( # Import only interfaces
    ComponentStoreInterface,
    QueryManagerInterface,
    UpdateManagerInterface,
    SystemInterface,
    WorldInterface,
    ProcessorInterface # Processor is not directly provided, but needed if other interfaces depend on it
)

class CoreContainerInterface(containers.DeclarativeContainer):
    """Defines the providers interface."""
    config = providers.Configuration()

    # Use providers.Dependency with instance_of=<Interface>
    store = providers.Dependency(instance_of=ComponentStoreInterface)
    query_interface = providers.Dependency(instance_of=QueryManagerInterface)
    update_manager = providers.Dependency(instance_of=UpdateManagerInterface)
    system = providers.Dependency(instance_of=SystemInterface)
    world = providers.Dependency(instance_of=WorldInterface)

    # Note: Processors are typically not provided directly by the container
    # as they are often instantiated manually or dynamically added.
    # If a specific processor instance *was* always required and managed
    # by the container, you would add:
    # some_processor = providers.Dependency(instance_of=SomeSpecificProcessorInterface) 