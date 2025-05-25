from .store import ArchetypeStore
from .querier import QueryManager
from .updater import UpdateManager
from .system import SimpleSystem
from .world import SimpleWorld 
from .base import Component
from .processor import Processor, processor
from daft.catalog import Catalog


def make_simple_world(uri: str, simulation: str | None = None, run: str | None = None, namespace: str | None = None, catalog: Catalog | None = None, debug: bool = False) -> SimpleWorld:    
    store   = ArchetypeStore(
        uri = uri, 
        simulation = simulation, 
        run = run, 
        namespace = namespace,
        catalog = catalog,
        debug = debug
    )
    querier = QueryManager(store=store)
    updater = UpdateManager(store=store)
    system  = SimpleSystem()
    
    world = SimpleWorld(
        store=store,
        querier=querier,
        updater=updater,
        system=system,
    )
    return world

__all__ = [
    "SimpleWorld",
    "Processor",
    "processor",
    "Component",
    "make_simple_world"
]
