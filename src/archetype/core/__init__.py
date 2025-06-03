from .store import ArchetypeStore
from .querier import QueryManager
from .updater import UpdateManager
from .system import SyncSystem
from .world import SimpleWorld 
from .base import Component
from .processor import Processor, processor
from daft.catalog import Catalog


def make_simple_world(uri: str, world_id: str | None = None, run_id: str | None = None, namespace: str | None = None, catalog: Catalog | None = None, debug: bool = False) -> SimpleWorld:    
    store   = ArchetypeStore(
        uri = uri, 
        namespace = namespace,
        catalog = catalog,
        debug = debug
    )
    querier = QueryManager(store=store)
    updater = UpdateManager(store=store)
    system  = SyncSystem()
    
    world = SimpleWorld(
        store=store,
        querier=querier,
        updater=updater,
        system=system,
        world_id=world_id,
        run_id=run_id,
        debug=debug,
    )
    return world

__all__ = [
    "SimpleWorld",
    "Processor",
    "processor",
    "Component",
    "make_simple_world",
    "InputProcessor"
]
