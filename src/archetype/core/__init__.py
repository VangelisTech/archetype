from .store import ArchetypeStore
from .querier import QueryManager
from .updater import UpdateManager
from .system import SimpleSystem
from .world import SimpleWorld 
from .base import Component
from .processor import Processor, processor

def make_simple_world(uri: str, simulation: str | None = None, run: str | None = None) -> SimpleWorld:    
    store   = ArchetypeStore(uri, simulation, run)
    querier = QueryManager(store=store)
    updater = UpdateManager(store=store)
    system  = SimpleSystem()
    
    world = SimpleWorld(
        store=store,
        querier=querier,
        updater=updater,
        system=system
    )
    return world

__all__ = [
    "SimpleWorld",
    "Processor",
    "processor",
    "Component",
    "make_simple_world"
]
