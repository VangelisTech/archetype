from .store import ArchetypeStore
from .querier       import QueryManager
from .updater      import UpdateManager
from .system        import SimpleSystem
from .world          import World 
from .base           import Component
from .processor      import Processor, processor

def make_world(uri: str, simulation: str | None = None, run: str | None = None) -> World:
    store   = ArchetypeStore(uri, simulation, run)
    querier = QueryManager(store=store)
    updater = UpdateManager(store=store)
    system  = SimpleSystem(querier=querier)
    world   = World(
        store=store,
        querier=querier,
        updater=updater,
        system=system
    )
    return world

__all__ = [
    "World",
    "Processor",
    "processor",
    "Component",
    "make_world"
]
