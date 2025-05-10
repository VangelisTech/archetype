from .store import ArchetypeStore
from .querier       import QueryManager
from .updater      import UpdateManager
from .system        import SequentialSystem
from .world          import World
from .base           import Component
from .processor      import processor, Processor


import daft

def make_world(session: daft.Session) -> World:
    store   = ArchetypeStore()
    querier = QueryManager(store=store)
    updater = UpdateManager(store=store)
    system  = SequentialSystem()
    world   = World(
        session=session,
        store=store,
        querier=querier,
        updater=updater,
        system=system
    )
    return world

__all__ = [
    "World",
    "processor",
    "Processor",
    "Component",
    "make_world"
]
