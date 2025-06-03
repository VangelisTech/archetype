from .async_world import AsyncWorld
from .async_store import AsyncStore
from .async_querier import AsyncQueryManager
from .async_system import AsyncSystem
from .async_processor import AsyncProcessor, async_processor
from .async_updater import AsyncUpdateManager


def make_async_world(uri: str, world_id: str | None = None, run_id: str | None = None, namespace: str | None = None, debug: bool = False, max_concurrent_archetypes: int = 10) -> AsyncWorld:    
    store   = AsyncStore(
        uri = uri, 
        namespace = namespace,
        debug = debug
    )
    querier = AsyncQueryManager(store=store)
    updater = AsyncUpdateManager(store=store)
    system  = AsyncSystem()
    world = AsyncWorld(
        store=store,
        querier=querier,
        updater=updater,
        system=system,
        world_id=world_id,
        run_id=run_id,
        max_concurrent_archetypes=max_concurrent_archetypes,
        debug=debug,
    )
    return world

__all__ = [
    "AsyncWorld",
    "AsyncStore",
    "AsyncQueryManager",
    "AsyncUpdateManager",
    "AsyncSystem",
    "AsyncProcessor",
    "async_processor", 
    "make_async_world",
]