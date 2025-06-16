# Copyright 2025 Vangelis Technologies Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from .async_world import AsyncWorld
from .async_store import AsyncStore
from .async_querier import AsyncQueryManager
from .async_system import AsyncSystem
from .async_processor import AsyncProcessor
from .async_updater import AsyncUpdateManager
import asyncio


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
        querier=querier,
        updater=updater,
        async_system=system,
        world_id=world_id,
        run_id=run_id,
        semaphore=asyncio.Semaphore(max_concurrent_archetypes),
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
