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

from .store import SyncStore
from .querier import QueryManager
from .updater import UpdateManager
from .system import SyncSystem
from .world import SyncWorld
from .processor import Processor, processor
from .interfaces import Component, ArchetypeSignature, Archetype


from daft.catalog import Catalog

def make_simple_world(
        uri: str,
        world_id: str | None = None,
        run_id: str | None = None,
        namespace: str | None = None,
        catalog: Catalog | None = None,
        debug: bool = False
    ) -> SyncWorld:

    store = SyncStore(
        uri = uri,
        namespace = namespace,
        catalog = catalog,
        debug = debug
    )
    querier = QueryManager(store=store, debug=debug)
    updater = UpdateManager(store=store)
    system  = SyncSystem()

    world = SyncWorld(
        querier=querier,
        updater=updater,
        system=system,
        world_id=world_id,
        run_id=run_id,
        debug=debug,
    )
    return world

__all__ = [
    "Component",
    "ArchetypeSignature",
    "Archetype",
    "make_simple_world",
    "SyncWorld",
    "SyncStore",
    "SyncSystem",
    "Processor",
    "processor",
    "Component",
]
