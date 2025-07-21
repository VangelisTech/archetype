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

import time
from typing import List, Optional, Tuple, Set, Dict, Any, Type
from itertools import count

from daft import DataFrame

from archetype.core.interfaces import Component, Archetype, ArchetypeSignature, iSystem, iQueryManager, iUpdateManager
from archetype.core.processor import Processor
from archetype.core.base import BaseWorld

from logging import getLogger

logger = getLogger(__name__)

import time
from typing import List, Optional, Tuple, Set, Dict, Any, Type
from itertools import count
import ulid

from daft import DataFrame

from archetype.core.interfaces import Component, Archetype, ArchetypeSignature, iSystem, iQueryManager, iUpdateManager
from archetype.core.processor import Processor
from archetype.core.base import BaseWorld
from archetype.core.broker import SyncCommandQueue
from archetype.core.command import Command

from logging import getLogger

logger = getLogger(__name__)

class SyncWorld(BaseWorld):
    def __init__(self,
        querier: iQueryManager,
        updater: iUpdateManager,
        system: iSystem,
        world_id: str | None = None,
        run_id: str | None = None,
        debug: bool = False,
    ):
        self.querier = querier
        self.updater = updater
        self.system = system

        # Set World Properties
        self.world_id = world_id or f"world_{str(ulid.ULID())}"
        self.run_id = run_id or f"run_{str(ulid.ULID())}"
        self.tick = 0
        self.debug = debug

        # Initialize internal caches
        self._entity2sig: Dict[int, ArchetypeSignature] = {}
        self._entity_counter = count(start=1)
        self._spawn_cache: Dict[ArchetypeSignature, List[Dict[str, Any]]] = {}
        self._despawn_cache: Dict[ArchetypeSignature, List[int]] = {}
        self._transmute_cache: Dict[Tuple[ArchetypeSignature, ArchetypeSignature], List[int]] = {}

    def step(self, *args, **kwargs):
        """
        Execute one simulation tick by processing commands and then running systems.
        """
        start = time.time()

        # 1. Dequeue and process commands
        due_commands = self.cmd_queue.dequeue_due(tick=self.tick)
        self._process_commands(due_commands)

        # 2. Materialize state changes from commands
        self._materialize_changes()

        # 3. Execute systems on active archetypes
        for sig in self.get_active_signatures():
            df = self.get_archetype(sig, self.tick -1)
            if df is not None and df.count_rows() > 0:
                processed_df = self.execute(df, sig, *args, **kwargs)
                self.update(processed_df, sig, self.tick)

        end = time.time()
        logger.info(f"Sync tick {self.tick} done in {end-start:.3f}s")

        self.tick += 1

    def get_active_signatures(self) -> Set[ArchetypeSignature]:
        """Return the set of active archetype signatures."""
        return set(self._entity2sig.values())

    def _process_commands(self, commands: List[Command]):
        """Route commands to their respective handlers."""
        for cmd in commands:
            handler = getattr(self, f"_{cmd.op}", None)
            if handler:
                handler(cmd.payload)
            else:
                logger.warning(f"No handler for command op: {cmd.op}")

    def _materialize_changes(self):
        """Flush the internal caches to the updater."""
        if self._spawn_cache:
            self.updater.materialize_spawns(self._spawn_cache, self.world_id, self.run_id)
            self._spawn_cache.clear()

        if self._despawn_cache:
            for sig, entity_ids in self._despawn_cache.items():
                for entity_id in entity_ids:
                    self.updater.remove_entity(entity_id, sig, self.tick, self.world_id, self.run_id)
            self._despawn_cache.clear()

        if self._transmute_cache:
            for (old_sig, new_sig), entity_ids in self._transmute_cache.items():
                # This is a simplification. A real implementation would need to
                # fetch the entity's data, create the new component data, and
                # then call a transition function on the updater.
                # For now, we'll just log it.
                logger.info(f"Transmuting entities {entity_ids} from {old_sig} to {new_sig}")
            self._transmute_cache.clear()

    # ---------------------------------------------------------------------
    # Internal Command Handlers
    # ---------------------------------------------------------------------

    def _create_entity(self, payload: Dict):
        components = [Component.from_dict(c) for c in payload["components"]]
        
        entity_id = next(self._entity_counter)
        archetype = Archetype(components)
        self._entity2sig[entity_id] = archetype.sig

        row_dict = Archetype.to_row_dict(
            entity_id=entity_id,
            tick=self.tick,
            components=components,
            world_id=self.world_id,
            run_id=self.run_id
        )

        if archetype.sig not in self._spawn_cache:
            self._spawn_cache[archetype.sig] = []
        self._spawn_cache[archetype.sig].append(row_dict)

    def _delete_entity(self, payload: Dict):
        entity_id = payload["entity_id"]
        sig = self._entity2sig.pop(entity_id, None)
        if sig:
            if sig not in self._despawn_cache:
                self._despawn_cache[sig] = []
            self._despawn_cache[sig].append(entity_id)

    def _add_component(self, payload: Dict):
        entity_id = payload["entity_id"]
        components = [Component.from_dict(c) for c in payload["components"]]
        sig = self._entity2sig.get(entity_id)
        if sig:
            new_sig = Archetype.add_components(sig, components)
            if new_sig != sig:
                self._entity2sig[entity_id] = new_sig
                if (sig, new_sig) not in self._transmute_cache:
                    self._transmute_cache[(sig, new_sig)] = []
                self._transmute_cache[(sig, new_sig)].append(entity_id)

    def _remove_component(self, payload: Dict):
        entity_id = payload["entity_id"]
        # This requires a way to get the type from the name.
        # This is a simplification for now.
        component_types = [Component.get_type_by_name(name) for name in payload["component_types"]]
        sig = self._entity2sig.get(entity_id)
        if sig:
            new_sig = Archetype.remove_components(sig, component_types)
            if new_sig != sig:
                self._entity2sig[entity_id] = new_sig
                if (sig, new_sig) not in self._transmute_cache:
                    self._transmute_cache[(sig, new_sig)] = []
                self._transmute_cache[(sig, new_sig)].append(entity_id)

    def _add_processor(self, payload: Dict):
        self.system.add_processor(payload["processor"])

    def _remove_processor(self, payload: Dict):
        self.system.remove_processor(payload["processor"])

    # ---------------------------------------------------------------------
    # Facade Methods for Querier, Updater, System
    # ---------------------------------------------------------------------

    def update(self, df: DataFrame, sig: ArchetypeSignature, tick: int):
        self.updater.update(df, sig, tick, self.world_id, self.run_id)

    def get_archetype(self, sig, tick: int) -> DataFrame:
        return self.querier.get_archetype(sig, tick, self.world_id, self.run_id)

    def archetype_for_entity(self, entity_id: int, tick: int) -> DataFrame:
        sig = self._entity2sig[entity_id]
        return self.querier.get_archetype_for_entity(entity_id, sig, tick, self.world_id, self.run_id)

    def execute(self, df: DataFrame, sig: ArchetypeSignature, *args, **kwargs) -> DataFrame:
        return self.system.execute(df, sig, *args, **kwargs)

