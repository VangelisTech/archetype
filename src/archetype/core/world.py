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
from typing import List, Optional, Tuple
from daft import DataFrame
from daft.session import Session
from .processor import Processor
from .base import Component
import ulid
from .interfaces import iSystem, iStore, iQuerier, iUpdater, iWorld, ArchetypeSignature

class SimpleWorld(iWorld):
    def __init__(self,
        store: iStore,
        querier: iQuerier,
        updater: iUpdater,
        system: iSystem,
        world_id: str | None = None,
        run_id: str | None = None,
        debug: bool = False,
    ):
        # Inject dependencies
        self.store      = store
        self.querier    = querier
        self.updater    = updater
        self.system     = system

        # Initialize the world state
        self.world_id: str = world_id or str(ulid.ULID())
        self.run_id: str = run_id or str(ulid.ULID())
        self.debug: bool = debug
        self.current_step = 0


    def step(self, *args, **kwargs):
        # Materialize any pending spawns before the first step
        self.materialize_spawns()

        start = time.time()

        # 1) run all processors in sequence
        updated_archetypes = self.execute(self.querier, self.current_step, *args, **kwargs)

        # 2) Materialize changes into the ArchetypeStore
        self.update(updated_archetypes, self.current_step)


        end = time.time()
        print(f"Step {self.current_step} done in {end-start:.3f}s")

        # Increment the step counter
        self.current_step += 1

    # ---------------------------------------------------------------------
    # Entity Management (Store Facade Methods)
    # ---------------------------------------------------------------------

    def spawn(self,
              *components: Component,
              step: Optional[int] = None
             ) -> int:
        """Create a new entity with these components."""
        return self.store.add_entity(list(components), step or 0, self.world_id, self.run_id)

    def despawn(self, entity_id: int, step: Optional[int] = None) -> None:
        """Mark an entity dead (is_active=False)."""
        current_step_val = self.current_step if step is None else step
        self.store.remove_entity(entity_id, current_step_val, self.world_id, self.run_id)

    def materialize_spawns(self) -> None:
        """Materialize any pending spawns."""
        self.store.materialize_spawns()

    def get_session(self) -> Session:
        """Get the Daft Session for this world."""
        return self.store.sess



    # ---------------------------------------------------------------------
    # QueryManager Facade
    # ---------------------------------------------------------------------

    def get_archetypes(self, step: Optional[int] = -1) -> List[Tuple[ArchetypeSignature, DataFrame]]:
        """Fetch the latest live state of these components at the given step."""
        return self.querier.get_archetypes(step, self.world_id, self.run_id)

    def archetype_for_entity(self, entity_id: int, step: int = -1) -> DataFrame:
        """Get a archetype for an entity."""
        return self.querier.archetype_for_entity(entity_id, step)

    # ---------------------------------------------------------------------
    # UpdateManager Facade
    # ---------------------------------------------------------------------

    def update(self, archetypes: List[Tuple[ArchetypeSignature, DataFrame]], step: int):
        self.updater.update(archetypes, step, self.world_id, self.run_id)

    # ---------------------------------------------------------------------
    # System Facade
    # ---------------------------------------------------------------------

    def add_processor(self, proc: Processor) -> None:
        """Install a Processor into the sequential system."""
        self.system.add_processor(proc)

    def remove_processor(self, proc: Processor) -> None:
        """Remove a Processor from the sequential system."""
        self.system.remove_processor(proc)

    def execute(self, querier: iQuerier, step: int, dt: float) -> List[Tuple[ArchetypeSignature, DataFrame]]:
        """Execute the system for a single step."""
        return self.system.execute(querier, step, dt, self.world_id, self.run_id)
