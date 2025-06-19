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
from itertools import count
from typing import List, Type, Optional, Dict, Any, Tuple, Set
import ulid


import ray
import daft
import asyncio 



from archetype.core.interfaces import Component, Archetype, ArchetypeSignature
from archetype.core.base import BaseWorld
from archetype.core.aio.async_interfaces import iAsyncQuerier, iAsyncUpdater, iAsyncSystem, iAsyncProcessor
from archetype.core.aio.async_world import AsyncWorld
# Ray processor removed - using RaySystem instead
from logging import getLogger

logger = getLogger(__name__)


class RayWorld(AsyncWorld):
    def __init__(
        self,
        querier: iAsyncQuerier,
        updater: iAsyncUpdater,
        async_system: iAsyncSystem,
        world_id: Optional[str] = None,
        run_id: Optional[str] = None,
        debug: bool = False,
        semaphore: Optional[asyncio.Semaphore] = None,
    ):
        """
        Initialize RayWorld using shared base functionality.
        """
        super().__init__(querier, updater, async_system, world_id, run_id, debug, semaphore)
        if self.semaphore:
            self.semaphore_limit = self.semaphore._value
        else:
            self.semaphore_limit = 10

        # Initialize internal caches
        self._spawn_cache: Dict[ArchetypeSignature, List[Dict[str, Any]]] = {}  # ArchetypeSignature -> List[row_dict]
        self._entity2sig: Dict[int, ArchetypeSignature] = {}  # entity_id -> ArchetypeSignature
        self._entity_counter = count(start=1)

    async def step(self, *args, **kwargs):
        """
        Async version of step() with concurrent archetype processing using Ray actors.
        """
        start_time = time.time()

        await self.materialize_spawns()
        self.current_step += 1

        active_signatures = self.get_active_signatures()

        # Create a pool of archetype processors
        processor_actors = [
            ArchetypeProcessor.remote(  # type: ignore
                sig,
                self.querier,
                self.updater,
                self.async_system,
                self.semaphore_limit,
            )
            for sig in active_signatures
        ]

        # Process all archetypes concurrently
        tasks = [
            actor.process.remote(self.current_step, self.world_id, self.run_id, *args, **kwargs)
            for actor in processor_actors
        ]

        await asyncio.gather(*tasks)

        end_time = time.time()
        logger.info(f"Ray Step {self.current_step} done in {end_time - start_time:.3f}s")
