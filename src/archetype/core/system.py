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

from typing import List
import logging
from daft import DataFrame

from archetype.core.base import BaseSystem
from archetype.core.processor import Processor as SyncProcessor
from archetype.core.interfaces import ArchetypeSignature
from archetype.core.base import EcsContext

logger = logging.getLogger(__name__)

class SyncSystem(BaseSystem):
    def __init__(self):
        self.processors: List[SyncProcessor] = []

    def add_processor(self, proc: SyncProcessor):
        self.processors.append(proc)

    def remove_processor(self, proc: SyncProcessor):
        self.processors.remove(proc)

    def execute(
        self,
        df: DataFrame,
        sig: ArchetypeSignature,
        context: "EcsContext",
        *args,
        **kwargs
    ) -> DataFrame:
        """
        Execute all processors on the given archetype in priority order.
        Returns a tuple of (archetype_signature, modified_df)
        """

        # Process archetype through all processors in priority order
        for proc_instance in sorted(self.processors, key=lambda x: x.priority):
            # Build the modified archetype list if the processor has the components to matching the archetype signature
            if set(proc_instance.components).issubset(set(sig)):
                try:
                    assert isinstance(proc_instance, SyncProcessor)
                    df = proc_instance.process(df, context, *args, **kwargs)
                except Exception as e:
                    logger.error(f"Error processing archetype {sig}: {e} with processor {proc_instance} of type {type(proc_instance)}")
                    df = None
                    break

        return df
