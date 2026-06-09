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

import logging

from daft import DataFrame

from archetype.core.config import RunConfig
from archetype.core.interfaces import ArchetypeSignature, iSystem
from archetype.core.resources import Resources
from archetype.core.sync.processor import SyncProcessor

logger = logging.getLogger(__name__)


class SyncSystem(iSystem):
    def __init__(self):
        self.processors: list[SyncProcessor] = []

    def add_processor(self, proc: SyncProcessor):
        self.processors.append(proc)

    def remove_processor(self, proc: SyncProcessor | type[SyncProcessor]):
        """Remove a processor instance, or all processors of a given type."""
        if isinstance(proc, type):
            self.processors = [p for p in self.processors if not isinstance(p, proc)]
        else:
            self.processors.remove(proc)

    def execute(
        self,
        df: DataFrame,
        sig: ArchetypeSignature,
        run_config: RunConfig | None = None,
        resources: Resources | None = None,
        **input_kwargs,
    ) -> DataFrame:
        """
        Execute all processors on the given archetype in priority order.

        Args:
            df: The DataFrame to process
            sig: The archetype signature
            run_config: Optional run configuration
            resources: Type-safe resource container (available as kwarg to processors)
            **input_kwargs: Additional kwargs passed to processors
        """
        # Include resources in kwargs for processors that want it
        if resources is not None:
            input_kwargs["resources"] = resources

        # Process archetype through all processors in priority order
        for proc_instance in sorted(self.processors, key=lambda x: x.priority):
            # Build the modified archetype list if the processor has the components to matching the archetype signature
            if set(proc_instance.components).issubset(set(sig)):
                try:
                    assert isinstance(proc_instance, SyncProcessor)
                    df = proc_instance.process(df, **input_kwargs)
                except Exception as e:
                    logger.error(
                        f"Error processing archetype {sig}: {e} with processor {proc_instance} of type {type(proc_instance)}"
                    )
                    # Keep world alive; skip failing processor
                    continue

        return df
