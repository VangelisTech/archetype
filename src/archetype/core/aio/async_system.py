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

import inspect
import logging

from daft import DataFrame

from archetype.core.aio.async_processor import AsyncProcessor
from archetype.core.archetype import Archetype
from archetype.core.interfaces import ArchetypeSignature, iAsyncSystem
from archetype.core.resources import Resources

logger = logging.getLogger(__name__)


class AsyncSystem(iAsyncSystem):
    """
    Async version of SyncSystem that processes archetypes concurrently.

    Key innovation: Each archetype is processed through all its relevant processors
    concurrently with other archetypes, while maintaining priority-based ordering
    within each archetype.

    This provides the same semantic guarantees as SyncSystem but with
    per-archetype parallelism.
    """

    def __init__(self):
        self.processors: list[AsyncProcessor] = []

    async def add_processor(self, proc: "AsyncProcessor"):
        """Add an async processor to the system."""
        self.processors.append(proc)

    async def remove_processor(self, proc_type: type["AsyncProcessor"]):
        """Remove all processors of the given type."""
        self.processors = [p for p in self.processors if not isinstance(p, proc_type)]

    async def execute(
        self,
        df: DataFrame,
        sig: ArchetypeSignature,
        resources: Resources | None = None,
        debug: bool = False,
        **input_kwargs,
    ) -> DataFrame:
        """
        Process a single archetype through all relevant processors.

        This is where the concurrency happens - each archetype gets its own task
        but within each archetype, processors run in priority order (same as sync).

        Args:
            df: The DataFrame to process
            sig: The archetype signature
            resources: Type-safe resource container (available as kwarg to processors)
            debug: Enable debug logging for processor execution
            **input_kwargs: Additional kwargs passed to processors
        """
        # Include resources in kwargs for processors that want it
        if resources is not None:
            input_kwargs["resources"] = resources

        archetype_name = Archetype.get_name(sig) if debug else None

        for proc_instance in sorted(self.processors, key=lambda x: x.priority):
            if set(proc_instance.components).issubset(set(sig)):
                proc_name = proc_instance.__class__.__name__

                if debug:
                    logger.debug(
                        f"[archetype] processor_start: {proc_name} "
                        f"(priority={proc_instance.priority}, archetype={archetype_name})"
                    )

                # Gracefully handle errors in processors.
                # Dataframes are immutable so we are continuously returning an updated variant of the original.
                try:
                    assert isinstance(proc_instance, AsyncProcessor)
                    # Filter input_kwargs to only what the processor accepts to avoid unexpected input_kwargs
                    sig_params = inspect.signature(proc_instance.process).parameters
                    filtered_input_kwargs = {
                        k: v for k, v in input_kwargs.items() if k in sig_params
                    }
                    df = await proc_instance.process(df, **filtered_input_kwargs)

                    if debug:
                        row_count = df.count_rows() if hasattr(df, "count_rows") else "?"
                        logger.debug(
                            f"[archetype] processor_end: {proc_name} (rows_out={row_count})"
                        )
                except Exception as e:
                    logger.error(
                        f"Error processing archetype {sig}: {e} with processor {proc_instance} of type {type(proc_instance)}"
                    )

        return df
