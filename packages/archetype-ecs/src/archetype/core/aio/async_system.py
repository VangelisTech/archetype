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

from archetype.core.archetype import Archetype
from archetype.core.interfaces import ArchetypeSignature, iAsyncProcessor, iAsyncSystem
from archetype.core.resources import Resources

logger = logging.getLogger(__name__)


class AsyncSystem(iAsyncSystem):
    """
    Executes matching processors in priority order for each archetype.

    Each archetype is processed through all its relevant processors concurrently
    with other archetypes, while preserving priority order within each archetype.

    Processor failures are isolated to their archetype while the other archetype
    tasks finish before the failure is reported.
    """

    def __init__(self):
        self.processors: list[iAsyncProcessor] = []

    async def add_processor(self, proc: iAsyncProcessor):
        """Add an async processor to the system."""
        self.processors.append(proc)

    async def remove_processor(self, proc_type: type[iAsyncProcessor]):
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
        # Forward resources and debug to processors via kwargs.
        if resources is not None:
            input_kwargs["resources"] = resources
        input_kwargs["debug"] = debug

        archetype_name = Archetype.get_name(sig) if debug else None

        for proc_instance in sorted(self.processors, key=lambda x: x.priority):
            if set(proc_instance.components).issubset(set(sig)):
                proc_name = proc_instance.__class__.__name__

                if debug:
                    logger.debug(
                        f"[archetype] processor_start: {proc_name} "
                        f"(priority={proc_instance.priority}, archetype={archetype_name})"
                    )

                # A processor failure fails the archetype's tick: the world
                # step surfaces it (gather → TickExecutionError) instead of
                # appending a frame the failed processor never transformed.
                try:
                    # Filter kwargs to what the processor accepts; pass all if it has **kwargs.
                    sig_params = inspect.signature(proc_instance.process).parameters
                    accepts_var_keyword = any(
                        p.kind is inspect.Parameter.VAR_KEYWORD for p in sig_params.values()
                    )
                    if accepts_var_keyword:
                        filtered_input_kwargs = dict(input_kwargs)
                    else:
                        filtered_input_kwargs = {
                            k: v for k, v in input_kwargs.items() if k in sig_params
                        }
                    df = await proc_instance.process(df, **filtered_input_kwargs)

                    if debug:
                        logger.debug(
                            "[archetype] processor_end: %s (archetype=%s)",
                            proc_name,
                            archetype_name,
                        )
                except Exception as e:
                    logger.error(
                        f"Error processing archetype {sig}: {e} with processor {proc_instance} of type {type(proc_instance)}"
                    )
                    raise

        return df
