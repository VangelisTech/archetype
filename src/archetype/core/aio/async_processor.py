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


from daft import DataFrame

from archetype.core.component import Component
from archetype.core.interfaces import iAsyncProcessor


class AsyncProcessor(iAsyncProcessor):
    """Base class for asynchronous dataframe processors.

    Subclasses declare the component types they require and implement
    `process()`. Lower priority values run first. Processors return a new Daft
    DataFrame rather than mutating their input.

    Examples:
        >>> from daft import col
        >>> class Health(Component):
        ...     value: int = 100
        >>> class Decay(AsyncProcessor):
        ...     components = (Health,)
        ...     priority = 10
        ...
        ...     async def process(self, df, **kwargs):
        ...         return df.with_column(
        ...             "health__value", col("health__value") - 1
        ...         )
    """

    components: tuple[type["Component"], ...] = ()
    priority: int = 10

    async def process(self, df: DataFrame, **input_kwargs) -> DataFrame:
        """Transform one archetype dataframe for a simulation tick.

        Override this method in subclasses and return a new dataframe.
        """
        return df
