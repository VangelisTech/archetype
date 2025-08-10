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

from typing import Tuple, Type
from daft import DataFrame
from archetype.core.interfaces import Component
from archetype.core.base import BaseProcessor


class SyncProcessor(BaseProcessor):
    components: Tuple[Type[Component], ...] = ()
    priority: int = 10

    def process(self, df: DataFrame, **input_kwargs) -> DataFrame:
        """
        Processor method are provided the state of the archetype at the current tick.
        Processors are not responsible for updating the tick value.
        """
        return df
