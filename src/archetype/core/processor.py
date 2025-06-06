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
from typing import Type, Tuple
from .base import Component, BaseProcessor

def processor(*component_types: Type[Component], priority: int = 0):
    """
    Class decorator to assign the list of components a Processor reads/writes.
    It also injects the `__init__`, `_fetch_state`, and `process` methods into the class.

    """
    def wrap(cls: Type[BaseProcessor]):
        cls.components = component_types
        cls.priority = priority
        return cls
    return wrap


class Processor(BaseProcessor):
    priority: int = 0
    components: Tuple[Type[Component], ...] = None

    def process(self, df: DataFrame, *args, **kwargs) -> DataFrame:
        """
        Processor method are provided the state of the archetype at the current step.
        Processors are not responsible for updating the step value.
        """
        return df
