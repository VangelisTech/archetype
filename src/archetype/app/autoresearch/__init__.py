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

"""AutoResearch module for autonomous software optimization."""

from .components import (
    BranchHead,
    Commit,
    Experiment,
    Repository,
    Result,
    Run,
)
from .processors import (
    ExperimentProcessor,
    RunProcessor,
)
from .specs import (
    get_canonical_evaluation_spec,
    get_canonical_run_spec,
    get_canonical_world_spec,
)
from .storage import AutoResearchStorage

__all__ = [
    "BranchHead",
    "Commit", 
    "Experiment",
    "Repository",
    "Result",
    "Run",
    "ExperimentProcessor",
    "RunProcessor",
    "AutoResearchStorage",
    "get_canonical_evaluation_spec",
    "get_canonical_run_spec", 
    "get_canonical_world_spec",
]