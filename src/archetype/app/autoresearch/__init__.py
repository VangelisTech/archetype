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

"""
AutoResearch: Karpathy-style autonomous optimization loop on Archetype

This module implements the core model for the AutoResearch experiment engine:
branch head → experiment → run → result → keep|discard|crash → maybe advance branch head
"""

from .components import (
    BranchHead,
    Commit,
    Experiment,
    Repository,
    Result,
    Run,
)
from .processors import (
    ExperimentStateProcessor,
    RunStateProcessor,
    FrontierEvaluationProcessor,
)

__all__ = [
    "Repository",
    "BranchHead", 
    "Commit",
    "Experiment",
    "Run",
    "Result",
    "ExperimentStateProcessor",
    "RunStateProcessor", 
    "FrontierEvaluationProcessor",
]