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

"""Provisional ECS-native experiment and trajectory helpers.

This package is not a supported application API. Its current contents are
candidate domain capabilities whose final homes depend on the coding-mission
and physical-AI service boundaries. Import paths and groupings may change
before v1; supported applications enter through :class:`ArchetypeRuntime`.

Experiments, runs, results, and branch heads as first-class archetype
Components. Runs become entities in an archetype world, which means
world forking, time-travel queries, and the full set of ECS tools work
on experiment state the same way they work on any other simulation.

Ingestion from archetype-runner's SQLite registry is provided by
:func:`load_runner_state_db` and :func:`ingest_runner_state`.
"""

from archetype.app.research.models import (
    BranchHead,
    Experiment,
    Result,
    Run,
    RunStatus,
)
from archetype.experiments.claude_sessions import (
    load_claude_session,
    load_claude_sessions,
)
from archetype.experiments.loaders import (
    ingest_runner_state,
    load_runner_state_db,
)
from archetype.experiments.trajectories import (
    Trajectory,
    TrajectoryAction,
    TrajectoryCommandEvent,
    TrajectoryObservation,
    TrajectoryReward,
    TrajectoryTurn,
    Turn,
    turns_to_components,
)

__all__ = [
    "BranchHead",
    "Experiment",
    "Result",
    "Run",
    "RunStatus",
    "Trajectory",
    "TrajectoryAction",
    "TrajectoryCommandEvent",
    "TrajectoryObservation",
    "TrajectoryReward",
    "TrajectoryTurn",
    "Turn",
    "ingest_runner_state",
    "load_claude_session",
    "load_claude_sessions",
    "load_runner_state_db",
    "turns_to_components",
]
