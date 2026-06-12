# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Experiment tracking — ECS-native.

Experiments, runs, results, and branch heads as first-class archetype
Components. Runs become entities in an archetype world, which means
world forking, time-travel queries, and the full set of ECS tools work
on experiment state the same way they work on any other simulation.

Ingestion from archetype-runner's SQLite registry is provided by
:func:`load_runner_state_db` and :func:`ingest_runner_state`.
"""

from archetype.experiments.claude_sessions import (
    load_claude_session,
    load_claude_sessions,
)
from archetype.experiments.components import (
    BranchHead,
    Experiment,
    Result,
    Run,
    RunStatus,
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
