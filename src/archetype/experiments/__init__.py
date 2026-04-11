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

__all__ = [
    "BranchHead",
    "Experiment",
    "Result",
    "Run",
    "RunStatus",
    "ingest_runner_state",
    "load_runner_state_db",
]
