# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Trajectory Analysis Pipeline
=============================

Components and processors for evaluating agent session trajectories
through configurable sampling regimes and labeling techniques.

Trajectories are entities. Processors are pipeline stages.
Fork a world to try a different labeling technique on the same data.
Storage is the version control.

Quick Start:
    >>> from archetype.trajectories import Trajectory, Label, TrajectoryPipeline
    >>> pipeline = TrajectoryPipeline(name="my-experiment")
    >>> pipeline.label("efficiency", "Rate how directly the agent reached the solution without backtracking")
    >>> await pipeline.ingest(trajectories)
    >>> await pipeline.run()
    >>> results = await pipeline.results()
"""

from archetype.trajectories.components import Label, Trajectory, Turn
from archetype.trajectories.pipeline import TrajectoryPipeline
from archetype.trajectories.processors import LabelingProcessor, SamplingProcessor, ScoringProcessor

__all__ = [
    "Trajectory",
    "Turn",
    "Label",
    "TrajectoryPipeline",
    "SamplingProcessor",
    "LabelingProcessor",
    "ScoringProcessor",
]
