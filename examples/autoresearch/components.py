# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""ECS Components for the AutoResearch experiment loop."""

from archetype.core.component import Component


class BranchHead(Component):
    """The tracked frontier — best known result."""

    branch_name: str = "autoresearch/v1"
    frontier_metric: float = 0.0
    frontier_direction: str = "max"  # "min" or "max"
    generation: int = 0


class Experiment(Component):
    """A proposed improvement to test against the frontier."""

    experiment_id: str = ""
    hypothesis: str = ""
    strategy: str = ""  # the actual prompt/config being tested
    status: str = "pending"  # pending → running → succeeded → kept|discarded|crashed
    generation: int = 0


class Run(Component):
    """A bounded execution of an experiment."""

    run_id: str = ""
    status: str = "pending"  # pending → running → completed|crashed
    budget_steps: int = 1
    started_at: str = ""
    finished_at: str = ""


class Result(Component):
    """Metrics from a completed run."""

    primary_metric: float = 0.0
    reasoning: str = ""
    beats_frontier: str = ""  # "yes" or "no"
    margin: float = 0.0
