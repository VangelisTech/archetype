# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
AutoResearch ECS Components.

Maps directly to the design spec at:
    docs/superpowers/specs/2026-03-13-autoresearch-archetype-design.md
"""

from archetype.core.component import Component


class Repository(Component):
    """Identifies the repo under optimization."""

    repository_id: str = ""
    canonical_path: str = ""
    default_branch: str = "main"


class BranchHead(Component):
    """The single tracked frontier for the active loop."""

    branch_name: str = "autoresearch/v1"
    current_commit_hash: str = ""
    frontier_metric_name: str = "score"
    frontier_metric_direction: str = "max"  # "min" or "max"
    frontier_metric_value: float = 0.0
    generation: int = 0


class Commit(Component):
    """A concrete git state."""

    commit_hash: str = ""
    parent_commit_hash: str = ""
    message: str = ""
    created_at: str = ""


class Experiment(Component):
    """A proposed advancement of the tracked frontier."""

    experiment_id: str = ""
    base_commit_hash: str = ""
    proposal_summary: str = ""
    strategy: str = ""  # the current/baseline strategy
    hypothesis: str = ""  # the LLM-proposed new strategy
    status: str = "pending"  # pending → running → succeeded → kept|discarded|crashed
    created_at: str = ""


class Run(Component):
    """One bounded execution of an experiment."""

    run_id: str = ""
    experiment_id: str = ""
    status: str = "pending"  # pending → running → completed|crashed|timed_out
    budget_kind: str = "steps"
    budget_value: int = 1
    started_at: str = ""
    finished_at: str = ""


class Result(Component):
    """Metrics and terminal outcome of a run."""

    result_id: str = ""
    run_id: str = ""
    primary_metric_value: float = 0.0
    judge_score: float = 0.0
    combined_score: float = 0.0
    reasoning: str = ""
    beats_frontier: str = ""  # "yes" or "no"
