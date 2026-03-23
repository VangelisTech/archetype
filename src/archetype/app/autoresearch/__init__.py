# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
AutoResearch — Karpathy-style Autonomous Software Optimization

Core model + state machine for the experiment loop:
    branch head → experiment → run → result → keep|discard → advance
"""

from .components import BranchHead, Commit, Experiment, Repository, Result, Run
from .loop import AutoResearchLoop

__all__ = [
    "Repository",
    "BranchHead",
    "Commit",
    "Experiment",
    "Run",
    "Result",
    "AutoResearchLoop",
]
