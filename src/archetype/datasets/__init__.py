# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Dataset and evaluation identity vocabulary.

The normative contract lives in :doc:`docs/guide/dataset-eval-ontology.md`.
This package defines immutable references and compositions; dataset readers,
exporters, and trial execution remain separate adapters and services.
"""

from archetype.datasets.definitions import (
    EpisodeRef,
    Eval,
    Grader,
    GraderKind,
    Rubric,
    RuntimeSlice,
    TaskRef,
    Trial,
)

__all__ = [
    "EpisodeRef",
    "Eval",
    "Grader",
    "GraderKind",
    "Rubric",
    "RuntimeSlice",
    "TaskRef",
    "Trial",
]
