# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Provisional package home for dataset and evaluation identity vocabulary.

The normative contract lives in :doc:`docs/guide/dataset-eval-ontology.md`.
This package defines immutable references and compositions; dataset readers,
exporters, and trial execution remain separate adapters and services. The
vocabulary is normative, but this subpackage is not a supported top-level API
and may move into a domain capability family before v1.
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
