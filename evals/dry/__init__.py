# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""DRY-detection eval support.

Ports the algorithm from unclebob/dry4clj to Python: normalize each
function-level form into a multiset of structural fingerprints (with
identifiers and literals replaced by generic markers), then score pairs
by Jaccard similarity.  The oracle's pair set is the ground truth that
the agent's enumeration is graded against.
"""

from evals.dry.oracle import (
    DuplicatePair,
    FormFingerprint,
    find_duplicates,
    fingerprint_module,
    fingerprint_paths,
)

__all__ = [
    "DuplicatePair",
    "FormFingerprint",
    "find_duplicates",
    "fingerprint_module",
    "fingerprint_paths",
]
