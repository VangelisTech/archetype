# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Test bootstrap for the bookmarks example.

Adds ``examples/`` to ``sys.path`` so the ``bookmarks`` package (which lives
at ``examples/bookmarks``) can be imported by test modules as ``bookmarks``.
This keeps the example self-contained and outside the ``archetype`` namespace.
"""

from __future__ import annotations

import sys
from pathlib import Path

_EXAMPLES_DIR = Path(__file__).resolve().parents[2]
if str(_EXAMPLES_DIR) not in sys.path:
    sys.path.insert(0, str(_EXAMPLES_DIR))
