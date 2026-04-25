# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Core Storage Layer
==================

Low-level storage primitives for ECS persistence.
"""

from .lancedb import AsyncLancedbStore

__all__ = [
    "AsyncLancedbStore",
]
