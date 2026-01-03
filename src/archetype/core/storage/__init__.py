# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Core Storage Layer
==================

Low-level storage primitives for ECS persistence.

For high-level storage with catalog and search, use:
    from archetype.app.services import ManagedStorage
"""

from .lancedb import AsyncLancedbStore

__all__ = [
    "AsyncLancedbStore",
]
