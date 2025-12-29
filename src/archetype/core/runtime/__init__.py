# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Core Runtime: Storage Context
=============================

Storage context initialization for Daft-backed persistence.
Runner configuration is handled via environment variables (DAFT_RUNNER, RAY_ADDRESS).
"""

from archetype.core.runtime.storage import StorageContext, StorageContextFactory

__all__ = [
    "StorageContext",
    "StorageContextFactory",
]
