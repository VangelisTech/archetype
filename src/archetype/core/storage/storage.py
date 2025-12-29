# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Storage Module
==============

Re-exports storage context from core.runtime for backwards compatibility.
"""

from archetype.core.runtime.storage import StorageContext, StorageContextFactory

__all__ = ["StorageContext", "StorageContextFactory"]
