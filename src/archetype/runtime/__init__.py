# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Archetype Runtime
=================

The script boundary. Owns the service container and provides ergonomic
world handles that route every operation through iCommandService.
"""

from archetype.runtime.runtime import (
    ArchetypeRuntime,
    SyncArchetypeRuntime,
    run_sync,
)
from archetype.runtime.session import configure_session
from archetype.runtime.world import RuntimeWorld, SyncRuntimeWorld

__all__ = [
    "ArchetypeRuntime",
    "SyncArchetypeRuntime",
    "RuntimeWorld",
    "SyncRuntimeWorld",
    "configure_session",
    "run_sync",
]
