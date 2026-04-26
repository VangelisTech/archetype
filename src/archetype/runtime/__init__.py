# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Archetype Runtime
=================

Process lifecycle, session management, and the user-facing scripting API.

- ``ArchetypeRuntime`` / ``SyncArchetypeRuntime``: process entry points
- ``RuntimeWorld`` / ``SyncRuntimeWorld``: lazy world handles
- ``configure_session``: Daft session setup for Iceberg storage
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
