# Copyright 2025 Vangelis Technologies Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Archetype Runtime
=================

The recommended script boundary. Owns process-level services and provides
ergonomic world handles.
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
