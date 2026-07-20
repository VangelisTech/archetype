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

"""Provisional local-transcript adapter awaiting artifact-owned ingestion.

This package is not a supported application API. Its remaining loader reads a
local source artifact and projects mission trajectory values, but it does not
yet provide the required redaction, quarantine, and durable publication
boundary. Import paths may change before v1; supported applications enter
through :class:`ArchetypeRuntime`.
"""

from archetype.experiments.claude_sessions import (
    load_claude_session,
    load_claude_sessions,
)

__all__ = [
    "load_claude_session",
    "load_claude_sessions",
]
