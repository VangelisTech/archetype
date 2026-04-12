# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Session entity components.

Each terminal session is modeled as an entity. Its stdio — commands issued
and query results received — is stored as components, enabling agents to
query session logs through the same DataFrame interface used for all other
data.

    archetype query <world-id> SessionInput --where "sessioninput__text LIKE '%Agent%'"
"""

from __future__ import annotations

from archetype.core.component import Component


class SessionInput(Component):
    """Command text submitted by a session."""

    text: str = ""
    timestamp: str = ""


class SessionOutput(Component):
    """Query result returned to a session."""

    text: str = ""
    row_count: int = 0
    timestamp: str = ""
