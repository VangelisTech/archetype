# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Interactive session supervision for mission sandboxes.

tmux owns each agent PTY server-side so sessions survive any client's death;
ttyd serves web lanes over that PTY — read-only for spectators, writable
behind explicit opt-in for takeover. Every session records its raw PTY
stream and a JSONL lifecycle event log suitable for ledger ingestion.
"""

from archetype.missions.sessions.tmux import (
    SessionLanes,
    SessionRecording,
    TmuxSessionSupervisor,
)

__all__ = [
    "SessionLanes",
    "SessionRecording",
    "TmuxSessionSupervisor",
]
