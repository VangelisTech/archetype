# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Service-layer error types.

Auth-specific errors live in ``archetype.app.auth.errors``. This module holds
the cross-service exception types that ``CommandService`` (and any future
gated surface) raise as part of their public contract.
"""


class WorldNotFoundError(LookupError):
    """Raised when a gated operation targets a ``world_id`` not in the registry.

    Per ``docs/guide/specification.md`` "Required Hardening Work" item 3,
    submission to an unknown world is rejected at the gate so callers get a
    typed signal instead of a silently orphaned broker entry.
    """

    def __init__(self, world_id) -> None:
        super().__init__(f"World with ID '{world_id}' not found.")
        self.world_id = world_id
