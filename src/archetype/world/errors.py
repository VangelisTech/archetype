# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Typed lifecycle-state errors owned by the world family."""

from __future__ import annotations


class WorldClosingError(RuntimeError):
    """Raised when a live world has entered its sticky close state."""

    def __init__(self, world_id: object) -> None:
        super().__init__(f"world {world_id} is closing")
        self.world_id = world_id


__all__ = ["WorldClosingError"]
