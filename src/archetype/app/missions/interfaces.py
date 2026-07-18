# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Ports owned by the mission family."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Protocol, runtime_checkable

from archetype.app.missions.models import MissionAttemptRequest


@runtime_checkable
class iMissionService(Protocol):
    """Prepare attempts and authorize task-state transitions."""

    def prepare_attempt(
        self, row: Mapping[str, Any], *, tick: int
    ) -> MissionAttemptRequest | None: ...

    def apply_attempt(
        self,
        row: Mapping[str, Any],
        request: MissionAttemptRequest,
        outcome: Mapping[str, Any],
    ) -> dict[str, Any]: ...
