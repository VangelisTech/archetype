# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Ports owned by the coding-agent mission family."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class iCodingAgentService(Protocol):
    """Coordinate repository missions through sandbox and mission ports."""

    async def start_episode(self, mission_id: str, provider: str, spec: Any) -> str: ...
    async def restore_episode(
        self,
        mission_id: str,
        provider: str,
        spec: Any,
        checkpoint_ref: str,
        *,
        resume_agent: bool = False,
    ) -> str: ...
    async def run_tick(
        self, mission_id: str, row: Mapping[str, Any], *, tick: int
    ) -> dict[str, Any]: ...
    async def close_episode(self, mission_id: str) -> None: ...
