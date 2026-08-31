# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Structural port for generic durable-activity coordination."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from archetype.activities.contracts import (
    ActivityAdmission,
    ActivityExecutionIdentity,
    ActivityResultRef,
    ActivitySettlement,
    ActivitySnapshot,
)


@runtime_checkable
class iActivitySettlementIndex(Protocol):
    """Strongly consistent ECS admission and settlement facts without execution leases."""

    async def admit(
        self,
        admission: ActivityAdmission,
        execution: ActivityExecutionIdentity,
    ) -> ActivitySnapshot: ...

    async def get(
        self,
        world_id: str,
        kind: str,
        activity_id: str,
    ) -> ActivitySnapshot | None: ...

    async def record_orchestrated_result(
        self,
        world_id: str,
        kind: str,
        activity_id: str,
        execution: ActivityExecutionIdentity,
        result: ActivityResultRef,
    ) -> ActivitySnapshot: ...

    async def has_unsettled(self, world_id: str) -> bool: ...

    async def pending_results(
        self,
        *,
        kind: str | None = None,
        world_id: str | None = None,
        limit: int = 100,
        after_sequence: int = 0,
    ) -> tuple[ActivitySnapshot, ...]: ...

    async def settle_observation(
        self,
        world_id: str,
        kind: str,
        activity_id: str,
        settlement: ActivitySettlement,
    ) -> ActivitySnapshot: ...


__all__ = ["iActivitySettlementIndex"]
