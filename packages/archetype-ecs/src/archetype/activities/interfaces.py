# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Structural port for generic durable-activity coordination."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from archetype.activities.contracts import (
    ActivityAdmission,
    ActivityClaim,
    ActivityExecutionIdentity,
    ActivityResultRef,
    ActivityRetryGuard,
    ActivitySettlement,
    ActivitySnapshot,
)


@runtime_checkable
class iActivityCoordinator(Protocol):
    """Coordinate durable work between two exact committed world states.

    ``pending`` and ``pending_results`` return snapshots in ascending
    catalog admission ``sequence`` order and honor ``after_sequence`` as a
    keyset cursor: only rows whose ``sequence`` is strictly greater are
    returned.  Every returned snapshot must carry its ``sequence`` so scans
    can page without an absolute offset.
    """

    async def admit(self, admission: ActivityAdmission) -> ActivitySnapshot: ...

    async def get(
        self,
        world_id: str,
        kind: str,
        activity_id: str,
    ) -> ActivitySnapshot | None: ...

    async def claim(
        self,
        world_id: str,
        kind: str,
        activity_id: str,
        owner: str,
        *,
        lease_seconds: float = 300.0,
    ) -> ActivityClaim: ...

    async def bind_provider_operation(
        self,
        claim: ActivityClaim,
        provider: str,
        operation_id: str,
    ) -> ActivityClaim: ...

    async def confirm_provider_operation_absent(
        self,
        claim: ActivityClaim,
        guard: ActivityRetryGuard,
        *,
        lease_seconds: float = 300.0,
    ) -> ActivityClaim: ...

    async def record_result(
        self,
        claim: ActivityClaim,
        result: ActivityResultRef,
    ) -> ActivitySnapshot: ...

    async def record_orchestrated_result(
        self,
        world_id: str,
        kind: str,
        activity_id: str,
        execution: ActivityExecutionIdentity,
        result: ActivityResultRef,
    ) -> ActivitySnapshot: ...

    async def release(self, claim: ActivityClaim) -> None: ...

    async def has_unsettled(self, world_id: str) -> bool: ...

    async def pending(
        self,
        *,
        kind: str | None = None,
        world_id: str | None = None,
        limit: int = 100,
        after_sequence: int = 0,
    ) -> tuple[ActivitySnapshot, ...]: ...

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


__all__ = ["iActivityCoordinator", "iActivitySettlementIndex"]
