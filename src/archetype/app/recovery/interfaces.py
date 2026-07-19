# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Narrow ports owned by the fleet-recovery family."""

from __future__ import annotations

from typing import Literal, Protocol, runtime_checkable

from archetype.app.recovery.models import (
    FleetRecoveryCursor,
    MaintenanceRecoveryKind,
    RecoveryException,
    RecoveryExceptionStatus,
    RecoveryItemResult,
    RecoveryKind,
    RecoveryLimits,
    RecoveryPage,
    RecoveryPassResult,
    RecoverySubject,
    RecoverySweep,
    RecoverySweepStatus,
)


@runtime_checkable
class iRecoverySource(Protocol):
    """Discover and authenticate exact source-family recovery subjects."""

    @property
    def kind(self) -> RecoveryKind: ...

    async def discover(
        self,
        world_id: str,
        cursor: str,
        *,
        limit: int,
    ) -> RecoveryPage: ...

    async def resolve(
        self,
        world_id: str,
        authority_key: str,
    ) -> RecoverySubject | None: ...


@runtime_checkable
class iMaintenanceRecoveryHandler(Protocol):
    """A capability-limited handler that can never submit a model call."""

    @property
    def kind(self) -> MaintenanceRecoveryKind: ...

    async def recover(self, subject: RecoverySubject) -> RecoveryItemResult: ...


@runtime_checkable
class iModelRecoveryHandler(Protocol):
    """Separate #504 capability for explicitly authorized model recovery."""

    @property
    def kind(self) -> Literal[RecoveryKind.MISSION_MODEL_RECOVERY]: ...

    async def recover_model(self, subject: RecoverySubject) -> RecoveryItemResult: ...


@runtime_checkable
class iFleetRecoveryService(Protocol):
    """Run bounded passes and inspect their durable scheduling evidence."""

    async def run_once(
        self,
        *,
        claimant: str,
        cursor: FleetRecoveryCursor | None = None,
        limits: RecoveryLimits | None = None,
        kinds: tuple[RecoveryKind, ...] | None = None,
    ) -> RecoveryPassResult: ...

    async def list_sweeps(
        self,
        *,
        world_id: str | None = None,
        kind: RecoveryKind | None = None,
        status: RecoverySweepStatus | None = None,
        limit: int = 100,
    ) -> tuple[RecoverySweep, ...]: ...

    async def list_exceptions(
        self,
        *,
        world_id: str | None = None,
        kind: RecoveryKind | None = None,
        status: RecoveryExceptionStatus | None = None,
        limit: int = 100,
    ) -> tuple[RecoveryException, ...]: ...
