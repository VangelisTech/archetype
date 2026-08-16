# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Narrow injected capabilities used by whole-storage migration."""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from archetype.migration.contracts import (
        ColdVerificationEvidence,
        ColdVerificationRequest,
    )


@runtime_checkable
class ColdMigrationVerifier(Protocol):
    """Trusted fresh destination-only verification supplied by composition.

    Implementations install the owning application's resumable code, receive
    no source authority, verify frozen snapshots and visible queries, and are
    retry-safe after an earlier verification tick already committed.
    """

    async def __call__(
        self,
        request: ColdVerificationRequest,
    ) -> ColdVerificationEvidence: ...


__all__ = ["ColdMigrationVerifier"]
