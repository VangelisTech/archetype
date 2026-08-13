# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Read-only Activity-catalog evidence for whole-storage migration."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from archetype.errors import PayloadRejectedError


class ActivityCatalogInspectionError(PayloadRejectedError):
    """An existing Activity catalog cannot be inventoried completely."""

    public_detail = "Activity history cannot be admitted by this migration profile"


@dataclass(frozen=True, slots=True)
class ActivityCatalogInventory:
    """Complete physical row counts for the current local Activity schema."""

    catalog_present: bool
    schema_version: int | None
    activity_count: int
    attempt_count: int
    provider_operation_count: int

    @property
    def is_empty(self) -> bool:
        return not any(
            (
                self.activity_count,
                self.attempt_count,
                self.provider_operation_count,
            )
        )


@runtime_checkable
class ActivityCatalogMigrationInspector(Protocol):
    """Administrative read-only capability used by migration preflight."""

    async def inspect_activity_catalog(self) -> ActivityCatalogInventory: ...


__all__ = [
    "ActivityCatalogInspectionError",
    "ActivityCatalogInventory",
    "ActivityCatalogMigrationInspector",
]
