# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Compatibility re-exports for :mod:`archetype.errors`."""

from archetype.errors import (
    AvailabilityError,
    ConflictError,
    PayloadRejectedError,
    WorldNotFoundError,
)

__all__ = [
    "AvailabilityError",
    "ConflictError",
    "PayloadRejectedError",
    "WorldNotFoundError",
]
