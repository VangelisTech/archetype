# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Stable cross-family boundary error contracts."""

from __future__ import annotations

from dataclasses import dataclass


class ConflictError(RuntimeError):
    """A requested operation conflicts with existing durable state.

    Concrete services subclass this public contract so transport adapters can
    map conflicts without importing private implementations. Internal messages
    may contain diagnostics; adapters expose only ``public_detail``.
    """

    public_detail = "Request conflicts with existing state"


class AvailabilityError(RuntimeError):
    """A dependency is temporarily unable to accept work.

    Concrete services subclass this public contract so transport adapters can
    expose a retryable signal without importing private implementations.
    """

    public_detail = "Service is temporarily unavailable"


class PayloadRejectedError(RuntimeError):
    """A well-formed payload cannot cross a safety boundary.

    Implementations use this contract without exposing internal safety
    findings; adapters return only the bounded ``public_detail``.
    """

    public_detail = "Payload rejected by safety policy"


class WorldNotFoundError(LookupError):
    """Raised when a governed operation targets an unknown world."""

    def __init__(self, world_id) -> None:
        super().__init__(f"World with ID '{world_id}' not found.")
        self.world_id = world_id


@dataclass(frozen=True, slots=True)
class RuntimeShutdownFailure:
    """One ordered process-shutdown failure with its original cause."""

    phase: str
    owner: str
    cause: BaseException


class RuntimeShutdownError(RuntimeError):
    """Bounded retryable process-shutdown boundary error."""

    def __init__(
        self,
        phase: str,
        failures: tuple[RuntimeShutdownFailure, ...],
    ) -> None:
        self.phase = phase
        self.failures = failures
        owners = ", ".join(failure.owner for failure in failures)
        super().__init__(
            f"Runtime shutdown incomplete in phase '{phase}'"
            + (f" for owners: {owners}" if owners else "")
        )


__all__ = [
    "AvailabilityError",
    "ConflictError",
    "PayloadRejectedError",
    "RuntimeShutdownError",
    "RuntimeShutdownFailure",
    "WorldNotFoundError",
]
