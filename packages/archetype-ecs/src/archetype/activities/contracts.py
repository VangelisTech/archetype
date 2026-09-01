# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Generic durable-activity control contracts.

These values describe coordination between committed world states.  They do
not describe a family's domain intent, provider recovery meaning, or ECS
observation schema.
"""

from __future__ import annotations

from dataclasses import dataclass

from archetype.core.interfaces import CommittedTickReceipt
from archetype.errors import ConflictError

_MAX_ID_CHARS = 512
_MAX_KIND_CHARS = 255
_MAX_REF_CHARS = 4096
_MAX_DIGEST_CHARS = 255
_MAX_MEDIA_TYPE_CHARS = 255
_MAX_PROVIDER_CHARS = 255
_MAX_PROVIDER_OPERATION_CHARS = 1024


def _require_bounded_text(value: str, field_name: str, max_chars: int) -> None:
    if not isinstance(value, str):
        raise TypeError(f"{field_name} must be a string")
    if not value.strip():
        raise ValueError(f"{field_name} must be non-empty")
    if len(value) > max_chars:
        raise ValueError(f"{field_name} must be at most {max_chars} characters")


def _require_receipt(value: CommittedTickReceipt, field_name: str) -> None:
    if not isinstance(value, CommittedTickReceipt):
        raise TypeError(f"{field_name} must be a CommittedTickReceipt")
    _require_bounded_text(value.world_id, f"{field_name}.world_id", _MAX_ID_CHARS)
    _require_bounded_text(value.run_id, f"{field_name}.run_id", _MAX_ID_CHARS)
    if value.committed_tick < 0:
        raise ValueError(f"{field_name}.committed_tick must be non-negative")
    if value.visibility_token is None:
        raise ValueError(f"{field_name}.visibility_token must be present")
    _require_bounded_text(
        value.visibility_token,
        f"{field_name}.visibility_token",
        _MAX_ID_CHARS,
    )


@dataclass(frozen=True, slots=True)
class ActivityAdmission:
    """One logical activity admitted from an exact committed tick.

    ``activity_id`` is scoped to ``(source.world_id, kind)``.  Family-owned
    identifiers such as mission dispatch IDs need not be globally unique.
    """

    activity_id: str
    kind: str
    source: CommittedTickReceipt
    input_ref: str
    input_digest: str

    def __post_init__(self) -> None:
        _require_bounded_text(self.activity_id, "activity_id", _MAX_ID_CHARS)
        _require_bounded_text(self.kind, "activity kind", _MAX_KIND_CHARS)
        _require_receipt(self.source, "activity source")
        object.__setattr__(self, "source", _identity_receipt(self.source))
        _require_bounded_text(self.input_ref, "activity input_ref", _MAX_REF_CHARS)
        _require_bounded_text(
            self.input_digest,
            "activity input_digest",
            _MAX_DIGEST_CHARS,
        )


@dataclass(frozen=True, slots=True)
class ActivityResultRef:
    """Bounded durable reference to an activity result.

    Large payloads belong in Iceberg or an artifact store.  The control plane
    retains only this address, digest, media type, and byte count.
    """

    ref: str
    digest: str
    media_type: str
    size_bytes: int

    def __post_init__(self) -> None:
        _require_bounded_text(self.ref, "activity result ref", _MAX_REF_CHARS)
        _require_bounded_text(
            self.digest,
            "activity result digest",
            _MAX_DIGEST_CHARS,
        )
        _require_bounded_text(
            self.media_type,
            "activity result media_type",
            _MAX_MEDIA_TYPE_CHARS,
        )
        if isinstance(self.size_bytes, bool) or not isinstance(self.size_bytes, int):
            raise TypeError("activity result size_bytes must be an integer")
        if self.size_bytes < 0:
            raise ValueError("activity result size_bytes must be non-negative")


@dataclass(frozen=True, slots=True)
class ActivityExecutionIdentity:
    """Stable external operation identity owned by a durable orchestrator."""

    provider: str
    operation_id: str

    def __post_init__(self) -> None:
        _require_bounded_text(self.provider, "activity provider", _MAX_PROVIDER_CHARS)
        _require_bounded_text(
            self.operation_id,
            "activity provider_operation_id",
            _MAX_PROVIDER_OPERATION_CHARS,
        )


@dataclass(frozen=True, slots=True)
class ActivitySettlement:
    """The exact later commit and result digest it durably observed."""

    receipt: CommittedTickReceipt
    result_digest: str

    def __post_init__(self) -> None:
        _require_receipt(self.receipt, "activity settlement receipt")
        object.__setattr__(self, "receipt", _identity_receipt(self.receipt))
        _require_bounded_text(
            self.result_digest,
            "activity settlement result_digest",
            _MAX_DIGEST_CHARS,
        )


@dataclass(frozen=True, slots=True)
class ActivitySnapshot:
    """Current durable facts for one logical activity, without a status enum.

    ``sequence`` is the catalog-assigned admission order: immutable,
    strictly monotonic across admissions, and never reassigned.  Pending
    and result scans use it as their keyset cursor.
    """

    admission: ActivityAdmission
    execution: ActivityExecutionIdentity | None = None
    result: ActivityResultRef | None = None
    settlement: ActivitySettlement | None = None
    sequence: int | None = None

    def __post_init__(self) -> None:
        if self.sequence is not None and self.sequence < 1:
            raise ValueError("activity sequence must be positive")
        if self.settlement is not None and self.result is None:
            raise ValueError("an activity cannot be settled before it has a result")
        if (
            self.settlement is not None
            and self.result is not None
            and self.settlement.result_digest != self.result.digest
        ):
            raise ValueError("activity settlement must bind the exact result digest")

    @property
    def result_pending_observation(self) -> bool:
        """Whether a durable result still needs a later committed observation."""

        return self.result is not None and self.settlement is None


class ActivityConflictError(ConflictError):
    """A logical activity identity or immutable fact conflicts."""


class ActivityNotFoundError(KeyError):
    """No activity exists for the supplied world-and-kind-scoped identity."""


def _identity_receipt(receipt: CommittedTickReceipt) -> CommittedTickReceipt:
    """Discard diagnostic command count while preserving exact commit identity."""

    if receipt.commands_applied == 0:
        return receipt
    return CommittedTickReceipt(
        world_id=receipt.world_id,
        run_id=receipt.run_id,
        committed_tick=receipt.committed_tick,
        visibility_token=receipt.visibility_token,
        commands_applied=0,
    )


__all__ = [
    "ActivityAdmission",
    "ActivityConflictError",
    "ActivityExecutionIdentity",
    "ActivityNotFoundError",
    "ActivityResultRef",
    "ActivitySettlement",
    "ActivitySnapshot",
]
