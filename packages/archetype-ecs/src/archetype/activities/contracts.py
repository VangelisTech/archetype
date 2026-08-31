# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Generic durable-activity control contracts.

These values describe coordination between committed world states.  They do
not describe a family's domain intent, provider recovery meaning, or ECS
observation schema.
"""

from __future__ import annotations

from dataclasses import dataclass
from math import isfinite

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
class ActivityRetryGuard:
    """Bounded proof that confirmed absence makes a fresh attempt safe.

    The owning provider adapter retains the guard's meaning.  Its durable
    reference and digest must prove either provider-side atomic
    idempotency/fencing or that every stale claimant is irrevocably unable to
    start the prior operation.
    """

    ref: str
    digest: str

    def __post_init__(self) -> None:
        _require_bounded_text(self.ref, "activity retry guard ref", _MAX_REF_CHARS)
        _require_bounded_text(
            self.digest,
            "activity retry guard digest",
            _MAX_DIGEST_CHARS,
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
    result_attempt: int | None = None
    result_fence: int | None = None
    sequence: int | None = None

    def __post_init__(self) -> None:
        if self.sequence is not None and self.sequence < 1:
            raise ValueError("activity sequence must be positive")
        result_identity = (self.result_attempt, self.result_fence)
        if self.result is None and any(value is not None for value in result_identity):
            raise ValueError("activity result identity cannot exist without a result")
        if (self.result_attempt is None) != (self.result_fence is None):
            raise ValueError("activity result attempt and fence identity must be complete")
        if self.result_attempt is not None and self.result_attempt < 1:
            raise ValueError("activity result_attempt must be positive")
        if self.result_fence is not None and self.result_fence < 1:
            raise ValueError("activity result_fence must be positive")
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


@dataclass(frozen=True, slots=True)
class ActivityClaim:
    """One fenced worker claim over an activity.

    An acquired claim with ``reconciliation_required`` authorizes only
    provider-specific reconciliation and result recording.  It never
    authorizes invocation of a new provider operation.
    """

    snapshot: ActivitySnapshot
    acquired: bool
    attempt: int | None = None
    fence: int | None = None
    owner: str | None = None
    lease_expires_at: float | None = None
    provider: str | None = None
    provider_operation_id: str | None = None
    retry_guard: ActivityRetryGuard | None = None
    reconciles_attempt: int | None = None
    reconciles_provider: str | None = None
    reconciles_provider_operation_id: str | None = None

    def __post_init__(self) -> None:
        claim_values = (
            self.attempt,
            self.fence,
            self.owner,
            self.lease_expires_at,
        )
        if self.acquired and any(value is None for value in claim_values):
            raise ValueError("an acquired activity claim requires complete lease identity")
        if self.attempt is not None and self.attempt < 1:
            raise ValueError("activity claim attempt must be positive")
        if self.fence is not None and self.fence < 1:
            raise ValueError("activity claim fence must be positive")
        if self.lease_expires_at is not None and not isfinite(self.lease_expires_at):
            raise ValueError("activity claim lease_expires_at must be finite")
        if self.owner is not None:
            _require_bounded_text(self.owner, "activity claim owner", _MAX_ID_CHARS)
        if self.provider is not None:
            _require_bounded_text(
                self.provider,
                "activity claim provider",
                _MAX_PROVIDER_CHARS,
            )
        if self.provider_operation_id is not None:
            _require_bounded_text(
                self.provider_operation_id,
                "activity claim provider_operation_id",
                _MAX_PROVIDER_OPERATION_CHARS,
            )
        if self.retry_guard is not None and not isinstance(
            self.retry_guard,
            ActivityRetryGuard,
        ):
            raise TypeError("activity claim retry_guard must be an ActivityRetryGuard")
        reconciliation_values = (
            self.reconciles_attempt,
            self.reconciles_provider,
            self.reconciles_provider_operation_id,
        )
        if any(value is not None for value in reconciliation_values) and any(
            value is None for value in reconciliation_values
        ):
            raise ValueError("activity reconciliation identity must be complete")

    @property
    def world_id(self) -> str:
        return self.snapshot.admission.source.world_id

    @property
    def activity_id(self) -> str:
        return self.snapshot.admission.activity_id

    @property
    def kind(self) -> str:
        return self.snapshot.admission.kind

    @property
    def reconciliation_required(self) -> bool:
        """Whether provider-specific reconciliation is required before settlement."""

        return self.reconciles_provider_operation_id is not None


class ActivityConflictError(ConflictError):
    """A logical activity identity or immutable fact conflicts."""


class ActivityNotFoundError(KeyError):
    """No activity exists for the supplied world-and-kind-scoped identity."""


class ActivityClaimError(ConflictError):
    """A claim is stale or does not authorize the requested operation."""


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
    "ActivityClaim",
    "ActivityClaimError",
    "ActivityConflictError",
    "ActivityExecutionIdentity",
    "ActivityNotFoundError",
    "ActivityResultRef",
    "ActivityRetryGuard",
    "ActivitySettlement",
    "ActivitySnapshot",
]
