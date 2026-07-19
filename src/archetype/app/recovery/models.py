# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Provider-neutral contracts for bounded fleet recovery."""

from __future__ import annotations

import hashlib
import re
from enum import StrEnum
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

_SHA256_RE = re.compile(r"[0-9a-f]{64}")
_MAX_SAFE_IDENTIFIER_CHARS = 512
_MAX_CURSOR_CHARS = 16_384
_MAX_SAFE_INTEGER = (1 << 53) - 1
_FROZEN_CONFIG = ConfigDict(frozen=True, revalidate_instances="always")

NonNegativeInt = Annotated[int, Field(strict=True, ge=0, le=_MAX_SAFE_INTEGER)]
PositiveInt = Annotated[int, Field(strict=True, ge=1, le=_MAX_SAFE_INTEGER)]


class RecoveryKind(StrEnum):
    """Closed fleet lanes; only the first value may require model capability."""

    MISSION_MODEL_RECOVERY = "mission_model_recovery"
    MISSION_FINALIZATION = "mission_finalization"
    ARTIFACT_PUBLICATION = "artifact_publication"
    EVENT_PROJECTION = "event_projection"
    ARTIFACT_RETENTION = "artifact_retention"
    CHECKPOINT_RETENTION = "checkpoint_retention"
    LOCAL_STAGING_RETENTION = "local_staging_retention"


type MaintenanceRecoveryKind = Literal[
    RecoveryKind.MISSION_FINALIZATION,
    RecoveryKind.ARTIFACT_PUBLICATION,
    RecoveryKind.EVENT_PROJECTION,
    RecoveryKind.ARTIFACT_RETENTION,
    RecoveryKind.CHECKPOINT_RETENTION,
    RecoveryKind.LOCAL_STAGING_RETENTION,
]

MAINTENANCE_RECOVERY_KINDS = frozenset(
    {
        RecoveryKind.MISSION_FINALIZATION,
        RecoveryKind.ARTIFACT_PUBLICATION,
        RecoveryKind.EVENT_PROJECTION,
        RecoveryKind.ARTIFACT_RETENTION,
        RecoveryKind.CHECKPOINT_RETENTION,
        RecoveryKind.LOCAL_STAGING_RETENTION,
    }
)


def _recovery_subject_digest(
    kind: RecoveryKind,
    world_id: str,
    authority_key: str,
) -> str:
    payload = "\0".join(
        ("archetype.fleet-recovery-subject.v1", kind.value, world_id, authority_key)
    )
    return hashlib.sha256(payload.encode()).hexdigest()


class RecoverySweepStatus(StrEnum):
    """Durable recurring-sweep scheduling states."""

    IDLE = "idle"
    LEASED = "leased"
    RETRY_WAIT = "retry_wait"
    PAUSED = "paused"


class RecoveryExceptionStatus(StrEnum):
    """Sparse per-subject retry states."""

    RETRY_WAIT = "retry_wait"
    DEAD_LETTER = "dead_letter"
    RESOLVED = "resolved"


class RecoveryItemDisposition(StrEnum):
    """Successful item outcomes; retryable failures are raised."""

    COMPLETED = "completed"
    OBSOLETE = "obsolete"


class RecoveryErrorCode(StrEnum):
    """Bounded durable failure categories, never raw exception messages."""

    DISCOVERY_FAILED = "discovery_failed"
    HANDLER_FAILED = "handler_failed"
    SOURCE_CORRUPT = "source_corrupt"
    POLICY_REJECTED = "policy_rejected"
    CAPABILITY_UNAVAILABLE = "capability_unavailable"


class FleetRecoveryCursor(BaseModel):
    """Advisory world-directory cursor returned by one bounded invocation."""

    model_config = _FROZEN_CONFIG

    after_world_id: str = ""

    @field_validator("after_world_id")
    @classmethod
    def _bounded_world_cursor(cls, value: str) -> str:
        if len(value) > _MAX_SAFE_IDENTIFIER_CHARS:
            raise ValueError("fleet recovery world cursor is too long")
        return value


class RecoveryLimits(BaseModel):
    """Hard budgets for a single non-recurring process invocation."""

    model_config = _FROZEN_CONFIG

    world_page_size: PositiveInt = 100
    max_sweeps: PositiveInt = 100
    items_per_sweep: PositiveInt = 100
    max_elapsed_ms: PositiveInt = 30_000

    @field_validator("world_page_size")
    @classmethod
    def _bounded_world_page(cls, value: int) -> int:
        if value > 10_000:
            raise ValueError("recovery world page size cannot exceed 10000")
        return value

    @field_validator("max_sweeps", "items_per_sweep")
    @classmethod
    def _bounded_work_budget(cls, value: int) -> int:
        if value > 10_000:
            raise ValueError("recovery work budgets cannot exceed 10000")
        return value

    @field_validator("max_elapsed_ms")
    @classmethod
    def _bounded_elapsed_budget(cls, value: int) -> int:
        if value > 86_400_000:
            raise ValueError("one recovery pass cannot exceed 24 hours")
        return value


class RecoveryPolicy(BaseModel):
    """Storage-authority durations and deterministic retry policy."""

    model_config = _FROZEN_CONFIG

    lease_ms: PositiveInt = 60_000
    recurring_delay_ms: NonNegativeInt = 5_000
    initial_retry_delay_ms: PositiveInt = 1_000
    maximum_retry_delay_ms: PositiveInt = 300_000
    maximum_exception_attempts: PositiveInt = 8
    maximum_sweep_failures: PositiveInt = 8
    jitter_basis_points: NonNegativeInt = 1_000

    @model_validator(mode="after")
    def _bounded_policy(self) -> RecoveryPolicy:
        if self.lease_ms > 86_400_000:
            raise ValueError("recovery lease cannot exceed 24 hours")
        if self.recurring_delay_ms > 31_536_000_000:
            raise ValueError("recovery recurring delay cannot exceed 365 days")
        if self.maximum_retry_delay_ms < self.initial_retry_delay_ms:
            raise ValueError("maximum retry delay must cover the initial delay")
        if self.maximum_retry_delay_ms > 31_536_000_000:
            raise ValueError("recovery retry delay cannot exceed 365 days")
        if self.jitter_basis_points > 5_000:
            raise ValueError("recovery jitter cannot exceed 50 percent")
        if self.maximum_exception_attempts > 1_000_000:
            raise ValueError("recovery exception attempts cannot exceed 1000000")
        if self.maximum_sweep_failures > 1_000_000:
            raise ValueError("recovery sweep failures cannot exceed 1000000")
        return self


class RecoverySubject(BaseModel):
    """Safe, exact source-family reference passed to a narrow handler."""

    model_config = _FROZEN_CONFIG

    world_id: str
    kind: RecoveryKind
    subject_key: str
    authority_key: str
    cursor_after: str = ""

    @field_validator("world_id")
    @classmethod
    def _safe_world_id(cls, value: str) -> str:
        if not value or len(value) > _MAX_SAFE_IDENTIFIER_CHARS:
            raise ValueError("recovery world_id must be a bounded non-empty identifier")
        return value

    @field_validator("subject_key", "authority_key")
    @classmethod
    def _digest_key(cls, value: str) -> str:
        if not _SHA256_RE.fullmatch(value):
            raise ValueError("recovery subject keys must be lowercase SHA-256 digests")
        return value

    @field_validator("cursor_after")
    @classmethod
    def _safe_cursor(cls, value: str) -> str:
        if value and not _SHA256_RE.fullmatch(value):
            raise ValueError("recovery cursors must be empty or lowercase SHA-256 digests")
        return value

    @model_validator(mode="after")
    def _authority_bound_subject(self) -> RecoverySubject:
        expected = _recovery_subject_digest(self.kind, self.world_id, self.authority_key)
        if self.subject_key != expected:
            raise ValueError("recovery subject_key must be derived from kind, world, and authority")
        return self


class RecoveryPage(BaseModel):
    """One deterministic bounded source page."""

    model_config = _FROZEN_CONFIG

    subjects: tuple[RecoverySubject, ...] = ()
    next_cursor: str = ""
    exhausted: bool = Field(default=True, strict=True)

    @field_validator("next_cursor")
    @classmethod
    def _safe_next_cursor(cls, value: str) -> str:
        if len(value) > _MAX_CURSOR_CHARS:
            raise ValueError("recovery page cursor is too long")
        if value and not _SHA256_RE.fullmatch(value):
            raise ValueError("recovery page cursor must be empty or a lowercase SHA-256 digest")
        return value

    @model_validator(mode="after")
    def _consistent_page(self) -> RecoveryPage:
        keys = [subject.subject_key for subject in self.subjects]
        if len(keys) != len(set(keys)):
            raise ValueError("recovery page cannot repeat a subject")
        cursors = [subject.cursor_after for subject in self.subjects]
        if any(not cursor for cursor in cursors):
            raise ValueError("every discovered recovery subject requires cursor_after")
        if cursors != sorted(cursors) or len(cursors) != len(set(cursors)):
            raise ValueError("recovery page subject cursors must increase strictly")
        if self.exhausted and self.next_cursor:
            raise ValueError("an exhausted recovery page cannot carry a continuation cursor")
        if not self.exhausted and (
            not cursors or not self.next_cursor or self.next_cursor != cursors[-1]
        ):
            raise ValueError("a non-exhausted recovery page requires its final subject cursor")
        return self


class RecoveryItemResult(BaseModel):
    """Typed maintenance feedback; source-family state remains authoritative."""

    model_config = _FROZEN_CONFIG

    subject_key: str
    disposition: RecoveryItemDisposition

    @field_validator("subject_key")
    @classmethod
    def _digest_subject(cls, value: str) -> str:
        if not _SHA256_RE.fullmatch(value):
            raise ValueError("recovery result subject_key must be a lowercase SHA-256 digest")
        return value


class RecoverySweep(BaseModel):
    """Safe durable sweep projection for operator inspection."""

    model_config = _FROZEN_CONFIG

    sweep_key: str
    storage_fingerprint: str
    world_id: str
    kind: RecoveryKind
    status: RecoverySweepStatus
    cursor: str = ""
    cycle: NonNegativeInt = 0
    claimant_digest: str = ""
    lease_expires_at_ms: NonNegativeInt = 0
    fence_epoch: NonNegativeInt = 0
    active_subject_key: str = ""
    consecutive_failures: NonNegativeInt = 0
    maximum_consecutive_failures: PositiveInt = 1
    next_due_at_ms: NonNegativeInt = 0
    last_error_code: RecoveryErrorCode | Literal[""] = ""
    created_at_ms: NonNegativeInt = 0
    updated_at_ms: NonNegativeInt = 0
    paused_at_ms: NonNegativeInt | None = None

    @field_validator("sweep_key", "storage_fingerprint")
    @classmethod
    def _sweep_digest(cls, value: str) -> str:
        if not _SHA256_RE.fullmatch(value):
            raise ValueError("recovery sweep identities must be lowercase SHA-256 digests")
        return value

    @field_validator("claimant_digest", "active_subject_key")
    @classmethod
    def _optional_digest(cls, value: str) -> str:
        if value and not _SHA256_RE.fullmatch(value):
            raise ValueError("recovery inspector keys must be empty or SHA-256 digests")
        return value

    @field_validator("cursor")
    @classmethod
    def _bounded_cursor(cls, value: str) -> str:
        if value and not _SHA256_RE.fullmatch(value):
            raise ValueError("recovery sweep cursor must be empty or a SHA-256 digest")
        return value

    @field_validator("world_id")
    @classmethod
    def _bounded_text(cls, value: str) -> str:
        if len(value) > _MAX_SAFE_IDENTIFIER_CHARS:
            raise ValueError("recovery inspector value is too long")
        return value


class RecoveryException(BaseModel):
    """Safe sparse retry/DLQ projection for operator inspection."""

    model_config = _FROZEN_CONFIG

    exception_key: str
    sweep_key: str
    storage_fingerprint: str
    world_id: str
    kind: RecoveryKind
    subject_key: str
    authority_key: str
    status: RecoveryExceptionStatus
    attempt_count: NonNegativeInt
    maximum_attempts: PositiveInt
    retry_at_ms: NonNegativeInt
    last_error_code: RecoveryErrorCode | Literal[""] = ""
    created_at_ms: NonNegativeInt
    updated_at_ms: NonNegativeInt
    resolved_at_ms: NonNegativeInt | None = None
    dead_lettered_at_ms: NonNegativeInt | None = None

    @field_validator(
        "exception_key",
        "sweep_key",
        "storage_fingerprint",
        "subject_key",
        "authority_key",
    )
    @classmethod
    def _exception_digest(cls, value: str) -> str:
        if not _SHA256_RE.fullmatch(value):
            raise ValueError("recovery exception identities must be SHA-256 digests")
        return value

    @field_validator("world_id")
    @classmethod
    def _bounded_exception_text(cls, value: str) -> str:
        if len(value) > _MAX_SAFE_IDENTIFIER_CHARS:
            raise ValueError("recovery exception inspector value is too long")
        return value


class RecoveryPassResult(BaseModel):
    """Bounded, durable-state-derived summary of one fleet invocation."""

    model_config = _FROZEN_CONFIG

    cursor: FleetRecoveryCursor = Field(default_factory=FleetRecoveryCursor)
    worlds_examined: NonNegativeInt = 0
    sweeps_examined: NonNegativeInt = 0
    sweeps_acquired: NonNegativeInt = 0
    lease_contentions: NonNegativeInt = 0
    items_examined: NonNegativeInt = 0
    completed: NonNegativeInt = 0
    obsolete: NonNegativeInt = 0
    failed: NonNegativeInt = 0
    dead_lettered: NonNegativeInt = 0
    paused: NonNegativeInt = 0
    elapsed_ms: NonNegativeInt = 0


def recovery_subject_key(kind: RecoveryKind, world_id: str, authority_key: str) -> str:
    """Return the stable digest persisted by the scheduling layer."""

    return _recovery_subject_digest(kind, world_id, authority_key)


def recovery_backoff_ms(
    stable_key: str,
    attempt: int,
    *,
    initial_delay_ms: int,
    maximum_delay_ms: int,
    jitter_basis_points: int,
) -> int:
    """Compute bounded exponential delay with deterministic integer jitter."""

    values = (attempt, initial_delay_ms, maximum_delay_ms, jitter_basis_points)
    if any(type(value) is not int for value in values):
        raise TypeError("recovery backoff inputs must be exact integers")
    if not _SHA256_RE.fullmatch(stable_key):
        raise ValueError("recovery backoff key must be a lowercase SHA-256 digest")
    if attempt < 1 or initial_delay_ms < 1 or maximum_delay_ms < initial_delay_ms:
        raise ValueError("recovery backoff bounds are invalid")
    if not 0 <= jitter_basis_points <= 5_000:
        raise ValueError("recovery jitter must be between 0 and 5000 basis points")

    nominal = min(maximum_delay_ms, initial_delay_ms * (1 << min(attempt - 1, 62)))
    window = nominal * jitter_basis_points // 10_000
    if window == 0:
        return nominal
    material = f"{stable_key}:{attempt}".encode()
    sample = int.from_bytes(hashlib.sha256(material).digest()[:8], "big")
    offset = sample % (2 * window + 1) - window
    return max(0, min(maximum_delay_ms, nominal + offset))
