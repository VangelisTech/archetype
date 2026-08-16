# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Provider execution and recovery contracts for Mission critic Activities."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from archetype.missions.critics.activities import (
    CRITIC_ACTIVITY_KIND,
    CRITIC_ACTIVITY_MEDIA_TYPE,
    CriticActivityRequest,
)
from archetype.missions.critics.contracts import CriticExecutionResult

_REF_PREFIX = "mission-critic+json:sha256:"
_DIGEST = re.compile(r"^[0-9a-f]{64}$")


def critic_provider_operation_id(world_id: str, review_id: str) -> str:
    """Return one bounded provider identity for an exact world-local review."""

    if not world_id.strip() or not review_id.strip():
        raise ValueError("critic provider operation requires world and review identities")
    encoded = json.dumps(
        {
            "activity_id": review_id,
            "kind": CRITIC_ACTIVITY_KIND,
            "world_id": world_id,
        },
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    return f"{CRITIC_ACTIVITY_KIND}:{hashlib.sha256(encoded).hexdigest()}"


@dataclass(frozen=True, slots=True)
class CriticActivityRequestRef:
    """Content identity retained by generic critic Activity admission."""

    ref: str
    digest: str

    def __post_init__(self) -> None:
        if not _DIGEST.fullmatch(self.digest) or self.ref != f"{_REF_PREFIX}{self.digest}":
            raise ValueError("critic request reference does not match its digest")


@dataclass(frozen=True, slots=True)
class CriticActivityResultRef:
    """Bounded content identity retained by generic critic result recording."""

    ref: str
    digest: str
    media_type: str = CRITIC_ACTIVITY_MEDIA_TYPE
    size_bytes: int = 0

    def __post_init__(self) -> None:
        if not _DIGEST.fullmatch(self.digest) or self.ref != f"{_REF_PREFIX}{self.digest}":
            raise ValueError("critic result reference does not match its digest")
        if self.media_type != CRITIC_ACTIVITY_MEDIA_TYPE:
            raise ValueError("critic result reference has an unsupported media type")
        if self.size_bytes < 0:
            raise ValueError("critic result size cannot be negative")


@dataclass(frozen=True, slots=True)
class CriticActivityRetryGuard:
    """Provider-side barrier authorizing one safe critic replay attempt."""

    ref: str
    digest: str

    def __post_init__(self) -> None:
        if not self.ref.strip() or not self.digest.strip():
            raise ValueError("critic Activity retry guard ref and digest cannot be empty")


@dataclass(frozen=True, slots=True)
class CriticRecovered:
    """Provider evidence proves the prior exact-candidate review completed."""

    result: CriticExecutionResult


@dataclass(frozen=True, slots=True)
class CriticConfirmedAbsent:
    """Provider evidence proves absence behind an atomic replay barrier."""

    guard: CriticActivityRetryGuard


@dataclass(frozen=True, slots=True)
class CriticRecoveryUnknown:
    """Provider evidence cannot prove critic completion or safe absence."""

    reason: str = ""


type CriticReconciliation = CriticRecovered | CriticConfirmedAbsent | CriticRecoveryUnknown


@runtime_checkable
class MissionCriticExecutor(Protocol):
    """Execute and reconcile provider-specific exact-candidate critic work."""

    @property
    def provider(self) -> str: ...

    async def execute(
        self,
        *,
        operation_id: str,
        request: CriticActivityRequest,
        attempt: int,
        fence: int,
        retry_guard: CriticActivityRetryGuard | None,
    ) -> CriticExecutionResult: ...

    async def reconcile(
        self,
        *,
        operation_id: str,
        request: CriticActivityRequest,
    ) -> CriticReconciliation: ...


__all__ = [
    "CriticActivityRequestRef",
    "CriticActivityResultRef",
    "CriticActivityRetryGuard",
    "CriticConfirmedAbsent",
    "CriticRecovered",
    "CriticRecoveryUnknown",
    "CriticReconciliation",
    "MissionCriticExecutor",
    "critic_provider_operation_id",
]
