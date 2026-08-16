# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Physical-AI meaning around the generic hosted-episode Activity boundary."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from typing import Literal, Protocol, TypedDict, cast, runtime_checkable

from archetype.core.component import Component
from archetype.physical_ai.hosted_episode import (
    decode_hosted_episode_manifest,
    decode_hosted_episode_requests,
    hosted_episode_request_digest,
    hosted_episode_results_digest,
    hosted_episode_trajectory_digest,
    validate_hosted_episode_result,
)

HOSTED_EPISODE_ACTIVITY_KIND = "physical_ai.hosted_episode"
HOSTED_EPISODE_RESULT_MEDIA_TYPE = (
    "application/vnd.archetype.physical-ai.hosted-episode-result+json"
)
HOSTED_EPISODE_ARROW_MEDIA_TYPE = "application/vnd.apache.arrow.stream"
HOSTED_EPISODE_REQUEST_REF_PREFIX = "physical-episode-request+arrow:sha256:"
HOSTED_EPISODE_TRAJECTORY_REF_PREFIX = "physical-episode-trajectory+arrow:sha256:"
HOSTED_EPISODE_RESULTS_REF_PREFIX = "physical-episode-results+arrow:sha256:"
HOSTED_EPISODE_MANIFEST_REF_PREFIX = "physical-episode-manifest+arrow:sha256:"
HOSTED_EPISODE_RESULT_REF_PREFIX = "physical-episode-result+json:sha256:"
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
type HostedEpisodePayloadKind = Literal["trajectory", "episode-results", "manifest"]
_PAYLOAD_PREFIX = {
    "trajectory": HOSTED_EPISODE_TRAJECTORY_REF_PREFIX,
    "episode-results": HOSTED_EPISODE_RESULTS_REF_PREFIX,
    "manifest": HOSTED_EPISODE_MANIFEST_REF_PREFIX,
}


def _bounded(value: str, field: str, *, maximum: int = 4096) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be a non-empty string")
    if len(value) > maximum:
        raise ValueError(f"{field} must be at most {maximum} characters")
    return value


def _digest(value: str, field: str) -> str:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise ValueError(f"{field} must be a lowercase SHA-256 digest")
    return value


def _content_ref(ref: str, digest: str, field: str, prefixes: frozenset[str]) -> None:
    _bounded(ref, f"{field} ref")
    _digest(digest, f"{field} digest")
    matching = tuple(prefix for prefix in prefixes if ref.startswith(prefix))
    if len(matching) != 1 or ref.removeprefix(matching[0]) != digest:
        raise ValueError(f"{field} ref must embed its exact lowercase SHA-256 digest")


def hosted_episode_provider_operation_id(world_id: str, activity_id: str) -> str:
    """Derive one stable, world-scoped provider operation identity."""

    _bounded(world_id, "world_id", maximum=512)
    _bounded(activity_id, "activity_id", maximum=512)
    encoded = json.dumps(
        {
            "activity_id": activity_id,
            "contract": HOSTED_EPISODE_ACTIVITY_KIND,
            "world_id": world_id,
        },
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    return f"physical-episode:{hashlib.sha256(encoded).hexdigest()}"


@dataclass(frozen=True, slots=True)
class HostedEpisodeRequestRef:
    """Content identity for one canonical hosted-episode request."""

    ref: str
    digest: str
    size_bytes: int
    media_type: str = HOSTED_EPISODE_ARROW_MEDIA_TYPE

    def __post_init__(self) -> None:
        _content_ref(
            self.ref,
            self.digest,
            "hosted request",
            frozenset({HOSTED_EPISODE_REQUEST_REF_PREFIX}),
        )
        if self.media_type != HOSTED_EPISODE_ARROW_MEDIA_TYPE:
            raise ValueError("hosted request must use canonical Arrow stream media type")
        if self.size_bytes < 1:
            raise ValueError("hosted request size must be positive")


@dataclass(frozen=True, slots=True)
class HostedEpisodeRequestIdentity:
    """Bounded request identity retained by the generic control catalog."""

    ref: str
    digest: str

    def __post_init__(self) -> None:
        _content_ref(
            self.ref,
            self.digest,
            "hosted request identity",
            frozenset({HOSTED_EPISODE_REQUEST_REF_PREFIX}),
        )


@dataclass(frozen=True, slots=True)
class HostedEpisodePayloadRef:
    """Content identity for one canonical hosted result payload."""

    kind: HostedEpisodePayloadKind
    ref: str
    digest: str
    size_bytes: int
    media_type: str = HOSTED_EPISODE_ARROW_MEDIA_TYPE

    def __post_init__(self) -> None:
        try:
            prefix = _PAYLOAD_PREFIX[self.kind]
        except KeyError:
            raise ValueError("hosted payload kind is not supported") from None
        _content_ref(
            self.ref,
            self.digest,
            f"hosted {self.kind} payload",
            frozenset({prefix}),
        )
        if self.media_type != HOSTED_EPISODE_ARROW_MEDIA_TYPE:
            raise ValueError("hosted payload must use canonical Arrow stream media type")
        if self.size_bytes < 1:
            raise ValueError("hosted payload size must be positive")


@dataclass(frozen=True, slots=True)
class HostedEpisodeActivityResultRef:
    """Bounded descriptor retained by the generic Activity catalog."""

    ref: str
    digest: str
    size_bytes: int
    media_type: str = HOSTED_EPISODE_RESULT_MEDIA_TYPE

    def __post_init__(self) -> None:
        _content_ref(
            self.ref,
            self.digest,
            "hosted result",
            frozenset({HOSTED_EPISODE_RESULT_REF_PREFIX}),
        )
        if self.media_type != HOSTED_EPISODE_RESULT_MEDIA_TYPE:
            raise ValueError("hosted result has a non-canonical media type")
        if self.size_bytes < 1:
            raise ValueError("hosted result size must be positive")


@dataclass(frozen=True, slots=True)
class HostedEpisodeRetryGuard:
    """Provider-side atomic-start evidence for one safe fresh claim."""

    ref: str
    digest: str

    def __post_init__(self) -> None:
        _bounded(self.ref, "hosted retry guard ref")
        _digest(self.digest, "hosted retry guard digest")


@dataclass(frozen=True, slots=True)
class HostedEpisodeProviderResult:
    """Complete provider-durable bytes, recovered before Activity recording."""

    request_ipc: bytes
    trajectory_ipc: bytes
    episode_results_ipc: bytes
    manifest_ipc: bytes

    def __post_init__(self) -> None:
        validate_hosted_episode_result(
            self.request_ipc,
            self.trajectory_ipc,
            self.episode_results_ipc,
            self.manifest_ipc,
        )

    @property
    def operation_id(self) -> str:
        return str(decode_hosted_episode_manifest(self.manifest_ipc)["operation_id"])

    @property
    def request_digest(self) -> str:
        return hosted_episode_request_digest(self.request_ipc)


@dataclass(frozen=True, slots=True)
class HostedEpisodePublishedResult:
    """Complete family publication plus its bounded Activity descriptor."""

    operation_id: str
    request: HostedEpisodeRequestRef
    trajectory: HostedEpisodePayloadRef
    episode_results: HostedEpisodePayloadRef
    manifest: HostedEpisodePayloadRef
    activity_result: HostedEpisodeActivityResultRef
    episode_count: int
    trajectory_row_count: int
    transition_count: int
    success_count: int

    def __post_init__(self) -> None:
        _bounded(self.operation_id, "hosted result operation_id", maximum=256)
        if (
            min(
                self.episode_count,
                self.trajectory_row_count,
                self.transition_count,
                self.success_count,
            )
            < 0
        ):
            raise ValueError("hosted result completeness counts cannot be negative")
        if self.episode_count < 1:
            raise ValueError("hosted result must contain at least one episode")
        if self.success_count > self.episode_count:
            raise ValueError("hosted success count exceeds episode count")
        if self.trajectory_row_count != self.episode_count + self.transition_count:
            raise ValueError("hosted trajectory count is not reset rows plus transitions")
        if (
            self.trajectory.kind,
            self.episode_results.kind,
            self.manifest.kind,
        ) != ("trajectory", "episode-results", "manifest"):
            raise ValueError("hosted result payload references have incorrect kinds")

    def observation(self, activity_id: str) -> HostedEpisodeObservation:
        """Return the exact factual marker to commit in a later tick."""

        return HostedEpisodeObservation(
            activity_id=activity_id,
            operation_id=self.operation_id,
            request_ref=self.request.ref,
            request_digest=self.request.digest,
            result_ref=self.activity_result.ref,
            result_digest=self.activity_result.digest,
            trajectory_ref=self.trajectory.ref,
            trajectory_digest=self.trajectory.digest,
            episode_results_ref=self.episode_results.ref,
            episode_results_digest=self.episode_results.digest,
            manifest_ref=self.manifest.ref,
            manifest_digest=self.manifest.digest,
            episode_count=self.episode_count,
            trajectory_row_count=self.trajectory_row_count,
            transition_count=self.transition_count,
            success_count=self.success_count,
        )


@dataclass(frozen=True, slots=True)
class HostedEpisodeRecovered:
    """Provider evidence proves the exact prior operation completed."""

    result: HostedEpisodeProviderResult


@dataclass(frozen=True, slots=True)
class HostedEpisodeConfirmedAbsent:
    """Provider evidence proves atomic-start absence behind a retry guard."""

    guard: HostedEpisodeRetryGuard


@dataclass(frozen=True, slots=True)
class HostedEpisodeRecoveryUnknown:
    """Provider truth cannot establish completion or safe absence."""

    reason: str = ""


type HostedEpisodeReconciliation = (
    HostedEpisodeRecovered | HostedEpisodeConfirmedAbsent | HostedEpisodeRecoveryUnknown
)


class HostedEpisodeManifestFacts(TypedDict):
    """Typed canonical manifest row returned after full validation."""

    contract_version: str
    operation_id: str
    manifest_id: str
    request_digest: str
    trajectory_digest: str
    episode_results_digest: str
    episode_count: int
    trajectory_row_count: int
    transition_count: int
    success_count: int


@runtime_checkable
class HostedEpisodeProvider(Protocol):
    """Execute or reconcile a whole episode under provider-owned semantics."""

    @property
    def provider(self) -> str: ...

    async def execute(
        self,
        *,
        operation_id: str,
        request_ipc: bytes,
        attempt: int,
        fence: int,
        retry_guard: HostedEpisodeRetryGuard | None,
    ) -> HostedEpisodeProviderResult: ...

    async def reconcile(
        self,
        *,
        operation_id: str,
        request_ipc: bytes,
    ) -> HostedEpisodeReconciliation: ...


class HostedEpisodeIntent(Component):
    """Committed decision to execute one immutable whole-episode request."""

    activity_id: str
    operation_id: str
    request_ref: str
    request_digest: str
    request_size_bytes: int
    episode_count: int


class HostedEpisodeObservation(Component):
    """Committed factual binding to one complete, durable hosted result."""

    activity_id: str
    operation_id: str
    request_ref: str
    request_digest: str
    result_ref: str
    result_digest: str
    trajectory_ref: str
    trajectory_digest: str
    episode_results_ref: str
    episode_results_digest: str
    manifest_ref: str
    manifest_digest: str
    episode_count: int
    trajectory_row_count: int
    transition_count: int
    success_count: int


def validate_hosted_provider_result(
    result: HostedEpisodeProviderResult,
    *,
    request_ipc: bytes,
    operation_id: str,
) -> HostedEpisodeManifestFacts:
    """Validate exact request identity and return the canonical manifest."""

    if result.request_ipc != request_ipc:
        raise ValueError("provider result does not bind the exact admitted request bytes")
    manifest = validate_hosted_episode_result(
        request_ipc,
        result.trajectory_ipc,
        result.episode_results_ipc,
        result.manifest_ipc,
    )
    request_rows = decode_hosted_episode_requests(request_ipc)
    if (
        result.operation_id != operation_id
        or manifest["operation_id"] != operation_id
        or any(row["operation_id"] != operation_id for row in request_rows)
    ):
        raise ValueError("provider result belongs to another operation")
    if manifest["request_digest"] != hosted_episode_request_digest(request_ipc):
        raise ValueError("provider manifest has another request digest")
    if manifest["trajectory_digest"] != hosted_episode_trajectory_digest(result.trajectory_ipc):
        raise ValueError("provider manifest has another trajectory digest")
    if manifest["episode_results_digest"] != hosted_episode_results_digest(
        result.episode_results_ipc
    ):
        raise ValueError("provider manifest has another episode-results digest")
    return cast(HostedEpisodeManifestFacts, manifest)


__all__ = [
    "HOSTED_EPISODE_ACTIVITY_KIND",
    "HOSTED_EPISODE_ARROW_MEDIA_TYPE",
    "HOSTED_EPISODE_MANIFEST_REF_PREFIX",
    "HOSTED_EPISODE_REQUEST_REF_PREFIX",
    "HOSTED_EPISODE_RESULT_MEDIA_TYPE",
    "HOSTED_EPISODE_RESULT_REF_PREFIX",
    "HOSTED_EPISODE_RESULTS_REF_PREFIX",
    "HOSTED_EPISODE_TRAJECTORY_REF_PREFIX",
    "HostedEpisodeActivityResultRef",
    "HostedEpisodeConfirmedAbsent",
    "HostedEpisodeIntent",
    "HostedEpisodeManifestFacts",
    "HostedEpisodeObservation",
    "HostedEpisodePayloadKind",
    "HostedEpisodePayloadRef",
    "HostedEpisodeProvider",
    "HostedEpisodeProviderResult",
    "HostedEpisodePublishedResult",
    "HostedEpisodeReconciliation",
    "HostedEpisodeRecovered",
    "HostedEpisodeRecoveryUnknown",
    "HostedEpisodeRequestIdentity",
    "HostedEpisodeRequestRef",
    "HostedEpisodeRetryGuard",
    "hosted_episode_provider_operation_id",
    "validate_hosted_provider_result",
]
