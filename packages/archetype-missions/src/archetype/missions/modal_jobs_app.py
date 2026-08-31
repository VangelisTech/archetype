# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Identity-only deployed Modal controller for durable Mission jobs.

This first controller slice deliberately stops at the provider-effect boundary.
It validates the bounded canonical request, self-registers the current Modal
Function call against the durable start marker, and returns a small identity
receipt.  It does not construct a sandbox, invoke an agent, touch Git, or route
production Mission traffic.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, cast

from archetype.missions.activity_values import AuthorValueRedactor, MissionAuthorValueCodec
from archetype.missions.critics.activities import (
    CriticActivityCodec,
    CriticActivityRedactor,
)
from archetype.missions.modal_jobs import (
    ModalMissionFamily,
    ModalMissionJobClient,
    ModalMissionJobNamespace,
    ModalMissionJobRef,
    ModalMissionJobUnknown,
    modal_mission_call_record,
)
from archetype.missions.modal_jobs_runtime import (
    ModalMissionJobRuntimeConfig,
    ModalNamedMissionJobRuntime,
)

MODAL_MISSION_CONTROLLER_MAX_REQUEST_BYTES = 1 << 20
MODAL_MISSION_CONTROLLER_MAX_RECEIPT_BYTES = 4 << 10


class ModalMissionControllerFailpoint(StrEnum):
    """Deterministic crash boundaries for offline and paid recovery proofs."""

    BEFORE_SELF_REGISTRATION = "before-self-registration"
    AFTER_SELF_REGISTRATION = "after-self-registration"


class ModalMissionControllerFailpointReached(RuntimeError):
    """A deployment-fixed controller failpoint stopped this call."""

    def __init__(self, failpoint: ModalMissionControllerFailpoint) -> None:
        self.failpoint = failpoint
        super().__init__(f"Modal Mission controller failpoint reached: {failpoint.value}")


class ModalMissionControllerRejected(RuntimeError):
    """The current Modal call does not own the exact durable operation."""

    def __init__(self, reason: str) -> None:
        if not isinstance(reason, str) or not reason.strip() or len(reason) > 4096:
            raise ValueError("Modal Mission controller rejection reason is invalid")
        self.reason = reason
        super().__init__(f"Modal Mission controller rejected the call: {reason}")


@dataclass(frozen=True, slots=True)
class ModalMissionControllerAppConfig:
    """Deployment-fixed identity and resources for the proof-only controller."""

    namespace: ModalMissionJobNamespace
    runtime: ModalMissionJobRuntimeConfig
    redactor: object
    timeout_seconds: int = 300
    failpoint: ModalMissionControllerFailpoint | None = None

    def __post_init__(self) -> None:
        if self.runtime.author_function_name != self.runtime.critic_function_name:
            raise ValueError(
                "Modal Mission controller requires one shared author/critic function name"
            )
        if not isinstance(self.redactor, AuthorValueRedactor) or not isinstance(
            self.redactor,
            CriticActivityRedactor,
        ):
            raise ValueError("Modal Mission controller requires a redaction capability")
        redactor = cast(AuthorValueRedactor, self.redactor)
        if self.namespace.redaction_policy_id != redactor.policy_id:
            raise ValueError(
                "Modal Mission controller redaction capability conflicts with its namespace"
            )
        if (
            isinstance(self.timeout_seconds, bool)
            or not isinstance(self.timeout_seconds, int)
            or self.timeout_seconds < 1
        ):
            raise ValueError("Modal Mission controller timeout must be a positive integer")
        if self.failpoint is not None and not isinstance(
            self.failpoint,
            ModalMissionControllerFailpoint,
        ):
            raise ValueError("Modal Mission controller failpoint is invalid")

    @property
    def function_name(self) -> str:
        return self.runtime.author_function_name


def _load_modal() -> Any:
    try:
        import modal
    except ImportError as exc:  # pragma: no cover - depends on an optional extra
        raise RuntimeError(
            "Modal Mission controllers require the archetype-missions[modal] extra"
        ) from exc
    return modal


def _canonical_request_digest(
    *,
    family: ModalMissionFamily,
    request_bytes: bytes,
    redactor: AuthorValueRedactor,
) -> str:
    if type(request_bytes) is not bytes:
        raise TypeError("Modal Mission controller request must be bytes")
    if not request_bytes:
        raise ValueError("Modal Mission controller request must not be empty")
    if len(request_bytes) > MODAL_MISSION_CONTROLLER_MAX_REQUEST_BYTES:
        raise ValueError("Modal Mission controller request exceeds its 1 MiB bound")
    if family == "author":
        MissionAuthorValueCodec(redactor=redactor).decode_request(request_bytes)
    elif family == "critic":
        CriticActivityCodec(cast(CriticActivityRedactor, redactor)).decode_request(request_bytes)
    else:  # pragma: no cover - closed Literal defense
        raise ValueError("Modal Mission controller family is invalid")
    return hashlib.sha256(request_bytes).hexdigest()


def _trip_failpoint(
    configured: ModalMissionControllerFailpoint | None,
    boundary: ModalMissionControllerFailpoint,
) -> None:
    if configured is boundary:
        raise ModalMissionControllerFailpointReached(boundary)


async def _result_not_routed(_ref: ModalMissionJobRef) -> bool:
    """Result observation belongs to a later production-routing slice."""

    return False


async def _run_controller(
    *,
    config: ModalMissionControllerAppConfig,
    family: ModalMissionFamily,
    operation_id: str,
    request_bytes: bytes,
    requested_namespace_digest: str,
    call_id: str,
) -> dict[str, str | int]:
    if requested_namespace_digest != config.namespace.digest:
        raise ValueError("Modal Mission controller namespace does not match deployment")
    request_digest = _canonical_request_digest(
        family=family,
        request_bytes=request_bytes,
        redactor=cast(AuthorValueRedactor, config.redactor),
    )
    # Validate the complete family/operation/request identity before named Modal
    # state is opened.  register_remote_call repeats this construction at the
    # durable boundary and remains the sole authority for call ownership.
    config.namespace.start_record(
        family=family,
        operation_id=operation_id,
        request_digest=request_digest,
    )
    _trip_failpoint(
        config.failpoint,
        ModalMissionControllerFailpoint.BEFORE_SELF_REGISTRATION,
    )
    runtime = ModalNamedMissionJobRuntime(
        config.runtime,
        result_ready=_result_not_routed,
    )
    outcome = await ModalMissionJobClient(config.namespace, runtime).register_remote_call(
        family=family,
        operation_id=operation_id,
        request_digest=request_digest,
        call_id=call_id,
    )
    if isinstance(outcome, ModalMissionJobUnknown):
        raise ModalMissionControllerRejected(outcome.reason)
    _trip_failpoint(
        config.failpoint,
        ModalMissionControllerFailpoint.AFTER_SELF_REGISTRATION,
    )
    receipt = modal_mission_call_record(outcome)
    encoded = json.dumps(
        receipt,
        ensure_ascii=True,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    if len(encoded) > MODAL_MISSION_CONTROLLER_MAX_RECEIPT_BYTES:
        raise RuntimeError("Modal Mission controller identity receipt exceeds its bound")
    return receipt


def build_modal_mission_controller_app(
    config: ModalMissionControllerAppConfig,
    *,
    image: object | None = None,
) -> tuple[object, object]:
    """Build one deployed identity-only controller shared by both families.

    The four function arguments are the complete remote input surface.  All
    durable object names and failpoints are deployment-fixed in ``config``.
    This proof route intentionally ends after self-registration and cannot run
    a production author or critic implementation.
    """

    modal = _load_modal()
    if image is None:
        image = (
            modal.Image.debian_slim(python_version="3.12")
            .uv_pip_install("archetype-missions[modal]==0.6.3")
            .add_local_python_source("archetype", copy=True)
        )
    app = modal.App(config.runtime.app_name)

    @app.function(
        name=config.function_name,
        image=image,
        retries=0,
        serialized=True,
        timeout=config.timeout_seconds,
    )
    async def mission_controller(
        family: ModalMissionFamily,
        operation_id: str,
        request_bytes: bytes,
        namespace_digest: str,
    ) -> dict[str, str | int]:
        modal = _load_modal()
        call_id = modal.current_function_call_id()
        if not isinstance(call_id, str) or not call_id:
            raise RuntimeError("Modal Mission controller has no current Function call identity")
        return await _run_controller(
            config=config,
            family=family,
            operation_id=operation_id,
            request_bytes=request_bytes,
            requested_namespace_digest=namespace_digest,
            call_id=call_id,
        )

    return app, mission_controller


__all__ = [
    "MODAL_MISSION_CONTROLLER_MAX_RECEIPT_BYTES",
    "MODAL_MISSION_CONTROLLER_MAX_REQUEST_BYTES",
    "ModalMissionControllerAppConfig",
    "ModalMissionControllerFailpoint",
    "ModalMissionControllerFailpointReached",
    "ModalMissionControllerRejected",
    "build_modal_mission_controller_app",
]
