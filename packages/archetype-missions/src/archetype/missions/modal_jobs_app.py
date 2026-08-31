# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Deployed built-in Modal controller for durable Mission jobs.

The controller validates a bounded canonical request, self-registers the
current Function call before effects, runs only deployment-fixed author or
critic implementations, persists sandbox resource identity as it is created,
and leaves one family-owned first result for result-first polling.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
from dataclasses import dataclass
from enum import StrEnum
from pathlib import PurePosixPath
from typing import Any, cast

from archetype.missions.activity_values import AuthorValueRedactor, MissionAuthorValueCodec
from archetype.missions.coding_agents.app_server import CodexAppServerDriver
from archetype.missions.coding_agents.harness import (
    CodingAgentHarness,
    CodingAgentHarnessConfig,
)
from archetype.missions.critics.activities import (
    CriticActivityCodec,
    CriticActivityRedactor,
)
from archetype.missions.critics.harness import (
    CodexAppServerCriticDriver,
    CriticHarness,
    CriticHarnessConfig,
)
from archetype.missions.modal_author import (
    ModalMissionAuthorExecutor,
    ModalMissionAuthorExecutorConfig,
)
from archetype.missions.modal_critic import (
    ModalMissionCriticExecutor,
    ModalMissionCriticExecutorConfig,
)
from archetype.missions.modal_jobs import (
    ModalMissionFamily,
    ModalMissionJobClient,
    ModalMissionJobNamespace,
    ModalMissionJobRef,
    ModalMissionJobResourceRecorder,
    ModalMissionJobResources,
    ModalMissionJobUnknown,
    modal_mission_call_record,
)
from archetype.missions.modal_jobs_runtime import (
    ModalMissionJobRuntimeConfig,
    ModalNamedMissionJobRuntime,
)
from archetype.missions.sandboxes.contracts import SandboxSpec
from archetype.missions.sandboxes.modal import (
    MODAL_ACTIVITY_PROTOCOL_EPOCH,
    ModalCodexAppServerConnector,
    ModalSandboxBackend,
    ModalSandboxConfig,
    ModalSandboxOperationCapability,
    ModalSandboxOperationResourceCleanup,
)
from archetype.missions.sandboxes.modal_barrier import ModalProviderStartBarrier
from archetype.redaction import RedactionService

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


class ModalMissionControllerExecutionFailed(RuntimeError):
    """A built-in controller phase failed without leaking its raw exception."""

    def __init__(self, error_type: str) -> None:
        if not error_type.strip() or len(error_type) > 128:
            raise ValueError("Modal Mission controller error type is invalid")
        self.error_type = error_type
        super().__init__(f"Modal Mission controller execution failed ({error_type})")


@dataclass(frozen=True, slots=True)
class ModalMissionControllerAppConfig:
    """Deployment-fixed identity and built-in execution policy."""

    namespace: ModalMissionJobNamespace
    runtime: ModalMissionJobRuntimeConfig
    redactor: object
    timeout_seconds: int = 24 * 60 * 60
    sandbox_timeout_seconds: int = 4 * 60 * 60
    sandbox_idle_timeout_seconds: int = 20 * 60
    author_model: str = ""
    author_workspace: str = "/workspace/repo"
    critic_workspace: str = "/workspace/review"
    auth_volume_name: str = "archetype-codex-auth"
    github_secret_name: str = "archetype-github"
    checkpoint_after_dispatch: bool = True
    failpoint: ModalMissionControllerFailpoint | None = None

    def __post_init__(self) -> None:
        if self.runtime.author_function_name != self.runtime.critic_function_name:
            raise ValueError(
                "Modal Mission controller requires one shared author/critic function name"
            )
        if type(self.redactor) is not RedactionService:
            raise ValueError("Modal Mission controller requires the built-in redaction service")
        redactor = cast(RedactionService, self.redactor)
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
        if not self.namespace.image_id.startswith("im-"):
            raise ValueError("Modal Mission controller requires a pinned im-... sandbox image")
        for label, value in (
            ("sandbox_timeout_seconds", self.sandbox_timeout_seconds),
            ("sandbox_idle_timeout_seconds", self.sandbox_idle_timeout_seconds),
        ):
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise ValueError(f"Modal Mission controller {label} must be positive")
        for label, value in (
            ("author_workspace", self.author_workspace),
            ("critic_workspace", self.critic_workspace),
        ):
            path = PurePosixPath(value)
            if not path.is_absolute() or str(path) in {"/", "."}:
                raise ValueError(f"Modal Mission controller {label} must be non-root absolute")
        if not self.auth_volume_name.strip() or not self.github_secret_name.strip():
            raise ValueError("Modal Mission controller named secrets must not be empty")
        if not isinstance(self.checkpoint_after_dispatch, bool):
            raise ValueError("Modal Mission controller checkpoint policy must be a boolean")
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
    """The remote controller does not poll its own family result."""

    return False


def _sandbox_backend(config: ModalMissionControllerAppConfig) -> ModalSandboxBackend:
    return ModalSandboxBackend(
        ModalSandboxConfig(
            app_name=config.runtime.app_name,
            image_id=config.namespace.image_id,
            auth_volume_name=config.auth_volume_name,
            github_secret_name=config.github_secret_name,
            workspace_name=config.runtime.workspace_name,
            environment_name=config.runtime.environment_name,
            operation_protocol_epoch=MODAL_ACTIVITY_PROTOCOL_EPOCH,
        )
    )


def _provider_barrier(config: ModalMissionControllerAppConfig) -> ModalProviderStartBarrier:
    return ModalProviderStartBarrier(
        workspace_name=config.runtime.workspace_name,
        environment_name=config.runtime.environment_name,
        app_name=config.runtime.app_name,
        protocol_epoch=MODAL_ACTIVITY_PROTOCOL_EPOCH,
    )


async def _cleanup_job_resources(
    *,
    capability: ModalSandboxOperationCapability,
    resources: ModalMissionJobResources,
    spec: SandboxSpec,
) -> None:
    identity = capability.identity(resources.intent.ref.operation_id)
    if identity.digest != resources.intent.operation_digest:
        raise ValueError("durable sandbox intent belongs to another provider operation")
    await capability.cleanup_resources(
        cleanup=ModalSandboxOperationResourceCleanup(
            identity=identity,
            cohort_id=resources.intent.cohort_id,
            auth_sandbox_id=(None if resources.auth is None else resources.auth.sandbox_id),
            mission_sandbox_id=(
                None if resources.mission is None else resources.mission.sandbox_id
            ),
        ),
        spec=spec,
    )


async def _execute_builtin_job(
    *,
    config: ModalMissionControllerAppConfig,
    client: ModalMissionJobClient,
    ref: ModalMissionJobRef,
    request_bytes: bytes,
) -> None:
    """Run one deployment-fixed family implementation and persist its result."""

    redactor = cast(RedactionService, config.redactor)
    recorder = ModalMissionJobResourceRecorder(client, ref)
    backend = _sandbox_backend(config)
    capability = ModalSandboxOperationCapability(
        backend,
        resource_observer=recorder.observe,
    )
    barrier = _provider_barrier(config)
    if ref.family == "author":
        request = MissionAuthorValueCodec(redactor=redactor).decode_request(request_bytes)
        executor = ModalMissionAuthorExecutor(
            capability=capability,
            barrier=barrier,
            harness=CodingAgentHarness(
                CodexAppServerDriver(
                    connector=ModalCodexAppServerConnector(),
                    model=config.author_model,
                    workspace=config.author_workspace,
                ),
                CodingAgentHarnessConfig(workspace=config.author_workspace),
            ),
            redactor=redactor,
            config=ModalMissionAuthorExecutorConfig(
                sandbox_environment=backend.environment,
                workspace=config.author_workspace,
                timeout_seconds=config.sandbox_timeout_seconds,
                idle_timeout_seconds=config.sandbox_idle_timeout_seconds,
                result_dict_name=config.namespace.result_dict_name,
                checkpoint_after_dispatch=config.checkpoint_after_dispatch,
            ),
        )
        spec = executor.sandbox_spec(request)

        async def execute() -> None:
            await executor.execute(
                operation_id=ref.operation_id,
                request=request,
                attempt=1,
                fence=1,
                retry_guard=None,
            )

    elif ref.family == "critic":
        request = CriticActivityCodec(redactor).decode_request(request_bytes)
        executor = ModalMissionCriticExecutor(
            capability=capability,
            barrier=barrier,
            harness=CriticHarness(
                CodexAppServerCriticDriver(
                    connector=ModalCodexAppServerConnector(),
                    workspace=config.critic_workspace,
                ),
                CriticHarnessConfig(workspace=config.critic_workspace),
            ),
            redactor=redactor,
            config=ModalMissionCriticExecutorConfig(
                sandbox_environment=backend.environment,
                workspace=config.critic_workspace,
                timeout_seconds=config.sandbox_timeout_seconds,
                idle_timeout_seconds=config.sandbox_idle_timeout_seconds,
                result_dict_name=config.namespace.result_dict_name,
            ),
        )
        spec = executor.sandbox_spec(request)

        async def execute() -> None:
            await executor.execute(
                operation_id=ref.operation_id,
                request=request,
                attempt=1,
                fence=1,
                retry_guard=None,
            )

    else:  # pragma: no cover - closed Literal defense
        raise ValueError("Modal Mission controller family is invalid")

    try:
        await execute()
    finally:
        await client.cleanup(
            ref,
            cleaner=lambda resources: _cleanup_job_resources(
                capability=capability,
                resources=resources,
                spec=spec,
            ),
        )


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
    client = ModalMissionJobClient(config.namespace, runtime)
    outcome = await client.register_remote_call(
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
    try:
        await _execute_builtin_job(
            config=config,
            client=client,
            ref=outcome,
            request_bytes=request_bytes,
        )
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        raise ModalMissionControllerExecutionFailed(type(exc).__name__[:128]) from exc
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
    """Build one deployed built-in controller shared by both families.

    The four function arguments are the complete remote input surface.  All
    durable object names and failpoints are deployment-fixed in ``config``.
    The remote surface carries no driver, callback, redactor, image, or secret
    override. Those choices are fixed in ``config`` at deployment.
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
        try:
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
        except asyncio.CancelledError:
            raise
        except (
            ModalMissionControllerExecutionFailed,
            ModalMissionControllerFailpointReached,
            ModalMissionControllerRejected,
        ):
            raise
        except Exception as exc:
            raise ModalMissionControllerExecutionFailed(type(exc).__name__[:128]) from exc

    return app, mission_controller


__all__ = [
    "MODAL_MISSION_CONTROLLER_MAX_RECEIPT_BYTES",
    "MODAL_MISSION_CONTROLLER_MAX_REQUEST_BYTES",
    "ModalMissionControllerAppConfig",
    "ModalMissionControllerExecutionFailed",
    "ModalMissionControllerFailpoint",
    "ModalMissionControllerFailpointReached",
    "ModalMissionControllerRejected",
    "build_modal_mission_controller_app",
]
