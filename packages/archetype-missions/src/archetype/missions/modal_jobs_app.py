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
import re
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from enum import StrEnum
from pathlib import PurePosixPath
from typing import Any

from temporalio.client import Client
from temporalio.worker import Worker

from archetype.missions.activity_values import MissionAuthorValueCodec
from archetype.missions.coding_agents.app_server import CodexAppServerDriver
from archetype.missions.coding_agents.harness import (
    CodingAgentHarness,
    CodingAgentHarnessConfig,
)
from archetype.missions.config import MissionsExtensionConfig, MissionTemporalActivityConfig
from archetype.missions.critics.activities import (
    CriticActivityCodec,
)
from archetype.missions.critics.harness import (
    CodexAppServerCriticDriver,
    CriticHarness,
    CriticHarnessConfig,
)
from archetype.missions.execution_profiles import ExecutionProfileCatalog
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
    ModalMissionJobPoll,
    ModalMissionJobRef,
    ModalMissionJobResourceRecorder,
    ModalMissionJobResources,
    ModalMissionJobResult,
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
from archetype.missions.temporal.activity_values import MissionModalActivityValueStore
from archetype.missions.temporal.client import MissionTemporalClient
from archetype.missions.temporal.contracts import (
    MISSION_MODAL_JOB_TASK_QUEUE,
    MISSION_TASK_QUEUE,
)
from archetype.missions.temporal.modal_job_activities import MissionModalJobValueStore
from archetype.missions.temporal.modal_job_client import MissionModalJobTemporalClient
from archetype.missions.temporal.modal_job_worker import create_mission_modal_job_worker
from archetype.redaction import RedactionService

MODAL_MISSION_CONTROLLER_MAX_REQUEST_BYTES = 1 << 20
MODAL_MISSION_CONTROLLER_MAX_RECEIPT_BYTES = 4 << 10
_DEPLOYMENT_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_DEPLOYMENT_SCHEMA_VERSION = 1


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
    timeout_seconds: int = 24 * 60 * 60
    sandbox_timeout_seconds: int = 4 * 60 * 60
    sandbox_idle_timeout_seconds: int = 20 * 60
    author_model: str = ""
    critic_model: str = ""
    author_workspace: str = "/workspace/repo"
    critic_workspace: str = "/workspace/review"
    author_turn_timeout_seconds: int = 45 * 60
    critic_turn_timeout_seconds: int = 45 * 60
    auth_volume_name: str = "archetype-codex-auth"
    github_secret_name: str = "archetype-github"
    checkpoint_after_dispatch: bool = True
    failpoint: ModalMissionControllerFailpoint | None = None

    def __post_init__(self) -> None:
        if self.runtime.author_function_name != self.runtime.critic_function_name:
            raise ValueError(
                "Modal Mission controller requires one shared author/critic function name"
            )
        if self.runtime.create_if_missing:
            raise ValueError("Modal Mission controller requires preprovisioned durable Dicts")
        if self.namespace.redaction_policy_id != RedactionService().policy_id:
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
            ("author_turn_timeout_seconds", self.author_turn_timeout_seconds),
            ("critic_turn_timeout_seconds", self.critic_turn_timeout_seconds),
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


@dataclass(frozen=True, slots=True)
class ModalMissionControllerDeploymentSpec:
    """Immutable inputs whose canonical digest identifies one deployment."""

    controller_image_id: str
    sandbox_image_id: str
    controller_artifact_digest: str
    workspace_name: str
    environment_name: str
    app_name: str
    job_dict_name: str
    result_dict_name: str
    function_name: str
    author_model: str
    critic_model: str
    author_workspace: str = "/workspace/repo"
    critic_workspace: str = "/workspace/review"
    controller_timeout_seconds: int = 24 * 60 * 60
    sandbox_timeout_seconds: int = 4 * 60 * 60
    sandbox_idle_timeout_seconds: int = 20 * 60
    author_turn_timeout_seconds: int = 45 * 60
    critic_turn_timeout_seconds: int = 45 * 60
    auth_volume_name: str = "archetype-codex-auth"
    github_secret_name: str = "archetype-github"
    task_queue: str = MISSION_MODAL_JOB_TASK_QUEUE
    checkpoint_after_dispatch: bool = True

    def __post_init__(self) -> None:
        if not self.controller_image_id.startswith("im-"):
            raise ValueError("Modal Mission controller image must be a pinned im-... image")
        if self.controller_image_id == self.sandbox_image_id:
            raise ValueError("Modal Mission controller and sandbox images must be distinct")
        if not self.task_queue.strip() or self.task_queue == MISSION_TASK_QUEUE:
            raise ValueError("Modal Mission deployment requires a dedicated task queue")
        if _DEPLOYMENT_DIGEST.fullmatch(self.controller_artifact_digest) is None:
            raise ValueError("Modal Mission controller artifact must be a lowercase sha256 digest")
        for label, value in (
            ("author_model", self.author_model),
            ("critic_model", self.critic_model),
        ):
            if not value or value != value.strip() or len(value) > 256:
                raise ValueError(f"Modal Mission deployment {label} must be pinned")
        # Reuse the exact production app/runtime validators during parsing, so
        # a manifest cannot hash successfully and fail only at Worker startup.
        self.app_config(expected_deployment_digest=self.deployment_digest)

    @property
    def manifest(self) -> dict[str, object]:
        """Return the canonical, secret-value-free deployment manifest."""

        return {
            "checkpoint_after_dispatch": self.checkpoint_after_dispatch,
            "controller_artifact_digest": self.controller_artifact_digest,
            "durable_references": {
                "app_name": self.app_name,
                "auth_volume_name": self.auth_volume_name,
                "environment_name": self.environment_name,
                "function_name": self.function_name,
                "github_secret_name": self.github_secret_name,
                "job_dict_name": self.job_dict_name,
                "result_dict_name": self.result_dict_name,
                "task_queue": self.task_queue,
                "workspace_name": self.workspace_name,
            },
            "images": {
                "controller": self.controller_image_id,
                "sandbox": self.sandbox_image_id,
            },
            "kind": "archetype.missions.modal-controller-deployment",
            "models": {
                "author": self.author_model,
                "critic": self.critic_model,
            },
            "protocols": {
                "modal_activity": MODAL_ACTIVITY_PROTOCOL_EPOCH,
                "modal_job": 1,
            },
            "redaction_policy_id": RedactionService().policy_id,
            "schema_version": _DEPLOYMENT_SCHEMA_VERSION,
            "timeouts_seconds": {
                "author_turn": self.author_turn_timeout_seconds,
                "controller": self.controller_timeout_seconds,
                "critic_turn": self.critic_turn_timeout_seconds,
                "sandbox": self.sandbox_timeout_seconds,
                "sandbox_idle": self.sandbox_idle_timeout_seconds,
            },
            "workspaces": {
                "author": self.author_workspace,
                "critic": self.critic_workspace,
            },
        }

    @property
    def deployment_digest(self) -> str:
        return hashlib.sha256(
            json.dumps(
                self.manifest,
                ensure_ascii=True,
                allow_nan=False,
                separators=(",", ":"),
                sort_keys=True,
            ).encode()
        ).hexdigest()

    def app_config(
        self,
        *,
        expected_deployment_digest: str,
        function_version: int | None = None,
        function_id: str | None = None,
    ) -> ModalMissionControllerAppConfig:
        """Build fixed controller config after server-owned digest validation."""

        if _DEPLOYMENT_DIGEST.fullmatch(expected_deployment_digest) is None:
            raise ValueError("Modal Mission expected deployment digest must be lowercase sha256")
        derived = self.deployment_digest
        if expected_deployment_digest != derived:
            raise ValueError(
                "Modal Mission deployment digest conflicts with the server-pinned manifest"
            )
        runtime = ModalMissionJobRuntimeConfig(
            workspace_name=self.workspace_name,
            environment_name=self.environment_name,
            app_name=self.app_name,
            job_dict_name=self.job_dict_name,
            author_function_name=self.function_name,
            critic_function_name=self.function_name,
            function_version=function_version,
            function_id=function_id,
            create_if_missing=False,
        )
        namespace = ModalMissionJobNamespace(
            deployment_digest=derived,
            image_id=self.sandbox_image_id,
            result_dict_name=self.result_dict_name,
            redaction_policy_id=RedactionService().policy_id,
        )
        return ModalMissionControllerAppConfig(
            namespace=namespace,
            runtime=runtime,
            timeout_seconds=self.controller_timeout_seconds,
            sandbox_timeout_seconds=self.sandbox_timeout_seconds,
            sandbox_idle_timeout_seconds=self.sandbox_idle_timeout_seconds,
            author_model=self.author_model,
            critic_model=self.critic_model,
            author_workspace=self.author_workspace,
            critic_workspace=self.critic_workspace,
            author_turn_timeout_seconds=self.author_turn_timeout_seconds,
            critic_turn_timeout_seconds=self.critic_turn_timeout_seconds,
            auth_volume_name=self.auth_volume_name,
            github_secret_name=self.github_secret_name,
            checkpoint_after_dispatch=self.checkpoint_after_dispatch,
        )


@dataclass(frozen=True, slots=True)
class ModalMissionControllerDeploymentReceipt:
    """Server-owned binding from a manifest to one deployed Modal Function."""

    deployment_digest: str
    controller_artifact_digest: str
    controller_image_id: str
    app_name: str
    function_name: str
    function_version: int | None
    function_id: str

    def __post_init__(self) -> None:
        for label, value in (
            ("deployment_digest", self.deployment_digest),
            ("controller_artifact_digest", self.controller_artifact_digest),
        ):
            if _DEPLOYMENT_DIGEST.fullmatch(value) is None:
                raise ValueError(f"Modal Mission receipt {label} must be lowercase sha256")
        if self.function_version is not None and (
            isinstance(self.function_version, bool)
            or not isinstance(self.function_version, int)
            or self.function_version < 1
        ):
            raise ValueError("Modal Mission receipt function_version must be positive")
        if not self.function_id.startswith("fu-") or len(self.function_id) > 1024:
            raise ValueError("Modal Mission receipt function_id is invalid")

    def require(
        self,
        spec: ModalMissionControllerDeploymentSpec,
        *,
        expected_deployment_digest: str,
    ) -> None:
        if self.deployment_digest != expected_deployment_digest:
            raise ValueError("Modal Mission receipt conflicts with the server deployment digest")
        expected = (
            spec.deployment_digest,
            spec.controller_artifact_digest,
            spec.controller_image_id,
            spec.app_name,
            spec.function_name,
        )
        observed = (
            self.deployment_digest,
            self.controller_artifact_digest,
            self.controller_image_id,
            self.app_name,
            self.function_name,
        )
        if observed != expected:
            raise ValueError("Modal Mission receipt does not describe the deployment manifest")


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
    config: ModalMissionControllerAppConfig,
    family: ModalMissionFamily,
    request_bytes: bytes,
) -> str:
    if type(request_bytes) is not bytes:
        raise TypeError("Modal Mission controller request must be bytes")
    if not request_bytes:
        raise ValueError("Modal Mission controller request must not be empty")
    if len(request_bytes) > MODAL_MISSION_CONTROLLER_MAX_REQUEST_BYTES:
        raise ValueError("Modal Mission controller request exceeds its 1 MiB bound")
    redactor = RedactionService()
    if family == "author":
        MissionAuthorValueCodec(redactor=redactor).decode_request(request_bytes)
    elif family == "critic":
        request = CriticActivityCodec(redactor).decode_request(request_bytes)
        if request.policy.driver != CodexAppServerCriticDriver.driver_id:
            raise ValueError("Modal Mission critic request requires the built-in Codex driver")
        if request.policy.model != config.critic_model:
            raise ValueError("Modal Mission critic request model conflicts with deployment")
        if request.policy.timeout_seconds != config.critic_turn_timeout_seconds:
            raise ValueError("Modal Mission critic timeout conflicts with deployment")
    else:  # pragma: no cover - closed Literal defense
        raise ValueError("Modal Mission controller family is invalid")
    return hashlib.sha256(request_bytes).hexdigest()


def _trip_failpoint(
    configured: ModalMissionControllerFailpoint | None,
    boundary: ModalMissionControllerFailpoint,
) -> None:
    if configured is boundary:
        raise ModalMissionControllerFailpointReached(boundary)


async def _result_not_routed(_ref: ModalMissionJobRef) -> bytes | None:
    """The remote controller does not poll its own family result."""

    return None


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


@dataclass(frozen=True, slots=True)
class _BuiltinMissionJob:
    """Deployment-fixed family implementation shared by remote and host paths."""

    capability: ModalSandboxOperationCapability
    spec: SandboxSpec
    execute: Callable[[str], Awaitable[None]]
    read_result: Callable[[str], Awaitable[bytes | None]]


def _builtin_mission_job(
    *,
    config: ModalMissionControllerAppConfig,
    family: ModalMissionFamily,
    request_bytes: bytes,
    recorder: ModalMissionJobResourceRecorder | None = None,
) -> _BuiltinMissionJob:
    redactor = RedactionService()
    backend = _sandbox_backend(config)
    capability = ModalSandboxOperationCapability(
        backend,
        resource_observer=None if recorder is None else recorder.observe,
    )
    barrier = _provider_barrier(config)
    if family == "author":
        author_request = MissionAuthorValueCodec(redactor=redactor).decode_request(request_bytes)
        author_executor = ModalMissionAuthorExecutor(
            capability=capability,
            barrier=barrier,
            harness=CodingAgentHarness(
                CodexAppServerDriver(
                    connector=ModalCodexAppServerConnector(),
                    model=config.author_model,
                    workspace=config.author_workspace,
                    timeout_seconds=config.author_turn_timeout_seconds,
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
                create_result_dict_if_missing=False,
            ),
        )

        async def execute(operation_id: str) -> None:
            await author_executor.execute(
                operation_id=operation_id,
                request=author_request,
                attempt=1,
                fence=1,
                retry_guard=None,
            )

        async def read_result(operation_id: str) -> bytes | None:
            return await author_executor.result_payload_for(
                operation_id=operation_id,
                request=author_request,
            )

        return _BuiltinMissionJob(
            capability=capability,
            spec=author_executor.sandbox_spec(author_request),
            execute=execute,
            read_result=read_result,
        )

    if family == "critic":
        critic_request = CriticActivityCodec(redactor).decode_request(request_bytes)
        if critic_request.policy.driver != CodexAppServerCriticDriver.driver_id:
            raise ValueError("Modal Mission critic request requires the built-in Codex driver")
        critic_executor = ModalMissionCriticExecutor(
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
                create_result_dict_if_missing=False,
            ),
        )

        async def execute(operation_id: str) -> None:
            await critic_executor.execute(
                operation_id=operation_id,
                request=critic_request,
                attempt=1,
                fence=1,
                retry_guard=None,
            )

        async def read_result(operation_id: str) -> bytes | None:
            return await critic_executor.result_payload_for(
                operation_id=operation_id,
                request=critic_request,
            )

        return _BuiltinMissionJob(
            capability=capability,
            spec=critic_executor.sandbox_spec(critic_request),
            execute=execute,
            read_result=read_result,
        )

    raise ValueError("Modal Mission controller family is invalid")


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

    recorder = ModalMissionJobResourceRecorder(client, ref)
    job = _builtin_mission_job(
        config=config,
        family=ref.family,
        request_bytes=request_bytes,
        recorder=recorder,
    )

    try:
        await job.execute(ref.operation_id)
    finally:
        await client.cleanup(
            ref,
            cleaner=lambda resources: _cleanup_job_resources(
                capability=job.capability,
                resources=resources,
                spec=job.spec,
            ),
        )


class ModalMissionBuiltinJobService:
    """Exact host-side adapter for the deployed built-in Mission controller.

    Only ``start`` may spawn. Poll and collect read the family-owned first-result
    register, while cancellation and cleanup target identities already recorded
    by the controller.
    """

    def __init__(
        self,
        config: ModalMissionControllerAppConfig,
        *,
        controller: object | None = None,
    ) -> None:
        self._config = config
        self._controller = controller

    async def start(
        self,
        *,
        family: ModalMissionFamily,
        operation_id: str,
        request_bytes: bytes,
    ) -> ModalMissionJobRef | ModalMissionJobUnknown:
        request_digest = self._request_digest(family, request_bytes)
        return await self._client(family, request_bytes).start(
            family=family,
            operation_id=operation_id,
            request_bytes=request_bytes,
            request_digest=request_digest,
        )

    async def poll(
        self,
        ref: ModalMissionJobRef,
        *,
        request_bytes: bytes,
    ) -> ModalMissionJobPoll:
        try:
            self._require_request(ref, request_bytes)
        except ValueError as exc:
            return ModalMissionJobUnknown(ref, str(exc))
        return await self._client(ref.family, request_bytes).poll(ref)

    async def collect(
        self,
        ref: ModalMissionJobRef,
        *,
        request_bytes: bytes,
    ) -> ModalMissionJobResult | ModalMissionJobUnknown:
        try:
            self._require_request(ref, request_bytes)
        except ValueError as exc:
            return ModalMissionJobUnknown(ref, str(exc))
        return await self._client(ref.family, request_bytes).collect(ref)

    async def cancel(
        self,
        ref: ModalMissionJobRef,
        *,
        request_bytes: bytes,
    ) -> ModalMissionJobRef:
        self._require_request(ref, request_bytes)
        return await self._client(ref.family, request_bytes).cancel(ref)

    async def cleanup(
        self,
        ref: ModalMissionJobRef,
        *,
        request_bytes: bytes,
    ) -> ModalMissionJobRef:
        self._require_request(ref, request_bytes)
        job = _builtin_mission_job(
            config=self._config,
            family=ref.family,
            request_bytes=request_bytes,
        )
        return await self._client(ref.family, request_bytes).cleanup(
            ref,
            cleaner=lambda resources: _cleanup_job_resources(
                capability=job.capability,
                resources=resources,
                spec=job.spec,
            ),
        )

    def _client(
        self,
        family: ModalMissionFamily,
        request_bytes: bytes,
    ) -> ModalMissionJobClient:
        request_digest = self._request_digest(family, request_bytes)

        async def read_result(ref: ModalMissionJobRef) -> bytes | None:
            if (
                ref.family != family
                or ref.request_digest != request_digest
                or ref.namespace_digest != self._config.namespace.digest
            ):
                raise ValueError("Modal Mission result read belongs to another request")
            job = _builtin_mission_job(
                config=self._config,
                family=family,
                request_bytes=request_bytes,
            )
            return await job.read_result(ref.operation_id)

        runtime = ModalNamedMissionJobRuntime(
            self._config.runtime,
            result_reader=read_result,
            functions=(
                None
                if self._controller is None
                else {"author": self._controller, "critic": self._controller}
            ),
        )
        return ModalMissionJobClient(self._config.namespace, runtime)

    def _require_request(self, ref: ModalMissionJobRef, request_bytes: bytes) -> None:
        if ref.namespace_digest != self._config.namespace.digest:
            raise ValueError("Modal Mission job belongs to another deployment namespace")
        if self._request_digest(ref.family, request_bytes) != ref.request_digest:
            raise ValueError("Modal Mission request bytes do not match the durable job")

    def _request_digest(
        self,
        family: ModalMissionFamily,
        request_bytes: bytes,
    ) -> str:
        return _canonical_request_digest(
            config=self._config,
            family=family,
            request_bytes=request_bytes,
        )


@dataclass(frozen=True, slots=True)
class ModalMissionControllerDeployment:
    """Pinned controller app definition ready for the deployment transaction."""

    spec: ModalMissionControllerDeploymentSpec
    config: ModalMissionControllerAppConfig
    app: object
    controller: object


@dataclass(frozen=True, slots=True)
class PreparedModalMissionJobWorker:
    """Split Worker composed only after deployment and state verification."""

    spec: ModalMissionControllerDeploymentSpec
    receipt: ModalMissionControllerDeploymentReceipt
    config: ModalMissionControllerAppConfig
    service: ModalMissionBuiltinJobService
    worker: Worker
    provisioned_dict_names: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class PreparedModalMissionTemporalRoute:
    """One shared value authority bound to both Worker and Mission extension."""

    jobs: PreparedModalMissionJobWorker
    extension_config: MissionsExtensionConfig


async def provision_modal_mission_controller_state(
    config: ModalMissionControllerAppConfig,
) -> tuple[str, ...]:
    """Provision both named Dicts as an explicit deployment-only operation."""

    modal = _load_modal()
    workspace = modal.Workspace.from_context()
    await workspace.hydrate.aio()
    if str(workspace.name or "") != config.runtime.workspace_name:
        raise RuntimeError("Modal workspace does not match the Mission controller namespace")
    names = tuple(
        dict.fromkeys(
            (
                config.runtime.job_dict_name,
                config.namespace.result_dict_name,
            )
        )
    )
    for name in names:
        dictionary = modal.Dict.from_name(
            name,
            environment_name=config.runtime.environment_name,
            create_if_missing=True,
            client=workspace.client,
        )
        await dictionary.hydrate.aio()
    return names


async def verify_modal_mission_controller_deployment(
    config: ModalMissionControllerAppConfig,
    receipt: ModalMissionControllerDeploymentReceipt,
    *,
    function: object | None = None,
) -> str:
    """Resolve once, then verify the receipt-pinned Function object ID."""

    if function is None:
        function = await _hydrated_modal_mission_controller_function(
            config,
            function_version=receipt.function_version,
        )
    observed = getattr(function, "object_id", None)
    if observed != receipt.function_id:
        raise RuntimeError("Modal Mission deployment receipt names another Function object")
    return receipt.function_id


async def _hydrated_modal_mission_controller_function(
    config: ModalMissionControllerAppConfig,
    *,
    function_version: int | None,
) -> Any:
    """Hydrate one version or active Function exactly once for caller-owned use."""

    modal = _load_modal()
    workspace = modal.Workspace.from_context()
    await workspace.hydrate.aio()
    if str(workspace.name or "") != config.runtime.workspace_name:
        raise RuntimeError("Modal workspace does not match the Mission controller namespace")
    lookup: dict[str, object] = {
        "environment_name": config.runtime.environment_name,
        "client": workspace.client,
    }
    if function_version is not None:
        lookup["version"] = function_version
    function = modal.Function.from_name(
        config.runtime.app_name,
        config.function_name,
        **lookup,
    )
    await function.hydrate.aio()
    observed = getattr(function, "object_id", None)
    if not isinstance(observed, str) or not observed.startswith("fu-"):
        raise RuntimeError("Modal Mission deployed Function has no durable object identity")
    return function


async def create_modal_mission_controller_deployment_receipt(
    spec: ModalMissionControllerDeploymentSpec,
    *,
    expected_deployment_digest: str,
    function_version: int | None,
) -> ModalMissionControllerDeploymentReceipt:
    """Finalize one post-deploy receipt after state and Function hydration."""

    config = spec.app_config(
        expected_deployment_digest=expected_deployment_digest,
        function_version=function_version,
    )
    await provision_modal_mission_controller_state(config)
    function = await _hydrated_modal_mission_controller_function(
        config,
        function_version=function_version,
    )
    function_id = function.object_id
    return ModalMissionControllerDeploymentReceipt(
        deployment_digest=spec.deployment_digest,
        controller_artifact_digest=spec.controller_artifact_digest,
        controller_image_id=spec.controller_image_id,
        app_name=spec.app_name,
        function_name=spec.function_name,
        function_version=function_version,
        function_id=function_id,
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
        config=config,
        family=family,
        request_bytes=request_bytes,
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
        result_reader=_result_not_routed,
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
        raise ModalMissionControllerExecutionFailed(type(exc).__name__[:128]) from None
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
            raise ModalMissionControllerExecutionFailed(type(exc).__name__[:128]) from None

    return app, mission_controller


def _pinned_controller_image(image_id: str) -> object:
    modal = _load_modal()
    image = modal.Image.from_id(image_id)
    if getattr(image, "object_id", None) != image_id:
        raise RuntimeError("Modal returned another image for the pinned Mission image ID")
    return image


def build_modal_mission_controller_deployment(
    spec: ModalMissionControllerDeploymentSpec,
    *,
    expected_deployment_digest: str,
) -> ModalMissionControllerDeployment:
    """Build the production app from the separately pinned controller image."""

    config = spec.app_config(expected_deployment_digest=expected_deployment_digest)
    image = _pinned_controller_image(spec.controller_image_id)
    app, controller = build_modal_mission_controller_app(config, image=image)
    return ModalMissionControllerDeployment(
        spec=spec,
        config=config,
        app=app,
        controller=controller,
    )


async def prepare_modal_mission_job_worker(
    client: Client,
    values: MissionModalJobValueStore,
    *,
    spec: ModalMissionControllerDeploymentSpec,
    receipt: ModalMissionControllerDeploymentReceipt,
    expected_deployment_digest: str,
) -> PreparedModalMissionJobWorker:
    """Validate, preprovision, verify, and compose the production job route.

    The Worker is deliberately constructed last. Consequently no returned
    Worker can accept a task before both Dicts and the exact deployed Function
    have hydrated successfully.
    """

    receipt.require(spec, expected_deployment_digest=expected_deployment_digest)
    config = spec.app_config(
        expected_deployment_digest=expected_deployment_digest,
        function_version=receipt.function_version,
        function_id=receipt.function_id,
    )
    provisioned = await provision_modal_mission_controller_state(config)
    controller = await _hydrated_modal_mission_controller_function(
        config,
        function_version=receipt.function_version,
    )
    await verify_modal_mission_controller_deployment(
        config,
        receipt,
        function=controller,
    )
    service = ModalMissionBuiltinJobService(config, controller=controller)
    worker = create_mission_modal_job_worker(
        client,
        service,
        values,
        task_queue=spec.task_queue,
    )
    return PreparedModalMissionJobWorker(
        spec=spec,
        receipt=receipt,
        config=config,
        service=service,
        worker=worker,
        provisioned_dict_names=provisioned,
    )


async def prepare_modal_mission_temporal_route(
    client: Client,
    values: MissionModalActivityValueStore,
    *,
    spec: ModalMissionControllerDeploymentSpec,
    receipt: ModalMissionControllerDeploymentReceipt,
    expected_deployment_digest: str,
    execution_profiles: ExecutionProfileCatalog | None = None,
) -> PreparedModalMissionTemporalRoute:
    """Prepare one split Worker and its exact ECS admission configuration."""

    jobs = await prepare_modal_mission_job_worker(
        client,
        values,
        spec=spec,
        receipt=receipt,
        expected_deployment_digest=expected_deployment_digest,
    )
    workflows = MissionModalJobTemporalClient(client, task_queue=spec.task_queue)
    temporal = MissionTemporalActivityConfig(
        workflows=workflows,
        values=values,
        namespace_digest=jobs.config.namespace.digest,
    )
    return PreparedModalMissionTemporalRoute(
        jobs=jobs,
        extension_config=MissionsExtensionConfig(
            execution_profiles=(execution_profiles or ExecutionProfileCatalog.empty()),
            temporal_activities=temporal,
            temporal_runs=MissionTemporalClient(client, task_queue=MISSION_TASK_QUEUE),
            temporal_workers=(jobs.worker,),
        ),
    )


__all__ = [
    "MODAL_MISSION_CONTROLLER_MAX_RECEIPT_BYTES",
    "MODAL_MISSION_CONTROLLER_MAX_REQUEST_BYTES",
    "ModalMissionControllerAppConfig",
    "ModalMissionBuiltinJobService",
    "ModalMissionControllerExecutionFailed",
    "ModalMissionControllerDeployment",
    "ModalMissionControllerDeploymentReceipt",
    "ModalMissionControllerDeploymentSpec",
    "ModalMissionControllerFailpoint",
    "ModalMissionControllerFailpointReached",
    "ModalMissionControllerRejected",
    "PreparedModalMissionJobWorker",
    "PreparedModalMissionTemporalRoute",
    "build_modal_mission_controller_deployment",
    "build_modal_mission_controller_app",
    "create_modal_mission_controller_deployment_receipt",
    "prepare_modal_mission_job_worker",
    "prepare_modal_mission_temporal_route",
    "provision_modal_mission_controller_state",
    "verify_modal_mission_controller_deployment",
]
