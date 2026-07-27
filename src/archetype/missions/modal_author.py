# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Guarded Modal execution and provider-durable recovery for Mission authors."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import re
import time
from dataclasses import dataclass, replace
from pathlib import PurePosixPath
from typing import Any

from pydantic import TypeAdapter

from archetype.errors import AvailabilityError
from archetype.missions.activities import (
    AuthorActivityRetryGuard,
    AuthorConfirmedAbsent,
    AuthorExecutionObservation,
    AuthorReconciliation,
    AuthorRecovered,
    AuthorRecoveryUnknown,
    DurableAuthorExecutionObservation,
)
from archetype.missions.activity_values import (
    AUTHOR_RESULT_MAX_BYTES,
    AuthorValueRedactor,
    MissionAuthorValueCodec,
)
from archetype.missions.coding_agents.contracts import (
    AgentExecutionResult,
    FrictionObservation,
    TaskDispatchRequest,
)
from archetype.missions.coding_agents.harness import CodingAgentHarness
from archetype.missions.sandboxes.contracts import (
    SandboxEvent,
    SandboxEventObserver,
    SandboxEventType,
    SandboxSpec,
)
from archetype.missions.sandboxes.modal import (
    MODAL_ACTIVITY_PROTOCOL_EPOCH,
    ModalSandboxOperationCapability,
    ModalSandboxOperationCleanup,
    ModalSandboxOperationIdentity,
)
from archetype.missions.sandboxes.modal_barrier import (
    ModalProviderBarrierUnknown,
    ModalProviderMarkerExists,
    ModalProviderOperationMissing,
    ModalProviderStartBarrier,
    ModalProviderStarted,
)

_DICT_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,62}")
_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_REQUEST_ADAPTER = TypeAdapter(TaskDispatchRequest)
_PROVIDER_RESULT_KIND = "missions.author.modal-result"
_PROVIDER_RESULT_SCHEMA_VERSION = 2
_RESULT_KEY_PREFIX = "author-result-v1-"
_RETRY_REF_PREFIX = "modal-author-retry+json:sha256:"
_PROVIDER_ENVELOPE_OVERHEAD_BYTES = 4_096


@dataclass(frozen=True, slots=True)
class ModalMissionAuthorExecutorConfig:
    """Bounded provider and sandbox settings for the author adapter."""

    sandbox_environment: str
    workspace: str = "/workspace/repo"
    timeout_seconds: int = 4 * 60 * 60
    idle_timeout_seconds: int = 20 * 60
    result_dict_name: str = ""
    max_result_bytes: int = AUTHOR_RESULT_MAX_BYTES
    checkpoint_after_dispatch: bool = True

    def __post_init__(self) -> None:
        if not self.sandbox_environment.strip():
            raise ValueError("Modal author sandbox environment cannot be empty")
        workspace = PurePosixPath(self.workspace)
        if not workspace.is_absolute() or str(workspace) in {"/", "."}:
            raise ValueError("Modal author workspace must be a non-root absolute path")
        if self.timeout_seconds < 1 or self.idle_timeout_seconds < 1:
            raise ValueError("Modal author sandbox timeouts must be positive")
        if self.result_dict_name and not _DICT_NAME.fullmatch(self.result_dict_name):
            raise ValueError("Modal author result Dict name is invalid")
        if self.max_result_bytes <= _PROVIDER_ENVELOPE_OVERHEAD_BYTES:
            raise ValueError(
                "Modal author result byte limit must exceed its bounded provider envelope"
            )


class ModalAuthorExecutionUnknown(AvailabilityError):
    """A guarded Modal execution cannot be completed or replayed safely."""

    public_detail = "Modal author execution state is temporarily unavailable"

    def __init__(self, operation_id: str, reason: str) -> None:
        self.operation_id = operation_id
        self.reason = reason[:512]
        super().__init__(f"Modal author operation {operation_id!r} is Unknown: {self.reason}")


@dataclass(frozen=True, slots=True)
class _StoredAuthorResult:
    observation: DurableAuthorExecutionObservation
    cleanup: ModalSandboxOperationCleanup


class _ModalAuthorResultCatalog:
    """One provider-native first-result register backed by a named Modal Dict."""

    def __init__(
        self,
        *,
        barrier: ModalProviderStartBarrier,
        codec: MissionAuthorValueCodec,
        name: str,
        max_result_bytes: int,
    ) -> None:
        if not _DICT_NAME.fullmatch(name):
            raise ValueError("Modal author result Dict name is invalid")
        self._barrier = barrier
        self._codec = codec
        self._name = name
        self._max_result_bytes = max_result_bytes

    @property
    def name(self) -> str:
        return self._name

    async def get(
        self,
        *,
        identity: ModalSandboxOperationIdentity,
        request_digest: str,
    ) -> _StoredAuthorResult | None:
        dictionary = await self._dictionary()
        key = self._key(identity)
        try:
            value = await dictionary.get.aio(key)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            raise ModalAuthorExecutionUnknown(
                identity.operation_id,
                f"provider result lookup failed ({type(exc).__name__[:128]})",
            ) from exc
        if value is None:
            return None
        if not isinstance(value, str):
            raise ModalAuthorExecutionUnknown(
                identity.operation_id,
                "provider result register returned a non-text value",
            )
        try:
            encoded = value.encode()
        except UnicodeEncodeError as exc:
            raise ModalAuthorExecutionUnknown(
                identity.operation_id,
                "provider result register returned invalid text",
            ) from exc
        return self._decode(
            encoded,
            identity=identity,
            request_digest=request_digest,
        )

    async def put_first(
        self,
        *,
        identity: ModalSandboxOperationIdentity,
        request_digest: str,
        observation: DurableAuthorExecutionObservation,
        cleanup: ModalSandboxOperationCleanup,
    ) -> _StoredAuthorResult:
        stored = _StoredAuthorResult(observation=observation, cleanup=cleanup)
        encoded = self._encode(
            identity=identity,
            request_digest=request_digest,
            stored=stored,
        )
        dictionary = await self._dictionary()
        try:
            added = await dictionary.put.aio(
                self._key(identity),
                encoded.decode(),
                skip_if_exists=True,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            recovered = await self._recover_ambiguous_put(
                identity=identity,
                request_digest=request_digest,
                expected=encoded,
                cause=exc,
            )
            return recovered
        if added:
            return stored
        existing = await self.get(
            identity=identity,
            request_digest=request_digest,
        )
        if existing is None:
            raise ModalAuthorExecutionUnknown(
                identity.operation_id,
                "provider rejected first result but no result is visible",
            )
        if (
            self._encode(
                identity=identity,
                request_digest=request_digest,
                stored=existing,
            )
            != encoded
        ):
            raise RuntimeError("Modal author operation has a conflicting first result")
        return existing

    async def _recover_ambiguous_put(
        self,
        *,
        identity: ModalSandboxOperationIdentity,
        request_digest: str,
        expected: bytes,
        cause: Exception,
    ) -> _StoredAuthorResult:
        try:
            existing = await self.get(
                identity=identity,
                request_digest=request_digest,
            )
        except ModalAuthorExecutionUnknown as lookup:
            raise ModalAuthorExecutionUnknown(
                identity.operation_id,
                "provider first-result write and reconciliation are ambiguous "
                f"({type(cause).__name__[:128]}; {type(lookup).__name__})",
            ) from cause
        if existing is None:
            raise ModalAuthorExecutionUnknown(
                identity.operation_id,
                "provider first-result write outcome is ambiguous and no result is visible "
                f"({type(cause).__name__[:128]})",
            ) from cause
        if (
            self._encode(
                identity=identity,
                request_digest=request_digest,
                stored=existing,
            )
            != expected
        ):
            raise RuntimeError(
                "Modal author operation has a conflicting first result after ambiguous write"
            ) from cause
        return existing

    def _encode(
        self,
        *,
        identity: ModalSandboxOperationIdentity,
        request_digest: str,
        stored: _StoredAuthorResult,
    ) -> bytes:
        self._validate_identity(identity)
        if not _DIGEST.fullmatch(request_digest):
            raise ValueError("Modal author request digest is invalid")
        if stored.cleanup.identity != identity:
            raise ValueError("Modal author cleanup belongs to another operation")
        result_envelope = json.loads(self._codec.encode_observation(stored.observation))
        encoded = json.dumps(
            {
                "cleanup": stored.cleanup.to_payload(),
                "kind": _PROVIDER_RESULT_KIND,
                "operation_digest": identity.digest,
                "request_digest": request_digest,
                "result": result_envelope,
                "schema_version": _PROVIDER_RESULT_SCHEMA_VERSION,
            },
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
        if len(encoded) > self._max_result_bytes:
            raise ValueError(
                f"Modal author provider result exceeds the {self._max_result_bytes}-byte limit"
            )
        return encoded

    def _decode(
        self,
        encoded: bytes,
        *,
        identity: ModalSandboxOperationIdentity,
        request_digest: str,
    ) -> _StoredAuthorResult:
        if len(encoded) > self._max_result_bytes:
            raise ModalAuthorExecutionUnknown(
                identity.operation_id,
                "provider result exceeds its configured byte limit",
            )
        try:
            envelope = json.loads(encoded)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ModalAuthorExecutionUnknown(
                identity.operation_id,
                "provider result is not canonical JSON",
            ) from exc
        if (
            not isinstance(envelope, dict)
            or set(envelope)
            != {
                "cleanup",
                "kind",
                "operation_digest",
                "request_digest",
                "result",
                "schema_version",
            }
            or envelope.get("kind") != _PROVIDER_RESULT_KIND
            or envelope.get("schema_version") != _PROVIDER_RESULT_SCHEMA_VERSION
            or envelope.get("operation_digest") != identity.digest
            or envelope.get("request_digest") != request_digest
        ):
            raise ModalAuthorExecutionUnknown(
                identity.operation_id,
                "provider result does not match the exact operation and request",
            )
        result_encoded = json.dumps(
            envelope["result"],
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
        try:
            observation = self._codec.decode_observation(result_encoded)
            cleanup = ModalSandboxOperationCleanup.from_payload(
                identity,
                envelope["cleanup"],
            )
        except (TypeError, ValueError) as exc:
            raise ModalAuthorExecutionUnknown(
                identity.operation_id,
                "provider result contains an invalid author observation",
            ) from exc
        if (
            self._encode(
                identity=identity,
                request_digest=request_digest,
                stored=_StoredAuthorResult(
                    observation=observation,
                    cleanup=cleanup,
                ),
            )
            != encoded
        ):
            raise ModalAuthorExecutionUnknown(
                identity.operation_id,
                "provider result is not canonically encoded",
            )
        return _StoredAuthorResult(observation=observation, cleanup=cleanup)

    async def _dictionary(self) -> Any:
        modal = self._load_modal()
        try:
            workspace = modal.Workspace.from_context()
            await workspace.hydrate.aio()
            observed = str(workspace.name or "")
            client = workspace.client
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            raise ModalAuthorExecutionUnknown(
                "provider-catalog",
                f"Modal workspace lookup failed ({type(exc).__name__[:128]})",
            ) from exc
        if observed != self._barrier.workspace_name:
            raise ModalAuthorExecutionUnknown(
                "provider-catalog",
                "Modal workspace does not match the configured result namespace",
            )
        try:
            dictionary = modal.Dict.from_name(
                self._name,
                create_if_missing=True,
                environment_name=self._barrier.environment_name,
                client=client,
            )
            await dictionary.hydrate.aio()
            return dictionary
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            raise ModalAuthorExecutionUnknown(
                "provider-catalog",
                f"Modal result Dict lookup failed ({type(exc).__name__[:128]})",
            ) from exc

    def _key(self, identity: ModalSandboxOperationIdentity) -> str:
        self._validate_identity(identity)
        return _RESULT_KEY_PREFIX + identity.digest.removeprefix("sha256:")

    def _validate_identity(self, identity: ModalSandboxOperationIdentity) -> None:
        expected = (
            self._barrier.workspace_name,
            self._barrier.environment_name,
            self._barrier.app_name,
            self._barrier.protocol_epoch,
        )
        observed = (
            identity.workspace_name,
            identity.environment_name,
            identity.app_name,
            identity.protocol_epoch,
        )
        if observed != expected:
            raise ValueError("Modal author result belongs to another provider namespace")

    @staticmethod
    def _load_modal() -> Any:
        try:
            import modal
        except ImportError as exc:  # pragma: no cover - optional dependency
            raise RuntimeError(
                "Modal support is optional; install it with `uv sync --extra coding-agent`"
            ) from exc
        return modal


class ModalMissionAuthorExecutor:
    """Mission author adapter with permanent start and first-result barriers.

    Raw harness output is redacted and bounded before the only provider
    durability write. A named Modal Dict acts as an atomic first-result
    register. If a permanent start marker exists without that result,
    reconciliation remains Unknown and never invokes the harness again.
    """

    provider = "modal"

    def __init__(
        self,
        *,
        capability: ModalSandboxOperationCapability,
        barrier: ModalProviderStartBarrier,
        harness: CodingAgentHarness,
        redactor: AuthorValueRedactor,
        config: ModalMissionAuthorExecutorConfig,
        observer: SandboxEventObserver | None = None,
    ) -> None:
        if barrier.protocol_epoch != MODAL_ACTIVITY_PROTOCOL_EPOCH:
            raise ValueError("Modal author executor requires the barrier-aware epoch")
        harness_config = getattr(harness, "config", None)
        if (
            harness_config is not None
            and getattr(harness_config, "workspace", config.workspace) != config.workspace
        ):
            raise ValueError("Modal author harness and executor workspaces must match")
        self._capability = capability
        self._barrier = barrier
        self._harness = harness
        self._config = config
        self._observer = observer
        self._codec = MissionAuthorValueCodec(
            redactor=redactor,
            max_result_bytes=(config.max_result_bytes - _PROVIDER_ENVELOPE_OVERHEAD_BYTES),
        )
        result_name = config.result_dict_name or self._default_result_dict_name(barrier)
        self._results = _ModalAuthorResultCatalog(
            barrier=barrier,
            codec=self._codec,
            name=result_name,
            max_result_bytes=config.max_result_bytes,
        )

    @property
    def result_dict_name(self) -> str:
        """Return the bounded provider catalog identity used for first results."""

        return self._results.name

    async def execute(
        self,
        *,
        operation_id: str,
        request: TaskDispatchRequest,
        attempt: int,
        fence: int,
        retry_guard: AuthorActivityRetryGuard | None,
    ) -> AuthorExecutionObservation:
        """Start only through the coupled barrier, then publish one safe result."""

        if attempt < 1 or fence < 1:
            raise ValueError("Modal author execution requires positive attempt and fence")
        sanitized_request = self._codec.sanitize_request(request)
        identity = self._capability.identity(operation_id)
        self._barrier.operation_marker_name(identity)
        request_digest = self._request_digest(sanitized_request)
        expected_retry_guard = self._retry_guard(identity, request_digest)
        if retry_guard is not None and retry_guard != expected_retry_guard:
            raise ValueError("Modal author retry route does not match the exact request")

        existing = await self._results.get(
            identity=identity,
            request_digest=request_digest,
        )
        if existing is not None:
            self._validate_result(sanitized_request, existing.observation.result)
            await self._cleanup_completed(existing, spec=self._sandbox_spec(sanitized_request))
            return self._codec.execution_observation(existing.observation)

        spec = self._sandbox_spec(sanitized_request)
        if retry_guard is None:
            outcome = await self._barrier.start_initial(
                identity=identity,
                capability=self._capability,
                spec=spec,
            )
        else:
            outcome = await self._barrier.start_retry(
                identity=identity,
                capability=self._capability,
                spec=spec,
            )
        if not isinstance(outcome, ModalProviderStarted):
            recovered = await self._results.get(
                identity=identity,
                request_digest=request_digest,
            )
            if recovered is not None:
                self._validate_result(sanitized_request, recovered.observation.result)
                await self._cleanup_completed(recovered, spec=spec)
                return self._codec.execution_observation(recovered.observation)
            if isinstance(outcome, ModalProviderMarkerExists):
                reason = f"permanent {outcome.phase} marker exists without a durable result"
            elif isinstance(outcome, ModalProviderBarrierUnknown):
                reason = outcome.reason
            else:  # pragma: no cover - closed union defense
                reason = "provider start returned an invalid outcome"
            raise ModalAuthorExecutionUnknown(operation_id, reason)

        session = outcome.session
        try:
            self._emit(SandboxEventType.READY, session.identity)
            self._emit(
                SandboxEventType.PROCESS_STARTED,
                session.identity,
                operation="coding-agent",
            )
            try:
                result = await self._harness.execute(session, sanitized_request)
            except BaseException:
                self._emit(
                    SandboxEventType.PROCESS_FINISHED,
                    session.identity,
                    operation="coding-agent",
                )
                raise
            self._emit(
                SandboxEventType.PROCESS_FINISHED,
                session.identity,
                operation="coding-agent",
                returncode=result.agent_returncode,
            )
            self._validate_result(sanitized_request, result)
            if result.sandbox != session.identity:
                raise ValueError("Modal author result belongs to another sandbox")
            checkpoint = None
            capabilities = getattr(session, "capabilities", None)
            if self._config.checkpoint_after_dispatch and bool(
                getattr(capabilities, "checkpoints", False)
            ):
                self._emit(SandboxEventType.CHECKPOINT_STARTED, session.identity)
                try:
                    checkpoint = await session.checkpoint()
                except Exception as exc:
                    message = f"{type(exc).__name__}: checkpoint failed"
                    self._emit(
                        SandboxEventType.CHECKPOINT_FAILED,
                        session.identity,
                        message=message,
                    )
                    result = replace(
                        result,
                        friction=(
                            *result.friction,
                            FrictionObservation(kind="checkpoint", message=message),
                        ),
                    )
                else:
                    self._emit(
                        SandboxEventType.CHECKPOINT_FINISHED,
                        session.identity,
                        checkpoint_uri=checkpoint.uri,
                    )
            observation = AuthorExecutionObservation(
                result=result,
                sandbox_status=await session.status(),
                checkpoint=checkpoint,
            )
            durable = self._codec.sanitize_observation(observation)
            stored = await self._results.put_first(
                identity=identity,
                request_digest=request_digest,
                observation=durable,
                cleanup=session.operation_cleanup,
            )
            self._validate_result(sanitized_request, stored.observation.result)
            return self._codec.execution_observation(stored.observation)
        finally:
            await session.close()

    def _emit(
        self,
        kind: SandboxEventType,
        identity: Any,
        *,
        operation: str = "",
        returncode: int | None = None,
        checkpoint_uri: str = "",
        message: str = "",
    ) -> None:
        observer = self._observer
        if observer is None:
            return
        try:
            observer(
                SandboxEvent(
                    kind=kind,
                    sandbox=identity,
                    timestamp_ms=int(time.time() * 1000),
                    operation=operation,
                    returncode=returncode,
                    checkpoint_uri=checkpoint_uri,
                    message=message,
                )
            )
        except Exception:
            return

    async def reconcile(
        self,
        *,
        operation_id: str,
        request: TaskDispatchRequest,
    ) -> AuthorReconciliation:
        """Recover an exact result or route through ``start_retry`` safely."""

        sanitized_request = self._codec.sanitize_request(request)
        identity = self._capability.identity(operation_id)
        self._barrier.operation_marker_name(identity)
        request_digest = self._request_digest(sanitized_request)
        try:
            existing = await self._results.get(
                identity=identity,
                request_digest=request_digest,
            )
        except ModalAuthorExecutionUnknown as exc:
            return AuthorRecoveryUnknown(exc.reason)
        if existing is not None:
            try:
                self._validate_result(sanitized_request, existing.observation.result)
                await self._cleanup_completed(existing, spec=self._sandbox_spec(sanitized_request))
            except (ModalAuthorExecutionUnknown, ValueError) as exc:
                return AuthorRecoveryUnknown(str(exc))
            return AuthorRecovered(self._codec.execution_observation(existing.observation))

        marker = await self._barrier.observe_operation_marker(identity=identity)
        if isinstance(marker, ModalProviderOperationMissing):
            return AuthorConfirmedAbsent(self._retry_guard(identity, request_digest))

        # Close the race where publication follows the first result read.
        try:
            existing = await self._results.get(
                identity=identity,
                request_digest=request_digest,
            )
        except ModalAuthorExecutionUnknown as exc:
            return AuthorRecoveryUnknown(exc.reason)
        if existing is not None:
            try:
                self._validate_result(sanitized_request, existing.observation.result)
                await self._cleanup_completed(existing, spec=self._sandbox_spec(sanitized_request))
            except (ModalAuthorExecutionUnknown, ValueError) as exc:
                return AuthorRecoveryUnknown(str(exc))
            return AuthorRecovered(self._codec.execution_observation(existing.observation))
        if isinstance(marker, ModalProviderMarkerExists):
            return AuthorRecoveryUnknown(
                f"permanent {marker.phase} marker exists without a durable result"
            )
        if isinstance(marker, ModalProviderBarrierUnknown):
            return AuthorRecoveryUnknown(marker.reason)
        return AuthorRecoveryUnknown("provider marker observation returned an invalid outcome")

    async def _cleanup_completed(
        self,
        stored: _StoredAuthorResult,
        *,
        spec: SandboxSpec,
    ) -> None:
        try:
            await self._capability.cleanup_completed(
                cleanup=stored.cleanup,
                spec=spec,
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            raise ModalAuthorExecutionUnknown(
                stored.cleanup.identity.operation_id,
                f"exact provider cleanup failed ({type(exc).__name__[:128]})",
            ) from exc

    def _sandbox_spec(self, request: TaskDispatchRequest) -> SandboxSpec:
        return SandboxSpec(
            provider=self.provider,
            environment=self._config.sandbox_environment,
            workdir=self._config.workspace,
            timeout_seconds=self._config.timeout_seconds,
            idle_timeout_seconds=self._config.idle_timeout_seconds,
            metadata=(
                ("mission", str(request.mission_id)),
                ("branch", request.branch),
                ("role", "author"),
                ("dispatch", request.dispatch_id),
            ),
        )

    @staticmethod
    def _request_digest(request: TaskDispatchRequest) -> str:
        encoded = json.dumps(
            _REQUEST_ADAPTER.dump_python(request, mode="json"),
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
        return hashlib.sha256(encoded).hexdigest()

    def _retry_guard(
        self,
        identity: ModalSandboxOperationIdentity,
        request_digest: str,
    ) -> AuthorActivityRetryGuard:
        material = json.dumps(
            {
                "kind": "modal_author_retry_route",
                "operation_digest": identity.digest,
                "request_digest": request_digest,
                "result_dict_name": self.result_dict_name,
                "schema_version": 1,
            },
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
        digest = hashlib.sha256(material).hexdigest()
        return AuthorActivityRetryGuard(
            ref=f"{_RETRY_REF_PREFIX}{digest}",
            digest=digest,
        )

    @staticmethod
    def _validate_result(
        request: TaskDispatchRequest,
        result: AgentExecutionResult,
    ) -> None:
        expected = (
            request.mission_id,
            request.task_id,
            request.dispatch_id,
            request.dispatch_sequence,
        )
        observed = (
            result.mission_id,
            result.task_id,
            result.dispatch_id,
            result.dispatch_sequence,
        )
        if observed != expected:
            raise ValueError("Modal author result does not match its exact request")

    @staticmethod
    def _default_result_dict_name(
        barrier: ModalProviderStartBarrier,
    ) -> str:
        material = json.dumps(
            {
                "app_name": barrier.app_name,
                "environment_name": barrier.environment_name,
                "protocol_epoch": barrier.protocol_epoch,
                "workspace_name": barrier.workspace_name,
            },
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
        suffix = base64.b32encode(hashlib.sha256(material).digest()).decode().lower()
        return f"arc-author-results-v1-{suffix[:32]}"


__all__ = [
    "ModalAuthorExecutionUnknown",
    "ModalMissionAuthorExecutor",
    "ModalMissionAuthorExecutorConfig",
]
