# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Guarded Modal execution and provider-durable recovery for Mission critics."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import re
import time
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import Any

from archetype.errors import AvailabilityError
from archetype.missions.critics import (
    CriticActivityCodec,
    CriticActivityRedactor,
    CriticActivityRequest,
    CriticActivityResult,
    CriticActivityRetryGuard,
    CriticConfirmedAbsent,
    CriticExecutionResult,
    CriticHarness,
    CriticPrewarmRequest,
    CriticReceiptValue,
    CriticReconciliation,
    CriticRecovered,
    CriticRecoveryUnknown,
)
from archetype.missions.sandboxes import SandboxSpec
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
_PROVIDER_RESULT_KIND = "missions.critic.modal-result"
_LEGACY_PROVIDER_RESULT_SCHEMA_VERSION = 1
_PROVIDER_RESULT_SCHEMA_VERSION = 2
_RESULT_KEY_PREFIX = "critic-result-v1-"
_RETRY_REF_PREFIX = "modal-critic-retry+json:sha256:"
_MAX_RESULT_BYTES = 1 << 20


@dataclass(frozen=True, slots=True)
class ModalMissionCriticExecutorConfig:
    """Bounded provider and sandbox settings for the critic adapter."""

    sandbox_environment: str
    workspace: str = "/workspace/review"
    timeout_seconds: int = 4 * 60 * 60
    idle_timeout_seconds: int = 20 * 60
    result_dict_name: str = ""
    max_result_bytes: int = _MAX_RESULT_BYTES

    def __post_init__(self) -> None:
        if not self.sandbox_environment.strip():
            raise ValueError("Modal critic sandbox environment cannot be empty")
        workspace = PurePosixPath(self.workspace)
        if not workspace.is_absolute() or str(workspace) in {"/", "."}:
            raise ValueError("Modal critic workspace must be a non-root absolute path")
        if self.timeout_seconds < 1 or self.idle_timeout_seconds < 1:
            raise ValueError("Modal critic sandbox timeouts must be positive")
        if self.result_dict_name and not _DICT_NAME.fullmatch(self.result_dict_name):
            raise ValueError("Modal critic result Dict name is invalid")
        if self.max_result_bytes < 1:
            raise ValueError("Modal critic result byte limit must be positive")


class ModalCriticExecutionUnknown(AvailabilityError):
    """A guarded Modal review cannot be completed or replayed safely."""

    public_detail = "Modal critic execution state is temporarily unavailable"

    def __init__(self, operation_id: str, reason: str) -> None:
        self.operation_id = operation_id
        self.reason = reason[:512]
        super().__init__(f"Modal critic operation {operation_id!r} is Unknown: {self.reason}")


class ModalCriticRecoveryRequired(ModalCriticExecutionUnknown):
    """A durable critic result exists but its exact cleanup remains unknown."""

    def __init__(self, operation_id: str, recovery: CriticRecoveryUnknown) -> None:
        self.recovery = recovery
        super().__init__(operation_id, recovery.reason)


@dataclass(frozen=True, slots=True)
class _StoredCriticResult:
    result: CriticActivityResult
    cleanup: ModalSandboxOperationCleanup | None


class _ModalCriticResultCatalog:
    """One provider-native first-result register backed by a named Modal Dict."""

    def __init__(
        self,
        *,
        barrier: ModalProviderStartBarrier,
        codec: CriticActivityCodec,
        name: str,
        max_result_bytes: int,
    ) -> None:
        if not _DICT_NAME.fullmatch(name):
            raise ValueError("Modal critic result Dict name is invalid")
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
    ) -> _StoredCriticResult | None:
        dictionary = await self._dictionary()
        try:
            value = await dictionary.get.aio(self._key(identity))
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            raise ModalCriticExecutionUnknown(
                identity.operation_id,
                f"provider result lookup failed ({type(exc).__name__[:128]})",
            ) from exc
        if value is None:
            return None
        if not isinstance(value, str):
            raise ModalCriticExecutionUnknown(
                identity.operation_id,
                "provider result register returned a non-text value",
            )
        return self._decode(
            value.encode(),
            identity=identity,
            request_digest=request_digest,
        )

    async def put_first(
        self,
        *,
        identity: ModalSandboxOperationIdentity,
        request_digest: str,
        result: CriticActivityResult,
        cleanup: ModalSandboxOperationCleanup,
    ) -> _StoredCriticResult:
        stored = _StoredCriticResult(result=result, cleanup=cleanup)
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
            existing = await self.get(
                identity=identity,
                request_digest=request_digest,
            )
            if existing is None:
                raise ModalCriticExecutionUnknown(
                    identity.operation_id,
                    "provider first-result write outcome is ambiguous and no result is visible "
                    f"({type(exc).__name__[:128]})",
                ) from exc
            if not self._matches_first_result(existing, stored):
                raise RuntimeError("Modal critic operation has a conflicting first result") from exc
            return existing
        if added:
            return stored
        existing = await self.get(
            identity=identity,
            request_digest=request_digest,
        )
        if existing is None:
            raise ModalCriticExecutionUnknown(
                identity.operation_id,
                "provider rejected first result but no result is visible",
            )
        if not self._matches_first_result(existing, stored):
            raise RuntimeError("Modal critic operation has a conflicting first result")
        return existing

    @staticmethod
    def _matches_first_result(
        existing: _StoredCriticResult,
        expected: _StoredCriticResult,
    ) -> bool:
        # A schema-1 result is already the immutable winner. The current
        # execute path still closes its own session in ``finally``; without
        # legacy object IDs, replacing or inventing cleanup authority is unsafe.
        return existing.result == expected.result and (
            existing.cleanup is None or existing.cleanup == expected.cleanup
        )

    def _encode(
        self,
        *,
        identity: ModalSandboxOperationIdentity,
        request_digest: str,
        stored: _StoredCriticResult,
    ) -> bytes:
        self._validate_identity(identity)
        if not _DIGEST.fullmatch(request_digest):
            raise ValueError("Modal critic request digest is invalid")
        cleanup = stored.cleanup
        if cleanup is None:
            raise ValueError("Modal critic schema-2 result requires cleanup metadata")
        if cleanup.identity != identity:
            raise ValueError("Modal critic cleanup belongs to another operation")
        result_payload = json.loads(self._codec.encode_result(stored.result).payload)
        encoded = json.dumps(
            {
                "cleanup": cleanup.to_payload(),
                "kind": _PROVIDER_RESULT_KIND,
                "operation_digest": identity.digest,
                "request_digest": request_digest,
                "result": result_payload,
                "schema_version": _PROVIDER_RESULT_SCHEMA_VERSION,
            },
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
        if len(encoded) > self._max_result_bytes:
            raise ValueError(
                f"Modal critic provider result exceeds the {self._max_result_bytes}-byte limit"
            )
        return encoded

    def _encode_legacy(
        self,
        *,
        identity: ModalSandboxOperationIdentity,
        request_digest: str,
        result: CriticActivityResult,
    ) -> bytes:
        self._validate_identity(identity)
        if not _DIGEST.fullmatch(request_digest):
            raise ValueError("Modal critic request digest is invalid")
        result_payload = json.loads(self._codec.encode_result(result).payload)
        encoded = json.dumps(
            {
                "kind": _PROVIDER_RESULT_KIND,
                "operation_digest": identity.digest,
                "request_digest": request_digest,
                "result": result_payload,
                "schema_version": _LEGACY_PROVIDER_RESULT_SCHEMA_VERSION,
            },
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
        if len(encoded) > self._max_result_bytes:
            raise ValueError(
                f"Modal critic provider result exceeds the {self._max_result_bytes}-byte limit"
            )
        return encoded

    def _decode(
        self,
        encoded: bytes,
        *,
        identity: ModalSandboxOperationIdentity,
        request_digest: str,
    ) -> _StoredCriticResult:
        if len(encoded) > self._max_result_bytes:
            raise ModalCriticExecutionUnknown(
                identity.operation_id,
                "provider result exceeds its configured byte limit",
            )
        try:
            envelope = json.loads(encoded)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ModalCriticExecutionUnknown(
                identity.operation_id,
                "provider result is not canonical JSON",
            ) from exc
        schema_version = envelope.get("schema_version") if isinstance(envelope, dict) else None
        if schema_version == _LEGACY_PROVIDER_RESULT_SCHEMA_VERSION:
            expected_fields = {
                "kind",
                "operation_digest",
                "request_digest",
                "result",
                "schema_version",
            }
        elif schema_version == _PROVIDER_RESULT_SCHEMA_VERSION:
            expected_fields = {
                "cleanup",
                "kind",
                "operation_digest",
                "request_digest",
                "result",
                "schema_version",
            }
        else:
            expected_fields = set()
        if (
            not isinstance(envelope, dict)
            or set(envelope) != expected_fields
            or envelope.get("kind") != _PROVIDER_RESULT_KIND
            or envelope.get("operation_digest") != identity.digest
            or envelope.get("request_digest") != request_digest
        ):
            raise ModalCriticExecutionUnknown(
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
            result = self._codec.decode_result(result_encoded)
            cleanup = (
                None
                if schema_version == _LEGACY_PROVIDER_RESULT_SCHEMA_VERSION
                else ModalSandboxOperationCleanup.from_payload(
                    identity,
                    envelope["cleanup"],
                )
            )
        except (TypeError, ValueError) as exc:
            raise ModalCriticExecutionUnknown(
                identity.operation_id,
                "provider result contains an invalid critic observation",
            ) from exc
        stored = _StoredCriticResult(result=result, cleanup=cleanup)
        canonical = (
            self._encode_legacy(
                identity=identity,
                request_digest=request_digest,
                result=result,
            )
            if schema_version == _LEGACY_PROVIDER_RESULT_SCHEMA_VERSION
            else self._encode(
                identity=identity,
                request_digest=request_digest,
                stored=stored,
            )
        )
        if canonical != encoded:
            raise ModalCriticExecutionUnknown(
                identity.operation_id,
                "provider result is not canonically encoded",
            )
        return stored

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
            raise ModalCriticExecutionUnknown(
                "provider-catalog",
                f"Modal workspace lookup failed ({type(exc).__name__[:128]})",
            ) from exc
        if observed != self._barrier.workspace_name:
            raise ModalCriticExecutionUnknown(
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
            raise ModalCriticExecutionUnknown(
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
            raise ValueError("Modal critic result belongs to another provider namespace")

    @staticmethod
    def _load_modal() -> Any:
        try:
            import modal
        except ImportError as exc:  # pragma: no cover - optional dependency
            raise RuntimeError(
                'Modal support is optional; install it with `uv add "archetype-missions[modal]"`'
            ) from exc
        return modal


class ModalMissionCriticExecutor:
    """Mission critic adapter with permanent start and first-result barriers."""

    provider = "modal"

    def __init__(
        self,
        *,
        capability: ModalSandboxOperationCapability,
        barrier: ModalProviderStartBarrier,
        harness: CriticHarness,
        redactor: CriticActivityRedactor,
        config: ModalMissionCriticExecutorConfig,
    ) -> None:
        if barrier.protocol_epoch != MODAL_ACTIVITY_PROTOCOL_EPOCH:
            raise ValueError("Modal critic executor requires the barrier-aware epoch")
        if harness.config.workspace != config.workspace:
            raise ValueError("Modal critic harness and executor workspaces must match")
        self._capability = capability
        self._barrier = barrier
        self._harness = harness
        self._config = config
        self._codec = CriticActivityCodec(redactor)
        result_name = config.result_dict_name or self._default_result_dict_name(barrier)
        self._results = _ModalCriticResultCatalog(
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
        request: CriticActivityRequest,
        attempt: int,
        fence: int,
        retry_guard: CriticActivityRetryGuard | None,
    ) -> CriticExecutionResult:
        """Start one independent review and publish one provider-durable result."""

        if attempt < 1 or fence < 1:
            raise ValueError("Modal critic execution requires positive attempt and fence")
        identity = self._capability.identity(operation_id)
        self._barrier.operation_marker_name(identity)
        request_digest = self._codec.encode_request(request).digest
        expected_retry_guard = self._retry_guard(identity, request_digest)
        if retry_guard is not None and retry_guard != expected_retry_guard:
            raise ValueError("Modal critic retry route does not match the exact request")

        existing = await self._results.get(
            identity=identity,
            request_digest=request_digest,
        )
        if existing is not None:
            await self._require_cleanup_before_recovered_result(
                existing,
                spec=self._sandbox_spec(request),
            )
            return self._execution_result(request, existing.result)

        provision_started_at_ms = int(time.time() * 1000)
        spec = self._sandbox_spec(request)
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
                await self._require_cleanup_before_recovered_result(
                    recovered,
                    spec=spec,
                )
                return self._execution_result(request, recovered.result)
            if isinstance(outcome, ModalProviderMarkerExists):
                reason = f"permanent {outcome.phase} marker exists without a durable result"
            elif isinstance(outcome, ModalProviderBarrierUnknown):
                reason = outcome.reason
            else:  # pragma: no cover - closed union defense
                reason = "provider start returned an invalid outcome"
            raise ModalCriticExecutionUnknown(operation_id, reason)

        session = outcome.session
        try:
            if session.identity.sandbox_id == request.author_sandbox_id:
                raise ValueError("Modal critic reused the author sandbox identity")
            sandbox_ready_at_ms = int(time.time() * 1000)
            await self._harness.prewarm(
                session,
                CriticPrewarmRequest(
                    mission_id=request.mission_id,
                    task_id=request.task_id,
                    dispatch_id=request.dispatch_id,
                    repository=request.repository,
                    branch=request.branch,
                    base_ref=request.base_ref,
                ),
            )
            base_hydrated_at_ms = int(time.time() * 1000)
            raw = await self._harness.execute(
                session,
                request.as_review_request(),
                provision_started_at_ms=provision_started_at_ms,
                sandbox_ready_at_ms=sandbox_ready_at_ms,
                base_hydrated_at_ms=base_hydrated_at_ms,
            )
            if raw.sandbox != session.identity:
                raise ValueError("Modal critic result belongs to another sandbox")
            durable = self._codec.prepare_result(raw, request)
            stored = await self._results.put_first(
                identity=identity,
                request_digest=request_digest,
                result=durable,
                cleanup=session.operation_cleanup,
            )
            return self._execution_result(request, stored.result)
        finally:
            await session.close()

    async def reconcile(
        self,
        *,
        operation_id: str,
        request: CriticActivityRequest,
    ) -> CriticReconciliation:
        """Recover an exact result or authorize a barrier-guarded retry."""

        identity = self._capability.identity(operation_id)
        self._barrier.operation_marker_name(identity)
        request_digest = self._codec.encode_request(request).digest
        try:
            existing = await self._results.get(
                identity=identity,
                request_digest=request_digest,
            )
        except ModalCriticExecutionUnknown as exc:
            return CriticRecoveryUnknown(exc.reason)
        if existing is not None:
            recovery = await self._cleanup_recovery(
                existing,
                spec=self._sandbox_spec(request),
            )
            if recovery is not None:
                return recovery
            return CriticRecovered(self._execution_result(request, existing.result))

        marker = await self._barrier.observe_operation_marker(identity=identity)
        if isinstance(marker, ModalProviderOperationMissing):
            return CriticConfirmedAbsent(self._retry_guard(identity, request_digest))

        try:
            existing = await self._results.get(
                identity=identity,
                request_digest=request_digest,
            )
        except ModalCriticExecutionUnknown as exc:
            return CriticRecoveryUnknown(exc.reason)
        if existing is not None:
            recovery = await self._cleanup_recovery(
                existing,
                spec=self._sandbox_spec(request),
            )
            if recovery is not None:
                return recovery
            return CriticRecovered(self._execution_result(request, existing.result))
        if isinstance(marker, ModalProviderMarkerExists):
            return CriticRecoveryUnknown(
                f"permanent {marker.phase} marker exists without a durable result"
            )
        if isinstance(marker, ModalProviderBarrierUnknown):
            return CriticRecoveryUnknown(marker.reason)
        return CriticRecoveryUnknown("provider marker observation returned an invalid outcome")

    async def _require_cleanup_before_recovered_result(
        self,
        stored: _StoredCriticResult,
        *,
        spec: SandboxSpec,
    ) -> None:
        recovery = await self._cleanup_recovery(stored, spec=spec)
        if recovery is not None:
            cleanup = stored.cleanup
            operation_id = (
                cleanup.identity.operation_id if cleanup is not None else "unknown-critic-operation"
            )
            raise ModalCriticRecoveryRequired(operation_id, recovery)

    async def _cleanup_recovery(
        self,
        stored: _StoredCriticResult,
        *,
        spec: SandboxSpec,
    ) -> CriticRecoveryUnknown | None:
        try:
            await self._cleanup_completed(stored, spec=spec)
        except ModalCriticExecutionUnknown as exc:
            return CriticRecoveryUnknown(exc.reason)
        return None

    async def _cleanup_completed(
        self,
        stored: _StoredCriticResult,
        *,
        spec: SandboxSpec,
    ) -> None:
        cleanup = stored.cleanup
        if cleanup is None:
            # Schema 1 made the result authoritative before exact teardown
            # identities existed. Never invent provider cleanup authority.
            return
        try:
            await self._capability.cleanup_completed(
                cleanup=cleanup,
                spec=spec,
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            raise ModalCriticExecutionUnknown(
                cleanup.identity.operation_id,
                f"exact provider cleanup failed ({type(exc).__name__[:128]})",
            ) from exc

    def _sandbox_spec(self, request: CriticActivityRequest) -> SandboxSpec:
        return SandboxSpec(
            provider=self.provider,
            environment=self._config.sandbox_environment,
            workdir=self._config.workspace,
            timeout_seconds=self._config.timeout_seconds,
            idle_timeout_seconds=self._config.idle_timeout_seconds,
            metadata=(
                ("mission", str(request.mission_id)),
                ("branch", request.branch),
                ("role", "critic"),
                ("review", request.review_id),
            ),
        )

    def _retry_guard(
        self,
        identity: ModalSandboxOperationIdentity,
        request_digest: str,
    ) -> CriticActivityRetryGuard:
        material = json.dumps(
            {
                "kind": "modal_critic_retry_route",
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
        return CriticActivityRetryGuard(
            ref=f"{_RETRY_REF_PREFIX}{digest}",
            digest=digest,
        )

    @staticmethod
    def _execution_result(
        request: CriticActivityRequest,
        result: CriticActivityResult,
    ) -> CriticExecutionResult:
        expected = (
            request.review_id,
            request.domain_review_attempt,
            request.candidate_digest,
            request.policy.digest,
            request.base_revision,
            request.head_revision,
            request.diff_digest,
            request.validator_bundle_digest,
            request.author_sandbox_id,
            request.redaction_policy_id,
        )
        observed = (
            result.review_id,
            result.domain_review_attempt,
            result.candidate_digest,
            result.policy_digest,
            result.base_revision,
            result.head_revision,
            result.diff_digest,
            result.validator_bundle_digest,
            result.author_sandbox_id,
            result.redaction_policy_id,
        )
        if observed != expected:
            raise ValueError("Modal critic result does not match its exact request")
        receipt = result.receipt
        raw_receipt = (
            None
            if receipt is None
            else CriticReceiptValue(
                review_id=receipt.review_id,
                conclusion=receipt.conclusion,
                candidate_digest=receipt.candidate_digest,
                policy_digest=receipt.policy_digest,
                evidence_digest=receipt.evidence_digest,
                reviewed_base_revision=receipt.reviewed_base_revision,
                reviewed_head_revision=receipt.reviewed_head_revision,
                reviewed_diff_digest=receipt.reviewed_diff_digest,
                validator_bundle_digest=receipt.validator_bundle_digest,
                subject_metadata_digest=receipt.subject.metadata_digest,
                subject_digest=receipt.subject.subject_digest,
                subject_content_size_bytes=receipt.subject.content_size_bytes,
                subject_metadata_size_bytes=receipt.subject.metadata_size_bytes,
                subject_size_bytes=receipt.subject.total_size_bytes,
                subject_media_type=receipt.subject.media_type,
                subject_transport=receipt.subject.transport.value,
                subject_ref=receipt.subject.ref,
                reviewed_scope=receipt.reviewed_scope,
                finding_count=receipt.finding_count,
                blocking_count=receipt.blocking_count,
                output_schema_version=receipt.output_schema_version,
                completed_at_ms=receipt.completed_at_ms,
            )
        )
        return CriticExecutionResult(
            request=request.as_review_request(),
            status=result.status,
            sandbox=result.sandbox,
            sandbox_status=result.sandbox_status,
            sandbox_acquired=result.sandbox_acquired,
            started_at_ms=result.started_at_ms,
            ended_at_ms=result.ended_at_ms,
            provision_started_at_ms=result.provision_started_at_ms,
            sandbox_ready_at_ms=result.sandbox_ready_at_ms,
            base_hydrated_at_ms=result.base_hydrated_at_ms,
            head_ready_at_ms=result.head_ready_at_ms,
            critic_started_at_ms=result.critic_started_at_ms,
            raw_output=result.raw_output,
            trace_uri=result.trace_uri,
            findings=result.findings,
            receipt=raw_receipt,
            error=result.error,
        )

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
        return f"arc-critic-results-v1-{suffix[:32]}"


__all__ = [
    "ModalCriticExecutionUnknown",
    "ModalCriticRecoveryRequired",
    "ModalMissionCriticExecutor",
    "ModalMissionCriticExecutorConfig",
]
