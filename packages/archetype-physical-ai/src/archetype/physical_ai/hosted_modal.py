# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Modal execution and first-result recovery for hosted Physical-AI Activities.

The generic Activity catalog binds ``operation_id`` before this adapter is
called.  This module then uses one permanent, atomic key in a named Modal Dict
to select the only remote start.  Complete canonical payloads are committed to
a named Modal Volume before a bounded result index is inserted into the Dict.

The Dict and Volume are provider durability, not ECS state.  A permanent start
without the complete result index is deliberately unknown and cannot be
replayed, even when the request is seeded and the Activity lease has expired.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
import subprocess
import tempfile
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol, cast, runtime_checkable
from urllib.parse import quote

from archetype.errors import AvailabilityError
from archetype.physical_ai.hosted_activity_contracts import (
    HostedEpisodeConfirmedAbsent,
    HostedEpisodeProviderResult,
    HostedEpisodeRecovered,
    HostedEpisodeRecoveryUnknown,
    HostedEpisodeRetryGuard,
    validate_hosted_provider_result,
)
from archetype.physical_ai.hosted_activity_values import SeededHostedEpisodeRunner
from archetype.physical_ai.hosted_episode import (
    build_hosted_episode_manifest,
    build_hosted_episode_results,
    decode_hosted_episode_requests,
    hosted_episode_manifest_digest,
    hosted_episode_request_digest,
    hosted_episode_results_digest,
    hosted_episode_trajectory_digest,
)

MODAL_HOSTED_EPISODE_PROTOCOL_EPOCH = 1
MODAL_HOSTED_EPISODE_VOLUME_MOUNT = "/archetype-hosted-episodes"

_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,62}$")
_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_OPERATION = re.compile(r"^physical-episode:[0-9a-f]{64}$")
_PAYLOAD_KINDS = ("request", "trajectory", "episode-results", "manifest")
_RESULT_SCHEMA_VERSION = 1
_START_SCHEMA_VERSION = 1
_RESULT_ROOT = "archetype-physical-ai-hosted-v1"


def _canonical_json(value: Mapping[str, Any]) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=True,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _bounded_name(value: str, field: str) -> str:
    if not isinstance(value, str) or _NAME.fullmatch(value) is None:
        raise ValueError(f"{field} must be a valid Modal name")
    return value


def _bounded_positive(value: int, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"{field} must be a positive integer")
    return value


def _operation_key(operation_id: str) -> str:
    if _OPERATION.fullmatch(operation_id) is None:
        raise ValueError("Modal hosted operation identity is invalid")
    return hashlib.sha256(operation_id.encode()).hexdigest()


def _start_key(operation_id: str) -> str:
    return f"start:{_operation_key(operation_id)}"


def _result_key(operation_id: str) -> str:
    return f"result:{_operation_key(operation_id)}"


def _blob_path(kind: str, digest: str) -> str:
    if kind not in _PAYLOAD_KINDS:
        raise ValueError("Modal hosted payload kind is invalid")
    if _DIGEST.fullmatch(digest) is None:
        raise ValueError("Modal hosted payload digest is invalid")
    return f"{_RESULT_ROOT}/{kind}/{digest[:2]}/{digest}.arrow"


def _payload_digest(kind: str, payload: bytes) -> str:
    if kind == "request":
        return hosted_episode_request_digest(payload)
    if kind == "trajectory":
        return hosted_episode_trajectory_digest(payload)
    if kind == "episode-results":
        return hosted_episode_results_digest(payload)
    if kind == "manifest":
        return hosted_episode_manifest_digest(payload)
    raise ValueError("Modal hosted payload kind is invalid")


@dataclass(frozen=True, slots=True)
class ModalHostedEpisodeConfig:
    """Exact persistent Modal namespace used by one hosted provider adapter."""

    workspace_name: str
    environment_name: str
    app_name: str
    function_name: str
    result_dict_name: str
    result_volume_name: str
    protocol_epoch: int = MODAL_HOSTED_EPISODE_PROTOCOL_EPOCH
    call_timeout_seconds: int = 900
    create_if_missing: bool = False

    def __post_init__(self) -> None:
        for field in (
            "workspace_name",
            "environment_name",
            "app_name",
            "function_name",
            "result_dict_name",
            "result_volume_name",
        ):
            _bounded_name(getattr(self, field), f"Modal hosted {field}")
        if (
            isinstance(self.protocol_epoch, bool)
            or not isinstance(self.protocol_epoch, int)
            or self.protocol_epoch != MODAL_HOSTED_EPISODE_PROTOCOL_EPOCH
        ):
            raise ValueError("Modal hosted provider protocol epoch is unsupported")
        _bounded_positive(self.call_timeout_seconds, "Modal hosted call timeout")
        if not isinstance(self.create_if_missing, bool):
            raise ValueError("Modal hosted create_if_missing must be a boolean")

    @property
    def namespace_digest(self) -> str:
        """Bind workspace, environment, code entrypoint, and durable objects."""

        return _sha256(
            _canonical_json(
                {
                    "app_name": self.app_name,
                    "environment_name": self.environment_name,
                    "function_name": self.function_name,
                    "protocol_epoch": self.protocol_epoch,
                    "provider": "modal-hosted-episode",
                    "result_dict_name": self.result_dict_name,
                    "result_volume_name": self.result_volume_name,
                    "schema_version": 1,
                    "workspace_name": self.workspace_name,
                }
            )
        )

    @property
    def provider_identity(self) -> str:
        return f"modal-hosted-episode:{self.namespace_digest}"

    def retry_guard(
        self,
        *,
        operation_id: str,
        request_digest: str,
    ) -> HostedEpisodeRetryGuard:
        """Describe the next atomic put-if-absent route, not transferable start authority."""

        key = _operation_key(operation_id)
        if _DIGEST.fullmatch(request_digest) is None:
            raise ValueError("Modal hosted request digest is invalid")
        payload = _canonical_json(
            {
                "barrier": "modal-dict-put-if-absent",
                "namespace_digest": self.namespace_digest,
                "operation_id": operation_id,
                "protocol_epoch": self.protocol_epoch,
                "request_digest": request_digest,
                "schema_version": 1,
            }
        )
        reference = (
            "modal-hosted-start://"
            + quote(self.workspace_name, safe="")
            + "/"
            + quote(self.environment_name, safe="")
            + "/"
            + quote(self.result_dict_name, safe="")
            + "/"
            + key
        )
        return HostedEpisodeRetryGuard(ref=reference, digest=_sha256(payload))


@runtime_checkable
class ModalHostedEpisodeRuntime(Protocol):
    """Narrow Modal control/data operations used by the family adapter."""

    async def get(self, key: str) -> object: ...

    async def put_if_absent(self, key: str, value: Mapping[str, Any]) -> bool: ...

    async def spawn(
        self,
        *,
        operation_id: str,
        request_ipc: bytes,
        namespace_digest: str,
    ) -> object: ...

    async def wait(self, call: object) -> object: ...

    async def read_blob(self, path: str) -> bytes: ...


class ModalHostedEpisodeProviderUnknown(AvailabilityError):
    """A permanent Modal start exists but no exact complete result is visible."""

    public_detail = "Hosted episode provider state is temporarily unavailable"

    def __init__(self, operation_id: str, reason: str) -> None:
        self.operation_id = operation_id
        self.reason = reason
        super().__init__(f"Modal hosted operation {operation_id!r} is unknown: {reason}")


class ModalHostedEpisodeProvider:
    """Execute or recover one canonical episode batch through Modal."""

    def __init__(
        self,
        config: ModalHostedEpisodeConfig,
        *,
        runtime: ModalHostedEpisodeRuntime | None = None,
    ) -> None:
        self._config = config
        self._runtime = runtime or ModalNamedHostedEpisodeRuntime(config)

    @property
    def provider(self) -> str:
        return self._config.provider_identity

    async def execute(
        self,
        *,
        operation_id: str,
        request_ipc: bytes,
        attempt: int,
        fence: int,
        retry_guard: HostedEpisodeRetryGuard | None,
    ) -> HostedEpisodeProviderResult:
        request_digest = self._validate_request(operation_id, request_ipc)
        _bounded_positive(attempt, "Modal hosted Activity attempt")
        _bounded_positive(fence, "Modal hosted Activity fence")
        expected_guard = self._config.retry_guard(
            operation_id=operation_id,
            request_digest=request_digest,
        )
        if retry_guard is not None and retry_guard != expected_guard:
            raise ModalHostedEpisodeProviderUnknown(
                operation_id,
                "retry guard does not bind this provider namespace and request",
            )

        recovered = await self._recover_if_complete(
            operation_id=operation_id,
            request_ipc=request_ipc,
        )
        if recovered is not None:
            return recovered

        marker = self._start_marker(
            operation_id=operation_id,
            request_digest=request_digest,
            attempt=attempt,
            fence=fence,
        )
        try:
            acquired = await self._runtime.put_if_absent(
                _start_key(operation_id),
                marker,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            recovered = await self._recover_if_complete(
                operation_id=operation_id,
                request_ipc=request_ipc,
            )
            if recovered is not None:
                return recovered
            raise ModalHostedEpisodeProviderUnknown(
                operation_id,
                f"start admission was ambiguous ({type(exc).__name__[:128]})",
            ) from exc

        if not acquired:
            recovered = await self._recover_if_complete(
                operation_id=operation_id,
                request_ipc=request_ipc,
            )
            if recovered is not None:
                return recovered
            raise ModalHostedEpisodeProviderUnknown(
                operation_id,
                "permanent start exists without a complete result index",
            )

        try:
            call = await self._runtime.spawn(
                operation_id=operation_id,
                request_ipc=request_ipc,
                namespace_digest=self._config.namespace_digest,
            )
            await self._runtime.wait(call)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            recovered = await self._recover_if_complete(
                operation_id=operation_id,
                request_ipc=request_ipc,
            )
            if recovered is not None:
                return recovered
            raise ModalHostedEpisodeProviderUnknown(
                operation_id,
                f"remote execution ended without provider result ({type(exc).__name__[:128]})",
            ) from exc

        recovered = await self._recover_if_complete(
            operation_id=operation_id,
            request_ipc=request_ipc,
        )
        if recovered is None:
            raise ModalHostedEpisodeProviderUnknown(
                operation_id,
                "remote completion returned without a complete result index",
            )
        return recovered

    async def reconcile(
        self,
        *,
        operation_id: str,
        request_ipc: bytes,
    ) -> HostedEpisodeRecovered | HostedEpisodeConfirmedAbsent | HostedEpisodeRecoveryUnknown:
        request_digest = self._validate_request(operation_id, request_ipc)
        try:
            raw_result = await self._runtime.get(_result_key(operation_id))
            raw_start = await self._runtime.get(_start_key(operation_id))
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            return HostedEpisodeRecoveryUnknown(
                f"Modal provider lookup failed ({type(exc).__name__[:128]})"
            )

        if raw_result is not None:
            if not self._start_matches(
                raw_start,
                operation_id=operation_id,
                request_digest=request_digest,
            ):
                return HostedEpisodeRecoveryUnknown(
                    "complete result exists without its exact permanent start"
                )
            result = await self._load_result(
                raw_result,
                operation_id=operation_id,
                request_ipc=request_ipc,
            )
            return HostedEpisodeRecovered(result)

        if raw_start is not None:
            if not self._start_matches(
                raw_start,
                operation_id=operation_id,
                request_digest=request_digest,
            ):
                return HostedEpisodeRecoveryUnknown(
                    "permanent start conflicts with this operation or request"
                )
            return HostedEpisodeRecoveryUnknown(
                "permanent start exists without a complete result index"
            )

        return HostedEpisodeConfirmedAbsent(
            self._config.retry_guard(
                operation_id=operation_id,
                request_digest=request_digest,
            )
        )

    async def result_for(
        self,
        operation_id: str,
        request_ipc: bytes,
    ) -> HostedEpisodeProviderResult | None:
        """Read the exact first result without creating retry evidence."""

        self._validate_request(operation_id, request_ipc)
        return await self._recover_if_complete(
            operation_id=operation_id,
            request_ipc=request_ipc,
        )

    async def _recover_if_complete(
        self,
        *,
        operation_id: str,
        request_ipc: bytes,
    ) -> HostedEpisodeProviderResult | None:
        try:
            conclusion = await self.reconcile(
                operation_id=operation_id,
                request_ipc=request_ipc,
            )
        except asyncio.CancelledError:
            raise
        if isinstance(conclusion, HostedEpisodeRecovered):
            return conclusion.result
        if isinstance(conclusion, HostedEpisodeRecoveryUnknown):
            raise ModalHostedEpisodeProviderUnknown(
                operation_id,
                conclusion.reason or "provider reconciliation is unknown",
            )
        return None

    async def _load_result(
        self,
        raw: object,
        *,
        operation_id: str,
        request_ipc: bytes,
    ) -> HostedEpisodeProviderResult:
        records = self._parse_result_index(
            raw,
            operation_id=operation_id,
            request_digest=hosted_episode_request_digest(request_ipc),
        )
        payloads: dict[str, bytes] = {}
        for kind in _PAYLOAD_KINDS:
            record = records[kind]
            payload = await self._runtime.read_blob(str(record["path"]))
            if (
                len(payload) != int(record["size_bytes"])
                or _payload_digest(kind, payload) != record["digest"]
            ):
                raise ValueError(f"Modal hosted {kind} bytes do not match result index")
            payloads[kind] = payload
        if payloads["request"] != request_ipc:
            raise ValueError("Modal hosted result index binds another request")
        result = HostedEpisodeProviderResult(
            request_ipc=payloads["request"],
            trajectory_ipc=payloads["trajectory"],
            episode_results_ipc=payloads["episode-results"],
            manifest_ipc=payloads["manifest"],
        )
        validate_hosted_provider_result(
            result,
            request_ipc=request_ipc,
            operation_id=operation_id,
        )
        return result

    def _parse_result_index(
        self,
        raw: object,
        *,
        operation_id: str,
        request_digest: str,
    ) -> dict[str, dict[str, str | int]]:
        if not isinstance(raw, dict):
            raise ValueError("Modal hosted result index is not an object")
        index = cast(dict[str, object], raw)
        expected_keys = {
            "schema_version",
            "protocol_epoch",
            "namespace_digest",
            "operation_id",
            "request_digest",
            "payloads",
        }
        if set(index) != expected_keys:
            raise ValueError("Modal hosted result index fields are incompatible")
        payloads = index["payloads"]
        if (
            index["schema_version"] != _RESULT_SCHEMA_VERSION
            or index["protocol_epoch"] != self._config.protocol_epoch
            or index["namespace_digest"] != self._config.namespace_digest
            or index["operation_id"] != operation_id
            or index["request_digest"] != request_digest
            or not isinstance(payloads, dict)
            or set(payloads) != set(_PAYLOAD_KINDS)
        ):
            raise ValueError("Modal hosted result index identity is incompatible")
        payload_records = cast(dict[str, object], payloads)
        parsed: dict[str, dict[str, str | int]] = {}
        for kind in _PAYLOAD_KINDS:
            record = payload_records[kind]
            if not isinstance(record, dict) or set(record) != {
                "digest",
                "path",
                "size_bytes",
            }:
                raise ValueError(f"Modal hosted {kind} index record is incompatible")
            fields = cast(dict[str, object], record)
            digest = fields["digest"]
            path = fields["path"]
            size_bytes = fields["size_bytes"]
            if (
                not isinstance(digest, str)
                or _DIGEST.fullmatch(digest) is None
                or path != _blob_path(kind, digest)
                or isinstance(size_bytes, bool)
                or not isinstance(size_bytes, int)
                or size_bytes < 1
            ):
                raise ValueError(f"Modal hosted {kind} index record is invalid")
            parsed[kind] = {
                "digest": digest,
                "path": cast(str, path),
                "size_bytes": cast(int, size_bytes),
            }
        return parsed

    def _start_matches(
        self,
        raw: object,
        *,
        operation_id: str,
        request_digest: str,
    ) -> bool:
        if not isinstance(raw, dict):
            return False
        marker = cast(dict[str, object], raw)
        return (
            set(marker)
            == {
                "attempt",
                "fence",
                "namespace_digest",
                "operation_id",
                "protocol_epoch",
                "request_digest",
                "schema_version",
            }
            and marker["schema_version"] == _START_SCHEMA_VERSION
            and marker["protocol_epoch"] == self._config.protocol_epoch
            and marker["namespace_digest"] == self._config.namespace_digest
            and marker["operation_id"] == operation_id
            and marker["request_digest"] == request_digest
            and isinstance(marker["attempt"], int)
            and not isinstance(marker["attempt"], bool)
            and marker["attempt"] > 0
            and isinstance(marker["fence"], int)
            and not isinstance(marker["fence"], bool)
            and marker["fence"] > 0
        )

    def _start_marker(
        self,
        *,
        operation_id: str,
        request_digest: str,
        attempt: int,
        fence: int,
    ) -> dict[str, str | int]:
        return {
            "attempt": attempt,
            "fence": fence,
            "namespace_digest": self._config.namespace_digest,
            "operation_id": operation_id,
            "protocol_epoch": self._config.protocol_epoch,
            "request_digest": request_digest,
            "schema_version": _START_SCHEMA_VERSION,
        }

    @staticmethod
    def _validate_request(operation_id: str, request_ipc: bytes) -> str:
        _operation_key(operation_id)
        rows = decode_hosted_episode_requests(request_ipc)
        if any(row["operation_id"] != operation_id for row in rows):
            raise ValueError("Modal hosted provider operation does not match its request")
        return hosted_episode_request_digest(request_ipc)


class ModalNamedHostedEpisodeRuntime:
    """Concrete named-Dict, named-Volume, named-Function Modal client."""

    def __init__(
        self,
        config: ModalHostedEpisodeConfig,
        *,
        function: object | None = None,
    ) -> None:
        self._config = config
        self._lock = asyncio.Lock()
        self._client: Any | None = None
        self._dictionary: Any | None = None
        self._volume: Any | None = None
        self._function: Any | None = function
        self._last_completion: object | None = None

    @property
    def last_completion(self) -> object | None:
        """Expose non-canonical placement diagnostics for a live proof only."""

        return self._last_completion

    async def get(self, key: str) -> object:
        dictionary, _, _ = await self._objects()
        return await dictionary.get.aio(key, None)

    async def put_if_absent(self, key: str, value: Mapping[str, Any]) -> bool:
        dictionary, _, _ = await self._objects()
        return bool(await dictionary.put.aio(key, dict(value), skip_if_exists=True))

    async def spawn(
        self,
        *,
        operation_id: str,
        request_ipc: bytes,
        namespace_digest: str,
    ) -> object:
        _, _, function = await self._objects(require_function=True)
        if function is None:
            raise RuntimeError("Modal hosted execution function is unavailable")
        return await function.spawn.aio(operation_id, request_ipc, namespace_digest)

    async def wait(self, call: object) -> object:
        get = getattr(call, "get", None)
        if get is None or not hasattr(get, "aio"):
            raise TypeError("Modal hosted function call has no async result boundary")
        self._last_completion = await get.aio(timeout=float(self._config.call_timeout_seconds))
        return self._last_completion

    async def read_blob(self, path: str) -> bytes:
        if not self._valid_blob_path(path):
            raise ValueError("Modal hosted result index contains an unsafe blob path")
        _, volume, _ = await self._objects()
        chunks = [chunk async for chunk in volume.read_file.aio(path)]
        return b"".join(chunks)

    async def _objects(
        self,
        *,
        require_function: bool = False,
    ) -> tuple[Any, Any, Any | None]:
        if (
            self._dictionary is not None
            and self._volume is not None
            and (not require_function or self._function is not None)
        ):
            return self._dictionary, self._volume, self._function
        async with self._lock:
            if self._client is None:
                modal = _load_modal()
                workspace = modal.Workspace.from_context()
                await workspace.hydrate.aio()
                observed = str(workspace.name or "")
                if observed != self._config.workspace_name:
                    raise RuntimeError("Modal workspace does not match hosted provider namespace")
                self._client = workspace.client
            modal = _load_modal()
            if self._dictionary is None:
                self._dictionary = modal.Dict.from_name(
                    self._config.result_dict_name,
                    environment_name=self._config.environment_name,
                    create_if_missing=self._config.create_if_missing,
                    client=self._client,
                )
                await self._dictionary.hydrate.aio()
            if self._volume is None:
                self._volume = modal.Volume.from_name(
                    self._config.result_volume_name,
                    environment_name=self._config.environment_name,
                    create_if_missing=self._config.create_if_missing,
                    client=self._client,
                )
                await self._volume.hydrate.aio()
            if require_function and self._function is None:
                self._function = modal.Function.from_name(
                    self._config.app_name,
                    self._config.function_name,
                    environment_name=self._config.environment_name,
                    client=self._client,
                )
                await self._function.hydrate.aio()
            return self._dictionary, self._volume, self._function

    @staticmethod
    def _valid_blob_path(path: str) -> bool:
        parts = Path(path).parts
        return (
            len(parts) == 4
            and parts[0] == _RESULT_ROOT
            and parts[1] in _PAYLOAD_KINDS
            and len(parts[2]) == 2
            and parts[3].endswith(".arrow")
            and _DIGEST.fullmatch(parts[3].removesuffix(".arrow")) is not None
            and parts[2] == parts[3][:2]
        )


def build_seeded_modal_hosted_episode_app(
    config: ModalHostedEpisodeConfig,
    *,
    gpu: str = "L40S",
    image: object | None = None,
) -> tuple[object, object]:
    """Build the disposable seeded GPU app used by parity proofs.

    Production robot or simulator deployments may register another named
    function with the same three-argument entrypoint and publication order.
    """

    modal = _load_modal()
    result_volume = modal.Volume.from_name(
        config.result_volume_name,
        environment_name=config.environment_name,
        create_if_missing=True,
    )
    if image is None:
        image = (
            modal.Image.debian_slim(python_version="3.12")
            .uv_pip_install("archetype-ecs==0.6.0")
            .add_local_python_source("archetype", copy=True)
        )
    app = modal.App(config.app_name)
    namespace_digest = config.namespace_digest
    protocol_epoch = config.protocol_epoch
    environment_name = config.environment_name
    result_dict_name = config.result_dict_name
    result_volume_name = config.result_volume_name

    @app.function(
        name=config.function_name,
        image=image,
        gpu=gpu,
        serialized=True,
        timeout=config.call_timeout_seconds,
        volumes={MODAL_HOSTED_EPISODE_VOLUME_MOUNT: result_volume},
    )
    def seeded_hosted_episode(
        operation_id: str,
        request_ipc: bytes,
        requested_namespace_digest: str,
    ) -> dict[str, str | int]:
        if requested_namespace_digest != namespace_digest:
            raise ValueError("Modal hosted remote namespace does not match deployment")
        modal = _load_modal()
        remote_result_dict = modal.Dict.from_name(
            result_dict_name,
            environment_name=environment_name,
            create_if_missing=False,
        )
        remote_result_volume = modal.Volume.from_name(
            result_volume_name,
            environment_name=environment_name,
            create_if_missing=False,
        )
        result = _run_seeded_episode(request_ipc)
        validate_hosted_provider_result(
            result,
            request_ipc=request_ipc,
            operation_id=operation_id,
        )
        index = _publish_remote_result(
            mount=Path(MODAL_HOSTED_EPISODE_VOLUME_MOUNT),
            operation_id=operation_id,
            namespace_digest=requested_namespace_digest,
            protocol_epoch=protocol_epoch,
            result=result,
        )
        remote_result_volume.commit()
        key = _result_key(operation_id)
        inserted = remote_result_dict.put(key, index, skip_if_exists=True)
        if not inserted and remote_result_dict.get(key) != index:
            raise RuntimeError("Modal hosted result index conflicts with first result")
        return {
            "gpu_count": _gpu_count(),
            "index_digest": _sha256(_canonical_json(index)),
            "schema_version": 1,
        }

    return app, seeded_hosted_episode


def _run_seeded_episode(request_ipc: bytes) -> HostedEpisodeProviderResult:
    trajectory_ipc = asyncio.run(SeededHostedEpisodeRunner().run(request_ipc))
    episode_results_ipc = build_hosted_episode_results(request_ipc, trajectory_ipc)
    manifest_ipc = build_hosted_episode_manifest(
        request_ipc,
        trajectory_ipc,
        episode_results_ipc,
    )
    return HostedEpisodeProviderResult(
        request_ipc=request_ipc,
        trajectory_ipc=trajectory_ipc,
        episode_results_ipc=episode_results_ipc,
        manifest_ipc=manifest_ipc,
    )


def _publish_remote_result(
    *,
    mount: Path,
    operation_id: str,
    namespace_digest: str,
    protocol_epoch: int,
    result: HostedEpisodeProviderResult,
) -> dict[str, Any]:
    payloads = {
        "request": result.request_ipc,
        "trajectory": result.trajectory_ipc,
        "episode-results": result.episode_results_ipc,
        "manifest": result.manifest_ipc,
    }
    records: dict[str, dict[str, str | int]] = {}
    for kind, payload in payloads.items():
        digest = _payload_digest(kind, payload)
        relative = _blob_path(kind, digest)
        _write_remote_blob(mount / relative, payload)
        records[kind] = {
            "digest": digest,
            "path": relative,
            "size_bytes": len(payload),
        }
    return {
        "namespace_digest": namespace_digest,
        "operation_id": operation_id,
        "payloads": records,
        "protocol_epoch": protocol_epoch,
        "request_digest": hosted_episode_request_digest(result.request_ipc),
        "schema_version": _RESULT_SCHEMA_VERSION,
    }


def _write_remote_blob(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if path.read_bytes() != payload:
            raise RuntimeError(f"Modal hosted immutable blob conflicts at {path.name}")
        return
    descriptor, temporary = tempfile.mkstemp(
        dir=path.parent,
        prefix=f".{path.name}.",
        suffix=".tmp",
    )
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        if path.exists():
            if path.read_bytes() != payload:
                raise RuntimeError(f"Modal hosted immutable blob conflicts at {path.name}")
        else:
            # Concurrent writers can only target the same digest. Replacing
            # identical bytes is safe and leaves no partial durable payload.
            os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def _gpu_count() -> int:
    """Return proof-only placement diagnostics; never enter canonical payloads."""

    try:
        completed = subprocess.run(
            ["nvidia-smi", "--query-gpu=name", "--format=csv,noheader"],
            check=True,
            capture_output=True,
            text=True,
            timeout=15,
        )
    except (OSError, subprocess.SubprocessError):
        return 0
    return len([line for line in completed.stdout.splitlines() if line.strip()])


def _load_modal() -> Any:
    try:
        import modal
    except ImportError as exc:  # pragma: no cover - optional dependency
        raise RuntimeError(
            'Modal support is optional; install it with `uv add "archetype-physical-ai[modal]"`'
        ) from exc
    return modal


__all__ = [
    "MODAL_HOSTED_EPISODE_PROTOCOL_EPOCH",
    "MODAL_HOSTED_EPISODE_VOLUME_MOUNT",
    "ModalHostedEpisodeConfig",
    "ModalHostedEpisodeProvider",
    "ModalHostedEpisodeProviderUnknown",
    "ModalHostedEpisodeRuntime",
    "ModalNamedHostedEpisodeRuntime",
    "build_seeded_modal_hosted_episode_app",
]
