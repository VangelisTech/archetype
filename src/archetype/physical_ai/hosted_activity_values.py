# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Local durable values and seeded provider for hosted Physical-AI Activities."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
import tempfile
from collections.abc import Callable, Mapping
from dataclasses import asdict
from pathlib import Path
from typing import Any, Protocol

from archetype.errors import AvailabilityError
from archetype.physical_ai.hosted_activity_contracts import (
    HOSTED_EPISODE_MANIFEST_REF_PREFIX,
    HOSTED_EPISODE_REQUEST_REF_PREFIX,
    HOSTED_EPISODE_RESULT_MEDIA_TYPE,
    HOSTED_EPISODE_RESULT_REF_PREFIX,
    HOSTED_EPISODE_RESULTS_REF_PREFIX,
    HOSTED_EPISODE_TRAJECTORY_REF_PREFIX,
    HostedEpisodeActivityResultRef,
    HostedEpisodeConfirmedAbsent,
    HostedEpisodePayloadKind,
    HostedEpisodePayloadRef,
    HostedEpisodeProviderResult,
    HostedEpisodePublishedResult,
    HostedEpisodeRecovered,
    HostedEpisodeRecoveryUnknown,
    HostedEpisodeRequestIdentity,
    HostedEpisodeRequestRef,
    HostedEpisodeRetryGuard,
    validate_hosted_provider_result,
)
from archetype.physical_ai.hosted_episode import (
    build_hosted_episode_manifest,
    build_hosted_episode_results,
    decode_hosted_episode_requests,
    encode_hosted_episode_trajectory,
    hosted_episode_manifest_digest,
    hosted_episode_request_digest,
    hosted_episode_result_id,
    hosted_episode_results_digest,
    hosted_episode_step_id,
    hosted_episode_trajectory_digest,
)

_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_BUNDLE_DOMAIN = "archetype.physical-ai.hosted-activity-result/v1"
_PROVIDER_RECORD_VERSION = 1


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _bundle_digest(payload: bytes) -> str:
    digest = hashlib.sha256()
    digest.update(_BUNDLE_DOMAIN.encode())
    digest.update(b"\0")
    digest.update(payload)
    return digest.hexdigest()


def _canonical_json(value: Mapping[str, Any]) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _write_once(path: Path, payload: bytes) -> None:
    """Publish immutable bytes atomically and accept only exact duplicates."""

    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if path.read_bytes() != payload:
            raise RuntimeError(f"immutable hosted value conflicts at {path.name}")
        return
    handle, temporary = tempfile.mkstemp(
        dir=path.parent,
        prefix=f".{path.name}.",
        suffix=".tmp",
    )
    try:
        with os.fdopen(handle, "wb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        try:
            os.link(temporary, path)
        except FileExistsError:
            if path.read_bytes() != payload:
                raise RuntimeError(f"immutable hosted value conflicts at {path.name}") from None
        _fsync_directory(path.parent)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def _create_once(path: Path, payload: bytes) -> bool:
    """Create one durable provider marker without replacing another claimant."""

    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        return False
    with os.fdopen(descriptor, "wb") as stream:
        stream.write(payload)
        stream.flush()
        os.fsync(stream.fileno())
    _fsync_directory(path.parent)
    return True


class LocalHostedEpisodeValueStore:
    """Content-address canonical IPC and one bounded completeness descriptor."""

    def __init__(self, root: str | Path) -> None:
        self._root = Path(root)

    async def put_request(self, request_ipc: bytes) -> HostedEpisodeRequestRef:
        return await asyncio.to_thread(self._put_request, request_ipc)

    async def get_request(
        self,
        value: HostedEpisodeRequestRef | HostedEpisodeRequestIdentity,
    ) -> bytes:
        return await asyncio.to_thread(self._get_request, value)

    async def publish_result(
        self,
        request: HostedEpisodeRequestRef,
        result: HostedEpisodeProviderResult,
    ) -> HostedEpisodePublishedResult:
        return await asyncio.to_thread(self._publish_result, request, result)

    async def get_result(
        self,
        value: HostedEpisodeActivityResultRef,
    ) -> HostedEpisodePublishedResult:
        return await asyncio.to_thread(self._get_result, value)

    def _put_request(self, request_ipc: bytes) -> HostedEpisodeRequestRef:
        digest = hosted_episode_request_digest(request_ipc)
        path = self._payload_path("request", digest)
        _write_once(path, request_ipc)
        return HostedEpisodeRequestRef(
            ref=f"{HOSTED_EPISODE_REQUEST_REF_PREFIX}{digest}",
            digest=digest,
            size_bytes=len(request_ipc),
        )

    def _get_request(
        self,
        value: HostedEpisodeRequestRef | HostedEpisodeRequestIdentity,
    ) -> bytes:
        digest = self._ref_digest(value.ref, HOSTED_EPISODE_REQUEST_REF_PREFIX)
        if digest != value.digest:
            raise ValueError("hosted request reference and digest disagree")
        payload = self._payload_path("request", digest).read_bytes()
        if isinstance(value, HostedEpisodeRequestRef) and len(payload) != value.size_bytes:
            raise ValueError("hosted request size does not match durable bytes")
        if hosted_episode_request_digest(payload) != digest:
            raise ValueError("hosted request digest does not match durable bytes")
        return payload

    def _publish_result(
        self,
        request: HostedEpisodeRequestRef,
        result: HostedEpisodeProviderResult,
    ) -> HostedEpisodePublishedResult:
        request_ipc = self._get_request(request)
        manifest = validate_hosted_provider_result(
            result,
            request_ipc=request_ipc,
            operation_id=result.operation_id,
        )
        trajectory = self._put_payload(
            "trajectory",
            result.trajectory_ipc,
            hosted_episode_trajectory_digest(result.trajectory_ipc),
            HOSTED_EPISODE_TRAJECTORY_REF_PREFIX,
        )
        episode_results = self._put_payload(
            "episode-results",
            result.episode_results_ipc,
            hosted_episode_results_digest(result.episode_results_ipc),
            HOSTED_EPISODE_RESULTS_REF_PREFIX,
        )
        manifest_ref = self._put_payload(
            "manifest",
            result.manifest_ipc,
            hosted_episode_manifest_digest(result.manifest_ipc),
            HOSTED_EPISODE_MANIFEST_REF_PREFIX,
        )
        descriptor_value: dict[str, Any] = {
            "schema_version": 1,
            "operation_id": result.operation_id,
            "request": asdict(request),
            "trajectory": asdict(trajectory),
            "episode_results": asdict(episode_results),
            "manifest": asdict(manifest_ref),
            "episode_count": int(manifest["episode_count"]),
            "trajectory_row_count": int(manifest["trajectory_row_count"]),
            "transition_count": int(manifest["transition_count"]),
            "success_count": int(manifest["success_count"]),
        }
        descriptor = _canonical_json(descriptor_value)
        descriptor_digest = _bundle_digest(descriptor)
        descriptor_path = self._descriptor_path(descriptor_digest)
        _write_once(descriptor_path, descriptor)
        published = HostedEpisodePublishedResult(
            operation_id=result.operation_id,
            request=request,
            trajectory=trajectory,
            episode_results=episode_results,
            manifest=manifest_ref,
            activity_result=HostedEpisodeActivityResultRef(
                ref=f"{HOSTED_EPISODE_RESULT_REF_PREFIX}{descriptor_digest}",
                digest=descriptor_digest,
                size_bytes=len(descriptor),
            ),
            episode_count=int(manifest["episode_count"]),
            trajectory_row_count=int(manifest["trajectory_row_count"]),
            transition_count=int(manifest["transition_count"]),
            success_count=int(manifest["success_count"]),
        )
        return published

    def _get_result(
        self,
        value: HostedEpisodeActivityResultRef,
    ) -> HostedEpisodePublishedResult:
        if value.media_type != HOSTED_EPISODE_RESULT_MEDIA_TYPE:
            raise ValueError("hosted result descriptor has an unsupported media type")
        digest = self._ref_digest(value.ref, HOSTED_EPISODE_RESULT_REF_PREFIX)
        if digest != value.digest:
            raise ValueError("hosted result reference and digest disagree")
        descriptor = self._descriptor_path(digest).read_bytes()
        if len(descriptor) != value.size_bytes or _bundle_digest(descriptor) != digest:
            raise ValueError("hosted result descriptor does not match its reference")
        envelope = json.loads(descriptor)
        if not isinstance(envelope, dict) or envelope.get("schema_version") != 1:
            raise ValueError("hosted result descriptor has an incompatible schema")
        request = HostedEpisodeRequestRef(**envelope["request"])
        trajectory = HostedEpisodePayloadRef(**envelope["trajectory"])
        episode_results = HostedEpisodePayloadRef(**envelope["episode_results"])
        manifest = HostedEpisodePayloadRef(**envelope["manifest"])
        request_ipc = self._get_request(request)
        provider_result = HostedEpisodeProviderResult(
            request_ipc=request_ipc,
            trajectory_ipc=self._get_payload(
                "trajectory",
                trajectory,
                HOSTED_EPISODE_TRAJECTORY_REF_PREFIX,
                hosted_episode_trajectory_digest,
            ),
            episode_results_ipc=self._get_payload(
                "episode-results",
                episode_results,
                HOSTED_EPISODE_RESULTS_REF_PREFIX,
                hosted_episode_results_digest,
            ),
            manifest_ipc=self._get_payload(
                "manifest",
                manifest,
                HOSTED_EPISODE_MANIFEST_REF_PREFIX,
                hosted_episode_manifest_digest,
            ),
        )
        operation_id = str(envelope["operation_id"])
        validated = validate_hosted_provider_result(
            provider_result,
            request_ipc=request_ipc,
            operation_id=operation_id,
        )
        counts = (
            int(envelope["episode_count"]),
            int(envelope["trajectory_row_count"]),
            int(envelope["transition_count"]),
            int(envelope["success_count"]),
        )
        expected_counts = (
            int(validated["episode_count"]),
            int(validated["trajectory_row_count"]),
            int(validated["transition_count"]),
            int(validated["success_count"]),
        )
        if counts != expected_counts:
            raise ValueError("hosted result descriptor completeness counts are false")
        return HostedEpisodePublishedResult(
            operation_id=operation_id,
            request=request,
            trajectory=trajectory,
            episode_results=episode_results,
            manifest=manifest,
            activity_result=value,
            episode_count=counts[0],
            trajectory_row_count=counts[1],
            transition_count=counts[2],
            success_count=counts[3],
        )

    def _put_payload(
        self,
        kind: HostedEpisodePayloadKind,
        payload: bytes,
        digest: str,
        prefix: str,
    ) -> HostedEpisodePayloadRef:
        _write_once(self._payload_path(kind, digest), payload)
        return HostedEpisodePayloadRef(
            kind=kind,
            ref=f"{prefix}{digest}",
            digest=digest,
            size_bytes=len(payload),
        )

    def _get_payload(
        self,
        kind: HostedEpisodePayloadKind,
        value: HostedEpisodePayloadRef,
        prefix: str,
        digest_fn: Callable[[bytes], str],
    ) -> bytes:
        if value.kind != kind:
            raise ValueError(f"hosted {kind} descriptor contains another payload kind")
        digest = self._ref_digest(value.ref, prefix)
        if digest != value.digest:
            raise ValueError(f"hosted {kind} reference and digest disagree")
        payload = self._payload_path(kind, digest).read_bytes()
        if len(payload) != value.size_bytes or digest_fn(payload) != digest:
            raise ValueError(f"hosted {kind} bytes do not match their reference")
        return payload

    @staticmethod
    def _ref_digest(ref: str, prefix: str) -> str:
        if not ref.startswith(prefix):
            raise ValueError("unsupported hosted value reference")
        digest = ref.removeprefix(prefix)
        if _DIGEST.fullmatch(digest) is None:
            raise ValueError("hosted value reference has an invalid digest")
        return digest

    def _payload_path(self, kind: str, digest: str) -> Path:
        if _DIGEST.fullmatch(digest) is None:
            raise ValueError("invalid hosted payload digest")
        return self._root / kind / digest[:2] / f"{digest}.arrow"

    def _descriptor_path(self, digest: str) -> Path:
        if _DIGEST.fullmatch(digest) is None:
            raise ValueError("invalid hosted descriptor digest")
        return self._root / "descriptor" / digest[:2] / f"{digest}.json"


class SeededHostedEpisodeRunner:
    """Small deterministic simulator that exercises the exact hosted contract."""

    def __init__(self, counter_path: str | Path | None = None) -> None:
        self._counter_path = Path(counter_path) if counter_path is not None else None
        self._memory_count = 0

    @property
    def execution_count(self) -> int:
        if self._counter_path is None:
            return self._memory_count
        if not self._counter_path.exists():
            return 0
        return int(json.loads(self._counter_path.read_text())["execution_count"])

    async def run(self, request_ipc: bytes) -> bytes:
        await asyncio.to_thread(self._increment)
        return encode_hosted_episode_trajectory(self._trajectory_rows(request_ipc))

    def _increment(self) -> None:
        if self._counter_path is None:
            self._memory_count += 1
            return
        count = self.execution_count + 1
        _write_replace(
            self._counter_path,
            _canonical_json({"execution_count": count}),
        )

    @staticmethod
    def _trajectory_rows(request_ipc: bytes) -> list[dict[str, Any]]:
        requests = decode_hosted_episode_requests(request_ipc)
        request_digest = hosted_episode_request_digest(request_ipc)
        rows: list[dict[str, Any]] = []
        for request in requests:
            config = json.loads(str(request["config_json"]))
            max_transitions = int(request["max_transitions"])
            success_after = int(config.get("success_after_transitions", max_transitions + 1))
            done_after = int(config.get("environment_done_after_transitions", max_transitions + 1))
            reward_per_transition = float(config.get("reward_per_transition", 0.0))
            terminal_step = min(max_transitions, success_after, done_after)
            if terminal_step < 0:
                raise ValueError("seeded hosted terminal step cannot be negative")
            for step_index in range(terminal_step + 1):
                is_final = step_index == terminal_step
                success = is_final and step_index > 0 and success_after == step_index
                environment_done = (
                    is_final and not success and step_index > 0 and done_after == step_index
                )
                terminal = is_final
                reason: str | None = None
                if terminal:
                    if success:
                        reason = "success"
                    elif environment_done:
                        reason = "environment_done"
                    else:
                        reason = "transition_budget"
                seed = int(request["seed"])
                scalar = ((seed % 997) / 997.0) + (step_index / 100.0)
                rows.append(
                    {
                        **{
                            field: request[field]
                            for field in (
                                "operation_id",
                                "episode_id",
                                "trial_id",
                                "suite",
                                "task_id",
                                "seed",
                                "instruction",
                                "max_transitions",
                                "environment_id",
                                "policy_id",
                                "config_digest",
                            )
                        },
                        "episode_result_id": hosted_episode_result_id(
                            str(request["operation_id"]),
                            str(request["episode_id"]),
                        ),
                        "step_id": hosted_episode_step_id(
                            str(request["operation_id"]),
                            str(request["episode_id"]),
                            step_index,
                        ),
                        "request_digest": request_digest,
                        "step_index": step_index,
                        "action": None if step_index == 0 else [scalar / 10.0] * 7,
                        "proprio": {
                            "eef_pos": [scalar, 0.0, 0.5],
                            "eef_quat": [1.0, 0.0, 0.0, 0.0],
                            "gripper": 0.0,
                            "gripper_qpos": [0.0, 0.0],
                        },
                        "reward": 0.0 if step_index == 0 else reward_per_transition,
                        "environment_done": environment_done,
                        "success": success,
                        "terminal": terminal,
                        "termination_reason": reason,
                        "agentview_frame": None,
                        "wrist_frame": None,
                    }
                )
        return rows


class HostedEpisodeRunner(Protocol):
    """Produce one complete trajectory from the canonical request bytes."""

    async def run(self, request_ipc: bytes) -> bytes: ...


class HostedEpisodeProviderStartBlocked(AvailabilityError):
    """Another provider claimant owns the permanent operation start marker."""

    public_detail = "Hosted episode provider start is temporarily unavailable"


class LocalDurableHostedEpisodeProvider:
    """At-most-one local seeded start with provider-durable first-result lookup.

    A permanent start marker is acquired with ``O_EXCL`` before invoking the
    runner. If the process dies before a complete result index is published,
    reconciliation remains unknown; neither lease expiry nor deterministic
    seeds authorize a replay.
    """

    def __init__(
        self,
        root: str | Path,
        *,
        runner: HostedEpisodeRunner,
        crash_after_publish: bool = False,
    ) -> None:
        self._root = Path(root).resolve()
        self._provider = (
            "local-seeded-hosted-episode:" + hashlib.sha256(str(self._root).encode()).hexdigest()
        )
        self._runner = runner
        self._crash_after_publish = crash_after_publish

    @property
    def provider(self) -> str:
        """Bind generic Activity ownership to this exact durable namespace."""

        return self._provider

    async def execute(
        self,
        *,
        operation_id: str,
        request_ipc: bytes,
        attempt: int,
        fence: int,
        retry_guard: HostedEpisodeRetryGuard | None,
    ) -> HostedEpisodeProviderResult:
        self._validate_request(operation_id, request_ipc)
        recovered = await asyncio.to_thread(
            self._load_result,
            operation_id,
            request_ipc,
        )
        if recovered is not None:
            return recovered
        await asyncio.to_thread(
            self._begin,
            operation_id,
            request_ipc,
            attempt,
            fence,
            retry_guard,
        )
        trajectory_ipc = await self._run(request_ipc)
        episode_results_ipc = build_hosted_episode_results(
            request_ipc,
            trajectory_ipc,
        )
        manifest_ipc = build_hosted_episode_manifest(
            request_ipc,
            trajectory_ipc,
            episode_results_ipc,
        )
        result = HostedEpisodeProviderResult(
            request_ipc=request_ipc,
            trajectory_ipc=trajectory_ipc,
            episode_results_ipc=episode_results_ipc,
            manifest_ipc=manifest_ipc,
        )
        validate_hosted_provider_result(
            result,
            request_ipc=request_ipc,
            operation_id=operation_id,
        )
        await asyncio.to_thread(self._publish_result, operation_id, result)
        if self._crash_after_publish:
            self._crash_after_publish = False
            raise RuntimeError("worker died after provider result publication")
        return result

    async def reconcile(
        self,
        *,
        operation_id: str,
        request_ipc: bytes,
    ) -> HostedEpisodeRecovered | HostedEpisodeConfirmedAbsent | HostedEpisodeRecoveryUnknown:
        self._validate_request(operation_id, request_ipc)
        recovered = await asyncio.to_thread(
            self._load_result,
            operation_id,
            request_ipc,
        )
        if recovered is not None:
            return HostedEpisodeRecovered(recovered)
        if self._start_path(operation_id).exists():
            return HostedEpisodeRecoveryUnknown(
                "provider start exists without a complete result index"
            )
        guard = await asyncio.to_thread(self._install_guard, operation_id, request_ipc)
        recovered = await asyncio.to_thread(
            self._load_result,
            operation_id,
            request_ipc,
        )
        if recovered is not None:
            return HostedEpisodeRecovered(recovered)
        if self._start_path(operation_id).exists():
            return HostedEpisodeRecoveryUnknown(
                "provider start raced confirmed-absence reconciliation"
            )
        return HostedEpisodeConfirmedAbsent(guard)

    async def _run(self, request_ipc: bytes) -> bytes:
        trajectory = await self._runner.run(request_ipc)
        if not isinstance(trajectory, bytes):
            raise TypeError("hosted episode runner must return Arrow IPC bytes")
        return trajectory

    def _begin(
        self,
        operation_id: str,
        request_ipc: bytes,
        attempt: int,
        fence: int,
        retry_guard: HostedEpisodeRetryGuard | None,
    ) -> None:
        if attempt < 1 or fence < 1:
            raise ValueError("hosted provider attempt and fence must be positive")
        if retry_guard is not None:
            expected = self._guard(operation_id, request_ipc)
            if retry_guard != expected:
                raise HostedEpisodeProviderStartBlocked(
                    "hosted retry guard does not bind this operation and request"
                )
            guard_path = self._guard_path(operation_id)
            if not guard_path.exists() or _sha256(guard_path.read_bytes()) != retry_guard.digest:
                raise HostedEpisodeProviderStartBlocked("hosted retry guard is absent or changed")
        marker = _canonical_json(
            {
                "attempt": attempt,
                "fence": fence,
                "operation_id": operation_id,
                "request_digest": hosted_episode_request_digest(request_ipc),
            }
        )
        if not _create_once(self._start_path(operation_id), marker):
            raise HostedEpisodeProviderStartBlocked(
                "provider operation already has a permanent start marker"
            )

    def _install_guard(
        self,
        operation_id: str,
        request_ipc: bytes,
    ) -> HostedEpisodeRetryGuard:
        guard = self._guard(operation_id, request_ipc)
        payload = self._guard_payload(operation_id, request_ipc)
        path = self._guard_path(operation_id)
        if not _create_once(path, payload) and path.read_bytes() != payload:
            raise RuntimeError("hosted provider retry guard conflicts")
        return guard

    def _guard(
        self,
        operation_id: str,
        request_ipc: bytes,
    ) -> HostedEpisodeRetryGuard:
        payload = self._guard_payload(operation_id, request_ipc)
        return HostedEpisodeRetryGuard(
            ref=f"local-hosted-start://{self._operation_key(operation_id)}",
            digest=_sha256(payload),
        )

    @staticmethod
    def _guard_payload(operation_id: str, request_ipc: bytes) -> bytes:
        return _canonical_json(
            {
                "barrier": "atomic-create-if-absent",
                "operation_id": operation_id,
                "request_digest": hosted_episode_request_digest(request_ipc),
                "version": 1,
            }
        )

    def _publish_result(
        self,
        operation_id: str,
        result: HostedEpisodeProviderResult,
    ) -> None:
        payloads = {
            "request": (
                result.request_ipc,
                hosted_episode_request_digest(result.request_ipc),
            ),
            "trajectory": (
                result.trajectory_ipc,
                hosted_episode_trajectory_digest(result.trajectory_ipc),
            ),
            "episode-results": (
                result.episode_results_ipc,
                hosted_episode_results_digest(result.episode_results_ipc),
            ),
            "manifest": (
                result.manifest_ipc,
                hosted_episode_manifest_digest(result.manifest_ipc),
            ),
        }
        records: dict[str, dict[str, Any]] = {}
        for kind, (payload, digest) in payloads.items():
            path = self._provider_blob_path(kind, digest)
            _write_once(path, payload)
            records[kind] = {
                "digest": digest,
                "size_bytes": len(payload),
            }
        index = _canonical_json(
            {
                "schema_version": _PROVIDER_RECORD_VERSION,
                "operation_id": operation_id,
                "payloads": records,
            }
        )
        _write_once(self._result_path(operation_id), index)

    def _load_result(
        self,
        operation_id: str,
        request_ipc: bytes,
    ) -> HostedEpisodeProviderResult | None:
        index_path = self._result_path(operation_id)
        if not index_path.exists():
            return None
        envelope = json.loads(index_path.read_bytes())
        if (
            not isinstance(envelope, dict)
            or envelope.get("schema_version") != _PROVIDER_RECORD_VERSION
            or envelope.get("operation_id") != operation_id
            or not isinstance(envelope.get("payloads"), dict)
        ):
            raise ValueError("hosted provider result index is incompatible")
        payloads = envelope["payloads"]

        def read(kind: str, digest_fn: Callable[[bytes], str]) -> bytes:
            record = payloads.get(kind)
            if not isinstance(record, dict):
                raise ValueError(f"hosted provider index omits {kind}")
            digest = str(record.get("digest", ""))
            size = int(record.get("size_bytes", -1))
            if _DIGEST.fullmatch(digest) is None or size < 1:
                raise ValueError(f"hosted provider index has invalid {kind} metadata")
            payload = self._provider_blob_path(kind, digest).read_bytes()
            if len(payload) != size or digest_fn(payload) != digest:
                raise ValueError(f"hosted provider {kind} bytes do not match their index")
            return payload

        durable_request = read("request", hosted_episode_request_digest)
        if durable_request != request_ipc:
            raise ValueError("provider result index binds another request")
        result = HostedEpisodeProviderResult(
            request_ipc=durable_request,
            trajectory_ipc=read("trajectory", hosted_episode_trajectory_digest),
            episode_results_ipc=read("episode-results", hosted_episode_results_digest),
            manifest_ipc=read("manifest", hosted_episode_manifest_digest),
        )
        validate_hosted_provider_result(
            result,
            request_ipc=request_ipc,
            operation_id=operation_id,
        )
        return result

    @staticmethod
    def _validate_request(operation_id: str, request_ipc: bytes) -> None:
        requests = decode_hosted_episode_requests(request_ipc)
        if any(row["operation_id"] != operation_id for row in requests):
            raise ValueError("hosted provider operation does not match its request")

    async def result_for(
        self,
        operation_id: str,
        request_ipc: bytes,
    ) -> HostedEpisodeProviderResult | None:
        """Read provider truth without creating a retry guard."""

        self._validate_request(operation_id, request_ipc)
        return await asyncio.to_thread(self._load_result, operation_id, request_ipc)

    @staticmethod
    def _operation_key(operation_id: str) -> str:
        return hashlib.sha256(operation_id.encode()).hexdigest()

    def _operation_dir(self, operation_id: str) -> Path:
        return self._root / "operations" / self._operation_key(operation_id)[:2]

    def _start_path(self, operation_id: str) -> Path:
        return self._operation_dir(operation_id) / f"{self._operation_key(operation_id)}.start.json"

    def _guard_path(self, operation_id: str) -> Path:
        return self._operation_dir(operation_id) / f"{self._operation_key(operation_id)}.guard.json"

    def _result_path(self, operation_id: str) -> Path:
        return (
            self._operation_dir(operation_id) / f"{self._operation_key(operation_id)}.result.json"
        )

    def _provider_blob_path(self, kind: str, digest: str) -> Path:
        if _DIGEST.fullmatch(digest) is None:
            raise ValueError("invalid hosted provider payload digest")
        return self._root / "blobs" / kind / digest[:2] / f"{digest}.arrow"


def _write_replace(path: Path, payload: bytes) -> None:
    """Atomically replace mutable local diagnostics; never used as authority."""

    path.parent.mkdir(parents=True, exist_ok=True)
    handle, temporary = tempfile.mkstemp(
        dir=path.parent,
        prefix=f".{path.name}.",
        suffix=".tmp",
    )
    try:
        with os.fdopen(handle, "wb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


__all__ = [
    "HostedEpisodeRunner",
    "HostedEpisodeProviderStartBlocked",
    "LocalDurableHostedEpisodeProvider",
    "LocalHostedEpisodeValueStore",
    "SeededHostedEpisodeRunner",
]
