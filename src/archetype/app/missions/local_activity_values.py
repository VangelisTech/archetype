# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Local durable values for the Mission author-activity substrate proof."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
import tempfile
from dataclasses import replace
from pathlib import Path
from typing import Any, Literal, overload

from pydantic import TypeAdapter

from archetype.app.missions.activities import MissionAuthorRedactor
from archetype.missions.activities import (
    AuthorActivityRequestRef,
    AuthorActivityResultRef,
    AuthorExecutionObservation,
    DurableAuthorExecutionObservation,
)
from archetype.missions.coding_agents.contracts import (
    CommitObservation,
    FrictionObservation,
    TaskDispatchRequest,
    ValidationObservation,
)

_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_REF_PREFIX = "mission-author+json:sha256:"
_REQUEST_ADAPTER = TypeAdapter(TaskDispatchRequest)
_RESULT_ADAPTER = TypeAdapter(DurableAuthorExecutionObservation)


class LocalMissionAuthorValueStore:
    """Content-addressed canonical JSON with mandatory result redaction.

    The generic activity catalog retains only the returned reference and
    digest.  Raw provider output is sanitized and bounded before any file is
    created.
    """

    def __init__(self, root: str | Path, *, redactor: MissionAuthorRedactor) -> None:
        self._root = Path(root)
        self._redactor = redactor

    async def put_request(self, request: TaskDispatchRequest) -> AuthorActivityRequestRef:
        durable = self._sanitize_request(request)
        value = await asyncio.to_thread(
            self._put,
            "request",
            _REQUEST_ADAPTER.dump_python(durable, mode="json"),
        )
        if not isinstance(value, AuthorActivityRequestRef):
            raise AssertionError("request persistence returned a result reference")
        return value

    async def get_request(self, value: AuthorActivityRequestRef) -> TaskDispatchRequest:
        payload = await asyncio.to_thread(self._read, value, "request")
        return _REQUEST_ADAPTER.validate_python(payload)

    async def put_result(
        self,
        observation: AuthorExecutionObservation,
    ) -> AuthorActivityResultRef:
        durable = self._sanitize(observation)
        value = await asyncio.to_thread(
            self._put,
            "result",
            _RESULT_ADAPTER.dump_python(durable, mode="json"),
        )
        if not isinstance(value, AuthorActivityResultRef):
            raise AssertionError("result persistence returned a request reference")
        return value

    async def get_result(
        self,
        value: AuthorActivityResultRef,
    ) -> DurableAuthorExecutionObservation:
        payload = await asyncio.to_thread(self._read, value, "result")
        return _RESULT_ADAPTER.validate_python(payload)

    def _sanitize_request(self, request: TaskDispatchRequest) -> TaskDispatchRequest:
        """Remove free-text secrets and reject unsafe structural identities."""

        for field, value in (
            ("author.request.dispatch_id", request.dispatch_id),
            ("author.request.repository", request.repository),
            ("author.request.branch", request.branch),
            ("author.request.base_ref", request.base_ref),
            ("author.request.task_base_revision", request.task_base_revision),
            (
                "author.request.previous_agent_session_id",
                request.previous_agent_session_id,
            ),
        ):
            self._safe(value, field)

        value = _REQUEST_ADAPTER.dump_python(request, mode="json")

        def visit(item: Any, path: tuple[str, ...]) -> Any:
            if isinstance(item, str):
                scope = "mission-author-request:" + ".".join(path)
                return self._redactor.redact_text(item, scope=scope).text
            if isinstance(item, list):
                return [visit(child, (*path, str(position))) for position, child in enumerate(item)]
            if isinstance(item, dict):
                return {key: visit(child, (*path, str(key))) for key, child in item.items()}
            return item

        return _REQUEST_ADAPTER.validate_python(visit(value, ("request",)))

    def _sanitize(
        self,
        observation: AuthorExecutionObservation,
    ) -> DurableAuthorExecutionObservation:
        result = observation.result
        dispatch_id = self._safe(result.dispatch_id, "author.dispatch_id")
        scope = f"mission:{result.mission_id}:author:{dispatch_id}"
        sandbox = result.sandbox
        safe_sandbox = replace(
            sandbox,
            provider=self._safe(sandbox.provider, "author.sandbox.provider"),
            sandbox_id=self._safe(sandbox.sandbox_id, "author.sandbox.id"),
            environment=self._safe(
                sandbox.environment,
                "author.sandbox.environment",
            ),
        )

        validation = tuple(
            ValidationObservation(
                validator_id=item.validator_id,
                name=self._redact(item.name, 1_000, f"{scope}:validator-name"),
                command=tuple(
                    self._redact(
                        argument,
                        2_000,
                        f"{scope}:validator-command:{position}",
                    )
                    for position, argument in enumerate(item.command)
                ),
                expected_returncode=item.expected_returncode,
                actual_returncode=item.actual_returncode,
                revision=self._safe(item.revision, "author.validation.revision"),
                stdout=self._redact(item.stdout, 4_000, f"{scope}:validator-stdout"),
                stderr=self._redact(item.stderr, 4_000, f"{scope}:validator-stderr"),
            )
            for item in result.validation
        )
        commits = tuple(
            CommitObservation(
                sha=self._safe(item.sha, "author.commit.sha"),
                message=self._redact(item.message, 4_000, f"{scope}:commit-message"),
                branch=self._safe(item.branch, "author.commit.branch"),
                pushed=item.pushed,
                final_revision=item.final_revision,
            )
            for item in result.commits
        )
        friction = tuple(
            FrictionObservation(
                kind=self._safe(item.kind, "author.friction.kind"),
                message=self._redact(item.message, 4_000, f"{scope}:friction"),
            )
            for item in result.friction
        )
        sanitized = replace(
            result,
            dispatch_id=dispatch_id,
            sandbox=safe_sandbox,
            worktree=self._safe(result.worktree, "author.worktree"),
            agent_session_id=self._safe(
                result.agent_session_id,
                "author.agent_session_id",
            ),
            starting_revision=self._safe(
                result.starting_revision,
                "author.starting_revision",
            ),
            final_revision=self._safe(
                result.final_revision,
                "author.final_revision",
            ),
            diff_digest=self._safe(result.diff_digest, "author.diff_digest"),
            validator_bundle_digest=self._safe(
                result.validator_bundle_digest,
                "author.validator_bundle_digest",
            ),
            agent_stdout=self._redact(result.agent_stdout, 16_000, f"{scope}:agent-stdout"),
            agent_stderr=self._redact(result.agent_stderr, 16_000, f"{scope}:agent-stderr"),
            trace_uri=self._safe(result.trace_uri, "author.trace_uri"),
            validation=validation,
            commits=commits,
            friction=friction,
            error=self._redact(result.error, 4_000, f"{scope}:error"),
        )
        return DurableAuthorExecutionObservation(
            result=sanitized,
            sandbox_status=observation.sandbox_status,
            redaction_policy_id=self._redactor.policy_id,
            bind_mission=observation.bind_mission,
        )

    def _redact(self, value: str, limit: int, scope: str) -> str:
        if not value:
            return ""
        return self._redactor.redact_text(value, scope=scope).text[-limit:]

    def _safe(self, value: str, field: str, *, limit: int = 4_096) -> str:
        if len(value) > limit:
            raise ValueError(f"{field} must be at most {limit} characters")
        if value:
            self._redactor.assert_safe_metadata(value, field=field)
        return value

    @overload
    def _put(
        self,
        kind: Literal["request"],
        value: Any,
    ) -> AuthorActivityRequestRef: ...

    @overload
    def _put(
        self,
        kind: Literal["result"],
        value: Any,
    ) -> AuthorActivityResultRef: ...

    def _put(
        self,
        kind: Literal["request", "result"],
        value: Any,
    ) -> AuthorActivityRequestRef | AuthorActivityResultRef:
        encoded = json.dumps(
            {
                "kind": kind,
                "schema_version": 1,
                "value": value,
            },
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
        digest = hashlib.sha256(encoded).hexdigest()
        path = self._path(digest)
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists():
            if path.read_bytes() != encoded:
                raise RuntimeError("author activity digest collision")
        else:
            handle, temporary = tempfile.mkstemp(
                dir=path.parent,
                prefix=f".{digest}.",
                suffix=".tmp",
            )
            try:
                with os.fdopen(handle, "wb") as stream:
                    stream.write(encoded)
                    stream.flush()
                    os.fsync(stream.fileno())
                os.replace(temporary, path)
                directory = os.open(path.parent, os.O_RDONLY)
                try:
                    os.fsync(directory)
                finally:
                    os.close(directory)
            finally:
                if os.path.exists(temporary):
                    os.unlink(temporary)
        if kind == "request":
            return AuthorActivityRequestRef(
                ref=f"{_REF_PREFIX}{digest}",
                digest=digest,
            )
        return AuthorActivityResultRef(
            ref=f"{_REF_PREFIX}{digest}",
            digest=digest,
            size_bytes=len(encoded),
        )

    def _read(
        self,
        value: AuthorActivityRequestRef | AuthorActivityResultRef,
        expected_kind: Literal["request", "result"],
    ) -> Any:
        digest = self._ref_digest(value)
        encoded = self._path(digest).read_bytes()
        observed = hashlib.sha256(encoded).hexdigest()
        if observed != value.digest or observed != digest:
            raise ValueError("author activity value digest does not match its contents")
        if isinstance(value, AuthorActivityResultRef) and (
            value.size_bytes and len(encoded) != value.size_bytes
        ):
            raise ValueError("author activity value size does not match its contents")
        envelope = json.loads(encoded)
        if (
            not isinstance(envelope, dict)
            or envelope.get("schema_version") != 1
            or envelope.get("kind") != expected_kind
            or "value" not in envelope
        ):
            raise ValueError("author activity value has an incompatible envelope")
        return envelope["value"]

    @staticmethod
    def _ref_digest(value: AuthorActivityRequestRef | AuthorActivityResultRef) -> str:
        if isinstance(value, AuthorActivityResultRef) and value.media_type != "application/json":
            raise ValueError("author activity value must be canonical JSON")
        if not value.ref.startswith(_REF_PREFIX):
            raise ValueError("unsupported author activity value reference")
        digest = value.ref.removeprefix(_REF_PREFIX)
        if not _DIGEST.fullmatch(digest) or value.digest != digest:
            raise ValueError("author activity value reference has an invalid digest")
        return digest

    def _path(self, digest: str) -> Path:
        if not _DIGEST.fullmatch(digest):
            raise ValueError("invalid author activity value digest")
        return self._root / digest[:2] / f"{digest}.json"


__all__ = ["LocalMissionAuthorValueStore"]
