# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Local durable values for the Mission author Activity."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
import tempfile
from pathlib import Path
from typing import Any, Literal, overload

from pydantic import TypeAdapter

from archetype.missions.activities import (
    AuthorActivityRequestRef,
    AuthorActivityResultRef,
    AuthorExecutionObservation,
    DurableAuthorExecutionObservation,
)
from archetype.missions.activity_values import MissionAuthorValueCodec
from archetype.missions.author_activity import MissionAuthorRedactor
from archetype.missions.coding_agents.contracts import TaskDispatchRequest

_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_REF_PREFIX = "mission-author+json:sha256:"
_REQUEST_ADAPTER = TypeAdapter(TaskDispatchRequest)


class LocalMissionAuthorValueStore:
    """Content-addressed canonical JSON with mandatory result redaction.

    The generic activity catalog retains only the returned reference and
    digest.  Raw provider output is sanitized and bounded before any file is
    created.
    """

    def __init__(self, root: str | Path, *, redactor: MissionAuthorRedactor) -> None:
        self._root = Path(root)
        self._codec = MissionAuthorValueCodec(redactor=redactor)

    async def put_request(self, request: TaskDispatchRequest) -> AuthorActivityRequestRef:
        durable = self._codec.sanitize_request(request)
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
        durable = self._codec.sanitize_observation(observation)
        value = await asyncio.to_thread(self._put_result, durable)
        if not isinstance(value, AuthorActivityResultRef):
            raise AssertionError("result persistence returned a request reference")
        return value

    async def get_result(
        self,
        value: AuthorActivityResultRef,
    ) -> DurableAuthorExecutionObservation:
        encoded = await asyncio.to_thread(self._read_encoded, value, "result")
        return self._codec.decode_observation(encoded)

    def _put_result(
        self,
        value: DurableAuthorExecutionObservation,
    ) -> AuthorActivityResultRef:
        return self._put_encoded("result", self._codec.encode_observation(value))

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
        return self._put_encoded(
            kind,
            json.dumps(
                {
                    "kind": kind,
                    "schema_version": 1,
                    "value": value,
                },
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            ).encode(),
        )

    @overload
    def _put_encoded(
        self,
        kind: Literal["request"],
        encoded: bytes,
    ) -> AuthorActivityRequestRef: ...

    @overload
    def _put_encoded(
        self,
        kind: Literal["result"],
        encoded: bytes,
    ) -> AuthorActivityResultRef: ...

    def _put_encoded(
        self,
        kind: Literal["request", "result"],
        encoded: bytes,
    ) -> AuthorActivityRequestRef | AuthorActivityResultRef:
        envelope = json.loads(encoded)
        if (
            not isinstance(envelope, dict)
            or envelope.get("schema_version") != 1
            or envelope.get("kind") != kind
            or "value" not in envelope
        ):
            raise ValueError("author activity value has an incompatible envelope")
        canonical = json.dumps(
            {
                "kind": kind,
                "schema_version": 1,
                "value": envelope["value"],
            },
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
        if canonical != encoded:
            raise ValueError("author activity value is not canonically encoded")
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
        encoded = self._read_encoded(value, expected_kind)
        return json.loads(encoded)["value"]

    def _read_encoded(
        self,
        value: AuthorActivityRequestRef | AuthorActivityResultRef,
        expected_kind: Literal["request", "result"],
    ) -> bytes:
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
        return encoded

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
