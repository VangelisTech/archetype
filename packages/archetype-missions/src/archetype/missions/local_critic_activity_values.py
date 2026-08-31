# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Local content-addressed values for the Mission critic Activity."""

from __future__ import annotations

import asyncio
import hashlib
import os
import re
import tempfile
from pathlib import Path

from archetype.missions.critics import (
    CRITIC_ACTIVITY_MEDIA_TYPE,
    CandidateReviewRequest,
    CriticActivityCodec,
    CriticActivityRequest,
    CriticActivityRequestRef,
    CriticActivityResult,
    CriticActivityResultRef,
    CriticActivityValue,
    CriticExecutionResult,
)

_DIGEST = re.compile(r"^[0-9a-f]{64}$")
CRITIC_ACTIVITY_VALUE_REF_PREFIX = "mission-critic+json:sha256:"
_REF_PREFIX = CRITIC_ACTIVITY_VALUE_REF_PREFIX


class LocalMissionCriticValueStore:
    """Persist canonical redacted critic values outside the control catalog.

    The codec performs identity validation, bounds checking, and mandatory
    pre-durability redaction before this store creates a file. Values are
    addressed by their canonical payload digest, so retrying a write is an
    exact idempotent operation.
    """

    def __init__(self, root: str | Path, *, codec: CriticActivityCodec) -> None:
        self._root = Path(root)
        self._codec = codec

    async def put_request(
        self,
        request: CandidateReviewRequest,
    ) -> CriticActivityRequestRef:
        value = self._codec.encode_request(self._codec.prepare_request(request))
        await asyncio.to_thread(self._put, value)
        return CriticActivityRequestRef(ref=value.ref, digest=value.digest)

    async def get_request(
        self,
        value: CriticActivityRequestRef,
    ) -> CriticActivityRequest:
        payload = await asyncio.to_thread(
            self._read,
            ref=value.ref,
            digest=value.digest,
            expected_size=0,
        )
        decoded = self._codec.decode_request(payload)
        encoded = self._codec.encode_request(decoded)
        if encoded.ref != value.ref or encoded.digest != value.digest:
            raise ValueError("critic request reference does not match its canonical value")
        return decoded

    async def get_encoded_request(self, value: CriticActivityRequestRef) -> bytes:
        """Read exact canonical request bytes for provider orchestration."""

        payload = await asyncio.to_thread(
            self._read,
            ref=value.ref,
            digest=value.digest,
            expected_size=0,
        )
        decoded = self._codec.decode_request(payload)
        if self._codec.encode_request(decoded).payload != payload:
            raise ValueError("critic request reference does not match its canonical value")
        return payload

    async def put_result(
        self,
        result: CriticExecutionResult,
        request: CriticActivityRequest,
    ) -> CriticActivityResultRef:
        value = self._codec.encode_result(
            self._codec.prepare_result(result, request),
        )
        await asyncio.to_thread(self._put, value)
        return CriticActivityResultRef(
            ref=value.ref,
            digest=value.digest,
            media_type=value.media_type,
            size_bytes=value.size_bytes,
        )

    async def get_result(
        self,
        value: CriticActivityResultRef,
    ) -> CriticActivityResult:
        if value.media_type != CRITIC_ACTIVITY_MEDIA_TYPE:
            raise ValueError("critic result reference has an unsupported media type")
        payload = await asyncio.to_thread(
            self._read,
            ref=value.ref,
            digest=value.digest,
            expected_size=value.size_bytes,
        )
        decoded = self._codec.decode_result(payload)
        encoded = self._codec.encode_result(decoded)
        if (
            encoded.ref != value.ref
            or encoded.digest != value.digest
            or encoded.size_bytes != value.size_bytes
        ):
            raise ValueError("critic result reference does not match its canonical value")
        return decoded

    async def put_encoded_result(
        self,
        payload: bytes,
        *,
        digest: str,
    ) -> CriticActivityResultRef:
        """Revalidate and persist one controller-produced canonical result."""

        if hashlib.sha256(payload).hexdigest() != digest:
            raise ValueError("critic encoded result does not match its provider digest")
        decoded = self._codec.decode_result(payload)
        value = self._codec.encode_result(decoded)
        if value.payload != payload or value.digest != digest:
            raise ValueError("critic encoded result is not its exact canonical value")
        await asyncio.to_thread(self._put, value)
        return CriticActivityResultRef(
            ref=value.ref,
            digest=value.digest,
            media_type=value.media_type,
            size_bytes=value.size_bytes,
        )

    def _put(self, value: CriticActivityValue) -> None:
        path = self._path(value.digest)
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists():
            if path.read_bytes() != value.payload:
                raise RuntimeError("critic Activity digest collision")
            return

        handle, temporary = tempfile.mkstemp(
            dir=path.parent,
            prefix=f".{value.digest}.",
            suffix=".tmp",
        )
        try:
            with os.fdopen(handle, "wb") as stream:
                stream.write(value.payload)
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

    def _read(
        self,
        *,
        ref: str,
        digest: str,
        expected_size: int,
    ) -> bytes:
        ref_digest = self._ref_digest(ref, digest)
        payload = self._path(ref_digest).read_bytes()
        observed = hashlib.sha256(payload).hexdigest()
        if observed != digest:
            raise ValueError("critic Activity value digest does not match its contents")
        if expected_size and len(payload) != expected_size:
            raise ValueError("critic Activity value size does not match its contents")
        return payload

    @staticmethod
    def _ref_digest(ref: str, digest: str) -> str:
        if not ref.startswith(_REF_PREFIX):
            raise ValueError("unsupported critic Activity value reference")
        ref_digest = ref.removeprefix(_REF_PREFIX)
        if not _DIGEST.fullmatch(digest) or ref_digest != digest:
            raise ValueError("critic Activity value reference has an invalid digest")
        return digest

    def _path(self, digest: str) -> Path:
        if not _DIGEST.fullmatch(digest):
            raise ValueError("invalid critic Activity value digest")
        return self._root / digest[:2] / f"{digest}.json"


__all__ = ["CRITIC_ACTIVITY_VALUE_REF_PREFIX", "LocalMissionCriticValueStore"]
