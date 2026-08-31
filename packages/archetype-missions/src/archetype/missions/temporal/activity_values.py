# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Family-owned canonical values adapted to ref-only Temporal Activities."""

from __future__ import annotations

import hashlib
from typing import Protocol

from archetype.missions.activities import (
    AuthorActivityRequestRef,
    AuthorActivityResultRef,
)
from archetype.missions.critics import (
    CriticActivityRequestRef,
    CriticActivityResultRef,
)

from .contracts import MissionJobValueRef, MissionModalJobFamily


class MissionModalAuthorValueStore(Protocol):
    """Canonical author bytes retained outside Temporal history."""

    async def get_encoded_request(self, value: AuthorActivityRequestRef) -> bytes: ...

    async def put_encoded_result(
        self,
        encoded: bytes,
        *,
        digest: str,
    ) -> AuthorActivityResultRef: ...


class MissionModalCriticValueStore(Protocol):
    """Canonical critic bytes retained outside Temporal history."""

    async def get_encoded_request(self, value: CriticActivityRequestRef) -> bytes: ...

    async def put_encoded_result(
        self,
        payload: bytes,
        *,
        digest: str,
    ) -> CriticActivityResultRef: ...


class MissionModalActivityValueStore:
    """Route exact author and critic values without copying bytes into history."""

    def __init__(
        self,
        *,
        author: MissionModalAuthorValueStore,
        critic: MissionModalCriticValueStore,
    ) -> None:
        self._author = author
        self._critic = critic

    async def author_request(self, value: AuthorActivityRequestRef) -> MissionJobValueRef:
        payload = await self._author.get_encoded_request(value)
        return _request_ref(value.ref, value.digest, payload)

    async def critic_request(self, value: CriticActivityRequestRef) -> MissionJobValueRef:
        payload = await self._critic.get_encoded_request(value)
        return _request_ref(value.ref, value.digest, payload)

    async def get_request(
        self,
        family: MissionModalJobFamily,
        ref: MissionJobValueRef,
    ) -> bytes:
        if family == "author":
            payload = await self._author.get_encoded_request(
                AuthorActivityRequestRef(ref=ref.ref, digest=ref.digest)
            )
        elif family == "critic":
            payload = await self._critic.get_encoded_request(
                CriticActivityRequestRef(ref=ref.ref, digest=ref.digest)
            )
        else:  # pragma: no cover - closed Literal defense
            raise ValueError("Mission job request family is invalid")
        _require_payload(ref, payload)
        return payload

    async def put_result(
        self,
        *,
        family: MissionModalJobFamily,
        operation_id: str,
        payload: bytes,
        payload_digest: str,
    ) -> MissionJobValueRef:
        if not operation_id.strip():
            raise ValueError("Mission job result requires its immutable operation identity")
        if hashlib.sha256(payload).hexdigest() != payload_digest:
            raise ValueError("Mission job result bytes do not match their provider digest")
        if family == "author":
            result = await self._author.put_encoded_result(payload, digest=payload_digest)
        elif family == "critic":
            result = await self._critic.put_encoded_result(payload, digest=payload_digest)
        else:  # pragma: no cover - closed Literal defense
            raise ValueError("Mission job result family is invalid")
        if result.digest != payload_digest or result.size_bytes != len(payload):
            raise ValueError("Mission family store returned another result value")
        return MissionJobValueRef(
            ref=result.ref,
            digest=result.digest,
            size_bytes=result.size_bytes,
        )

    @staticmethod
    def author_result(ref: MissionJobValueRef) -> AuthorActivityResultRef:
        return AuthorActivityResultRef(
            ref=ref.ref,
            digest=ref.digest,
            size_bytes=ref.size_bytes,
        )

    @staticmethod
    def critic_result(ref: MissionJobValueRef) -> CriticActivityResultRef:
        return CriticActivityResultRef(
            ref=ref.ref,
            digest=ref.digest,
            size_bytes=ref.size_bytes,
        )


def _request_ref(ref: str, digest: str, payload: bytes) -> MissionJobValueRef:
    value = MissionJobValueRef(
        ref=ref,
        digest=digest,
        size_bytes=len(payload),
    )
    _require_payload(value, payload)
    return value


def _require_payload(ref: MissionJobValueRef, payload: bytes) -> None:
    if type(payload) is not bytes:
        raise TypeError("Mission family value store returned a non-bytes request")
    if len(payload) != ref.size_bytes:
        raise ValueError("Mission family request size conflicts with its reference")
    if hashlib.sha256(payload).hexdigest() != ref.digest:
        raise ValueError("Mission family request bytes conflict with their reference")


__all__ = [
    "MissionModalActivityValueStore",
    "MissionModalAuthorValueStore",
    "MissionModalCriticValueStore",
]
