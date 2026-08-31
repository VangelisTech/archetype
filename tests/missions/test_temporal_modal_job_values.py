# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Canonical family-value routing for ref-only Temporal Mission jobs."""

from __future__ import annotations

import hashlib

import pytest

from archetype.missions.activities import (
    AuthorActivityRequestRef,
    AuthorActivityResultRef,
)
from archetype.missions.critics import (
    CriticActivityRequestRef,
    CriticActivityResultRef,
)
from archetype.missions.local_activity_values import AUTHOR_ACTIVITY_VALUE_REF_PREFIX
from archetype.missions.local_critic_activity_values import (
    CRITIC_ACTIVITY_VALUE_REF_PREFIX,
)
from archetype.missions.temporal.activity_values import MissionModalActivityValueStore
from archetype.missions.temporal.contracts import (
    MissionJobValueRef,
    MissionModalJobFamily,
)


def _ref(prefix: str, payload: bytes) -> tuple[str, str]:
    digest = hashlib.sha256(payload).hexdigest()
    return f"{prefix}{digest}", digest


class _AuthorValues:
    def __init__(self, request: bytes) -> None:
        self.request = request
        self.request_ref, self.request_digest = _ref(
            AUTHOR_ACTIVITY_VALUE_REF_PREFIX,
            request,
        )
        self.results: dict[str, bytes] = {}

    async def get_encoded_request(self, value: AuthorActivityRequestRef) -> bytes:
        assert value == AuthorActivityRequestRef(
            ref=self.request_ref,
            digest=self.request_digest,
        )
        return self.request

    async def put_encoded_result(
        self,
        encoded: bytes,
        *,
        digest: str,
    ) -> AuthorActivityResultRef:
        ref, observed = _ref(AUTHOR_ACTIVITY_VALUE_REF_PREFIX, encoded)
        assert observed == digest
        self.results.setdefault(digest, encoded)
        assert self.results[digest] == encoded
        return AuthorActivityResultRef(
            ref=ref,
            digest=digest,
            size_bytes=len(encoded),
        )


class _CriticValues:
    def __init__(self, request: bytes) -> None:
        self.request = request
        self.request_ref, self.request_digest = _ref(
            CRITIC_ACTIVITY_VALUE_REF_PREFIX,
            request,
        )
        self.results: dict[str, bytes] = {}

    async def get_encoded_request(self, value: CriticActivityRequestRef) -> bytes:
        assert value == CriticActivityRequestRef(
            ref=self.request_ref,
            digest=self.request_digest,
        )
        return self.request

    async def put_encoded_result(
        self,
        payload: bytes,
        *,
        digest: str,
    ) -> CriticActivityResultRef:
        ref, observed = _ref(CRITIC_ACTIVITY_VALUE_REF_PREFIX, payload)
        assert observed == digest
        self.results.setdefault(digest, payload)
        assert self.results[digest] == payload
        return CriticActivityResultRef(
            ref=ref,
            digest=digest,
            size_bytes=len(payload),
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("family", ["author", "critic"])
async def test_family_values_stay_outside_history_and_round_trip_exactly(
    family: MissionModalJobFamily,
) -> None:
    author = _AuthorValues(b'{"kind":"request","family":"author"}')
    critic = _CriticValues(b'{"kind":"request","family":"critic"}')
    values = MissionModalActivityValueStore(author=author, critic=critic)
    if family == "author":
        request = await values.author_request(
            AuthorActivityRequestRef(
                ref=author.request_ref,
                digest=author.request_digest,
            )
        )
        expected = author.request
    else:
        request = await values.critic_request(
            CriticActivityRequestRef(
                ref=critic.request_ref,
                digest=critic.request_digest,
            )
        )
        expected = critic.request

    assert await values.get_request(family, request) == expected
    payload = f'{{"kind":"result","family":"{family}"}}'.encode()
    digest = hashlib.sha256(payload).hexdigest()
    result = await values.put_result(
        family=family,
        operation_id=f"missions.{family}:operation-1",
        payload=payload,
        payload_digest=digest,
    )
    assert result.digest == digest
    assert result.size_bytes == len(payload)
    if family == "author":
        assert values.author_result(result).ref.startswith(AUTHOR_ACTIVITY_VALUE_REF_PREFIX)
        assert author.results == {digest: payload}
    else:
        assert values.critic_result(result).ref.startswith(CRITIC_ACTIVITY_VALUE_REF_PREFIX)
        assert critic.results == {digest: payload}


@pytest.mark.asyncio
async def test_request_size_or_digest_mismatch_fails_before_provider_use() -> None:
    author = _AuthorValues(b'{"kind":"request","family":"author"}')
    values = MissionModalActivityValueStore(
        author=author,
        critic=_CriticValues(b'{"kind":"request","family":"critic"}'),
    )
    valid = await values.author_request(
        AuthorActivityRequestRef(
            ref=author.request_ref,
            digest=author.request_digest,
        )
    )
    wrong_size = MissionJobValueRef(
        ref=valid.ref,
        digest=valid.digest,
        size_bytes=valid.size_bytes + 1,
    )

    with pytest.raises(ValueError, match="size conflicts"):
        await values.get_request("author", wrong_size)


@pytest.mark.asyncio
async def test_result_digest_mismatch_never_reaches_family_store() -> None:
    author = _AuthorValues(b'{"kind":"request","family":"author"}')
    values = MissionModalActivityValueStore(
        author=author,
        critic=_CriticValues(b'{"kind":"request","family":"critic"}'),
    )

    with pytest.raises(ValueError, match="provider digest"):
        await values.put_result(
            family="author",
            operation_id="missions.author:operation-1",
            payload=b'{"kind":"result"}',
            payload_digest="0" * 64,
        )
    assert author.results == {}
