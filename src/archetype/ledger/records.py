# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Backend-neutral contracts for linearizable durable control records."""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any, Protocol, runtime_checkable

from daft import DataFrame
from pydantic import BaseModel, ConfigDict, Field, model_validator

from archetype.ledger.canonical import (
    canonical_json,
    durable_record_content_digest,
)
from archetype.ledger.models import Identifier, InternalDigest

MAX_RECORD_PAYLOAD_BYTES = 1_000_000


class DurableRecord(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", strict=True)

    kind: Identifier
    scope: Identifier
    key: Identifier
    revision: int = Field(ge=0)
    content_digest: InternalDigest
    previous_digest: InternalDigest | None = None
    payload_json: str
    committed_at_ms: int = Field(ge=0)

    @model_validator(mode="after")
    def validate_record(self) -> DurableRecord:
        if self.revision == 0 and self.previous_digest is not None:
            raise ValueError("revision zero must not have a previous digest")
        if self.revision > 0 and self.previous_digest is None:
            raise ValueError("later revisions require a previous digest")
        if len(self.payload_json.encode("utf-8")) > MAX_RECORD_PAYLOAD_BYTES:
            raise ValueError("durable record payload exceeds the bounded control-record limit")
        try:
            payload = json.loads(self.payload_json)
        except json.JSONDecodeError as exc:
            raise ValueError("payload_json must contain valid JSON") from exc
        if canonical_json(payload) != self.payload_json:
            raise ValueError("payload_json must use archetype-jcs-v1 canonical JSON")
        expected = durable_record_content_digest(
            kind=self.kind,
            scope=self.scope,
            key=self.key,
            revision=self.revision,
            previous_digest=self.previous_digest,
            payload_json=self.payload_json,
        )
        if self.content_digest != expected:
            raise ValueError(
                f"content_digest mismatch: expected {expected}, got {self.content_digest}"
            )
        return self

    @classmethod
    def create(
        cls,
        *,
        kind: str,
        scope: str,
        key: str,
        revision: int,
        payload: Mapping[str, Any],
        previous_digest: InternalDigest | None = None,
        committed_at_ms: int = 0,
    ) -> DurableRecord:
        payload_json = canonical_json(payload)
        digest = durable_record_content_digest(
            kind=kind,
            scope=scope,
            key=key,
            revision=revision,
            previous_digest=previous_digest,
            payload_json=payload_json,
        )
        return cls(
            kind=kind,
            scope=scope,
            key=key,
            revision=revision,
            content_digest=digest,
            previous_digest=previous_digest,
            payload_json=payload_json,
            committed_at_ms=committed_at_ms,
        )


class AtomicPutResult(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", strict=True)

    record: DurableRecord
    replayed: bool


@runtime_checkable
class iAsyncAtomicRecordStore(Protocol):
    async def put_if_absent(self, record: DurableRecord) -> AtomicPutResult: ...

    async def get(
        self,
        *,
        kind: str,
        scope: str,
        key: str,
        revision: int = 0,
    ) -> DurableRecord | None: ...

    async def get_latest(
        self,
        *,
        kind: str,
        scope: str,
        key: str,
    ) -> DurableRecord | None: ...

    async def compare_and_swap(
        self,
        record: DurableRecord,
        *,
        expected_revision: int | None,
        expected_digest: InternalDigest | None,
    ) -> AtomicPutResult: ...

    async def scan(self, *, kind: str, scope: str | None = None) -> DataFrame: ...

    async def scan_latest(self, *, kind: str, scope: str | None = None) -> DataFrame: ...


@runtime_checkable
class iAsyncReadExistingStore(Protocol):
    """Optional physical-table reads that can never create a missing table."""

    async def table_exists(self, table_id: str) -> bool: ...

    async def list_existing_table_ids(self) -> list[str]: ...

    async def get_table_schema(self, table_id: str) -> Any: ...

    async def get_table_df(
        self,
        table_id: str,
        world_id: str,
        run_id: str,
        *,
        ticks: list[int] | None = None,
        entity_ids: list[int] | None = None,
        active_only: bool = False,
    ) -> DataFrame: ...
