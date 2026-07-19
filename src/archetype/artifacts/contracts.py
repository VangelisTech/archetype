# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Typed artifact-table contracts and claim-backed publication receipts.

Pure artifact value contracts: the processor protocol, table-name and
envelope constants, content/identity digest helpers, and publication receipt
values. The persistent ECS schemas live in ``archetype.artifacts.components``;
publication, indexing, and storage authority remain under
``archetype.app.artifacts``.
"""

from __future__ import annotations

import hashlib
import json
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from daft import DataFrame
from pydantic_core import to_jsonable_python

from archetype.artifacts.components import AssetRef
from archetype.core.component import Component

_ARTIFACT_DIGEST_DOMAIN = "archetype.artifact.v1"
ARTIFACT_ID_COLUMN = "artifact_id"
ARTIFACT_KEY_COLUMNS = ("world_id", "run_id", "source_uri", "content_hash")
ARTIFACT_ENVELOPE_COLUMNS = (ARTIFACT_ID_COLUMN, *ARTIFACT_KEY_COLUMNS)
_ARTIFACT_TABLE_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,62}$")


class ArtifactProcessor(Protocol):
    """Transform each ``daft.File`` input into one typed artifact row.

    Processors preserve ``source_uri`` and ``content_hash``. ``ArtifactTableService``
    owns the remaining envelope columns and removes the execution-only
    ``file`` column before persistence.
    """

    table_name: str

    def process(self, files: DataFrame) -> DataFrame: ...


def artifact_table_id(table_name: str) -> str:
    """Return the physical Iceberg identifier for a logical artifact table."""
    if not _ARTIFACT_TABLE_NAME.fullmatch(table_name):
        raise ValueError(
            "artifact table names must start with a letter or underscore, contain "
            "only letters, digits, and underscores, and be at most 63 characters"
        )
    return f"artifacts__{table_name}"


def digest_bytes(data: bytes) -> str:
    """Content digest for asset bytes (sha256, hex)."""
    return hashlib.sha256(data).hexdigest()


def digest_file(path: str | Path, chunk_size: int = 1 << 20) -> str:
    """Content digest for a file on disk, streamed."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(chunk_size):
            h.update(chunk)
    return h.hexdigest()


def asset_ref_for_file(path: str | Path, *, media_type: str = "") -> AssetRef:
    """Build a content-addressed reference for a local artifact."""
    p = Path(path)
    return AssetRef(
        digest=digest_file(p),
        uri=str(p),
        media_type=media_type,
        size_bytes=p.stat().st_size,
        created_at_ms=int(time.time() * 1000),
    )


def artifact_payload_digest(components: list[Component]) -> str:
    """Server-computed canonical digest of an artifact payload.

    Caller-supplied hashes are never trusted; the digest is derived from
    the component types and field values in canonical JSON, order-invariant
    across the component list.
    """
    payload = sorted(
        (
            {
                "type": type(c).__name__,
                "fields": to_jsonable_python(c.model_dump()),
            }
            for c in components
        ),
        key=lambda item: json.dumps(item, sort_keys=True, separators=(",", ":")),
    )
    canonical = json.dumps(
        {"domain": _ARTIFACT_DIGEST_DOMAIN, "payload": payload},
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class ArtifactReceipt:
    """Describe one claim-backed durable artifact publication."""

    world_id: str
    run_id: str
    producer: str
    external_id: str
    payload_digest: str
    commit_token: str
    artifact_entity_id: int
    tick: int
    table_id: str
    duplicate: bool


@dataclass(frozen=True)
class ArtifactWriteReceipt:
    """Describe one committed write to a typed Iceberg artifact table."""

    world_id: str
    run_id: str
    table_name: str
    table_id: str
    sources_matched: int | None
    rows_written: int
    snapshot_id: int | None

    @property
    def duplicate(self) -> bool | None:
        if self.rows_written > 0:
            return False
        if self.sources_matched is None:
            return None
        return self.sources_matched > 0
