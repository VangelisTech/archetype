# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Typed fact-table contracts and claim-backed receipt compatibility."""

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

from archetype.core.component import Component

_FACT_DIGEST_DOMAIN = "archetype.fact.v1"
FACT_ID_COLUMN = "fact_id"
FACT_KEY_COLUMNS = ("world_id", "run_id", "source_uri", "content_hash")
FACT_ENVELOPE_COLUMNS = (FACT_ID_COLUMN, *FACT_KEY_COLUMNS)
_FACT_TABLE_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,62}$")


class FactProcessor(Protocol):
    """Transform each ``daft.File`` input into one typed fact row.

    Processors preserve ``source_uri`` and ``content_hash``. ``FactService``
    owns the remaining envelope columns and removes the execution-only
    ``file`` column before persistence.
    """

    table_name: str

    def process(self, files: DataFrame) -> DataFrame: ...


def fact_table_id(table_name: str) -> str:
    """Return the physical Iceberg identifier for a logical fact table."""
    if not _FACT_TABLE_NAME.fullmatch(table_name):
        raise ValueError(
            "fact table names must start with a letter or underscore, contain "
            "only letters, digits, and underscores, and be at most 63 characters"
        )
    return f"facts__{table_name}"


class FactMeta(Component):
    """Claim identity on legacy evaluation-receipt rows."""

    producer: str = ""
    external_id: str = ""
    payload_digest: str = ""
    commit_id: str = ""


class AssetRef(Component):
    """Content-addressed reference to an external artifact.

    The digest is the identity; the uri is a hint that may rot. Fact
    components embed these fields (or this component) to reference sidecar
    artifacts durably.
    """

    digest: str = ""
    uri: str = ""
    media_type: str = ""
    size_bytes: int = 0
    created_at_ms: int = 0


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


def fact_payload_digest(components: list[Component]) -> str:
    """Server-computed canonical digest of a fact payload.

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
        {"domain": _FACT_DIGEST_DOMAIN, "payload": payload},
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class FactReceipt:
    """Describe a claim-backed durable evaluation receipt."""

    world_id: str
    run_id: str
    producer: str
    external_id: str
    payload_digest: str
    commit_token: str
    fact_entity_id: int
    tick: int
    table_id: str
    duplicate: bool


@dataclass(frozen=True)
class FactWriteReceipt:
    """Describe one committed write to a typed Iceberg fact table."""

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
