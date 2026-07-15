# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Durable external facts (issue #274): components and receipts.

A fact is an externally-produced record ingested into a world's history
exactly once (one logically VISIBLE fact per external id). Fact rows carry
their external identity ON the data plane via :class:`FactMeta`, so crash
recovery can find an already-appended orphan and finalize its claim without
re-appending.

Assets (frames, exports, reports) are referenced by content digest — the
digest is the identity, the uri is a retrieval hint.
"""

from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass
from pathlib import Path

from pydantic_core import to_jsonable_python

from archetype.core.component import Component

_FACT_DIGEST_DOMAIN = "archetype.fact.v1"


class FactMeta(Component):
    """External identity riding the data plane on every fact row."""

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
    """The durable outcome of an ingestion: exactly one per external id.

    ``duplicate`` is True when this call matched an already-visible fact
    (same external id, same digest) — the original receipt is returned and
    nothing was appended.
    """

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
