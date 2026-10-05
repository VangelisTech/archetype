# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Local preview storage port for file metadata attached to a verified cut.

The injected host borrows the existing native owner. This port executes Daft
file plans and converts bounded metadata to Parquet; it owns no catalog or
native lifetime. It does not import the retained runtime or optional host wheel.
"""

from __future__ import annotations

import base64
import json
import re
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path
from typing import Any, Protocol

import pyarrow as pa
import pyarrow.parquet as pq
from daft import DataFrame


class CutHost(Protocol):
    def request(self, op: str, **arguments: Any) -> Any: ...


@dataclass(frozen=True)
class CutCoordinates:
    world: str
    run: str
    tick: int
    cut_id: str

    def __post_init__(self) -> None:
        if any(re.fullmatch(r"[a-z][a-z0-9_]{0,63}", v) is None for v in (self.world, self.run)):
            raise ValueError("Invalid world/run")
        if type(self.tick) is not int or not 1 <= self.tick < 2**63:
            raise ValueError("Expected exact positive Int64 tick")
        if re.fullmatch(r"[0-9a-f]{64}", self.cut_id) is None:
            raise ValueError("Invalid cut identity")

    def as_dict(self) -> dict[str, str | int]:
        return {"world": self.world, "run": self.run, "tick": self.tick, "cut_id": self.cut_id}


@dataclass(frozen=True)
class OccurrenceMetadata:
    """Exact retained bytes for one occurrence; safe to serialize for restart."""

    common: bytes
    typed: tuple[tuple[str, bytes], ...]

    def as_dict(self) -> dict[str, Any]:
        return {
            "common": base64.b64encode(self.common).decode("ascii"),
            "typed": {name: base64.b64encode(value).decode("ascii") for name, value in self.typed},
        }


def _parquet(table: pa.Table) -> bytes:
    output = BytesIO()
    # Flat pipeline metadata: Arrow large strings and uint32 image dimensions
    # are represented as Iceberg strings and lossless signed Int64 values.
    fields = []
    for field in table.schema:
        dtype = field.type
        if pa.types.is_large_string(dtype):
            dtype = pa.string()
        elif pa.types.is_uint32(dtype):
            dtype = pa.int64()
        fields.append(pa.field(field.name, dtype, nullable=field.nullable))
    table = table.cast(pa.schema(fields), safe=True)
    # Native Parquet is intentionally built without compression codecs.
    pq.write_table(table, output, compression="NONE", version="2.6")
    return output.getvalue()


class CutArtifactStorage:
    """Borrow a trusted preview host with an immutable analytical binding."""

    def __init__(self, host: CutHost, binding: dict[str, Any]) -> None:
        self._host = host
        self._binding = json.dumps(binding, allow_nan=False, sort_keys=True)

    def _request(self, operation: str, cut: CutCoordinates, **arguments: Any) -> Any:
        binding = json.loads(self._binding)
        scope = binding["scope"]
        if scope["world"] != cut.world or scope["run"] != cut.run:
            raise ValueError("Attachment cut differs from storage binding")
        return self._host.request(operation, binding=binding, receipt=cut.as_dict(), **arguments)

    def verify(self, cut: CutCoordinates) -> Path:
        """Full canonical cut verification before any source-file effects."""
        return Path(self._request("artifact_cut", cut)["object_root"])

    @staticmethod
    def materialize(frame: DataFrame) -> DataFrame:
        """Freeze occurrence identity or file persistence exactly once."""
        return frame.collect()

    @staticmethod
    def encode(
        common: DataFrame, typed: tuple[tuple[str, DataFrame], ...]
    ) -> tuple[OccurrenceMetadata, ...]:
        common_rows = common.to_arrow()
        if common_rows.num_rows > 32:
            raise ValueError("At most 32 occurrences per attachment call")
        ids = common_rows["artifact_id"].to_pylist()
        branches: dict[str, dict[str, bytes]] = {artifact_id: {} for artifact_id in ids}
        for name, frame in typed:
            rows = frame.to_arrow()
            for index, artifact_id in enumerate(rows["artifact_id"].to_pylist()):
                if artifact_id not in branches or name in branches[artifact_id]:
                    raise ValueError("Unexpected or duplicate typed occurrence")
                branches[artifact_id][name] = _parquet(rows.slice(index, 1))
        return tuple(
            OccurrenceMetadata(
                _parquet(common_rows.slice(index, 1)), tuple(branches[artifact_id].items())
            )
            for index, artifact_id in enumerate(ids)
        )

    def publish(
        self, cut: CutCoordinates, occurrences: tuple[OccurrenceMetadata, ...]
    ) -> tuple[dict[str, Any], ...]:
        """All typed appends precede per-occurrence common roots. Partial success
        is possible; repeat with the same retained bytes after a lost response.
        """
        return tuple(
            self._request(
                "attach_artifacts", cut, attachments=[item.as_dict() for item in occurrences]
            )
        )

    def read(self, cut: CutCoordinates, *, offset: int = 0, limit: int = 32) -> dict[str, Any]:
        """Read and verify common roots, their typed proofs, and original bytes."""
        return self._request("read_artifacts", cut, offset=offset, limit=limit)

    @staticmethod
    def decode(encoded: str) -> pa.Table:
        return pq.read_table(BytesIO(base64.b64decode(encoded, validate=True)))
