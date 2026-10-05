"""Exact scalar and object validation shared by typed local and wire models."""

from __future__ import annotations

import math
import re
import struct
import unicodedata
from typing import Any

MAX_CELLS = 64
MAX_STRING_BYTES = 4096


def bool_cell(value: Any) -> bool:
    if type(value) is not bool:
        raise ValueError("Expected exact Bool")
    return value


def float_cell(value: Any) -> float:
    if type(value) is not float or not math.isfinite(value):
        raise ValueError("Expected finite Float64")
    return 0.0 if value == 0.0 else value


def float_bits(value: Any) -> str:
    return struct.pack(">d", float_cell(value)).hex()


def decode_float_bits(value: Any) -> float:
    if type(value) is not str or not re.fullmatch(r"[0-9a-f]{16}", value):
        raise ValueError("Expected canonical Float64 bits")
    result = struct.unpack(">d", bytes.fromhex(value))[0]
    if not math.isfinite(result) or value == "8000000000000000":
        raise ValueError("Noncanonical or nonfinite Float64")
    return result


def fields(value: Any, names: str) -> dict[str, Any]:
    if type(value) is not dict or set(value) != set(names.split()):
        raise ValueError("Unexpected object fields")
    return value


def identifier(value: Any) -> str:
    if type(value) is not str or not re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_.:-]{0,127}", value):
        raise ValueError("Invalid identifier")
    return value


def digest(value: Any) -> str:
    if type(value) is not str or not re.fullmatch(r"[0-9a-f]{64}", value):
        raise ValueError("Invalid digest")
    return value


def optional_digest(value: Any) -> str | None:
    return None if value is None else digest(value)


def decimal(value: Any, *, signed: bool = False) -> int:
    # Strings prevent a JavaScript parser from rounding before validation.
    pattern = r"(?:0|[1-9][0-9]*|-[1-9][0-9]*)" if signed else r"(?:0|[1-9][0-9]*)"
    if type(value) is not str or len(value) > 20 or not re.fullmatch(pattern, value):
        raise ValueError("Expected canonical decimal string")
    result = int(value)
    lower, upper = (-(2**63), 2**63) if signed else (0, 2**64)
    if not lower <= result < upper:
        raise ValueError("Integer outside exact range")
    return result


def unsigned(value: Any) -> str:
    if type(value) is not int or not 0 <= value < 2**64:
        raise ValueError("Invalid native unsigned integer")
    return str(value)


def string_cell(value: Any) -> str:
    if (
        type(value) is not str
        or len(value.encode("utf-8")) > MAX_STRING_BYTES
        or any(unicodedata.category(c) == "Cc" for c in value)
    ):
        raise ValueError("Invalid string cell")
    return value


ARTIFACT_INDEX_FIELDS = {
    "files": "ingested_at logical_path size_bytes mime_type media_family sha256 xxhash3_64",
    "images": "width height format mode",
    "audio": "sample_rate channels frames format subtype duration_seconds",
    "video": "width height fps frame_count time_base duration_seconds",
    "pdf": "page_count encrypted title author",
    "text": "text_kind language line_count utf8",
    "diff": "format file_count hunk_count additions deletions binary_file_count",
}
ARTIFACT_ATTRIBUTION = "artifact_id context_id world run tick cut_id"


def artifact_facts(value: Any, index: str) -> dict[str, Any]:
    """Validate nullable metadata independently of non-null live cells."""
    raw = fields(value, ARTIFACT_ATTRIBUTION + " " + ARTIFACT_INDEX_FIELDS[index])
    result = {}
    for name, cell in raw.items():
        if cell is None:
            result[name] = None
            continue
        if type(cell) is not dict or len(cell) != 1:
            raise ValueError("Expected tagged artifact fact")
        tag, fact = next(iter(cell.items()))
        expected = (
            "timestamp_us"
            if name == "ingested_at"
            else "float64"
            if name in {"frames", "duration_seconds", "fps", "time_base"}
            else "bool"
            if name in {"encrypted", "utf8"}
            else "int64"
            if name
            in {
                "size_bytes",
                "tick",
                "width",
                "height",
                "sample_rate",
                "channels",
                "frame_count",
                "page_count",
                "line_count",
                "file_count",
                "hunk_count",
                "additions",
                "deletions",
                "binary_file_count",
            }
            else "string"
        )
        if tag != expected:
            raise ValueError("Artifact fact type differs from index contract")
        if tag in {"int64", "timestamp_us"}:
            decimal(fact, signed=True)
        elif tag == "float64":
            decode_float_bits(fact)
        elif tag == "bool":
            bool_cell(fact)
        elif tag == "string":
            if type(fact) is not str or len(fact.encode("utf-8")) > MAX_STRING_BYTES:
                raise ValueError("Artifact string fact exceeds bound")
        else:
            raise ValueError("Unsupported artifact fact tag")
        result[name] = {tag: fact}
    return result
