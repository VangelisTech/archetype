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
