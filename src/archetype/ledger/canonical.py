# Copyright 2026 Vangelis Technologies Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Versioned canonical encodings used by durable ledger identities.

The implementation deliberately owns its encoder instead of relying on
``json.dumps(sort_keys=True)``.  Python's mapping order and float rendering are
close to RFC 8785, but are not identical at important exponent boundaries.
"""

from __future__ import annotations

import base64
import json
import math
from collections.abc import Mapping, Sequence
from decimal import Decimal
from enum import Enum
from hashlib import sha256
from pathlib import Path
from typing import Any
from uuid import UUID

import pyarrow as pa
from pydantic import BaseModel

CANONICAL_PROFILE = "archetype-jcs-v1"
COMPONENT_SERIALIZER_VERSION = "lance-model-v1"


class CanonicalizationError(ValueError):
    """Raised when a value has no representation in the canonical profile."""


def _utf16_sort_key(value: str) -> bytes:
    """RFC 8785 object keys sort by UTF-16 code units."""

    return value.encode("utf-16-be", errors="surrogatepass")


def _float_text(value: float) -> str:
    if not math.isfinite(value):
        raise CanonicalizationError("non-finite numbers are not canonical JSON")
    if value == 0:
        return "0"

    # ``repr`` supplies Python's shortest round-tripping significand.  JCS uses
    # ECMAScript's fixed notation in [1e-6, 1e21), unlike Python's repr.
    decimal = Decimal(repr(value))
    magnitude = abs(value)
    if 1e-6 <= magnitude < 1e21:
        text = format(decimal, "f")
        if "." in text:
            text = text.rstrip("0").rstrip(".")
        return text

    mantissa, exponent = format(decimal.normalize(), "e").split("e", 1)
    mantissa = mantissa.rstrip("0").rstrip(".")
    exponent_value = int(exponent)
    exponent_text = f"+{exponent_value}" if exponent_value >= 0 else str(exponent_value)
    return f"{mantissa}e{exponent_text}"


def _json_compatible(value: Any) -> Any:
    """Convert supported boundary values without losing semantic type data."""

    if isinstance(value, BaseModel):
        return value.model_dump(mode="json", exclude_none=False)
    if isinstance(value, Enum):
        return _json_compatible(value.value)
    if isinstance(value, UUID):
        return str(value).lower()
    if isinstance(value, Path):
        return str(value)
    if value is None or isinstance(value, str | bool | int | float):
        return value
    if isinstance(value, Mapping):
        if not all(isinstance(key, str) for key in value):
            raise CanonicalizationError("canonical JSON object keys must be strings")
        return {key: _json_compatible(item) for key, item in value.items()}
    if isinstance(value, Sequence) and not isinstance(value, str | bytes | bytearray):
        return [_json_compatible(item) for item in value]
    raise CanonicalizationError(
        f"unsupported canonical JSON value: {type(value).__module__}.{type(value).__qualname__}"
    )


def _encode(value: Any) -> str:
    if value is None:
        return "null"
    if value is True:
        return "true"
    if value is False:
        return "false"
    if isinstance(value, str):
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    if isinstance(value, int):
        try:
            binary64 = float(value)
        except OverflowError as exc:
            raise CanonicalizationError(
                "integers not exactly representable as IEEE-754 binary64 must be strings"
            ) from exc
        if not math.isfinite(binary64):
            raise CanonicalizationError(
                "integers not exactly representable as IEEE-754 binary64 must be strings"
            )
        rendered = _float_text(binary64)
        if int(binary64) != value and rendered != str(value):
            raise CanonicalizationError(
                "integers not canonically representable as IEEE-754 binary64 must be strings"
            )
        return rendered
    if isinstance(value, float):
        return _float_text(value)
    if isinstance(value, list):
        return "[" + ",".join(_encode(item) for item in value) + "]"
    if isinstance(value, dict):
        items = sorted(value.items(), key=lambda item: _utf16_sort_key(item[0]))
        return "{" + ",".join(f"{_encode(key)}:{_encode(item)}" for key, item in items) + "}"
    raise CanonicalizationError(f"unsupported canonical JSON value: {type(value)!r}")


def jcs_encode(value: Any) -> bytes:
    """Encode *value* as deterministic UTF-8 bytes under ``archetype-jcs-v1``."""

    try:
        return _encode(_json_compatible(value)).encode("utf-8")
    except UnicodeEncodeError as exc:
        raise CanonicalizationError("canonical JSON strings must contain valid Unicode") from exc


def canonical_json(value: Any) -> str:
    """Return the canonical JSON text form for a supported value."""

    return jcs_encode(value).decode("utf-8")


def internal_digest(kind: str, payload: Mapping[str, object]) -> str:
    """Hash a typed canonical envelope with SHA-256."""

    if not kind:
        raise ValueError("digest kind must be non-empty")
    envelope = {"profile": CANONICAL_PROFILE, "kind": kind, "payload": payload}
    return "sha256:" + sha256(jcs_encode(envelope)).hexdigest()


def _metadata_descriptor(metadata: Mapping[bytes, bytes] | None) -> list[list[str]]:
    if not metadata:
        return []
    return [
        [
            base64.urlsafe_b64encode(key).rstrip(b"=").decode("ascii"),
            base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii"),
        ]
        for key, value in sorted(metadata.items(), key=lambda item: item[0])
    ]


def arrow_schema_descriptor(schema: pa.Schema) -> dict[str, object]:
    """Return the ordered, metadata-preserving descriptor used in schema IDs."""

    return {
        "fields": [
            {
                "name": field.name,
                "logical_type": str(field.type),
                "nullable": field.nullable,
                "metadata": _metadata_descriptor(field.metadata),
            }
            for field in schema
        ],
        "metadata": _metadata_descriptor(schema.metadata),
    }


def component_schema_digest(
    component_id: str,
    schema: pa.Schema,
    *,
    serializer_version: str = COMPONENT_SERIALIZER_VERSION,
) -> str:
    """Digest one registered component's persisted, prefixed Arrow schema."""

    return internal_digest(
        "archetype-component-schema-v1",
        {
            "component_id": component_id,
            "serializer_version": serializer_version,
            "schema": arrow_schema_descriptor(schema),
        },
    )


def composite_schema_digest(schema: pa.Schema) -> str:
    """Digest the ordered composite schema for one signature."""

    return internal_digest(
        "archetype-signature-schema-v1", {"schema": arrow_schema_descriptor(schema)}
    )


def durable_record_content_digest(
    *,
    kind: str,
    scope: str,
    key: str,
    revision: int,
    previous_digest: str | None,
    payload_json: str,
) -> str:
    """Digest the semantic fields of a durable control record."""

    return internal_digest(
        "archetype-durable-record-v1",
        {
            "kind": kind,
            "scope": scope,
            "key": key,
            "revision": revision,
            "previous_digest": previous_digest,
            "payload_json": payload_json,
        },
    )
