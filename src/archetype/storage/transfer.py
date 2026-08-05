# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Physical Iceberg snapshot evidence used by storage migration."""

from __future__ import annotations

import base64
import hashlib
import math
import struct
from dataclasses import dataclass
from decimal import Decimal

import pyarrow as pa

from archetype.storage.catalog.records import schema_fingerprint

_DIGEST_DOMAIN = b"archetype.storage-migration.rows.v1\x00"
_TYPE_NORMALIZATION = {
    "large_string": "string",
    "large_binary": "binary",
}


@dataclass(frozen=True, slots=True)
class TableSnapshotEvidence:
    """Logical evidence for one exact Iceberg table snapshot."""

    name: str
    snapshot_id: int | None
    schema_ipc_base64: str
    schema_fingerprint: str
    row_count: int
    content_digest: str

    @property
    def arrow_schema(self) -> pa.Schema:
        raw = base64.b64decode(self.schema_ipc_base64.encode("ascii"), validate=True)
        return pa.ipc.read_schema(pa.BufferReader(raw))


@dataclass(frozen=True, slots=True)
class ImportedTableReceipt:
    """Verified destination result for one imported table."""

    name: str
    source_snapshot_id: int | None
    destination_snapshot_id: int | None
    source_schema_fingerprint: str
    destination_schema_fingerprint: str
    row_count: int
    source_content_digest: str
    destination_content_digest: str


def encode_schema(schema: pa.Schema) -> str:
    """Preserve the exact Arrow schema without relying on text parsing."""

    return base64.b64encode(schema.serialize().to_pybytes()).decode("ascii")


def _part(payload: bytes) -> bytes:
    return len(payload).to_bytes(8, "big", signed=False) + payload


def _integer_payload(value: int) -> bytes:
    magnitude = abs(value)
    width = max(1, (magnitude.bit_length() + 7) // 8)
    sign = b"-" if value < 0 else b"+"
    return sign + magnitude.to_bytes(width, "big", signed=False)


def _logical_type_tag(dtype: pa.DataType) -> bytes:
    """Match the physical-encoding normalization used by schema_fingerprint."""

    value = str(dtype)
    for physical, logical in _TYPE_NORMALIZATION.items():
        value = value.replace(physical, logical)
    return value.encode("utf-8")


def logical_arrow_schemas_equal(left: pa.Schema, right: pa.Schema) -> bool:
    """Compare migration-relevant Arrow shape while ignoring physical metadata.

    Iceberg and Arrow adapters may add or remove schema/field metadata and may
    expose equivalent string or binary widths.  Column order, names, logical
    types, and nullability remain part of the durable table contract.
    """

    if not isinstance(left, pa.Schema) or not isinstance(right, pa.Schema):
        raise TypeError("logical Arrow schema comparison requires pyarrow.Schema values")
    return tuple(
        (field.name, _logical_type_tag(field.type), bool(field.nullable)) for field in left
    ) == tuple((field.name, _logical_type_tag(field.type), bool(field.nullable)) for field in right)


def _scalar_payload(scalar: pa.Scalar, dtype: pa.DataType) -> bytes:
    """Encode one Arrow scalar with explicit type and nesting boundaries."""

    type_tag = _logical_type_tag(dtype)
    if not scalar.is_valid:
        return _part(type_tag) + b"N"

    if pa.types.is_dictionary(dtype):
        return (
            _part(type_tag)
            + b"D"
            + _part(_scalar_payload(scalar.cast(dtype.value_type), dtype.value_type))
        )
    # Native Arrow logical types such as ``pa.uuid()`` derive from
    # BaseExtensionType rather than the user-defined ExtensionType class.
    # Preserve the logical extension tag while encoding its storage scalar.
    if isinstance(dtype, pa.BaseExtensionType):
        storage = dtype.storage_type
        storage_scalar = scalar.value
        if not isinstance(storage_scalar, pa.Scalar):
            storage_scalar = pa.scalar(storage_scalar, type=storage)
        return _part(type_tag) + b"X" + _part(_scalar_payload(storage_scalar, storage))
    if pa.types.is_struct(dtype):
        chunks = []
        for index, field in enumerate(dtype):
            chunks.append(_part(field.name.encode("utf-8")))
            chunks.append(_part(_scalar_payload(scalar[index], field.type)))
        return _part(type_tag) + b"S" + b"".join(chunks)
    if (
        pa.types.is_list(dtype)
        or pa.types.is_large_list(dtype)
        or pa.types.is_fixed_size_list(dtype)
    ):
        values = scalar.values
        return (
            _part(type_tag)
            + b"L"
            + b"".join(
                _part(_scalar_payload(values[index], dtype.value_type))
                for index in range(len(values))
            )
        )
    if pa.types.is_map(dtype):
        entries = scalar.values
        chunks = []
        for index in range(len(entries)):
            entry = entries[index]
            chunks.append(_part(_scalar_payload(entry[0], dtype.key_type)))
            chunks.append(_part(_scalar_payload(entry[1], dtype.item_type)))
        return _part(type_tag) + b"M" + b"".join(chunks)
    if pa.types.is_boolean(dtype):
        payload = b"\x01" if scalar.as_py() else b"\x00"
    elif pa.types.is_integer(dtype):
        payload = _integer_payload(int(scalar.as_py()))
    elif pa.types.is_floating(dtype):
        value = float(scalar.as_py())
        if math.isnan(value):
            payload = b"N"
        elif math.isinf(value):
            payload = b"P" if value > 0 else b"M"
        elif pa.types.is_float16(dtype):
            payload = b"F" + struct.pack(">e", value)
        elif pa.types.is_float32(dtype):
            payload = b"F" + struct.pack(">f", value)
        else:
            payload = b"F" + struct.pack(">d", value)
    elif pa.types.is_decimal(dtype):
        value = scalar.as_py()
        assert isinstance(value, Decimal)
        sign, digits, exponent = value.as_tuple()
        payload = (
            (b"1" if sign else b"0") + b":" + bytes(digits) + b":" + str(exponent).encode("ascii")
        )
    elif (
        pa.types.is_binary(dtype)
        or pa.types.is_large_binary(dtype)
        or pa.types.is_fixed_size_binary(dtype)
    ):
        payload = bytes(scalar.as_py())
    elif pa.types.is_string(dtype) or pa.types.is_large_string(dtype):
        payload = str(scalar.as_py()).encode("utf-8")
    elif (
        pa.types.is_timestamp(dtype)
        or pa.types.is_time(dtype)
        or pa.types.is_date(dtype)
        or pa.types.is_duration(dtype)
    ):
        payload = _integer_payload(int(scalar.value))
    elif pa.types.is_null(dtype):
        payload = b""
    else:
        raise TypeError(f"unsupported Arrow type in migration digest: {dtype}")
    return _part(type_tag) + b"V" + _part(payload)


def table_content_digest(table: pa.Table) -> str:
    """Return an order-independent, duplicate-preserving, schema-bound digest.

    Each row is hashed with explicit Arrow type and nested-value boundaries.
    Sorting the fixed-width row digests removes scan-order dependence while
    retaining duplicate multiplicity.  The final hash also binds the logical
    schema fingerprint and row count.
    """

    table = table.combine_chunks()
    logical_schema = schema_fingerprint(table.schema)
    row_digests: list[bytes] = []
    # Iceberg tables with no snapshot can expose zero-chunk columns.  A
    # ChunkedArray's combine operation still returns the correctly typed empty
    # Array, while chunk(0) has no value to return.
    columns = [table.column(index).combine_chunks() for index in range(table.num_columns)]
    for row_index in range(table.num_rows):
        row = hashlib.sha256()
        row.update(_DIGEST_DOMAIN)
        row.update(logical_schema.encode("ascii"))
        for field, column in zip(table.schema, columns, strict=True):
            row.update(_part(field.name.encode("utf-8")))
            row.update(_part(_scalar_payload(column[row_index], field.type)))
        row_digests.append(row.digest())
    row_digests.sort()
    result = hashlib.sha256()
    result.update(_DIGEST_DOMAIN)
    result.update(logical_schema.encode("ascii"))
    result.update(table.num_rows.to_bytes(16, "big", signed=False))
    for digest in row_digests:
        result.update(digest)
    return result.hexdigest()


def table_evidence(
    name: str,
    snapshot_id: int | None,
    table: pa.Table,
) -> TableSnapshotEvidence:
    """Freeze the logical evidence for a materialized snapshot."""

    return TableSnapshotEvidence(
        name=name,
        snapshot_id=snapshot_id,
        schema_ipc_base64=encode_schema(table.schema),
        schema_fingerprint=schema_fingerprint(table.schema),
        row_count=table.num_rows,
        content_digest=table_content_digest(table),
    )


__all__ = [
    "ImportedTableReceipt",
    "TableSnapshotEvidence",
    "encode_schema",
    "logical_arrow_schemas_equal",
    "table_content_digest",
    "table_evidence",
]
