# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Pure contracts for deterministic Iceberg table migration evidence."""

from __future__ import annotations

import struct
from datetime import UTC, datetime
from decimal import Decimal

import pyarrow as pa

from archetype.storage.transfer import (
    encode_schema,
    logical_arrow_schemas_equal,
    table_content_digest,
    table_evidence,
)


def _nested_table(rows: list[dict[str, object]]) -> pa.Table:
    schema = pa.schema(
        [
            pa.field("event_id", pa.string()),
            pa.field("amount", pa.decimal128(12, 3)),
            pa.field("observed_at", pa.timestamp("us", tz="UTC")),
            pa.field("payload", pa.binary()),
            pa.field("labels", pa.list_(pa.string())),
            pa.field(
                "detail",
                pa.struct(
                    [
                        pa.field("attempt", pa.int32()),
                        pa.field("note", pa.string()),
                    ]
                ),
            ),
            pa.field("metrics", pa.map_(pa.string(), pa.float64())),
        ]
    )
    return pa.Table.from_pylist(rows, schema=schema)


def _rows() -> list[dict[str, object]]:
    repeated = {
        "event_id": "same",
        "amount": Decimal("12.340"),
        "observed_at": datetime(2026, 8, 2, 12, 30, tzinfo=UTC),
        "payload": b"\x00\xff",
        "labels": ["alpha", None, "omega"],
        "detail": {"attempt": 2, "note": None},
        "metrics": [("latency", 1.25), ("cost", float("nan"))],
    }
    other = {
        "event_id": "other",
        "amount": Decimal("-0.125"),
        "observed_at": None,
        "payload": b"",
        "labels": [],
        "detail": None,
        "metrics": [],
    }
    return [repeated, other, repeated]


def test_table_digest_is_order_independent_and_preserves_duplicate_multiplicity() -> None:
    rows = _rows()
    forward = _nested_table(rows)
    reordered = _nested_table([rows[2], rows[0], rows[1]])
    one_duplicate_removed = _nested_table(rows[:2])

    assert table_content_digest(forward) == table_content_digest(reordered)
    assert table_content_digest(forward) != table_content_digest(one_duplicate_removed)


def test_table_digest_is_schema_bound() -> None:
    strings = pa.table({"value": pa.array(["1", "2"], type=pa.string())})
    binaries = pa.table({"value": pa.array([b"1", b"2"], type=pa.binary())})
    renamed = pa.table({"renamed": pa.array(["1", "2"], type=pa.string())})

    assert table_content_digest(strings) != table_content_digest(binaries)
    assert table_content_digest(strings) != table_content_digest(renamed)


def test_table_digest_distinguishes_signed_integers_and_finite_float_bit_patterns() -> None:
    negative_one = pa.table({"value": pa.array([-1], type=pa.int64())})
    positive_255 = pa.table({"value": pa.array([255], type=pa.int64())})
    positive_infinity = pa.table({"value": pa.array([float("inf")], type=pa.float32())})
    finite_inf_marker_bits = pa.table(
        {"value": pa.array([struct.unpack(">f", b"+inf")[0]], type=pa.float32())}
    )

    assert table_content_digest(negative_one) != table_content_digest(positive_255)
    assert table_content_digest(positive_infinity) != table_content_digest(finite_inf_marker_bits)


def test_table_digest_normalizes_iceberg_string_and_binary_physical_widths() -> None:
    narrow = pa.table(
        {
            "text": pa.array(["value"], type=pa.string()),
            "blob": pa.array([b"value"], type=pa.binary()),
        }
    )
    large = pa.table(
        {
            "text": pa.array(["value"], type=pa.large_string()),
            "blob": pa.array([b"value"], type=pa.large_binary()),
        }
    )

    assert table_content_digest(narrow) == table_content_digest(large)
    assert logical_arrow_schemas_equal(narrow.schema, large.schema)


def test_logical_schema_comparison_binds_order_type_and_nullability_not_metadata() -> None:
    expected = pa.schema(
        [
            pa.field("first", pa.int64(), nullable=False, metadata={b"field": b"source"}),
            pa.field("second", pa.string(), nullable=True),
        ],
        metadata={b"schema": b"source"},
    )
    metadata_only = pa.schema(
        [
            pa.field("first", pa.int64(), nullable=False, metadata={b"field": b"destination"}),
            pa.field("second", pa.string(), nullable=True),
        ],
        metadata={b"schema": b"destination"},
    )
    reordered = pa.schema([expected.field("second"), expected.field("first")])
    changed_type = pa.schema(
        [
            pa.field("first", pa.int32(), nullable=False),
            pa.field("second", pa.string(), nullable=True),
        ]
    )
    changed_nullability = pa.schema(
        [
            pa.field("first", pa.int64(), nullable=True),
            pa.field("second", pa.string(), nullable=True),
        ]
    )

    assert logical_arrow_schemas_equal(expected, metadata_only)
    assert not logical_arrow_schemas_equal(expected, reordered)
    assert not logical_arrow_schemas_equal(expected, changed_type)
    assert not logical_arrow_schemas_equal(expected, changed_nullability)


def test_table_digest_supports_native_arrow_uuid_and_binds_its_logical_type() -> None:
    values = [b"0123456789abcdef", b"fedcba9876543210"]
    uuids = pa.table({"value": pa.array(values, type=pa.uuid())})
    fixed_binary = pa.table({"value": pa.array(values, type=pa.binary(16))})

    assert len(table_content_digest(uuids)) == 64
    assert table_content_digest(uuids) != table_content_digest(fixed_binary)


def test_table_digest_is_independent_of_arrow_chunking() -> None:
    single = pa.table({"value": pa.array([1, 2, 3, 2], type=pa.int64())})
    chunked = pa.Table.from_arrays(
        [
            pa.chunked_array(
                [
                    pa.array([1], type=pa.int64()),
                    pa.array([2, 3, 2], type=pa.int64()),
                ]
            )
        ],
        names=["value"],
    )

    assert table_content_digest(single) == table_content_digest(chunked)


def test_table_evidence_round_trips_exact_arrow_schema() -> None:
    table = _nested_table(_rows())

    evidence = table_evidence("nested_events", 123, table)

    assert evidence.arrow_schema == table.schema
    assert encode_schema(evidence.arrow_schema) == evidence.schema_ipc_base64
    assert evidence.snapshot_id == 123
    assert evidence.row_count == 3
    assert len(evidence.schema_fingerprint) == 64
    assert len(evidence.content_digest) == 64
