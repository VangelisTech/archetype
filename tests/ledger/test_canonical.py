# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Golden vectors for the A1 canonical identity profile."""

import json
import math
import struct

import pyarrow as pa
import pytest

from archetype.ledger.canonical import (
    CanonicalizationError,
    arrow_schema_descriptor,
    canonical_json,
    component_schema_digest,
    internal_digest,
    jcs_encode,
)


def test_jcs_golden_vector_and_float_boundaries():
    value = {
        "z": None,
        "a": [1, 1.0, 1e-7, 1e-6, 1e20, 1e21, -0.0],
        "text": "é",
    }

    expected = '{"a":[1,1,1e-7,0.000001,100000000000000000000,1e+21,0],"text":"é","z":null}'
    assert jcs_encode(value) == expected.encode()
    assert canonical_json(value) == expected


@pytest.mark.parametrize(
    ("binary64_hex", "expected"),
    [
        ("0000000000000000", "0"),
        ("8000000000000000", "0"),
        ("0000000000000001", "5e-324"),
        ("8000000000000001", "-5e-324"),
        ("7fefffffffffffff", "1.7976931348623157e+308"),
        ("ffefffffffffffff", "-1.7976931348623157e+308"),
        ("4340000000000000", "9007199254740992"),
        ("c340000000000000", "-9007199254740992"),
        ("4430000000000000", "295147905179352830000"),
        ("44b52d02c7e14af5", "9.999999999999997e+22"),
        ("44b52d02c7e14af6", "1e+23"),
        ("44b52d02c7e14af7", "1.0000000000000001e+23"),
        ("444b1ae4d6e2ef4e", "999999999999999700000"),
        ("444b1ae4d6e2ef4f", "999999999999999900000"),
        ("444b1ae4d6e2ef50", "1e+21"),
        ("3eb0c6f7a0b5ed8c", "9.999999999999997e-7"),
        ("3eb0c6f7a0b5ed8d", "0.000001"),
        ("41b3de4355555553", "333333333.3333332"),
        ("41b3de4355555554", "333333333.33333325"),
        ("41b3de4355555555", "333333333.3333333"),
        ("41b3de4355555556", "333333333.3333334"),
        ("41b3de4355555557", "333333333.33333343"),
        ("becbf647612f3696", "-0.0000033333333333333333"),
        ("43143ff3c1cb0959", "1424953923781206.2"),
    ],
)
def test_rfc_8785_appendix_b_number_vectors(binary64_hex, expected):
    value = struct.unpack(">d", bytes.fromhex(binary64_hex))[0]

    assert canonical_json(value) == expected
    assert canonical_json(json.loads(expected)) == expected


def test_internal_digest_golden_vector():
    payload = {"alpha": 1, "nullable": None, "values": [True, "é"]}
    envelope = {
        "profile": "archetype-jcs-v1",
        "kind": "golden-example-v1",
        "payload": payload,
    }

    assert (
        jcs_encode(envelope)
        == (
            '{"kind":"golden-example-v1","payload":{"alpha":1,"nullable":null,'
            '"values":[true,"é"]},"profile":"archetype-jcs-v1"}'
        ).encode()
    )
    assert internal_digest("golden-example-v1", payload) == (
        "sha256:d680d1816a03fbb96510dbe2f1f8fe1f7cc39c86cceb93d9e386fa54ccd76e24"
    )


@pytest.mark.parametrize("value", [math.nan, math.inf, -math.inf])
def test_non_finite_numbers_fail_closed(value):
    with pytest.raises(CanonicalizationError, match="non-finite"):
        jcs_encode({"value": value})


def test_mapping_keys_must_be_strings_and_unknown_values_do_not_use_repr():
    with pytest.raises(CanonicalizationError, match="keys must be strings"):
        jcs_encode({1: "value"})
    with pytest.raises(CanonicalizationError, match="unsupported"):
        jcs_encode({"value": object()})


def test_ieee_754_integer_domain_and_lone_surrogate_fail_closed():
    assert canonical_json(2**53) == "9007199254740992"
    assert canonical_json(2**68) == "295147905179352830000"
    with pytest.raises(CanonicalizationError, match="IEEE-754"):
        jcs_encode({"value": 2**53 + 1})
    with pytest.raises(CanonicalizationError, match="valid Unicode"):
        jcs_encode({"value": "\ud800"})


def test_arrow_schema_descriptor_preserves_order_nullability_and_metadata():
    schema = pa.schema(
        [
            pa.field("b", pa.int64(), nullable=False, metadata={b"z": b"2", b"a": b"1"}),
            pa.field("a", pa.string(), nullable=True),
        ],
        metadata={b"schema": b"v1"},
    )

    descriptor = arrow_schema_descriptor(schema)

    assert [field["name"] for field in descriptor["fields"]] == ["b", "a"]
    assert descriptor["fields"][0]["nullable"] is False
    assert descriptor["fields"][0]["metadata"] == [["YQ", "MQ"], ["eg", "Mg"]]
    assert descriptor["metadata"] == [["c2NoZW1h", "djE"]]


def test_component_schema_digest_is_sensitive_to_order_nullability_and_metadata():
    base = pa.schema([pa.field("a", pa.int64(), nullable=False), pa.field("b", pa.string())])
    reordered = pa.schema([pa.field("b", pa.string()), pa.field("a", pa.int64(), nullable=False)])
    nullable = pa.schema([pa.field("a", pa.int64()), pa.field("b", pa.string())])
    metadata = pa.schema(
        [
            pa.field("a", pa.int64(), nullable=False, metadata={b"unit": b"count"}),
            pa.field("b", pa.string()),
        ]
    )

    digests = {
        component_schema_digest("test:component", schema)
        for schema in (base, reordered, nullable, metadata)
    }

    assert len(digests) == 4
