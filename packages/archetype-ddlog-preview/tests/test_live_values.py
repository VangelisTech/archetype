"""Canonical public cells; the separate native oracle compiles real DDlog."""

import math
import os
import struct
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from archetype_ddlog_preview import Host, ProtocolError
from archetype_ddlog_preview.programs import native_types
from archetype_ddlog_preview.values import float_bits
from archetype_ddlog_preview.wire import Cell


class LiveValueTests(unittest.TestCase):
    def test_invalid_local_cells_fail_before_dispatch(self):
        host = Host.__new__(Host)
        host._pid = os.getpid()
        for value in [float("nan"), float("inf"), -float("inf"), None, 2**63, -(2**63) - 1]:
            with (
                self.subTest(value=value),
                patch.object(
                    host, "request", side_effect=AssertionError("invalid local cell dispatched")
                ),
                self.assertRaises(ValueError),
            ):
                host.admit(
                    {"scope": {"native_world": "never"}},
                    expected_head=None,
                    generation=1,
                    revision=1,
                    key="invalid",
                    changes=[{"op": "insert", "predicate": "seed", "values": [value]}],
                )

    def test_contract_probe_rejects_missing_old_and_future_before_open(self):
        class Scalar:
            def __init__(self, value):
                self.value = value

            def __call__(self):
                return self.value

        class Library:
            arct_ddlog_abi_version = Scalar(1)

            def arct_ddlog_open(self, *_):
                raise AssertionError("Incompatible library reached open")

        for version in [None, 0, 1, 3, 2**32 - 1]:
            with self.subTest(version=version), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                library = Library()
                if version is not None:
                    library.arct_ddlog_contract_version = Scalar(version)
                with (
                    patch("archetype_ddlog_preview.ctypes.CDLL", return_value=library),
                    self.assertRaises(ProtocolError),
                ):
                    Host(
                        library=root / "library",
                        registry_root=root / "registry",
                        build_root=root / "worlds",
                        driver=root / "driver",
                        store_root=root / "storage",
                    )
                self.assertEqual(list(root.iterdir()), [])

    def test_float64_tag_preserves_bits_and_normalizes_local_zero(self):
        values = [
            0.0,
            -0.0,
            1.0,
            math.nextafter(1.0, 2.0),
            float.fromhex("0x1.fffffffffffffp1023"),
            -float.fromhex("0x1.fffffffffffffp1023"),
            float.fromhex("0x0.0000000000001p-1022"),
            -float.fromhex("0x0.0000000000001p-1022"),
        ]
        for value in values:
            with self.subTest(value=value):
                cell = Cell.decode({"float64": float_bits(value)})
                self.assertEqual(cell.kind, "float64")
                self.assertIs(type(cell.value), float)
                self.assertEqual(
                    struct.pack(">d", cell.value).hex(),
                    "0000000000000000" if value == 0.0 else struct.pack(">d", value).hex(),
                )

    def test_noncanonical_or_nonfinite_float64_tags_are_rejected(self):
        for value in [
            "8000000000000000",
            "7ff0000000000000",
            "fff0000000000000",
            "7ff8000000000000",
            "3FF0000000000000",
            "0",
            "00000000000000000",
            1.0,
            True,
            None,
        ]:
            with self.subTest(value=value), self.assertRaises(ValueError):
                Cell.decode({"float64": value})

    def test_bool_tag_is_exact(self):
        for value in [True, False]:
            with self.subTest(value=value):
                self.assertEqual(Cell.decode({"bool": value}), Cell("bool", value))

    def test_bool_tag_rejects_coercion(self):
        for value in [0, 1, "true", None, 1.0]:
            with self.subTest(value=value), self.assertRaises(ValueError):
                Cell.decode({"bool": value})

    def test_public_types_map_to_actual_native_types(self):
        self.assertEqual(
            native_types(("int64", "string", "bool", "float64")),
            ["int", "string", "bool", "double"],
        )
