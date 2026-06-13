# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Focused unit tests for the boundary helper (boundary.py).

Tests three properties declared in the helper's docstring:

1. **Input mapping** — ``series_to_rows`` correctly zips col_names with Series
   values into row dicts, matching positional order.

2. **Struct unpack** — ``unpack_struct`` applies ``output_map`` field-to-column
   assignments and excludes the scratch column.

3. **Frozen-row passthrough** — the helpers themselves are neutral: they do not
   perform freezing logic (that is the UDF's responsibility), but the row dict
   produced by ``series_to_rows`` carries the ``done`` flag so callers can
   implement passthrough.

4. **Error handling** — ``series_to_rows`` raises on col_name/series count mismatch.

These are pure-Python unit tests.  No Daft executor, no world, no MuJoCo.
"""

from __future__ import annotations

import pytest
from daft import Series

from archetype.experiments.boundary import series_to_rows, unpack_struct

# ---------------------------------------------------------------------------
# series_to_rows
# ---------------------------------------------------------------------------


class TestSeriesToRows:
    def test_single_column(self):
        s = Series.from_pylist([10, 20, 30])
        rows = series_to_rows(["x"], s)
        assert rows == [{"x": 10}, {"x": 20}, {"x": 30}]

    def test_multiple_columns_positional_order(self):
        a = Series.from_pylist([1, 2])
        b = Series.from_pylist([3, 4])
        c = Series.from_pylist([5, 6])
        rows = series_to_rows(["alpha", "beta", "gamma"], a, b, c)
        assert rows == [
            {"alpha": 1, "beta": 3, "gamma": 5},
            {"alpha": 2, "beta": 4, "gamma": 6},
        ]

    def test_done_flag_included_in_row(self):
        """The done column is included verbatim so callers can perform freeze checks."""
        done = Series.from_pylist([False, True, False])
        val = Series.from_pylist([1.0, 2.0, 3.0])
        rows = series_to_rows(["done", "val"], done, val)
        assert rows[0] == {"done": False, "val": 1.0}
        assert rows[1] == {"done": True, "val": 2.0}
        assert rows[2] == {"done": False, "val": 3.0}

    def test_empty_series(self):
        s = Series.from_pylist([])
        rows = series_to_rows(["x"], s)
        assert rows == []

    def test_heterogeneous_types(self):
        ints = Series.from_pylist([1, 2])
        floats = Series.from_pylist([0.5, 1.5])
        strs = Series.from_pylist(["a", "b"])
        rows = series_to_rows(["i", "f", "s"], ints, floats, strs)
        assert rows[0] == {"i": 1, "f": 0.5, "s": "a"}
        assert rows[1] == {"i": 2, "f": 1.5, "s": "b"}

    def test_mismatch_raises_value_error(self):
        s1 = Series.from_pylist([1, 2])
        s2 = Series.from_pylist([3, 4])
        with pytest.raises(ValueError, match="col_names has 1 entries but 2 Series"):
            series_to_rows(["only_one"], s1, s2)

    def test_list_valued_column(self):
        """Series containing list values (e.g. eef_pos) round-trips correctly."""
        vals = Series.from_pylist([[1.0, 2.0], [3.0, 4.0]])
        rows = series_to_rows(["pos"], vals)
        assert rows == [{"pos": [1.0, 2.0]}, {"pos": [3.0, 4.0]}]


# ---------------------------------------------------------------------------
# unpack_struct — tested against a real Daft DataFrame
# ---------------------------------------------------------------------------


class TestUnpackStruct:
    def test_basic_unpack(self):
        import daft
        from daft import DataType, col

        struct_dtype = DataType.struct(
            {
                "x": DataType.float64(),
                "y": DataType.float64(),
            }
        )

        @daft.cls()
        class _MakeStruct:
            def __init__(self):
                pass

            @daft.method.batch(return_dtype=struct_dtype)
            def run(self, a: Series) -> Series:
                vals = a.to_pylist()
                return Series.from_pylist([{"x": float(v), "y": float(v) * 2} for v in vals])

        maker = _MakeStruct()
        df = daft.from_pydict({"a": [1.0, 2.0, 3.0], "b__x": [0.0, 0.0, 0.0]})
        df = df.with_column("_scratch", maker.run(col("a")))
        df = unpack_struct(df, "_scratch", {"x": "b__x", "y": "b__y"})

        rows = df.collect().to_pylist()
        assert "_scratch" not in df.schema().column_names()
        assert rows[0] == {"a": 1.0, "b__x": 1.0, "b__y": 2.0}
        assert rows[1] == {"a": 2.0, "b__x": 2.0, "b__y": 4.0}
        assert rows[2] == {"a": 3.0, "b__x": 3.0, "b__y": 6.0}

    def test_scratch_column_excluded(self):
        import daft
        from daft import DataType, col

        struct_dtype = DataType.struct({"z": DataType.int64()})

        @daft.cls()
        class _Noop:
            def __init__(self):
                pass

            @daft.method.batch(return_dtype=struct_dtype)
            def run(self, a: Series) -> Series:
                return Series.from_pylist([{"z": v} for v in a.to_pylist()])

        noop = _Noop()
        df = daft.from_pydict({"a": [5, 6]})
        df = df.with_column("_tmp", noop.run(col("a")))
        df = unpack_struct(df, "_tmp", {"z": "out__z"})

        assert "_tmp" not in df.schema().column_names()
        assert "out__z" in df.schema().column_names()

    def test_empty_output_map(self):
        """Empty output_map: struct column is added then excluded, leaving df unchanged."""
        import daft
        from daft import DataType, col

        struct_dtype = DataType.struct({"ignore": DataType.int64()})

        @daft.cls()
        class _Ignore:
            def __init__(self):
                pass

            @daft.method.batch(return_dtype=struct_dtype)
            def run(self, a: Series) -> Series:
                return Series.from_pylist([{"ignore": v} for v in a.to_pylist()])

        ig = _Ignore()
        df = daft.from_pydict({"a": [1, 2]})
        df_with = df.with_column("_scratch", ig.run(col("a")))
        df_out = unpack_struct(df_with, "_scratch", {})

        assert "_scratch" not in df_out.schema().column_names()
        assert "a" in df_out.schema().column_names()


# ---------------------------------------------------------------------------
# Frozen-row passthrough — caller responsibility documented via example
# ---------------------------------------------------------------------------


class TestFrozenRowPassthrough:
    """Demonstrate that callers use series_to_rows to drive freeze logic.

    The helpers do not enforce freezing themselves — that is the UDF's
    responsibility.  These tests document the expected call-site pattern.
    """

    def test_caller_can_implement_passthrough_with_done_flag(self):
        done = Series.from_pylist([False, True, False])
        value = Series.from_pylist([1.0, 99.0, 3.0])
        rows = series_to_rows(["done", "value"], done, value)

        out = []
        for row in rows:
            if row["done"]:
                # Passthrough: keep previous value unchanged.
                out.append({"result": row["value"]})
            else:
                # Live: apply transformation.
                out.append({"result": row["value"] * 2})

        assert out == [{"result": 2.0}, {"result": 99.0}, {"result": 6.0}]

    def test_no_live_rows_produces_all_passthroughs(self):
        done = Series.from_pylist([True, True])
        value = Series.from_pylist([10.0, 20.0])
        rows = series_to_rows(["done", "value"], done, value)

        live = [i for i, row in enumerate(rows) if not row["done"]]
        assert live == [], "all rows done — no live rows"

        # All rows pass through unchanged.
        out = [{"result": row["value"]} for row in rows]
        assert out == [{"result": 10.0}, {"result": 20.0}]
