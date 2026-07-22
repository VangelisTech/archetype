# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Tests for scripts/check_lazy_audit.py — AST scope detection.

Covers the two key policy assertions:

1. **Positive (sanctioned):** ``Series.to_pylist()`` on a parameter of a
   ``@daft.method.batch`` / ``@daft.func.batch`` decorated function is
   detected as *udf-boundary* and does NOT require an allowlist entry.

2. **Negative (gated):** A plain ``df.to_pylist()`` at module scope (or
   outside a batch-UDF) is still flagged as an audited site that requires
   an allowlist entry.
"""

from __future__ import annotations

import sys
import textwrap
from pathlib import Path

import pytest

# Make scripts/ importable without installing it as a package.
_SCRIPTS_DIR = Path(__file__).resolve().parent.parent.parent / "scripts"
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))

import check_lazy_audit as mod  # noqa: E402
from check_lazy_audit import (  # noqa: E402
    _collect_batch_udf_param_lines,
    _scan_file,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _write_py(tmp_path: Path, name: str, source: str) -> Path:
    p = tmp_path / name
    p.write_text(textwrap.dedent(source), encoding="utf-8")
    return p


def _write_toml(tmp_path: Path, entries: list[dict]) -> Path:
    lines: list[str] = [
        "# Lazy-evaluation audit allowlist (test fixture).",
        "#",
        "# Each entry documents an exception to Archetype's lazy execution contract.",
        "",
    ]
    for e in entries:
        lines.append("[[entries]]")
        for k, v in e.items():
            lines.append(f"{k} = {v!r}")
        lines.append("")
    p = tmp_path / "lazy_audit.toml"
    p.write_text("\n".join(lines), encoding="utf-8")
    return p


# ---------------------------------------------------------------------------
# Unit: _collect_batch_udf_param_lines
# ---------------------------------------------------------------------------


def test_method_batch_params_detected():
    """Parameters of @daft.method.batch functions are collected."""
    source = textwrap.dedent("""\
        import daft
        from daft import Series

        @daft.cls()
        class Stepper:
            @daft.method.batch(return_dtype=None)
            def step(self, cart_pos: Series, pole_angle: Series) -> Series:
                cp = cart_pos.to_pylist()
                pa = pole_angle.to_pylist()
                return Series.from_pylist([])
    """)
    mapping = _collect_batch_udf_param_lines(source)
    # Lines 8 and 9 are inside the method body; params exclude "self".
    assert mapping, "should detect at least one body line"
    param_sets = list(mapping.values())
    assert all("cart_pos" in ps for ps in param_sets)
    assert all("pole_angle" in ps for ps in param_sets)
    assert all("self" not in ps for ps in param_sets)


def test_func_batch_params_detected():
    """Parameters of @daft.func.batch functions are collected."""
    source = textwrap.dedent("""\
        import daft
        from daft import Series

        @daft.func.batch(return_dtype=None)
        def process(values: Series) -> Series:
            data = values.to_pylist()
            return Series.from_pylist(data)
    """)
    mapping = _collect_batch_udf_param_lines(source)
    assert mapping
    assert all("values" in ps for ps in mapping.values())


def test_non_batch_function_not_detected():
    """Regular (non-batch) functions do not appear in the mapping."""
    source = textwrap.dedent("""\
        def regular(values):
            return values.to_pylist()
    """)
    mapping = _collect_batch_udf_param_lines(source)
    assert not mapping, "regular functions must not be detected as batch-UDF scope"


# ---------------------------------------------------------------------------
# Integration: _scan_file — sanctioned sites are marked
# ---------------------------------------------------------------------------


def test_batch_udf_series_access_is_sanctioned(tmp_path):
    """Series.to_pylist() on a batch-UDF parameter → sanctioned=True."""
    py = _write_py(
        tmp_path,
        "sanctioned.py",
        """\
        import daft
        from daft import Series

        @daft.cls()
        class Stepper:
            @daft.method.batch(return_dtype=None)
            def step(self, cart_pos: Series, pole_angle: Series) -> Series:
                cp = cart_pos.to_pylist()
                pa = pole_angle.to_pylist()
                return Series.from_pylist([])
        """,
    )
    sites = _scan_file(py, "sanctioned.py")
    assert sites, "should detect .to_pylist() calls"
    for s in sites:
        assert s.sanctioned, (
            f"Line {s.line} should be sanctioned (inside batch-UDF scope), got sanctioned=False"
        )


def test_module_level_to_pylist_is_not_sanctioned(tmp_path):
    """DataFrame.to_pylist() at module scope → sanctioned=False."""
    py = _write_py(
        tmp_path,
        "bad.py",
        """\
        import daft

        df = daft.from_pydict({"x": [1, 2, 3]})
        rows = df.to_pylist()  # plain DataFrame materialization
        """,
    )
    sites = _scan_file(py, "bad.py")
    assert sites, "should detect .to_pylist() call"
    for s in sites:
        assert not s.sanctioned, (
            f"Line {s.line} must NOT be sanctioned (module-level df.to_pylist())"
        )


def test_collect_is_never_sanctioned(tmp_path):
    """Only .to_pylist() on batch-UDF params is sanctioned; .collect() never is."""
    py = _write_py(
        tmp_path,
        "collect_test.py",
        """\
        import daft
        from daft import Series

        @daft.cls()
        class X:
            @daft.method.batch(return_dtype=None)
            def go(self, s: Series) -> Series:
                # .collect() inside a batch-UDF is still not sanctioned
                df2 = daft.from_pydict({"y": [1]})
                df2.collect()
                return s
        """,
    )
    sites = _scan_file(py, "collect_test.py")
    collect_sites = [s for s in sites if s.method == "collect"]
    assert collect_sites, "should detect .collect()"
    for s in collect_sites:
        assert not s.sanctioned, ".collect() must never be sanctioned"


def test_bound_collect_reference_is_gated(tmp_path):
    py = _write_py(
        tmp_path,
        "bound_collect.py",
        """\
        async def execute(df, blocking):
            return await blocking(df.collect)
        """,
    )

    sites = _scan_file(py, "bound_collect.py")

    assert [(site.line, site.method, site.sanctioned) for site in sites] == [(2, "collect", False)]


def test_collect_call_reports_one_attribute_site(tmp_path):
    py = _write_py(tmp_path, "one_collect.py", "result = frame.collect()\n")

    sites = _scan_file(py, "one_collect.py")

    assert [(site.line, site.method) for site in sites] == [(1, "collect")]


def test_non_call_text_is_ignored(tmp_path):
    py = _write_py(
        tmp_path,
        "non_calls.py",
        """\
        message = "documentation mentions frame.collect()"
        value = 1  # no call here: frame.to_pylist()
        """,
    )

    assert _scan_file(py, "non_calls.py") == []


def test_multiline_call_reports_the_method_line(tmp_path):
    py = _write_py(
        tmp_path,
        "multiline.py",
        """\
        result = (
            frame
            .collect()
        )
        """,
    )

    sites = _scan_file(py, "multiline.py")

    assert [(site.line, site.snippet) for site in sites] == [(3, ".collect()")]


def test_syntax_error_fails_the_scan(tmp_path):
    py = _write_py(tmp_path, "broken.py", "result = (frame.collect()")

    with pytest.raises(SyntaxError):
        _scan_file(py, "broken.py")


# ---------------------------------------------------------------------------
# End-to-end: full scan + allowlist gating
# ---------------------------------------------------------------------------


def _make_fake_src(tmp_path: Path) -> Path:
    """Create a minimal fake src/ tree with one sanctioned and one gated site."""
    src = tmp_path / "src" / "mypkg"
    src.mkdir(parents=True)

    # Sanctioned: Series param inside @daft.method.batch
    _write_py(
        src,
        "udf_module.py",
        """\
        import daft
        from daft import Series

        @daft.cls()
        class Worker:
            @daft.method.batch(return_dtype=None)
            def run(self, items: Series) -> Series:
                data = items.to_pylist()
                return Series.from_pylist(data)
        """,
    )

    # Gated: plain DataFrame.to_pylist() — needs an allowlist entry
    _write_py(
        src,
        "boundary_module.py",
        """\
        import daft

        def query_rows():
            df = daft.from_pydict({"x": [1]})
            return df.to_pylist()  # genuine DataFrame materialisation
        """,
    )

    return tmp_path


def test_full_scan_sanctioned_exempt(tmp_path):
    """Sanctioned sites are exempt; the gated site requires an entry."""
    fake_root = _make_fake_src(tmp_path)

    # Patch _project_root inside check_lazy_audit so scan() looks in our fake tree.

    orig_root = mod._project_root

    def patched_root():
        return fake_root

    mod._project_root = patched_root  # type: ignore[method-assign]
    try:
        sites = mod.scan(fake_root)
    finally:
        mod._project_root = orig_root  # type: ignore[method-assign]

    sanctioned = [s for s in sites if s.sanctioned]
    gated = [s for s in sites if not s.sanctioned]

    assert sanctioned, "batch-UDF param access must be classified sanctioned"
    assert gated, "module-level to_pylist must be classified as needing allowlist"


# ---------------------------------------------------------------------------
# Regression matrix: issue #544 — complete, provenance-aware Daft terminals
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("method", "statement"),
    [
        ("count_rows", "df.count_rows()"),
        ("show", "df.show()"),
        ("to_arrow", "df.to_arrow()"),
        ("to_pandas", "df.to_pandas()"),
        ("to_pydict", "df.to_pydict()"),
        ("to_arrow_iter", "df.to_arrow_iter()"),
        ("iter_rows", "df.iter_rows()"),
        ("iter_partitions", "df.iter_partitions()"),
        ("to_torch_dataloader", "df.to_torch_dataloader()"),
        ("to_torch_iter_dataset", "df.to_torch_iter_dataset()"),
        ("to_torch_map_dataset", "df.to_torch_map_dataset()"),
        ("__arrow_c_stream__", "df.__arrow_c_stream__()"),
        ("__iter__", "iter(df)"),
        ("__iter__", "[row for row in df]"),
    ],
)
def test_unlisted_daft_execution_adapters_are_gated(tmp_path, method, statement):
    py = _write_py(
        tmp_path,
        "terminal.py",
        f"""\
        import daft

        df = daft.from_pydict({{"x": [1]}})
        result = {statement}
        """,
    )

    sites = _scan_file(py, "terminal.py")

    assert [(site.method, site.sanctioned) for site in sites] == [(method, False)]


@pytest.mark.parametrize(
    "source",
    [
        """\
        import daft

        df = daft.from_pydict({"x": [1]})
        for row in df:
            consume(row)
        """,
        """\
        import daft

        df = daft.from_pydict({"x": [1]})
        alias = df
        for row in alias:
            consume(row)
        """,
    ],
)
def test_implicit_dataframe_for_iteration_is_gated(tmp_path, source):
    py = _write_py(tmp_path, "iteration.py", source)

    assert [site.method for site in _scan_file(py, "iteration.py")] == ["__iter__"]


@pytest.mark.parametrize("method", ["to_dask_dataframe", "to_ray_dataset"])
def test_locked_daft_execution_adapters_are_gated(tmp_path, method):
    py = _write_py(
        tmp_path,
        "locked_adapter.py",
        f"""\
        import daft

        df = daft.from_pydict({{"x": [1]}})
        df.{method}()
        """,
    )

    assert [site.method for site in _scan_file(py, "locked_adapter.py")] == [method]


def test_attribute_held_dataframe_iteration_is_gated(tmp_path):
    py = _write_py(
        tmp_path,
        "attribute_frame.py",
        """\
        import daft

        class Holder:
            def __init__(self):
                self.frame = daft.from_pydict({"x": [1]})

            def rows(self):
                for row in self.frame:
                    yield row
        """,
    )

    assert [site.method for site in _scan_file(py, "attribute_frame.py")] == ["__iter__"]


@pytest.mark.parametrize(
    "method",
    [
        "write_bigtable",
        "write_clickhouse",
        "write_csv",
        "write_deltalake",
        "write_huggingface",
        "write_iceberg",
        "write_json",
        "write_lance",
        "write_paimon",
        "write_parquet",
        "write_sink",
        "write_sql",
        "write_turbopuffer",
    ],
)
def test_unlisted_daft_eager_writes_are_gated(tmp_path, method):
    py = _write_py(
        tmp_path,
        "write.py",
        f"""\
        import daft

        df = daft.from_pydict({{"x": [1]}})
        df.{method}("destination")
        """,
    )

    sites = _scan_file(py, "write.py")

    assert [site.method for site in sites] == [method]


@pytest.mark.parametrize("method", ["append", "write", "overwrite"])
def test_unlisted_daft_catalog_table_writes_are_gated(tmp_path, method):
    py = _write_py(
        tmp_path,
        "catalog_write.py",
        f"""\
        from daft import DataFrame
        from daft.catalog import Table

        def persist(table: Table, df: DataFrame):
            table.{method}(df)
        """,
    )

    sites = _scan_file(py, "catalog_write.py")

    assert [site.method for site in sites] == [method]


def test_catalog_write_provenance_flows_through_assignment(tmp_path):
    py = _write_py(
        tmp_path,
        "catalog_alias.py",
        """\
        from daft.catalog import Table

        def persist(table: Table, frame):
            destination = table
            destination.append(frame)
        """,
    )

    assert [site.method for site in _scan_file(py, "catalog_alias.py")] == ["append"]


def test_session_write_is_gated_from_annotation(tmp_path):
    py = _write_py(
        tmp_path,
        "session_write.py",
        """\
        from daft import Session

        def persist(session: Session, frame):
            session.write_table("destination", frame)
        """,
    )

    assert [site.method for site in _scan_file(py, "session_write.py")] == ["write_table"]


def test_unlisted_daft_module_write_table_is_gated(tmp_path):
    py = _write_py(
        tmp_path,
        "module_write.py",
        """\
        import daft

        df = daft.from_pydict({"x": [1]})
        daft.write_table(df, "destination")
        """,
    )

    sites = _scan_file(py, "module_write.py")

    assert [site.method for site in sites] == ["write_table"]


def test_imported_daft_write_table_is_gated(tmp_path):
    py = _write_py(
        tmp_path,
        "imported_write.py",
        """\
        import daft
        from daft import write_table

        df = daft.from_pydict({"x": [1]})
        write_table(df, "destination")
        """,
    )

    assert [site.method for site in _scan_file(py, "imported_write.py")] == ["write_table"]


@pytest.mark.parametrize(
    ("owner_import", "owner_type"),
    [
        ("from daft.catalog import Catalog", "Catalog"),
        ("from daft import Session", "Session"),
    ],
)
@pytest.mark.parametrize("method", ["append", "write", "overwrite"])
def test_get_table_result_writes_are_gated(tmp_path, owner_import, owner_type, method):
    py = _write_py(
        tmp_path,
        "get_table_write.py",
        f"""\
        {owner_import}

        def persist(owner: {owner_type}, frame):
            table = owner.get_table("destination")
            table.{method}(frame)
        """,
    )

    assert [site.method for site in _scan_file(py, "get_table_write.py")] == [method]


def test_reassignment_drops_dataframe_provenance(tmp_path):
    py = _write_py(
        tmp_path,
        "reassigned.py",
        """\
        import daft

        value = daft.from_pydict({"x": [1]})
        value = CustomResult()
        value.to_arrow()
        """,
    )

    assert _scan_file(py, "reassigned.py") == []


def test_same_name_provenance_does_not_cross_function_boundaries(tmp_path):
    py = _write_py(
        tmp_path,
        "function_scopes.py",
        """\
        import daft

        def execute_daft():
            result = daft.from_pydict({"x": [1]})
            return result.to_arrow()

        def execute_custom():
            result = CustomResult()
            return result.to_arrow()
        """,
    )

    sites = _scan_file(py, "function_scopes.py")

    assert [(site.line, site.method) for site in sites] == [(5, "to_arrow")]


@pytest.mark.parametrize(
    ("source", "method"),
    [
        (
            "import daft\ndef build() -> daft.DataFrame:\n"
            '    return daft.from_pydict({"x": [1]})\nbuild().count_rows()\n',
            "count_rows",
        ),
        (
            "from daft import DataFrame\ndef emit(frame: DataFrame | None):\n"
            "    if frame is not None:\n        return frame.to_arrow()\n",
            "to_arrow",
        ),
        (
            "import daft\nclass Holder:\n    frame: daft.DataFrame\n"
            "def emit(holder: Holder):\n    return holder.frame.count_rows()\n",
            "count_rows",
        ),
    ],
)
def test_declared_return_union_and_attribute_provenance(tmp_path, source, method):
    py = _write_py(tmp_path, "declared.py", source)

    assert [site.method for site in _scan_file(py, "declared.py")] == [method]


def test_dict_values_comprehension_uses_value_provenance(tmp_path):
    py = _write_py(
        tmp_path,
        "dict_values.py",
        """\
        from daft import DataFrame

        def inspect(frames: dict[str, DataFrame], labels: dict[DataFrame, str]):
            frame_counts = [frame.count_rows() for frame in frames.values()]
            label_counts = [label.count_rows() for label in labels.values()]
            return frame_counts, label_counts
        """,
    )

    assert [(site.line, site.method) for site in _scan_file(py, "dict_values.py")] == [
        (4, "count_rows")
    ]


def test_list_parameter_comprehension_preserves_dataframe_provenance(tmp_path):
    py = _write_py(
        tmp_path,
        "list_parameter.py",
        """\
        from daft import DataFrame

        def inspect(frames: list[DataFrame]):
            return [frame.count_rows() for frame in frames]
        """,
    )

    assert [site.method for site in _scan_file(py, "list_parameter.py")] == ["count_rows"]


def test_list_attribute_comprehension_preserves_dataframe_provenance(tmp_path):
    py = _write_py(
        tmp_path,
        "list_attribute.py",
        """\
        from daft import DataFrame

        class Holder:
            frames: list[DataFrame]

        def inspect(holder: Holder):
            return [frame.count_rows() for frame in holder.frames]
        """,
    )

    assert [site.method for site in _scan_file(py, "list_attribute.py")] == ["count_rows"]


def test_dataframe_iteration_rows_do_not_inherit_terminal_authority(tmp_path):
    py = _write_py(
        tmp_path,
        "dataframe_rows.py",
        """\
        from daft import DataFrame

        def inspect(frame: DataFrame):
            return [row.count_rows() for row in frame]
        """,
    )

    assert [site.method for site in _scan_file(py, "dataframe_rows.py")] == ["__iter__"]


def test_foreign_sequence_and_mapping_elements_are_not_dataframe_provenance(tmp_path):
    py = _write_py(
        tmp_path,
        "foreign_collections.py",
        """\
        from collections.abc import Mapping, Sequence

        class Custom:
            def count_rows(self): ...

        def inspect(items: Sequence[Custom], indexed: Mapping[str, Custom]):
            sequence_counts = [item.count_rows() for item in items]
            mapping_counts = [item.count_rows() for item in indexed.values()]
            return sequence_counts, mapping_counts
        """,
    )

    assert _scan_file(py, "foreign_collections.py") == []


@pytest.mark.parametrize(
    ("source", "expected"),
    [
        (
            "from daft import DataFrame\n"
            "class StorageService:\n"
            "    async def materialize(self, frame: DataFrame) -> DataFrame: ...\n"
            "async def load(storage: StorageService, frame: DataFrame):\n"
            "    materialized = await storage.materialize(frame)\n"
            "    return materialized.to_pydict()\n",
            [(6, "to_pydict", "return materialized.to_pydict()")],
        ),
        (
            "from daft import DataFrame\n"
            "class StorageService:\n"
            "    async def materialize(self, frame: DataFrame) -> DataFrame: ...\n"
            "async def load(storage: StorageService, frame: DataFrame):\n"
            "    return (await storage.materialize(frame)).to_pydict()\n",
            [(5, "to_pydict", "return (await storage.materialize(frame)).to_pydict()")],
        ),
        (
            "from collections.abc import Callable\n"
            "from typing import Any\n"
            "from daft import DataFrame\n"
            "class Store:\n"
            "    async def _blocking(self, call: Callable[..., Any], *args: Any) -> Any: ...\n"
            "    async def persist(self, frame: DataFrame):\n"
            "        frozen = await self._blocking(frame.collect)\n"
            "        count = frozen.count_rows()\n"
            "        await self._blocking(frozen.write_iceberg, 'table')\n",
            [
                (7, "collect", "frozen = await self._blocking(frame.collect)"),
                (8, "count_rows", "count = frozen.count_rows()"),
                (9, "write_iceberg", "await self._blocking(frozen.write_iceberg, 'table')"),
            ],
        ),
        (
            "from collections.abc import Callable\n"
            "from typing import Any\n"
            "from daft import DataFrame\n"
            "class Store:\n"
            "    async def _blocking(self, call: Callable[..., Any]) -> Any: ...\n"
            "    async def inspect(self, frame: DataFrame):\n"
            "        return await self._blocking(frame.where(True).limit(1).count_rows)\n",
            [
                (
                    7,
                    "count_rows",
                    "return await self._blocking(frame.where(True).limit(1).count_rows)",
                )
            ],
        ),
    ],
)
def test_storage_materialization_and_blocking_wrappers_preserve_dataframe_provenance(
    tmp_path, source, expected
):
    py = _write_py(tmp_path, "storage_wrappers.py", source)

    assert [
        (site.line, site.method, site.snippet) for site in _scan_file(py, "storage_wrappers.py")
    ] == expected


@pytest.mark.parametrize(
    ("source", "expected"),
    [
        (
            "from typing import Protocol\n"
            "from daft import DataFrame\n"
            "class FrameSource(Protocol):\n"
            "    def get_frame(self) -> DataFrame: ...\n"
            "def inspect(source: FrameSource):\n"
            "    frame = source.get_frame()\n"
            "    frame.count_rows()\n"
            "    frame.show()\n",
            [(7, "count_rows", "frame.count_rows()"), (8, "show", "frame.show()")],
        ),
        (
            "from typing import Protocol\n"
            "from daft import DataFrame\n"
            "class Processor(Protocol):\n"
            "    async def process(self, frame: DataFrame) -> DataFrame: ...\n"
            "async def execute(processor: Processor, frame: DataFrame):\n"
            "    frame = await processor.process(frame)\n"
            "    return frame.count_rows()\n",
            [(7, "count_rows", "return frame.count_rows()")],
        ),
    ],
)
def test_typed_protocol_and_processor_results_preserve_dataframe_provenance(
    tmp_path, source, expected
):
    py = _write_py(tmp_path, "typed_results.py", source)

    assert [
        (site.line, site.method, site.snippet) for site in _scan_file(py, "typed_results.py")
    ] == expected


def test_local_foreign_annotation_suppresses_legacy_spelling(tmp_path):
    py = _write_py(
        tmp_path,
        "foreign_annotation.py",
        "import daft\nclass Custom:\n    def collect(self): ...\n"
        "def actual(frame: daft.DataFrame):\n    return frame.count_rows()\n"
        "def foreign(frame: Custom):\n    return frame.collect()\n",
    )

    assert [site.method for site in _scan_file(py, "foreign_annotation.py")] == ["count_rows"]


def test_imported_custom_collect_is_not_gated(tmp_path):
    py = _write_py(
        tmp_path,
        "custom_collect.py",
        """\
        from custom_executor import collect

        collect([1, 2, 3])
        """,
    )

    assert _scan_file(py, "custom_collect.py") == []


@pytest.mark.parametrize(
    "source",
    [
        "import lancedb\nquery = lancedb.connect('db').open_table('t').search()\nquery.to_arrow()\n",
        "import pyarrow as pa\ntable = pa.table({'x': [1]})\ntable.to_pydict()\n",
        "items = []\nitems.append(1)\n",
        "class Custom:\n    def collect(self): ...\n    def to_pylist(self): ...\nobj = Custom()\nobj.collect()\nobj.to_pylist()\n",
        "class Writer:\n    def append(self, value): ...\n    def write(self, value): ...\n    def overwrite(self, value): ...\nwriter = Writer()\nwriter.append(1)\nwriter.write(1)\nwriter.overwrite(1)\n",
    ],
)
def test_foreign_overlapping_method_names_are_not_gated(tmp_path, source):
    py = _write_py(tmp_path, "foreign.py", source)

    assert _scan_file(py, "foreign.py") == []


def test_daft_arrow_export_is_gated_but_followup_pyarrow_conversion_is_not(tmp_path):
    py = _write_py(
        tmp_path,
        "arrow_export.py",
        "import daft\narrow = daft.from_pydict({'x': [1]}).to_arrow()\narrow.to_pylist()\n",
    )

    assert [site.method for site in _scan_file(py, "arrow_export.py")] == ["to_arrow"]


def test_real_lancedb_store_only_audits_daft_to_arrow():
    relative = "src/archetype/core/aio/async_lancedb_store.py"
    path = Path(__file__).resolve().parent.parent.parent / relative

    sites = _scan_file(path, relative)

    assert [(site.line, site.method) for site in sites] == [(359, "to_arrow")]
    assert not {201, 288, 333} & {site.line for site in sites}


def test_real_repository_typed_dataframe_terminals_are_complete():
    root = Path(__file__).resolve().parent.parent.parent
    expected = {
        ("src/archetype/app/artifacts/service.py", 103, "to_pydict"),
        ("src/archetype/app/artifacts/service.py", 119, "to_pydict"),
        ("src/archetype/app/evaluation/service.py", 404, "to_pydict"),
        ("src/archetype/app/storage/service.py", 468, "count_rows"),
        ("src/archetype/app/storage/service.py", 471, "write_iceberg"),
        ("src/archetype/app/storage/service.py", 520, "count_rows"),
        ("src/archetype/app/storage/service.py", 525, "count_rows"),
        ("src/archetype/app/storage/service.py", 534, "count_rows"),
        ("src/archetype/app/storage/service.py", 547, "count_rows"),
        ("src/archetype/app/storage/service.py", 556, "write_iceberg"),
        ("src/archetype/core/aio/async_system.py", 106, "count_rows"),
        ("src/archetype/core/aio/async_world.py", 994, "count_rows"),
        ("src/archetype/core/sync/querier.py", 79, "count_rows"),
        ("src/archetype/core/sync/querier.py", 81, "show"),
    }
    scanned = {
        (site.path, site.line, site.method)
        for relative in sorted({path for path, _, _ in expected})
        for site in _scan_file(root / relative, relative)
    }

    assert expected <= scanned, f"missing real Daft terminals: {sorted(expected - scanned)}"


def test_batch_udf_series_to_pylist_remains_executor_owned(tmp_path):
    py = _write_py(
        tmp_path,
        "batch.py",
        """\
        import daft

        @daft.func.batch(return_dtype=None)
        def convert(values: daft.Series) -> daft.Series:
            return daft.Series.from_pylist(values.to_pylist())
        """,
    )

    sites = _scan_file(py, "batch.py")

    assert [(site.method, site.sanctioned) for site in sites] == [("to_pylist", True)]


def test_locked_daft_version_drift_fails_closed(tmp_path, monkeypatch, capsys):
    (tmp_path / "src").mkdir()
    _write_toml(tmp_path, [])
    (tmp_path / "uv.lock").write_text(
        'version = 1\nrevision = 3\n\n[[package]]\nname = "daft"\nversion = "0.8.0"\n',
        encoding="utf-8",
    )
    monkeypatch.setattr(mod, "_project_root", lambda: tmp_path)
    monkeypatch.setattr(sys, "argv", ["check_lazy_audit.py"])

    assert mod.main() != 0
    output = capsys.readouterr()
    assert "version" in (output.out + output.err).lower()
    assert "0.7.19" in output.out + output.err


@pytest.mark.parametrize(
    "root_name",
    ["src", "tests", "bench", "evals", "examples", "experiments", "scripts", "quality"],
)
def test_scan_covers_every_checked_in_python_root(tmp_path, root_name):
    target = tmp_path / root_name
    target.mkdir()
    _write_py(target, "terminal.py", "frame.collect()\n")

    sites = mod.scan(tmp_path)

    assert [(site.path, site.method) for site in sites] == [(f"{root_name}/terminal.py", "collect")]


def test_failure_guidance_does_not_recommend_execution_for_diagnostics():
    guidance = mod.STERN_BODY.lower()

    assert "count_rows" not in guidance
    assert "debug logging" not in guidance


def test_legacy_disposition_requires_numbered_removal_issue():
    complete = mod.Entry(
        path="src/probe.py",
        line=1,
        method="count_rows",
        boundary_kind="legacy",
        owner="core",
        rationale="Temporary execution boundary retained only while its focused repair is pending.",
        removal_issue="#538",
    )
    missing_issue = mod.Entry(
        path=complete.path,
        line=complete.line,
        method=complete.method,
        boundary_kind=complete.boundary_kind,
        owner=complete.owner,
        rationale=complete.rationale,
        removal_issue=None,
    )

    assert mod._entry_is_complete(complete)
    assert not mod._entry_is_complete(missing_issue)


def test_full_scan_with_allowlist_passes(tmp_path):
    """When the gated site has an allowlist entry the audit exits 0."""
    fake_root = _make_fake_src(tmp_path)

    # Write an allowlist that covers the gated boundary_module.py site.
    # The fake scan will find it at line 5.
    _write_toml(
        fake_root,
        [
            {
                "path": "src/mypkg/boundary_module.py",
                "line": 5,
                "method": "to_pylist",
                "boundary_kind": "query-result",
                "owner": "mypkg query adapter",
                "rationale": "Terminal query result returned to caller; cannot remain lazy past function boundary.",
            }
        ],
    )

    import check_lazy_audit as mod

    orig_root = mod._project_root

    def patched_root():
        return fake_root

    mod._project_root = patched_root  # type: ignore[method-assign]
    try:
        sites = mod.scan(fake_root)
        allow, err = mod.load_allowlist(fake_root)

        assert err is None
        audited = [s for s in sites if not s.sanctioned]
        site_keys = {(s.path, s.line, s.method) for s in audited}
        allow_keys = {(e.path, e.line, e.method) for e in allow}
        new_sites = site_keys - allow_keys
        stale = allow_keys - site_keys
        assert not new_sites, f"unexpected new sites: {new_sites}"
        assert not stale, f"unexpected stale entries: {stale}"
    finally:
        mod._project_root = orig_root  # type: ignore[method-assign]


def test_missing_allowlist_entry_fails(tmp_path):
    """A gated site with no allowlist entry is detected (new_sites non-empty)."""
    fake_root = _make_fake_src(tmp_path)
    # Write an *empty* allowlist — the gated site has no entry.
    _write_toml(fake_root, [])

    import check_lazy_audit as mod

    orig_root = mod._project_root

    def patched_root():
        return fake_root

    mod._project_root = patched_root  # type: ignore[method-assign]
    try:
        sites = mod.scan(fake_root)
        allow, err = mod.load_allowlist(fake_root)

        assert err is None
        audited = [s for s in sites if not s.sanctioned]
        site_keys = {(s.path, s.line, s.method) for s in audited}
        allow_keys = {(e.path, e.line, e.method) for e in allow}
        new_sites = site_keys - allow_keys
        assert new_sites, "boundary_module.py gated site must be flagged as missing"
    finally:
        mod._project_root = orig_root  # type: ignore[method-assign]
