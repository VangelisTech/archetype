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
    import check_lazy_audit as mod

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
                "qualname": "query_rows",
                "reason": "Terminal query result returned to caller; cannot remain lazy past function boundary.",
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


def test_allowlist_entry_does_not_authorize_replacement_call_at_same_line(tmp_path, monkeypatch):
    """A same-method replacement at the pinned line is a new call site."""
    source = tmp_path / "src" / "mypkg" / "migration.py"
    source.parent.mkdir(parents=True)
    _write_py(
        source.parent,
        source.name,
        """\
        def whole_table_export(frame):
            return frame.to_pylist()
        """,
    )
    _write_toml(
        tmp_path,
        [
            {
                "path": "src/mypkg/migration.py",
                "line": 2,
                "method": "to_pylist",
                "qualname": "single_row_migration",
                "reason": "Single-row migration extract at the storage boundary.",
            }
        ],
    )

    import check_lazy_audit as mod

    monkeypatch.setattr(mod, "_project_root", lambda: tmp_path)
    monkeypatch.setattr(sys, "argv", ["check_lazy_audit.py"])

    assert mod.main() == 1


def test_allowlist_entry_survives_line_drift(tmp_path, monkeypatch):
    source = tmp_path / "src" / "mypkg" / "migration.py"
    source.parent.mkdir(parents=True)
    _write_py(
        source.parent,
        source.name,
        """\
        def single_row_migration(frame):
            # An unrelated edit moves the call without changing its identity.
            return frame.to_pylist()
        """,
    )
    _write_toml(
        tmp_path,
        [
            {
                "path": "src/mypkg/migration.py",
                "line": 2,
                "method": "to_pylist",
                "qualname": "single_row_migration",
                "reason": "Single-row migration extract at the storage boundary.",
            }
        ],
    )

    import check_lazy_audit as mod

    monkeypatch.setattr(mod, "_project_root", lambda: tmp_path)
    monkeypatch.setattr(sys, "argv", ["check_lazy_audit.py"])

    assert mod.main() == 0


def test_ambiguous_qualname_identity_fails_closed(tmp_path, monkeypatch):
    source = tmp_path / "src" / "mypkg" / "migration.py"
    source.parent.mkdir(parents=True)
    _write_py(
        source.parent,
        source.name,
        """\
        def migration(frame, other):
            first = frame.to_pylist()
            return first, other.to_pylist()
        """,
    )
    _write_toml(
        tmp_path,
        [
            {
                "path": "src/mypkg/migration.py",
                "line": 2,
                "method": "to_pylist",
                "qualname": "migration",
                "reason": "Single-row migration extract at the storage boundary.",
            }
        ],
    )

    import check_lazy_audit as mod

    monkeypatch.setattr(mod, "_project_root", lambda: tmp_path)
    monkeypatch.setattr(sys, "argv", ["check_lazy_audit.py"])

    assert mod.main() == 1
