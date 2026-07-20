# Copyright 2025 Vangelis Technologies Inc.
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

"""Lazy-evaluation audit.

Daft is lazily evaluated. DataFrames represent computations, not results.
Calling ``.collect()`` or ``.to_pylist()`` forces the frame through Python
memory, defeats query planning, and stops scaling at in-memory size.

Every such call inside ``src/`` is a contract exception against Archetype's
lazy execution model. This script enumerates production call sites and
gates them against ``lazy_audit.toml``. Entries identify calls by path,
enclosing function qualname, and method; line numbers are diagnostic hints.
New, undocumented sites and stale entries both cause a non-zero exit.

**UDF-boundary exemption (sanctioned pattern)**

``Series.to_pylist()`` called on a *parameter* of a function decorated with
``@daft.method.batch`` or ``@daft.func.batch`` is the sanctioned escape hatch
at the executor boundary.  When Daft invokes such a function the batch is
already materialised — the executor hands each parameter as a concrete
``Series``.  Converting those parameters to Python lists is therefore not a
premature materialisation; it is the expected interface.

The checker detects this pattern via AST analysis:

- Parse each file into an AST.
- Walk all function definitions (``FunctionDef`` / ``AsyncFunctionDef``).
- If the function has a decorator that resolves to ``daft.method.batch`` or
  ``daft.func.batch`` (direct attribute access or aliased import — the common
  forms are handled; pathological aliasing is not), collect the parameter
  names as the *batch-scope parameter set*.
- A ``.to_pylist()`` call whose receiver is one of those parameters is
  classified as **udf-boundary (sanctioned)** and reported separately.  It
  does not need an entry in ``lazy_audit.toml``.

``DataFrame.collect()`` and ``DataFrame.to_pylist()`` anywhere, and
``Series.to_pylist()`` *outside* batch-UDF scope, still require entries.

Tests are intentionally out of scope: terminal materialization at the
assertion boundary is expected. The contract being audited is the
production execution model, not test ergonomics.

Run via ``make lazy-audit`` or as a pre-commit hook.
"""

from __future__ import annotations

import argparse
import ast
import sys
import tomllib
from dataclasses import dataclass
from pathlib import Path

ROOTS: tuple[str, ...] = ("src",)
ALLOWLIST_FILENAME = "lazy_audit.toml"
SELF_RELATIVE = "scripts/check_lazy_audit.py"

_MATERIALIZATION_METHODS = frozenset({"collect", "to_pylist"})


@dataclass(frozen=True)
class Site:
    path: str
    line: int
    method: str
    qualname: str
    snippet: str
    sanctioned: bool = False  # True → udf-boundary, no allowlist entry needed


@dataclass(frozen=True)
class Entry:
    path: str
    line: int
    method: str
    qualname: str
    reason: str


def _project_root() -> Path:
    return Path(__file__).resolve().parent.parent


# ---------------------------------------------------------------------------
# AST-based batch-UDF scope detection
# ---------------------------------------------------------------------------

_BATCH_DECORATOR_ATTRS = frozenset(
    {
        # @daft.method.batch  →  Attribute(Attribute(Name("daft"), "method"), "batch")
        ("daft", "method", "batch"),
        # @daft.func.batch  →  Attribute(Attribute(Name("daft"), "func"), "batch")
        ("daft", "func", "batch"),
    }
)


def _decorator_attr_chain(node: ast.expr) -> tuple[str, ...]:
    """Return the dotted attribute chain for a decorator node, innermost last.

    Handles plain ``Name``, ``Attribute``, and ``Call`` (e.g. ``@daft.cls()``).
    Returns an empty tuple for anything too complex to classify.
    """
    if isinstance(node, ast.Call):
        node = node.func
    parts: list[str] = []
    while isinstance(node, ast.Attribute):
        parts.append(node.attr)
        node = node.value
    if isinstance(node, ast.Name):
        parts.append(node.id)
    parts.reverse()
    return tuple(parts)


def _is_batch_decorator(node: ast.expr) -> bool:
    chain = _decorator_attr_chain(node)
    return chain in _BATCH_DECORATOR_ATTRS


def _collect_batch_udf_param_lines(source: str) -> dict[int, frozenset[str]]:
    """Return a mapping of {line_number: frozenset_of_param_names} for every
    line inside a batch-UDF function body.

    ``line_number`` ranges over every line from the first line of the function
    body up to and including the last line, so callers can do a simple
    ``line in line_to_params`` check.
    """
    tree = ast.parse(source)

    line_to_params: dict[int, frozenset[str]] = {}

    for node in ast.walk(tree):
        if not isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
            continue
        if not any(_is_batch_decorator(d) for d in node.decorator_list):
            continue

        # Collect parameter names (skip 'self' — it is not a Series param).
        params: frozenset[str] = frozenset(a.arg for a in node.args.args if a.arg != "self")
        if not params:
            continue

        # Determine body line range.
        body_lines = [
            lineno
            for child in ast.walk(node)
            for lineno in ([child.lineno] if hasattr(child, "lineno") else [])
        ]
        if not body_lines:
            continue
        first_body = min(body_lines)
        last_body = max(body_lines)
        for ln in range(first_body, last_body + 1):
            line_to_params[ln] = params

    return line_to_params


class _MaterializationVisitor(ast.NodeVisitor):
    """Find calls while retaining their stable enclosing Python qualname."""

    def __init__(self) -> None:
        self.scope: list[tuple[str, bool]] = []
        self.calls: list[tuple[ast.Call, str]] = []

    def _visit_scope(self, node: ast.ClassDef | ast.FunctionDef | ast.AsyncFunctionDef) -> None:
        is_function = isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef)
        self.scope.append((node.name, is_function))
        self.generic_visit(node)
        self.scope.pop()

    visit_ClassDef = _visit_scope
    visit_FunctionDef = _visit_scope
    visit_AsyncFunctionDef = _visit_scope

    def visit_Call(self, node: ast.Call) -> None:
        if isinstance(node.func, ast.Attribute) and node.func.attr in _MATERIALIZATION_METHODS:
            parts: list[str] = []
            for index, (name, is_function) in enumerate(self.scope):
                parts.append(name)
                if is_function and index < len(self.scope) - 1:
                    parts.append("<locals>")
            self.calls.append((node, ".".join(parts) or "<module>"))
        self.generic_visit(node)


def _scan_file(path: Path, rel: str) -> list[Site]:
    try:
        text = path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError):
        return []

    tree = ast.parse(text)

    batch_line_params = _collect_batch_udf_param_lines(text)
    lines = text.splitlines()
    sites: list[Site] = []
    visitor = _MaterializationVisitor()
    visitor.visit(tree)
    for node, qualname in visitor.calls:
        method = node.func.attr
        method_line = node.func.end_lineno or node.lineno
        receiver = node.func.value
        sanctioned = (
            method == "to_pylist"
            and method_line in batch_line_params
            and isinstance(receiver, ast.Name)
            and receiver.id in batch_line_params[method_line]
        )
        sites.append(
            Site(
                path=rel,
                line=method_line,
                method=method,
                qualname=qualname,
                snippet=lines[method_line - 1].lstrip(),
                sanctioned=sanctioned,
            )
        )
    return sorted(sites, key=lambda site: (site.line, site.method))


def scan(root: Path) -> list[Site]:
    sites: list[Site] = []
    for top in ROOTS:
        base = root / top
        if not base.exists():
            continue
        for path in sorted(base.rglob("*.py")):
            rel = path.relative_to(root).as_posix()
            if rel == SELF_RELATIVE:
                continue
            sites.extend(_scan_file(path, rel))
    return sites


def load_allowlist(root: Path) -> tuple[list[Entry], str | None]:
    path = root / ALLOWLIST_FILENAME
    if not path.exists():
        return [], f"missing {ALLOWLIST_FILENAME}"
    try:
        data = tomllib.loads(path.read_text(encoding="utf-8"))
    except tomllib.TOMLDecodeError as exc:
        return [], f"could not parse {ALLOWLIST_FILENAME}: {exc}"
    raw_entries = data.get("entries", [])
    out: list[Entry] = []
    for raw in raw_entries:
        try:
            out.append(
                Entry(
                    path=str(raw["path"]),
                    line=int(raw["line"]),
                    method=str(raw["method"]),
                    qualname=str(raw["qualname"]),
                    reason=str(raw.get("reason", "")).strip(),
                )
            )
        except (KeyError, TypeError, ValueError) as exc:
            return [], f"malformed entry in {ALLOWLIST_FILENAME}: {raw!r} ({exc})"
    return out, None


_BANNED_REASON_TOKENS = {
    "",
    "todo",
    "fixme",
    "needed",
    "necessary",
    "required",
    "for the test",
    "for tests",
    "easier",
    "convenience",
    "tbd",
}


def _reason_is_substantive(reason: str) -> bool:
    norm = reason.strip().lower().rstrip(".")
    if norm in _BANNED_REASON_TOKENS:
        return False
    return len(norm) >= 20


STERN_HEADER = (
    "─────────────────────────────────────────────────────────────────────\n"
    "LAZY EXECUTION AUDIT FAILED\n"
    "─────────────────────────────────────────────────────────────────────"
)

STERN_BODY = """\
Daft is lazily evaluated. DataFrames represent computations, not results.
.collect() and .to_pylist() force the frame through Python memory, defeat
query planning, and stop scaling at in-memory dataset sizes. Every such
call is a contract exception against Archetype's lazy execution model.

If you are reading this, the most likely answer is to rewrite the
expression in Daft. Reach for where, select, with_column, agg, join,
sort, distinct, count_rows before pulling rows into Python. See
LEARNINGS.md and docs/guide/specification.md for the lazy contract.

If the materialization is genuinely unavoidable (storage write boundary,
single-row migration extract, debug logging, terminal test assertion),
the exception must be documented in writing and visible in code review:

  * The reason field in lazy_audit.toml must state the technical reason
    the boundary cannot be expressed lazily. Generic phrases ("needed",
    "for the test", "convenience") are rejected automatically.
  * The new entry must be called out in the PR description or as a PR
    comment so a human reviewer signs off on the exception explicitly.

**Sanctioned exemption — batch-UDF Series access**

Series.to_pylist() called on a *parameter* of a function decorated with
@daft.method.batch or @daft.func.batch does NOT require an allowlist
entry. Daft has already materialised the batch before calling the
function; converting parameters to Python lists is the expected interface
at the executor boundary. These sites are reported separately as
"udf-boundary (sanctioned)".

Quietly adding entries to silence this check is itself a signal that
the change is gaming the lazy-evaluation contract. PRs that do this
will be reverted at review.
"""


def _format_section(title: str, lines: list[str]) -> str:
    if not lines:
        return ""
    return f"\n{title}\n" + "\n".join(f"  {ln}" for ln in lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--list",
        action="store_true",
        help="Print every detected materialization site and exit 0 (no gating).",
    )
    parser.add_argument(
        "files",
        nargs="*",
        help="Optional file list (used by pre-commit). The full repo is always scanned.",
    )
    args = parser.parse_args()
    del args  # we always scan the full repo so a moved call elsewhere can't slip through

    root = _project_root()
    sites = scan(root)

    if "--list" in sys.argv:
        for s in sites:
            tag = " [udf-boundary]" if s.sanctioned else ""
            print(f"{s.path}:{s.line}  {s.qualname} .{s.method}(){tag}  {s.snippet}")
        return 0

    allow, allow_err = load_allowlist(root)
    if allow_err is not None:
        print(STERN_HEADER, file=sys.stderr)
        print(f"\nallowlist error: {allow_err}\n", file=sys.stderr)
        print(STERN_BODY, file=sys.stderr)
        return 2

    # Sanctioned sites never need an allowlist entry.
    audited_sites = [s for s in sites if not s.sanctioned]
    sanctioned_sites = [s for s in sites if s.sanctioned]

    site_groups: dict[tuple[str, str, str], list[Site]] = {}
    allow_groups: dict[tuple[str, str, str], list[Entry]] = {}
    for site in audited_sites:
        site_groups.setdefault((site.path, site.qualname, site.method), []).append(site)
    for entry in allow:
        allow_groups.setdefault((entry.path, entry.qualname, entry.method), []).append(entry)

    ambiguous_keys = {key for key, values in site_groups.items() if len(values) != 1} | {
        key for key, values in allow_groups.items() if len(values) != 1
    }
    site_keys = {key: values[0] for key, values in site_groups.items() if key not in ambiguous_keys}
    allow_keys = {
        key: values[0] for key, values in allow_groups.items() if key not in ambiguous_keys
    }

    new_sites = [site_keys[k] for k in site_keys.keys() - allow_keys.keys()]
    stale_entries = [allow_keys[k] for k in allow_keys.keys() - site_keys.keys()]
    weak_reasons = [e for e in allow if not _reason_is_substantive(e.reason)]

    new_sites.sort(key=lambda s: (s.path, s.line))
    stale_entries.sort(key=lambda e: (e.path, e.line))
    weak_reasons.sort(key=lambda e: (e.path, e.line))

    # Always print sanctioned summary for visibility.
    if sanctioned_sites:
        print(
            f"lazy audit: {len(sanctioned_sites)} udf-boundary site(s) exempt "
            f"(sanctioned @daft.method.batch / @daft.func.batch parameter access)."
        )

    if not (new_sites or stale_entries or weak_reasons or ambiguous_keys):
        print(f"lazy audit: {len(audited_sites)} audited site(s), all accounted for.")
        return 0

    print(STERN_HEADER, file=sys.stderr)

    if ambiguous_keys:
        rendered = [
            f"{path}  {qualname} .{method}()" for path, qualname, method in sorted(ambiguous_keys)
        ]
        sys.stderr.write(
            _format_section(
                "Ambiguous call-site identities (one qualname/method must identify exactly one call and entry):",
                rendered,
            )
        )

    if new_sites:
        rendered = [
            f"{s.path}:{s.line}  {s.qualname} .{s.method}()  {s.snippet}" for s in new_sites
        ]
        sys.stderr.write(_format_section("New, undocumented materialization points:", rendered))

    if stale_entries:
        rendered = [
            f"{e.path}:{e.line}  {e.qualname} .{e.method}()  reason={e.reason!r}"
            for e in stale_entries
        ]
        sys.stderr.write(
            _format_section(
                "Stale allowlist entries (identity no longer has a matching call):",
                rendered,
            )
        )

    if weak_reasons:
        rendered = [
            f"{e.path}:{e.line}  {e.qualname} .{e.method}()  reason={e.reason!r}"
            for e in weak_reasons
        ]
        sys.stderr.write(
            _format_section(
                "Allowlist entries with unjustified reasons (rejected at review):",
                rendered,
            )
        )

    print(file=sys.stderr)
    print(STERN_BODY, file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
