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
Execution-capable terminals such as ``.collect()``, ``.count_rows()``,
``.show()``, ``.to_arrow()``, ``.to_pylist()``, and ``.write_*()`` execute
the lazy plan. Conversion terminals may also force the frame through Python
memory and stop scaling at in-memory size.

Every such reference inside ``src/`` is a contract exception against
Archetype's lazy execution model. This includes bound callables such as
``await blocking(frame.collect)``. This script enumerates production sites and
gates them against ``lazy_audit.toml``. New, undocumented sites cause a
non-zero exit; stale entries (allowlisted keys that no longer hold a matching
call) are also surfaced so the audit stays honest under refactors.

**Entry keying**

An entry is keyed by ``(path, symbol, expr, method)`` — never by line number.
``symbol`` is the dotted path of the enclosing def/class (``<module>`` at file
scope) and ``expr`` is ``ast.unparse`` of the audited attribute access. An
unrelated edit above the call cannot invalidate an entry, and neither can
reformatting the call, because both inputs are structural. Editing the audited
expression or moving it to another symbol *does* invalidate the entry, which is
the review the audit exists to force.

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

Every other execution-capable terminal, plus ``Series.to_pylist()`` *outside*
batch-UDF scope, still requires an entry.

**Zero-exception ban — core row counting (issue #538)**

``.count_rows()`` under ``src/archetype/core/`` has no diagnostic or
empty-check exception and cannot be allowlisted. Core submits a lazy frame to
its owning terminal append/write boundary; it never runs a count first to
decide whether the work exists, and a receipt whose count the write did not
naturally produce reports ``None``. The checker fails both the call site and
any ``lazy_audit.toml`` entry that tries to admit one.

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

# Daft 0.7.19 DataFrame methods that execute a plan, create an execution-backed
# iterator/dataset, or write a sink. Keep this as the single source of truth;
# tests exercise representative conversion, diagnostic, iterator, and sink
# terminals without carrying a second full inventory.
_MATERIALIZATION_METHODS = frozenset(
    {
        "collect",
        "count_rows",
        "iter_partitions",
        "iter_rows",
        "show",
        "to_arrow",
        "to_arrow_iter",
        "to_dask_dataframe",
        "to_pandas",
        "to_pydict",
        "to_pylist",
        "to_ray_dataset",
        "to_torch_dataloader",
        "to_torch_iter_dataset",
        "to_torch_map_dataset",
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
    }
)


# Maintainer decision (issue #538): core does not count rows to decide
# whether work exists. Each pair is (path prefix, method); a matching site
# fails the audit even with an allowlist entry, and a matching entry is
# itself rejected.
_BANNED_TERMINALS: tuple[tuple[str, str], ...] = (("src/archetype/core/", "count_rows"),)


def is_banned(path: str, method: str) -> bool:
    """True when (path, method) falls under a zero-exception terminal ban."""
    return any(
        path.startswith(prefix) and method == banned_method
        for prefix, banned_method in _BANNED_TERMINALS
    )


MODULE_SCOPE = "<module>"


@dataclass(frozen=True)
class Site:
    path: str
    line: int
    method: str
    snippet: str
    symbol: str = MODULE_SCOPE
    expr: str = ""
    sanctioned: bool = False  # True → udf-boundary, no allowlist entry needed

    @property
    def key(self) -> tuple[str, str, str, str]:
        return (self.path, self.symbol, self.expr, self.method)


@dataclass(frozen=True)
class Entry:
    path: str
    symbol: str
    expr: str
    method: str
    reason: str

    @property
    def key(self) -> tuple[str, str, str, str]:
        return (self.path, self.symbol, self.expr, self.method)


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


def _symbol_scopes(tree: ast.Module) -> dict[int, str]:
    """Map each line to the dotted symbol path of its innermost def/class.

    Lines outside any definition map to ``<module>``. This is what makes an
    allowlist entry survive a line shift: the entry names the enclosing symbol,
    not the offset of the call inside the file.
    """
    scopes: dict[int, str] = {}

    def walk(parent: ast.AST, prefix: str) -> None:
        for child in ast.iter_child_nodes(parent):
            if not isinstance(child, ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef):
                walk(child, prefix)
                continue
            qualified = f"{prefix}.{child.name}" if prefix else child.name
            for line in range(child.lineno, (child.end_lineno or child.lineno) + 1):
                scopes[line] = qualified
            walk(child, qualified)

    walk(tree, "")
    return scopes


def _scan_file(path: Path, rel: str) -> list[Site]:
    # A file we cannot read must not read as "no materialization sites here".
    # Silently returning [] is how an audit reports clean without auditing.
    text = path.read_text(encoding="utf-8")

    tree = ast.parse(text)

    batch_line_params = _collect_batch_udf_param_lines(text)
    symbol_scopes = _symbol_scopes(tree)
    lines = text.splitlines()
    sites: list[Site] = []
    seen: set[tuple[int, str]] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Attribute):
            continue
        method = node.attr
        if method not in _MATERIALIZATION_METHODS:
            continue
        method_line = node.end_lineno or node.lineno
        key = (method_line, method)
        if key in seen:
            continue
        seen.add(key)
        receiver = node.value
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
                snippet=lines[method_line - 1].lstrip(),
                symbol=symbol_scopes.get(node.lineno, MODULE_SCOPE),
                # ast.unparse normalizes formatting, so reflowing the call does
                # not change the key; editing the expression itself does.
                expr=ast.unparse(node),
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
        if "line" in raw:
            return [], (
                f"entry still keyed by line number in {ALLOWLIST_FILENAME}: {raw!r}. "
                "Entries are keyed by (path, symbol, expr, method); rerun "
                "scripts/check_lazy_audit.py --list to regenerate."
            )
        try:
            out.append(
                Entry(
                    path=str(raw["path"]),
                    symbol=str(raw["symbol"]),
                    expr=str(raw["expr"]),
                    method=str(raw["method"]),
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
Execution-capable terminals run the lazy plan. Conversion terminals may also
force the frame through Python memory, defeat query planning, and stop scaling
at in-memory dataset sizes. Every such call is a contract exception against
Archetype's lazy execution model.

If you are reading this, the most likely answer is to rewrite the
expression in Daft. Reach for where, select, with_column, agg, join,
sort, and distinct, or use explain for plan diagnostics. See LEARNINGS.md
and docs/guide/specification.md for the lazy contract.

If the materialization is genuinely unavoidable (storage write boundary,
single-row migration extract, terminal transport conversion), the exception
must be documented in writing and visible in code review:

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
        help="Print every detected execution-capable terminal and exit 0 (no gating).",
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
            print(f"{s.path}:{s.line}  {s.symbol}  .{s.method}(){tag}  {s.expr}")
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

    # Zero-exception terminal bans (issue #538): a banned site fails outright
    # — no allowlist entry can admit it, so it is excluded from the entry
    # matching below (which would otherwise print an entry template for it),
    # and any entry that targets a banned (path, method) is itself rejected.
    banned_sites = [s for s in audited_sites if is_banned(s.path, s.method)]
    banned_entries = [e for e in allow if is_banned(e.path, e.method)]
    gated_sites = [s for s in audited_sites if not is_banned(s.path, s.method)]
    gated_allow = [e for e in allow if not is_banned(e.path, e.method)]

    site_keys = {s.key: s for s in gated_sites}
    allow_keys = {e.key: e for e in gated_allow}

    new_sites = [site_keys[k] for k in site_keys.keys() - allow_keys.keys()]
    stale_entries = [allow_keys[k] for k in allow_keys.keys() - site_keys.keys()]
    weak_reasons = [e for e in gated_allow if not _reason_is_substantive(e.reason)]

    new_sites.sort(key=lambda s: (s.path, s.line))
    stale_entries.sort(key=lambda e: (e.path, e.symbol, e.expr))
    weak_reasons.sort(key=lambda e: (e.path, e.symbol, e.expr))
    banned_sites.sort(key=lambda s: (s.path, s.line))
    banned_entries.sort(key=lambda e: (e.path, e.symbol, e.expr))

    # Always print sanctioned summary for visibility.
    if sanctioned_sites:
        print(
            f"lazy audit: {len(sanctioned_sites)} udf-boundary site(s) exempt "
            f"(sanctioned @daft.method.batch / @daft.func.batch parameter access)."
        )

    if not (banned_sites or banned_entries or new_sites or stale_entries or weak_reasons):
        print(f"lazy audit: {len(audited_sites)} audited site(s), all accounted for.")
        return 0

    print(STERN_HEADER, file=sys.stderr)

    if banned_sites:
        rendered = [f"{s.path}:{s.line}  {s.symbol}  .{s.method}()  {s.expr}" for s in banned_sites]
        sys.stderr.write(
            _format_section(
                "Banned terminals (issue #538 — zero exceptions, not allowlistable):",
                rendered,
            )
        )

    if banned_entries:
        rendered = [
            f"{e.path}  {e.symbol}  .{e.method}()  {e.expr}  reason={e.reason!r}"
            for e in banned_entries
        ]
        sys.stderr.write(
            _format_section(
                "Allowlist entries targeting banned terminals (remove them; the ban has no exceptions):",
                rendered,
            )
        )

    if new_sites:
        rendered = [
            f"{s.path}:{s.line}  {s.symbol}  .{s.method}()  {s.expr}\n"
            f'    [[entries]]\n    path = "{s.path}"\n    symbol = "{s.symbol}"\n'
            f'    expr = "{s.expr}"\n    method = "{s.method}"\n    reason = "..."'
            for s in new_sites
        ]
        sys.stderr.write(_format_section("New, undocumented execution points:", rendered))

    if stale_entries:
        rendered = [
            f"{e.path}  {e.symbol}  .{e.method}()  {e.expr}  reason={e.reason!r}"
            for e in stale_entries
        ]
        sys.stderr.write(
            _format_section(
                "Stale allowlist entries (no matching call for this symbol/expression):",
                rendered,
            )
        )

    if weak_reasons:
        rendered = [
            f"{e.path}  {e.symbol}  .{e.method}()  reason={e.reason!r}" for e in weak_reasons
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
