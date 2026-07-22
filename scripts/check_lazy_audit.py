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
Daft exposes several conversions, iterators, display helpers, and writes that
can execute or commit a lazy plan.

Every such reference in checked-in product and repository-harness Python is a
reviewed execution boundary against
Archetype's lazy execution model. This includes bound callables such as
``await blocking(frame.collect)``. This script enumerates production sites and
gates them against ``lazy_audit.toml``. New, undocumented sites cause a
non-zero exit; stale entries (allowlisted lines that no longer hold a
matching call) are also surfaced so the audit stays honest under refactors.

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

Run via ``make lazy-audit`` or as a pre-commit hook.
"""

from __future__ import annotations

import argparse
import ast
import sys
import tomllib
from collections import Counter
from dataclasses import dataclass
from pathlib import Path

ROOTS: tuple[str, ...] = (
    "src",
    "tests",
    "bench",
    "evals",
    "examples",
    "experiments",
    "scripts",
    "quality",
)
ALLOWLIST_FILENAME = "lazy_audit.toml"
REGISTRY_RELATIVE = "quality/daft_lazy_terminals.toml"
SELF_RELATIVE = "scripts/check_lazy_audit.py"


@dataclass(frozen=True)
class TerminalRegistry:
    daft_version: str
    dataframe_types: frozenset[str]
    dataframe_constructors: frozenset[str]
    dataframe_methods: frozenset[str]
    unproven_methods: frozenset[str]
    module_functions: frozenset[str]
    typed_methods: dict[str, frozenset[str]]


def _load_registry() -> TerminalRegistry:
    path = Path(__file__).resolve().parent.parent / REGISTRY_RELATIVE
    data = tomllib.loads(path.read_text(encoding="utf-8"))
    dataframe = data["dataframe"]
    typed_methods: dict[str, frozenset[str]] = {}
    for family in ("catalog_table", "catalog"):
        section = data[family]
        methods = frozenset(section["methods"])
        for type_name in section["types"]:
            typed_methods[str(type_name)] = methods
    return TerminalRegistry(
        daft_version=str(data["daft_version"]),
        dataframe_types=frozenset(dataframe["types"]),
        dataframe_constructors=frozenset(dataframe["constructors"]),
        dataframe_methods=frozenset(dataframe["methods"]),
        unproven_methods=frozenset(dataframe["unproven_methods"]),
        module_functions=frozenset(data["module"]["functions"]),
        typed_methods=typed_methods,
    )


_REGISTRY = _load_registry()


@dataclass(frozen=True)
class Site:
    path: str
    line: int
    method: str
    snippet: str
    sanctioned: bool = False  # True → udf-boundary, no allowlist entry needed


@dataclass(frozen=True)
class Entry:
    path: str
    line: int
    method: str
    boundary_kind: str
    owner: str
    rationale: str
    removal_issue: str | None


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


def _qualified_name(node: ast.expr, imports: dict[str, str]) -> str | None:
    chain = _decorator_attr_chain(node)
    if not chain:
        return None
    head = imports.get(chain[0], chain[0])
    return ".".join((head, *chain[1:]))


@dataclass
class _Provenance:
    imports: dict[str, str]
    names: dict[str, str]
    foreign: set[str]
    foreign_types: set[str]
    direct_dataframe_names: set[str]

    def receiver_type(self, node: ast.expr) -> str | None:
        if isinstance(node, ast.Name):
            if node.id in self.foreign:
                return "foreign"
            return self.names.get(node.id)
        if isinstance(node, ast.Call):
            called = _qualified_name(node.func, self.imports)
            if called in _REGISTRY.dataframe_constructors:
                return "daft.DataFrame"
            if called in _REGISTRY.typed_methods:
                return called
            if called and (
                called in self.foreign_types or called.split(".", 1)[0] in {"lancedb", "pyarrow"}
            ):
                return "foreign"
            if isinstance(node.func, ast.Attribute):
                if node.func.attr in {"to_arrow", "to_pandas", "to_pydict", "to_pylist"}:
                    return "foreign"
                return self.receiver_type(node.func.value)
        if isinstance(node, ast.Attribute):
            return self.receiver_type(node.value)
        return None


def _discover_provenance(tree: ast.AST) -> _Provenance:
    """Collect bounded module/function import, annotation, and assignment facts."""
    imports: dict[str, str] = {}
    local_types = {node.name for node in ast.walk(tree) if isinstance(node, ast.ClassDef)}
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                imports[alias.asname or alias.name.split(".")[0]] = alias.name
        elif isinstance(node, ast.ImportFrom) and node.module:
            for alias in node.names:
                imports[alias.asname or alias.name] = f"{node.module}.{alias.name}"

    provenance = _Provenance(imports, {}, set(), set(local_types), set())

    def record(
        name: str, annotation: ast.expr | None = None, value: ast.expr | None = None
    ) -> None:
        annotation_name = _qualified_name(annotation, imports) if annotation else None
        if (
            annotation_name in _REGISTRY.dataframe_types
            or annotation_name in _REGISTRY.typed_methods
        ):
            provenance.names[name] = annotation_name
            return
        if annotation_name and (
            annotation_name in provenance.foreign_types
            or annotation_name.split(".", 1)[0] in {"lancedb", "pyarrow"}
        ):
            provenance.foreign.add(name)
            return
        if value is not None:
            inferred = provenance.receiver_type(value)
            if inferred == "foreign":
                provenance.foreign.add(name)
            elif inferred:
                provenance.names[name] = inferred
                called = (
                    _qualified_name(value.func, imports) if isinstance(value, ast.Call) else None
                )
                if called in _REGISTRY.dataframe_constructors:
                    provenance.direct_dataframe_names.add(name)

    # A few passes allow simple aliases without becoming a data-flow engine.
    for _ in range(3):
        for node in ast.walk(tree):
            if isinstance(node, ast.arg):
                record(node.arg, node.annotation)
            elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
                record(node.target.id, node.annotation, node.value)
            elif isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Name):
                        record(target.id, value=node.value)
    return provenance


@dataclass(frozen=True)
class _Candidate:
    node: ast.AST
    receiver: ast.expr | None
    qualified: str | None
    method: str


def _discover_candidates(tree: ast.AST, imports: dict[str, str]) -> list[_Candidate]:
    """Discover and normalize calls, bound methods, and implicit iteration."""
    candidates: list[_Candidate] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Attribute):
            candidates.append(
                _Candidate(node, node.value, _qualified_name(node, imports), node.attr)
            )
        elif (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "iter"
            and node.args
        ):
            candidates.append(_Candidate(node, node.args[0], "implicit", "__iter__"))
        elif isinstance(node, ast.comprehension):
            candidates.append(_Candidate(node.iter, node.iter, "implicit", "__iter__"))
    return candidates


def _candidate_is_terminal(candidate: _Candidate, provenance: _Provenance) -> bool:
    if candidate.qualified in _REGISTRY.module_functions:
        return True
    if candidate.receiver is None:
        return False
    if candidate.qualified == "implicit":
        return (
            isinstance(candidate.receiver, ast.Name)
            and candidate.receiver.id in provenance.direct_dataframe_names
        )
    receiver_type = provenance.receiver_type(candidate.receiver)
    if receiver_type == "foreign":
        return False
    if receiver_type in _REGISTRY.dataframe_types:
        return candidate.method in _REGISTRY.dataframe_methods
    if receiver_type in _REGISTRY.typed_methods:
        return candidate.method in _REGISTRY.typed_methods[receiver_type]
    # Conservatively cover terminal spellings that are sufficiently Daft-specific
    # when bounded provenance is inconclusive. Overloaded conversion and generic
    # write names remain provenance-gated above.
    return candidate.method in _REGISTRY.unproven_methods


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


def _scan_file(path: Path, rel: str) -> list[Site]:
    try:
        text = path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError):
        return []

    tree = ast.parse(text)
    provenance = _discover_provenance(tree)

    batch_line_params = _collect_batch_udf_param_lines(text)
    lines = text.splitlines()
    sites: list[Site] = []
    seen: set[tuple[int, str]] = set()
    for candidate in _discover_candidates(tree, provenance.imports):
        if not _candidate_is_terminal(candidate, provenance):
            continue
        node = candidate.node
        method = candidate.method
        method_line = node.end_lineno or node.lineno
        key = (method_line, method)
        if key in seen:
            continue
        seen.add(key)
        receiver = candidate.receiver
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
                sanctioned=sanctioned,
            )
        )
    return sorted(sites, key=lambda site: (site.line, site.method))


def _locked_daft_version(root: Path) -> str | None:
    lock = root / "uv.lock"
    if not lock.exists():
        return None
    data = tomllib.loads(lock.read_text(encoding="utf-8"))
    for package in data.get("package", []):
        if package.get("name") == "daft":
            return str(package.get("version"))
    return None


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
                    boundary_kind=str(raw["boundary_kind"]).strip(),
                    owner=str(raw["owner"]).strip(),
                    rationale=str(raw["rationale"]).strip(),
                    removal_issue=(
                        str(raw["removal_issue"]).strip() if "removal_issue" in raw else None
                    ),
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


def _entry_is_complete(entry: Entry) -> bool:
    if not entry.boundary_kind or not entry.owner or not _reason_is_substantive(entry.rationale):
        return False
    return entry.boundary_kind != "legacy" or bool(
        entry.removal_issue
        and entry.removal_issue.startswith("#")
        and entry.removal_issue[1:].isdigit()
    )


STERN_HEADER = (
    "─────────────────────────────────────────────────────────────────────\n"
    "LAZY EXECUTION AUDIT FAILED\n"
    "─────────────────────────────────────────────────────────────────────"
)

STERN_BODY = """\
Daft is lazily evaluated. DataFrames represent computations, not results.
Execution adapters and eager writes cross the lazy plan boundary. Every
such operation is a contract exception against Archetype's lazy execution model.

If you are reading this, the most likely answer is to rewrite the
expression in Daft. Reach for where, select, with_column, agg, join,
sort, and distinct before pulling rows into Python. See
LEARNINGS.md and docs/guide/specification.md for the lazy contract.

If the materialization is genuinely unavoidable (storage write boundary,
single-row migration extract or terminal test assertion),
the exception must be documented in writing and visible in code review:

  * Every entry must name its boundary kind and owner, and its rationale must
    state the technical reason
    the boundary cannot be expressed lazily. Generic phrases ("needed",
    "for the test", "convenience") are rejected automatically.
  * Legacy entries must name the issue that removes them.
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
    locked_version = _locked_daft_version(root)
    if locked_version != _REGISTRY.daft_version:
        print(STERN_HEADER, file=sys.stderr)
        print(
            f"\nDaft terminal registry expects version {_REGISTRY.daft_version}; "
            f"uv.lock resolves {locked_version or 'no Daft package'}. "
            f"Review and update {REGISTRY_RELATIVE} before accepting this version.\n",
            file=sys.stderr,
        )
        return 2
    sites = scan(root)

    if "--list" in sys.argv:
        for s in sites:
            tag = " [udf-boundary]" if s.sanctioned else ""
            print(f"{s.path}:{s.line}  .{s.method}(){tag}  {s.snippet}")
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

    site_keys = {(s.path, s.line, s.method): s for s in audited_sites}
    allow_keys = {(e.path, e.line, e.method): e for e in allow}
    key_counts = Counter((e.path, e.line, e.method) for e in allow)

    new_sites = [site_keys[k] for k in site_keys.keys() - allow_keys.keys()]
    stale_entries = [allow_keys[k] for k in allow_keys.keys() - site_keys.keys()]
    duplicate_entries = [allow_keys[k] for k, count in key_counts.items() if count > 1]
    incomplete_entries = [e for e in allow if not _entry_is_complete(e)]

    new_sites.sort(key=lambda s: (s.path, s.line))
    stale_entries.sort(key=lambda e: (e.path, e.line))
    duplicate_entries.sort(key=lambda e: (e.path, e.line))
    incomplete_entries.sort(key=lambda e: (e.path, e.line))

    # Always print sanctioned summary for visibility.
    if sanctioned_sites:
        print(
            f"lazy audit: {len(sanctioned_sites)} udf-boundary site(s) exempt "
            f"(sanctioned @daft.method.batch / @daft.func.batch parameter access)."
        )

    if not (new_sites or stale_entries or duplicate_entries or incomplete_entries):
        print(f"lazy audit: {len(audited_sites)} audited site(s), all accounted for.")
        return 0

    print(STERN_HEADER, file=sys.stderr)

    if new_sites:
        rendered = [f"{s.path}:{s.line}  .{s.method}()  {s.snippet}" for s in new_sites]
        sys.stderr.write(_format_section("New, undocumented materialization points:", rendered))

    if stale_entries:
        rendered = [
            f"{e.path}:{e.line}  .{e.method}()  rationale={e.rationale!r}" for e in stale_entries
        ]
        sys.stderr.write(
            _format_section(
                "Stale allowlist entries (line no longer holds a matching call):",
                rendered,
            )
        )

    if duplicate_entries:
        rendered = [f"{e.path}:{e.line}  .{e.method}()" for e in duplicate_entries]
        sys.stderr.write(
            _format_section("Duplicate allowlist dispositions (one entry is required):", rendered)
        )

    if incomplete_entries:
        rendered = [
            f"{e.path}:{e.line}  .{e.method}()  kind={e.boundary_kind!r} "
            f"owner={e.owner!r} rationale={e.rationale!r} removal_issue={e.removal_issue!r}"
            for e in incomplete_entries
        ]
        sys.stderr.write(
            _format_section(
                "Allowlist entries with incomplete dispositions (rejected at review):",
                rendered,
            )
        )

    print(file=sys.stderr)
    print(STERN_BODY, file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
