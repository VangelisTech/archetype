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
    dataframe_container_attributes: frozenset[str]
    dataframe_return_methods: frozenset[str]


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
        dataframe_container_attributes=frozenset(
            data.get("provenance", {}).get("dataframe_container_attributes", ())
        ),
        dataframe_return_methods=frozenset(
            data.get("provenance", {}).get("dataframe_return_methods", ())
        ),
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


class _FlowAnalyzer(ast.NodeVisitor):
    """Bounded lexical data flow for the terminal forms in the registry.

    Environments are deliberately statement ordered and function local.  This
    is not general type inference: only imports, annotations, constructors,
    aliases, attributes, and Catalog/Session ``get_table`` results propagate.
    """

    def __init__(self, rel: str, lines: list[str], batch_params: dict[int, frozenset[str]]) -> None:
        self.rel = rel
        self.lines = lines
        self.batch_params = batch_params
        self.imports: dict[str, str] = {}
        self.env: dict[str, str] = {}
        self.class_name: str | None = None
        self.class_attrs: dict[tuple[str, str], str] = {}
        self.local_types: set[str] = set()
        self.return_types: dict[str, str] = {}
        self.method_return_types: dict[tuple[str, str], str] = {}
        self.sites: list[Site] = []
        self.seen: set[tuple[int, str]] = set()

    def _annotation(self, node: ast.expr | None) -> str | None:
        if node is None:
            return None
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.BitOr):
            left = self._annotation(node.left)
            right = self._annotation(node.right)
            proven = _REGISTRY.dataframe_types | frozenset(_REGISTRY.typed_methods)
            if left in proven:
                return left
            if right in proven:
                return right
            return left or right
        if isinstance(node, ast.Subscript):
            generic = _qualified_name(node.value, self.imports)
            arguments = list(node.slice.elts) if isinstance(node.slice, ast.Tuple) else [node.slice]
            argument_types = [self._annotation(argument) or "foreign" for argument in arguments]
            generic_name = generic.rsplit(".", 1)[-1] if generic else ""
            if (
                generic_name in {"dict", "Dict", "Mapping", "MutableMapping"}
                and len(argument_types) == 2
            ):
                return f"mapping:{argument_types[0]}|{argument_types[1]}"
            if (
                generic_name
                in {
                    "list",
                    "List",
                    "set",
                    "Set",
                    "frozenset",
                    "FrozenSet",
                    "Sequence",
                    "Iterable",
                    "Iterator",
                    "Collection",
                }
                and len(argument_types) == 1
            ):
                return f"sequence:{argument_types[0]}"
        return _qualified_name(node, self.imports)

    def _iter_element_type(self, node: ast.expr) -> str | None:
        """Return the declared element yielded by an iterable expression."""
        iterable_type = self._type(node)
        if iterable_type in _REGISTRY.dataframe_types:
            # Iterating a DataFrame is a terminal, but its rows are not frames.
            return "foreign"
        if iterable_type and iterable_type.startswith("sequence:"):
            return iterable_type.removeprefix("sequence:")
        if iterable_type and iterable_type.startswith("mapping:"):
            key_type, _, _ = iterable_type.removeprefix("mapping:").partition("|")
            return key_type
        return None

    def _type(self, node: ast.expr) -> str | None:
        if isinstance(node, ast.Name):
            return self.env.get(node.id)
        if isinstance(node, ast.Attribute):
            if (
                isinstance(node.value, ast.Name)
                and node.value.id in {"self", "cls"}
                and self.class_name
            ):
                return self.class_attrs.get((self.class_name, node.attr))
            owner = self._type(node.value)
            if owner and owner.startswith("class:"):
                qualified_attribute = f"{owner.removeprefix('class:')}.{node.attr}"
                if qualified_attribute in _REGISTRY.dataframe_container_attributes:
                    return "mapping:foreign|daft.DataFrame"
                return self.class_attrs.get((owner.removeprefix("class:"), node.attr))
            return owner
        if isinstance(node, ast.Call):
            called = _qualified_name(node.func, self.imports)
            if called in self.return_types:
                return self.return_types[called]
            if called in _REGISTRY.dataframe_constructors:
                return "daft.DataFrame"
            if called in _REGISTRY.typed_methods:
                return called
            if isinstance(node.func, ast.Attribute):
                owner = self._type(node.func.value)
                if owner and owner.startswith("class:"):
                    qualified_method = f"{owner.removeprefix('class:')}.{node.func.attr}"
                    if qualified_method in _REGISTRY.dataframe_return_methods:
                        return "daft.DataFrame"
                    returned = self.method_return_types.get(
                        (owner.removeprefix("class:"), node.func.attr)
                    )
                    if returned:
                        return returned
                if owner and owner.startswith("mapping:"):
                    key_type, _, value_type = owner.removeprefix("mapping:").partition("|")
                    if node.func.attr == "keys":
                        return f"sequence:{key_type}"
                    if node.func.attr == "values":
                        return f"sequence:{value_type}"
                    if node.func.attr == "items":
                        return f"sequence:tuple:{key_type}|{value_type}"
                # The blocking adapter returns the result of the bound method
                # supplied as its first argument.  Preserve provenance only
                # when that callable's receiver is already proven.
                if node.func.attr == "_blocking" and node.args:
                    bound = node.args[0]
                    if isinstance(bound, ast.Attribute):
                        bound_owner = self._type(bound.value)
                        if bound_owner in _REGISTRY.dataframe_types:
                            return bound_owner
                if node.func.attr == "materialize" and node.args:
                    frame_type = self._type(node.args[0])
                    if frame_type in _REGISTRY.dataframe_types:
                        return frame_type
                # Processor contracts transform and return the proven frame
                # passed as their first argument.  This is argument-backed
                # provenance, not a global inference from the method name.
                if node.func.attr == "process" and node.args:
                    frame_type = self._type(node.args[0])
                    if frame_type in _REGISTRY.dataframe_types:
                        return frame_type
                if node.func.attr == "get_table" and owner in {
                    "daft.catalog.Catalog",
                    "daft.Session",
                }:
                    return "daft.catalog.Table"
                if node.func.attr == "schema" and owner in _REGISTRY.dataframe_types:
                    return "foreign"
                if node.func.attr in {"to_arrow", "to_pandas", "to_pydict", "to_pylist"}:
                    return "foreign"
                if owner == "foreign":
                    return "foreign"
                if owner:
                    return owner
                if node.func.attr == "collect":
                    return "daft.DataFrame"
            if called and (
                called.split(".", 1)[0] in {"lancedb", "pyarrow"}
                or called.rsplit(".", 1)[-1] in self.local_types
            ):
                return "foreign"
            # A direct unknown constructor is positive evidence that the old
            # value was replaced.  An unproven method chain stays unknown so
            # the two historical receiver spellings can still be recognized.
            if isinstance(node.func, ast.Name):
                return "foreign"
            return None
        if isinstance(node, ast.Await):
            return self._type(node.value)
        return None

    def visit_Module(self, node: ast.Module) -> None:
        for statement in node.body:
            if isinstance(statement, ast.Import | ast.ImportFrom):
                self.visit(statement)
        self.local_types.update(
            statement.name for statement in node.body if isinstance(statement, ast.ClassDef)
        )
        for statement in node.body:
            if isinstance(statement, ast.FunctionDef | ast.AsyncFunctionDef):
                returned = self._annotation(statement.returns)
                if returned in _REGISTRY.dataframe_types or returned in _REGISTRY.typed_methods:
                    self.return_types[statement.name] = returned
            elif isinstance(statement, ast.ClassDef):
                for member in statement.body:
                    if isinstance(member, ast.AnnAssign) and isinstance(member.target, ast.Name):
                        field_type = self._annotation(member.annotation)
                        if field_type:
                            self.class_attrs[(statement.name, member.target.id)] = field_type
                    elif isinstance(member, ast.FunctionDef | ast.AsyncFunctionDef):
                        returned = self._annotation(member.returns)
                        if (
                            returned in _REGISTRY.dataframe_types
                            or returned in _REGISTRY.typed_methods
                        ):
                            self.method_return_types[(statement.name, member.name)] = returned
        for statement in node.body:
            if not isinstance(statement, ast.Import | ast.ImportFrom):
                self.visit(statement)

    def _record(self, node: ast.AST, method: str, receiver: ast.expr | None) -> None:
        line = node.end_lineno or node.lineno
        key = (line, method)
        if key in self.seen:
            return
        self.seen.add(key)
        sanctioned = (
            method == "to_pylist"
            and line in self.batch_params
            and isinstance(receiver, ast.Name)
            and receiver.id in self.batch_params[line]
        )
        self.sites.append(Site(self.rel, line, method, self.lines[line - 1].lstrip(), sanctioned))

    def _check(self, node: ast.AST, receiver: ast.expr | None, method: str) -> None:
        if receiver is None:
            return
        receiver_type = self._type(receiver)
        if receiver_type == "foreign" or (
            receiver_type is not None and receiver_type.startswith("class:")
        ):
            return
        if receiver_type in _REGISTRY.dataframe_types:
            terminal = method in _REGISTRY.dataframe_methods
        elif receiver_type in _REGISTRY.typed_methods:
            terminal = method in _REGISTRY.typed_methods[receiver_type]
        else:
            terminal = method in _REGISTRY.unproven_methods
        if terminal:
            self._record(node, method, receiver)

    def visit_Import(self, node: ast.Import) -> None:
        for alias in node.names:
            self.imports[alias.asname or alias.name.split(".")[0]] = alias.name

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        if node.module:
            for alias in node.names:
                self.imports[alias.asname or alias.name] = f"{node.module}.{alias.name}"

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        self.local_types.add(node.name)
        previous = self.class_name
        self.class_name = node.name
        for statement in node.body:
            self.visit(statement)
        self.class_name = previous

    def visit_FunctionDef(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> None:
        outer = self.env
        self.env = {}
        for arg in (*node.args.posonlyargs, *node.args.args, *node.args.kwonlyargs):
            annotation = self._annotation(arg.annotation)
            if (
                annotation in _REGISTRY.dataframe_types
                or annotation in _REGISTRY.typed_methods
                or (annotation and annotation.startswith(("mapping:", "sequence:")))
            ):
                self.env[arg.arg] = annotation
            elif annotation in self.local_types:
                self.env[arg.arg] = f"class:{annotation}"
            elif annotation and any(
                fact.startswith(f"{annotation}.")
                for fact in (
                    *_REGISTRY.dataframe_container_attributes,
                    *_REGISTRY.dataframe_return_methods,
                )
            ):
                self.env[arg.arg] = f"class:{annotation}"
            elif annotation and annotation.split(".", 1)[0] in {"lancedb", "pyarrow"}:
                self.env[arg.arg] = "foreign"
        if self.class_name and node.args.args and node.args.args[0].arg in {"self", "cls"}:
            self.env[node.args.args[0].arg] = f"class:{self.class_name}"
        for statement in node.body:
            self.visit(statement)
        self.env = outer

    visit_AsyncFunctionDef = visit_FunctionDef

    def _assign(self, target: ast.expr, value_type: str | None) -> None:
        if isinstance(target, ast.Name):
            self.env.pop(target.id, None)
            if value_type:
                self.env[target.id] = value_type
        elif (
            isinstance(target, ast.Attribute)
            and isinstance(target.value, ast.Name)
            and target.value.id in {"self", "cls"}
            and self.class_name
        ):
            key = (self.class_name, target.attr)
            self.class_attrs.pop(key, None)
            if value_type:
                self.class_attrs[key] = value_type
        elif isinstance(target, ast.Tuple | ast.List):
            item_types: list[str | None]
            if value_type and value_type.startswith("tuple:"):
                item_types = value_type.removeprefix("tuple:").split("|")
            else:
                item_types = [None] * len(target.elts)
            for index, item in enumerate(target.elts):
                self._assign(item, item_types[index] if index < len(item_types) else None)

    def visit_Assign(self, node: ast.Assign) -> None:
        self.visit(node.value)
        value_type = self._type(node.value)
        for target in node.targets:
            self._assign(target, value_type)

    def visit_AnnAssign(self, node: ast.AnnAssign) -> None:
        if node.value:
            self.visit(node.value)
        value_type = self._annotation(node.annotation) or (
            self._type(node.value) if node.value else None
        )
        self._assign(node.target, value_type)

    def visit_Attribute(self, node: ast.Attribute) -> None:
        self.visit(node.value)
        self._check(node, node.value, node.attr)

    def visit_Call(self, node: ast.Call) -> None:
        qualified = _qualified_name(node.func, self.imports)
        if qualified in _REGISTRY.module_functions:
            method = qualified.rsplit(".", 1)[-1]
            self._record(node.func, method, None)
        elif isinstance(node.func, ast.Name) and node.func.id == "iter" and node.args:
            self._check(node, node.args[0], "__iter__")
        self.generic_visit(node)

    def visit_For(self, node: ast.For | ast.AsyncFor) -> None:
        self.visit(node.iter)
        if not isinstance(node.iter, ast.Call):
            self._check(node.iter, node.iter, "__iter__")
        for statement in node.body:
            self.visit(statement)
        for statement in node.orelse:
            self.visit(statement)

    visit_AsyncFor = visit_For

    def visit_comprehension(self, node: ast.comprehension) -> None:
        self.visit(node.iter)
        if not isinstance(node.iter, ast.Call):
            self._check(node.iter, node.iter, "__iter__")
        for condition in node.ifs:
            self.visit(condition)

    def _visit_comp(self, node: ast.GeneratorExp | ast.ListComp | ast.SetComp) -> None:
        outer = self.env.copy()
        for generator in node.generators:
            self.visit(generator.iter)
            if not isinstance(generator.iter, ast.Call):
                self._check(generator.iter, generator.iter, "__iter__")
            self._assign(generator.target, self._iter_element_type(generator.iter))
            for condition in generator.ifs:
                self.visit(condition)
        self.visit(node.elt)
        self.env = outer

    visit_GeneratorExp = _visit_comp
    visit_ListComp = _visit_comp
    visit_SetComp = _visit_comp

    def visit_DictComp(self, node: ast.DictComp) -> None:
        outer = self.env.copy()
        for generator in node.generators:
            self.visit(generator.iter)
            if not isinstance(generator.iter, ast.Call):
                self._check(generator.iter, generator.iter, "__iter__")
            self._assign(generator.target, self._iter_element_type(generator.iter))
            for condition in generator.ifs:
                self.visit(condition)
        self.visit(node.key)
        self.visit(node.value)
        self.env = outer


def _scan_file(path: Path, rel: str) -> list[Site]:
    try:
        text = path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError):
        return []

    tree = ast.parse(text)
    batch_line_params = _collect_batch_udf_param_lines(text)
    lines = text.splitlines()
    analyzer = _FlowAnalyzer(rel, lines, batch_line_params)
    analyzer.visit(tree)
    return sorted(analyzer.sites, key=lambda site: (site.line, site.method))


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
