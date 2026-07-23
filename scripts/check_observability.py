#!/usr/bin/env python3
# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Enforce safe observability syntax and exact family dispositions.

The audit is deliberately AST-only: it never imports shipped code and it reads
the literal signal vocabulary from ``archetype._obs`` rather than maintaining
a second allowlist.
"""

from __future__ import annotations

import argparse
import ast
import re
import sys
import tomllib
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MANIFEST_ROOT = ROOT / "quality" / "observability"

SIGNALS = frozenset({"root", "child", "metric", "log_only", "none"})
OUTCOME_DISPOSITIONS = frozenset(
    {"propagated_failure", "handled_outcome", "retryable_attempt", "advisory_suppression"}
)
HOST_CAPABILITIES = frozenset(
    {
        "invoke_configuration",
        "provider_configuration",
        "logging_configuration",
        "console_export",
    }
)
VOCABULARY_SETS = (
    "SPAN_NAMES",
    "LEGACY_SPAN_NAMES",
    "TRACE_ATTRIBUTE_KEYS",
    "METRIC_NAMES",
    "METRIC_LABEL_KEYS",
    "EVENT_NAMES",
    "FAILURE_DISPOSITIONS",
    "OUTCOMES",
    "ERROR_TYPES",
)
VOCABULARY_MAPS = (
    "SPAN_NAME_ALIASES",
    "TRACE_ATTRIBUTE_ALIASES",
    "RECORDER_METRIC_NAMES",
    "RECORDER_METRIC_LABEL_KEYS",
)
RECORDER_EMITTERS = frozenset({"record_failure", "record_outcome"})
RECORDER_LABEL_COORDINATES = frozenset(
    {
        "record_failure.disposition",
        "record_failure.error_type",
        "record_outcome.operation",
        "record_outcome.outcome",
    }
)
OBS_EMITTERS = frozenset(
    {"span", "instrument", "counter_add", "record_failure", "record_outcome", "bind_context"}
)
LOG_METHODS = frozenset({"debug", "info", "warning", "error", "critical", "exception", "log"})
VENDOR_PREFIXES = ("opentelemetry", "logfire")
PRIVATE_ADAPTER_MODULES = frozenset({"archetype._obs", "archetype._logging"})
AMBIGUOUS_OBSERVABILITY_BINDING = "__observability_sensitive__"
INVOKE_CONFIGURATION_CALLS = frozenset(
    {
        "archetype._obs.configure_tracing",
        "archetype._logging.configure_archetype_logging",
        "archetype._logging.configure_host_observability",
    }
)
PROVIDER_MUTATOR_NAMES = frozenset(
    {
        "add_span_processor",
        "add_log_record_processor",
        "add_metric_reader",
        "set_tracer_provider",
        "set_meter_provider",
        "set_logger_provider",
    }
)
SENSITIVE_LOG_COORDINATES = frozenset(
    {"body", "content", "password", "path", "payload", "prompt", "secret", "token", "uri", "url"}
)


@dataclass(frozen=True)
class Violation:
    """One exact source violation eligible for an exact legacy entry."""

    rule: str
    path: str
    qualified_scope: str
    target: str
    line: int
    detail: str

    @property
    def key(self) -> tuple[str, str, str, str]:
        return (self.rule, self.path, self.qualified_scope, self.target)


@dataclass
class AuditResult:
    violations: list[Violation] = field(default_factory=list)
    exempted: list[Violation] = field(default_factory=list)
    policy_errors: list[str] = field(default_factory=list)
    files_scanned: int = 0
    protocols_scanned: int = 0
    operations_scanned: int = 0
    workflows_scanned: int = 0

    @property
    def ok(self) -> bool:
        return not self.violations and not self.policy_errors


@dataclass(frozen=True)
class SourceUnit:
    path: Path
    relative_path: str
    module: str
    tree: ast.Module
    bindings: dict[str, str]


@dataclass(frozen=True)
class Vocabulary:
    sets: dict[str, frozenset[str]]
    maps: dict[str, dict[str, str]]

    @property
    def span_names(self) -> frozenset[str]:
        return self.sets["SPAN_NAMES"]

    @property
    def legacy_span_names(self) -> frozenset[str]:
        return self.sets["LEGACY_SPAN_NAMES"]

    @property
    def attribute_keys(self) -> frozenset[str]:
        return self.sets["TRACE_ATTRIBUTE_KEYS"]

    @property
    def metric_names(self) -> frozenset[str]:
        return self.sets["METRIC_NAMES"]

    @property
    def metric_label_keys(self) -> frozenset[str]:
        return self.sets["METRIC_LABEL_KEYS"]

    def recorder_metric_name(self, emitter: str) -> str | None:
        return self.maps["RECORDER_METRIC_NAMES"].get(emitter)

    def recorder_metric_labels(self, emitter: str) -> dict[str, str]:
        prefix = f"{emitter}."
        return {
            coordinate.removeprefix(prefix): key
            for coordinate, key in self.maps["RECORDER_METRIC_LABEL_KEYS"].items()
            if coordinate.startswith(prefix)
        }


@dataclass
class ScopeEmissions:
    """Literal signal vocabulary observed in one exact callable scope."""

    span_names: set[str] = field(default_factory=set)
    attribute_keys: set[str] = field(default_factory=set)
    metric_names: set[str] = field(default_factory=set)
    metric_label_keys: set[str] = field(default_factory=set)

    def update(self, other: ScopeEmissions) -> None:
        self.span_names.update(other.span_names)
        self.attribute_keys.update(other.attribute_keys)
        self.metric_names.update(other.metric_names)
        self.metric_label_keys.update(other.metric_label_keys)


@dataclass
class ManifestState:
    operations: dict[str, dict[str, list[str]]] = field(
        default_factory=lambda: defaultdict(lambda: defaultdict(list))
    )
    dispositions: list[dict[str, Any]] = field(default_factory=list)
    workflows: list[dict[str, Any]] = field(default_factory=list)
    hosts: dict[str, frozenset[str]] = field(default_factory=dict)
    legacy: dict[tuple[str, str, str, str], dict[str, Any]] = field(default_factory=dict)


def _module_name(path: Path, source_root: Path) -> str:
    relative = path.relative_to(source_root).with_suffix("")
    parts = list(relative.parts)
    if parts[-1] == "__init__":
        parts.pop()
    return ".".join(parts)


def _resolve_import_from(node: ast.ImportFrom, consumer: str) -> str:
    if node.level == 0:
        return node.module or ""
    package = consumer.split(".")[:-1]
    keep = len(package) - (node.level - 1)
    prefix = package[: max(keep, 0)]
    if node.module:
        prefix.extend(node.module.split("."))
    return ".".join(prefix)


def _bindings(tree: ast.AST, module: str) -> dict[str, str]:
    return _scope_bindings(tree.body, {}, module, module)


def _dotted_name(node: ast.AST | None) -> str:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        base = _dotted_name(node.value)
        return f"{base}.{node.attr}" if base else node.attr
    return ""


def _resolved_name(node: ast.AST | None, bindings: dict[str, str]) -> str:
    dotted = _dotted_name(node)
    if not dotted:
        return ""
    parts = dotted.split(".")
    for end in range(len(parts), 0, -1):
        prefix = ".".join(parts[:end])
        if prefix in bindings:
            tail = parts[end:]
            return ".".join((bindings[prefix], *tail)) if tail else bindings[prefix]
    return dotted


def _same_scope_nodes(statements: list[ast.stmt]) -> list[ast.AST]:
    nodes: list[ast.AST] = []
    stack: list[ast.AST] = list(reversed(statements))
    boundaries = (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda)
    while stack:
        node = stack.pop()
        nodes.append(node)
        if isinstance(node, boundaries):
            continue
        stack.extend(reversed(list(ast.iter_child_nodes(node))))
    return nodes


def _target_nodes(node: ast.Assign | ast.AnnAssign) -> list[ast.AST]:
    return list(node.targets) if isinstance(node, ast.Assign) else [node.target]


def _import_binding(node: ast.Import | ast.ImportFrom, module: str) -> dict[str, str]:
    bindings: dict[str, str] = {}
    if isinstance(node, ast.Import):
        for alias in node.names:
            local = alias.asname or alias.name.split(".")[0]
            bindings[local] = alias.name if alias.asname else local
        return bindings
    base = _resolve_import_from(node, module)
    for alias in node.names:
        if alias.name == "*":
            continue
        local = alias.asname or alias.name
        bindings[local] = ".".join(part for part in (base, alias.name) if part)
    return bindings


def _assignment_binding(value: ast.AST | None, bindings: dict[str, str]) -> str:
    if isinstance(value, ast.Call):
        factory = _resolved_name(value.func, bindings)
        if factory in {"logging.getLogger", "logging.Logger", "logging.LoggerAdapter"}:
            return "logging.Logger"
        if factory.startswith((*VENDOR_PREFIXES, "logging.")):
            return factory
        return ""
    if not isinstance(value, (ast.Name, ast.Attribute)):
        return ""
    dotted = _dotted_name(value)
    if dotted.rsplit(".", 1)[-1] in PROVIDER_MUTATOR_NAMES:
        return AMBIGUOUS_OBSERVABILITY_BINDING
    parts = dotted.split(".")
    if any(".".join(parts[:end]) in bindings for end in range(1, len(parts) + 1)):
        return _resolved_name(value, bindings)
    return ""


def _sensitive_binding(value: str | None) -> bool:
    if value is None:
        return False
    tail = value.rsplit(".", 1)[-1]
    return (
        value == AMBIGUOUS_OBSERVABILITY_BINDING
        or value.startswith((*VENDOR_PREFIXES, "logging."))
        or value in INVOKE_CONFIGURATION_CALLS
        or tail in PROVIDER_MUTATOR_NAMES
    )


def _join_binding_states(states: list[dict[str, str]]) -> dict[str, str]:
    if not states:
        return {}
    joined: dict[str, str] = {}
    keys = set().union(*(state.keys() for state in states))
    for key in keys:
        values = [state.get(key) for state in states]
        if all(value == values[0] for value in values):
            if values[0] is not None:
                joined[key] = values[0]
            continue
        sensitive = [value for value in values if _sensitive_binding(value)]
        if sensitive:
            joined[key] = AMBIGUOUS_OBSERVABILITY_BINDING
    return joined


def _prefix_binding_states(
    statements: list[ast.stmt],
    initial: dict[str, str],
    module: str,
    scope: str,
) -> list[dict[str, str]]:
    states = [dict(initial)]
    current = dict(initial)
    for statement in statements:
        current = _flow_bindings([statement], current, module, scope)
        states.append(current)
    return states


def _nested_statement_blocks(node: ast.stmt) -> list[list[ast.stmt]]:
    if isinstance(node, ast.If):
        return [node.body, node.orelse]
    if isinstance(node, (ast.For, ast.AsyncFor, ast.While)):
        return [node.body, node.orelse]
    if isinstance(node, (ast.Try, ast.TryStar)):
        return [
            node.body,
            node.orelse,
            *(handler.body for handler in node.handlers),
            node.finalbody,
        ]
    if isinstance(node, (ast.With, ast.AsyncWith)):
        return [node.body]
    if isinstance(node, ast.Match):
        return [case.body for case in node.cases]
    return []


def _exception_binding_states(
    statements: list[ast.stmt],
    initial: dict[str, str],
    module: str,
    scope: str,
) -> list[dict[str, str]]:
    states = [dict(initial)]
    current = dict(initial)
    for statement in statements:
        for block in _nested_statement_blocks(statement):
            states.extend(_exception_binding_states(block, current, module, scope))
        current = _flow_bindings([statement], current, module, scope)
        states.append(current)
    return states


def _contains_control_transfer(statements: list[ast.stmt]) -> bool:
    return any(
        isinstance(node, (ast.Break, ast.Continue))
        for statement in statements
        for node in ast.walk(statement)
    )


def _flow_bindings(
    statements: list[ast.stmt],
    initial: dict[str, str],
    module: str,
    scope: str,
) -> dict[str, str]:
    state = dict(initial)
    for node in statements:
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            state.update(_import_binding(node, module))
        elif isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
            state[node.name] = f"{scope}.{node.name}"
        elif isinstance(node, (ast.Assign, ast.AnnAssign)) and node.value is not None:
            binding = _assignment_binding(node.value, state)
            for target in _target_nodes(node):
                target_name = _dotted_name(target)
                if not target_name:
                    continue
                if binding:
                    state[target_name] = binding
                else:
                    state.pop(target_name, None)
        elif isinstance(node, ast.If):
            body = _flow_bindings(node.body, state, module, scope)
            otherwise = (
                _flow_bindings(node.orelse, state, module, scope) if node.orelse else dict(state)
            )
            state = _join_binding_states([body, otherwise])
        elif isinstance(node, (ast.For, ast.AsyncFor, ast.While)):
            body_states = _prefix_binding_states(node.body, state, module, scope)
            loop_states = [state, body_states[-1]]
            if _contains_control_transfer(node.body):
                loop_states.extend(body_states)
            loop = _join_binding_states(loop_states)
            otherwise = _flow_bindings(node.orelse, loop, module, scope) if node.orelse else loop
            state = _join_binding_states([loop, otherwise])
        elif isinstance(node, (ast.Try, ast.TryStar)):
            body_states = _exception_binding_states(node.body, state, module, scope)
            success = _flow_bindings(node.orelse, body_states[-1], module, scope)
            exception_entry = _join_binding_states(body_states)
            outcomes = [success]
            outcomes.extend(
                _flow_bindings(handler.body, exception_entry, module, scope)
                for handler in node.handlers
            )
            state = _join_binding_states(outcomes)
            state = _flow_bindings(node.finalbody, state, module, scope)
        elif isinstance(node, (ast.With, ast.AsyncWith)):
            state = _flow_bindings(node.body, state, module, scope)
        elif isinstance(node, ast.Match):
            outcomes = [_flow_bindings(case.body, state, module, scope) for case in node.cases]
            outcomes.append(dict(state))
            state = _join_binding_states(outcomes)
    return state


def _scope_bindings(
    statements: list[ast.stmt],
    parent: dict[str, str],
    module: str,
    scope: str,
    *,
    parameters: frozenset[str] = frozenset(),
) -> dict[str, str]:
    nodes = _same_scope_nodes(statements)
    imports = [node for node in nodes if isinstance(node, (ast.Import, ast.ImportFrom))]
    definitions = [
        node
        for node in nodes
        if isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef))
    ]
    assignments = [node for node in nodes if isinstance(node, (ast.Assign, ast.AnnAssign))]
    named_expressions = [node for node in nodes if isinstance(node, ast.NamedExpr)]

    local_names = set(parameters)
    for node in imports:
        local_names.update(_import_binding(node, module))
    local_names.update(node.name for node in definitions)
    for node in assignments:
        local_names.update(
            target.id for target in _target_nodes(node) if isinstance(target, ast.Name)
        )
    local_names.update(
        node.target.id for node in named_expressions if isinstance(node.target, ast.Name)
    )

    initial = {name: value for name, value in parent.items() if name not in local_names}
    return _flow_bindings(statements, initial, module, scope)


def _parameter_names(
    node: ast.FunctionDef | ast.AsyncFunctionDef | ast.Lambda,
) -> frozenset[str]:
    arguments = node.args
    names = {
        argument.arg
        for argument in (*arguments.posonlyargs, *arguments.args, *arguments.kwonlyargs)
    }
    if arguments.vararg is not None:
        names.add(arguments.vararg.arg)
    if arguments.kwarg is not None:
        names.add(arguments.kwarg.arg)
    return frozenset(names)


def _function_bindings(
    node: ast.FunctionDef | ast.AsyncFunctionDef,
    parent: dict[str, str],
    module: str,
    scope: str,
) -> dict[str, str]:
    nodes = _same_scope_nodes(node.body)
    local_names = set(_parameter_names(node))
    for child in nodes:
        if isinstance(child, (ast.Import, ast.ImportFrom)):
            local_names.update(_import_binding(child, module))
        elif isinstance(child, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
            local_names.add(child.name)
        elif isinstance(child, (ast.Assign, ast.AnnAssign)):
            local_names.update(
                target.id for target in _target_nodes(child) if isinstance(target, ast.Name)
            )
        elif isinstance(child, ast.NamedExpr) and isinstance(child.target, ast.Name):
            local_names.add(child.target.id)
    return {name: value for name, value in parent.items() if name not in local_names}


def _class_bindings(
    node: ast.ClassDef,
    parent: dict[str, str],
    module: str,
    scope: str,
) -> dict[str, str]:
    class_parent = {
        name: value for name, value in parent.items() if not name.startswith(("self.", "cls."))
    }
    result = _scope_bindings(node.body, class_parent, module, scope)
    methods = [
        member
        for member in node.body
        if isinstance(member, (ast.FunctionDef, ast.AsyncFunctionDef))
    ]
    changed = True
    while changed:
        changed = False
        for method in methods:
            bindings = _scope_bindings(
                method.body,
                result,
                module,
                f"{scope}.{method.name}",
                parameters=_parameter_names(method),
            )
            for name, value in bindings.items():
                if name.startswith(("self.", "cls.")) and name not in result:
                    result[name] = value
                    changed = True
    return result


def _parse_sources(repo_root: Path, result: AuditResult) -> tuple[Path, list[SourceUnit]]:
    source_root = repo_root / "src"
    package_root = source_root / "archetype"
    units: list[SourceUnit] = []
    if not package_root.is_dir():
        result.policy_errors.append("missing source package: src/archetype")
        return source_root, units
    for path in sorted(package_root.rglob("*.py")):
        relative = path.relative_to(repo_root).as_posix()
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=relative)
        except (OSError, SyntaxError) as error:
            result.policy_errors.append(f"cannot parse {relative}: {error}")
            continue
        module = _module_name(path, source_root)
        units.append(SourceUnit(path, relative, module, tree, _bindings(tree, module)))
    result.files_scanned = len(units)
    return source_root, units


class _ScopeCollector(ast.NodeVisitor):
    def __init__(self, module: str) -> None:
        self.parts = [module]
        self.scopes: set[str] = set()

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        self.parts.append(node.name)
        self.generic_visit(node)
        self.parts.pop()

    def _visit_function(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> None:
        self.parts.append(node.name)
        self.scopes.add(".".join(self.parts))
        self.generic_visit(node)
        self.parts.pop()

    visit_FunctionDef = _visit_function
    visit_AsyncFunctionDef = _visit_function


def _callable_scope_paths(units: list[SourceUnit]) -> dict[str, str]:
    scopes: dict[str, str] = {}
    for unit in units:
        collector = _ScopeCollector(unit.module)
        collector.visit(unit.tree)
        for scope in collector.scopes:
            scopes[scope] = unit.relative_path
    return scopes


def _natural_owner(path: str) -> str:
    parts = Path(path).parts
    try:
        package_index = parts.index("archetype")
    except ValueError:
        return ""
    package_parts = parts[package_index + 1 :]
    if not package_parts:
        return ""
    if package_parts[0] == "app" and len(package_parts) >= 2:
        return package_parts[1]
    return package_parts[0].removesuffix(".py")


def _owner_may_claim_source(owner: str, path: str, scope: str) -> bool:
    if owner == _natural_owner(path):
        return True
    return (
        owner == "world"
        and path == "src/archetype/core/aio/async_world.py"
        and scope
        in {
            "archetype.core.aio.async_world.AsyncWorld._compute_archetype",
            "archetype.core.aio.async_world.AsyncWorld._commit_archetype",
        }
    )


def _host_scope_allowed(scope: str, capability: str) -> bool:
    if capability == "provider_configuration":
        return scope.startswith("archetype._obs.")
    if capability == "logging_configuration":
        return scope.startswith("archetype._logging.")
    if capability == "console_export":
        return scope.startswith("archetype._obs.")
    if capability == "invoke_configuration":
        return scope.startswith(
            (
                "archetype._logging.",
                "archetype.api.",
                "archetype.cli.",
                "archetype.runtime.",
            )
        )
    return False


def _protocol_base(node: ast.AST) -> ast.AST:
    """Return the unsubscripted base used to declare a protocol.

    Generic protocols appear as ``Protocol[T]`` in the AST. Treating only a
    bare ``Protocol`` name as a protocol would silently remove their callable
    members from the manifest universe.
    """

    while isinstance(node, ast.Subscript):
        node = node.value
    return node


def _is_protocol(node: ast.ClassDef, bindings: dict[str, str]) -> bool:
    return any(
        _resolved_name(_protocol_base(base), bindings)
        in {"Protocol", "typing.Protocol", "typing_extensions.Protocol"}
        for base in node.bases
    )


def _protocol_operations(
    units: list[SourceUnit],
    source_root: Path,
    registered_family_scopes: frozenset[str],
    result: AuditResult,
) -> dict[str, set[str]]:
    operations: dict[str, set[str]] = defaultdict(set)
    definitions: dict[tuple[str, str], list[str]] = defaultdict(list)
    protocols = 0
    app_root = source_root / "archetype" / "app"
    for unit in units:
        family_roots: list[tuple[str, str]] = []
        for scope in registered_family_scopes:
            if unit.module == scope or unit.module.startswith(scope + "."):
                family_roots.append((scope.rsplit(".", 1)[-1], scope))

        # Compatibility for the application-family layout that remains in use
        # during the package migration. PR-11 removes this path after all
        # protocol owners live in registered top-level families.
        try:
            relative = unit.path.relative_to(app_root)
        except ValueError:
            pass
        else:
            if len(relative.parts) >= 2:
                family_roots.append(
                    (
                        relative.parts[0],
                        f"archetype.app.{relative.parts[0]}",
                    )
                )

        for family, scope in family_roots:
            module_prefix = unit.module.removeprefix(scope).removeprefix(".")
            for node in unit.tree.body:
                if not isinstance(node, ast.ClassDef) or not _is_protocol(node, unit.bindings):
                    continue
                protocols += 1
                for member in node.body:
                    if not isinstance(member, (ast.FunctionDef, ast.AsyncFunctionDef)):
                        continue
                    prefix = ".".join(part for part in (module_prefix, node.name) if part)
                    operation = f"{prefix}.{member.name}"
                    operations[family].add(operation)
                    definitions[(family, operation)].append(
                        f"{unit.module}.{node.name}.{member.name} "
                        f"({unit.relative_path}:{member.lineno})"
                    )
    for (family, operation), sources in sorted(definitions.items()):
        if len(sources) > 1:
            result.policy_errors.append(
                f"duplicate discovered protocol operation {family}:{operation}: "
                + ", ".join(sources)
            )
    result.protocols_scanned = protocols
    result.operations_scanned = sum(len(values) for values in operations.values())
    return operations


def _registered_top_level_family_scopes(
    repo_root: Path,
    result: AuditResult,
) -> frozenset[str]:
    """Read the top-level family registry without importing architecture tooling."""

    policy_path = repo_root / "quality" / "architecture.toml"
    if not policy_path.is_file():
        result.policy_errors.append("missing architecture policy: quality/architecture.toml")
        return frozenset()

    fragment_root = policy_path.parent / f"{policy_path.stem}.d"
    paths = [policy_path]
    if fragment_root.is_dir():
        paths.extend(sorted(fragment_root.glob("*.toml")))

    scopes: set[str] = set()
    for path in paths:
        relative = path.relative_to(repo_root).as_posix()
        try:
            document = tomllib.loads(path.read_text(encoding="utf-8"))
        except (OSError, tomllib.TOMLDecodeError) as error:
            result.policy_errors.append(
                f"cannot parse architecture family registry {relative}: {error}"
            )
            continue
        rows = document.get("top_level_family_rule", [])
        if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
            result.policy_errors.append(
                f"{relative}.top_level_family_rule must be an array of tables"
            )
            continue
        for index, row in enumerate(rows):
            scope = row.get("consumer")
            if (
                not isinstance(scope, str)
                or re.fullmatch(r"archetype\.[A-Za-z_][A-Za-z0-9_]*", scope) is None
            ):
                result.policy_errors.append(
                    f"{relative}.top_level_family_rule[{index}].consumer "
                    "must be a top-level archetype family scope"
                )
                continue
            if scope in scopes:
                result.policy_errors.append(f"duplicate registered top-level family scope: {scope}")
                continue
            scopes.add(scope)
    return frozenset(scopes)


def _assignment(tree: ast.Module, name: str) -> ast.AST | None:
    for node in tree.body:
        if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            if node.target.id == name:
                return node.value
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == name:
                    return node.value
    return None


def _unwrap_literal(node: ast.AST | None, wrappers: frozenset[str]) -> ast.AST | None:
    current = node
    while isinstance(current, ast.Call):
        if _dotted_name(current.func).split(".")[-1] not in wrappers or len(current.args) != 1:
            return None
        current = current.args[0]
    return current


def _literal_string_set(node: ast.AST | None, name: str) -> frozenset[str]:
    literal = _unwrap_literal(node, frozenset({"frozenset", "set", "tuple", "list"}))
    if not isinstance(literal, (ast.Set, ast.List, ast.Tuple)):
        raise ValueError(f"{name} must be a literal string collection")
    values: list[str] = []
    for item in literal.elts:
        if not isinstance(item, ast.Constant) or not isinstance(item.value, str):
            raise ValueError(f"{name} must contain only literal strings")
        values.append(item.value)
    if len(values) != len(set(values)):
        raise ValueError(f"{name} contains duplicate entries")
    return frozenset(values)


def _literal_string_map(node: ast.AST | None, name: str) -> dict[str, str]:
    literal = _unwrap_literal(node, frozenset({"MappingProxyType", "dict"}))
    if not isinstance(literal, ast.Dict):
        raise ValueError(f"{name} must be a literal string mapping")
    result: dict[str, str] = {}
    for key_node, value_node in zip(literal.keys, literal.values, strict=True):
        if (
            not isinstance(key_node, ast.Constant)
            or not isinstance(key_node.value, str)
            or not isinstance(value_node, ast.Constant)
            or not isinstance(value_node.value, str)
        ):
            raise ValueError(f"{name} must contain only literal string pairs")
        if key_node.value in result:
            raise ValueError(f"{name} contains duplicate key {key_node.value!r}")
        result[key_node.value] = value_node.value
    return result


def _load_vocabulary(repo_root: Path, result: AuditResult) -> Vocabulary:
    path = repo_root / "src" / "archetype" / "_obs.py"
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=path.as_posix())
    except (OSError, SyntaxError) as error:
        result.policy_errors.append(f"cannot parse observability vocabulary: {error}")
        return Vocabulary({name: frozenset() for name in VOCABULARY_SETS}, {})

    sets: dict[str, frozenset[str]] = {}
    maps: dict[str, dict[str, str]] = {}
    for name in VOCABULARY_SETS:
        try:
            sets[name] = _literal_string_set(_assignment(tree, name), name)
        except ValueError as error:
            result.policy_errors.append(str(error))
            sets[name] = frozenset()
    for name in VOCABULARY_MAPS:
        try:
            maps[name] = _literal_string_map(_assignment(tree, name), name)
        except ValueError as error:
            result.policy_errors.append(str(error))
            maps[name] = {}

    if not sets["LEGACY_SPAN_NAMES"] <= sets["SPAN_NAMES"]:
        result.policy_errors.append("LEGACY_SPAN_NAMES must be a subset of SPAN_NAMES")
    if not set(maps["SPAN_NAME_ALIASES"].values()) <= sets["SPAN_NAMES"]:
        result.policy_errors.append("SPAN_NAME_ALIASES targets must be canonical SPAN_NAMES")
    if not set(maps["TRACE_ATTRIBUTE_ALIASES"].values()) <= sets["TRACE_ATTRIBUTE_KEYS"]:
        result.policy_errors.append(
            "TRACE_ATTRIBUTE_ALIASES targets must be canonical TRACE_ATTRIBUTE_KEYS"
        )
    if not sets["METRIC_LABEL_KEYS"] <= sets["TRACE_ATTRIBUTE_KEYS"]:
        result.policy_errors.append("METRIC_LABEL_KEYS must be a subset of TRACE_ATTRIBUTE_KEYS")
    recorder_metrics = maps["RECORDER_METRIC_NAMES"]
    if set(recorder_metrics) != RECORDER_EMITTERS:
        result.policy_errors.append(
            "RECORDER_METRIC_NAMES must define record_failure and record_outcome exactly"
        )
    if not set(recorder_metrics.values()) <= sets["METRIC_NAMES"]:
        result.policy_errors.append("RECORDER_METRIC_NAMES targets must be canonical METRIC_NAMES")
    recorder_labels = maps["RECORDER_METRIC_LABEL_KEYS"]
    if set(recorder_labels) != RECORDER_LABEL_COORDINATES:
        result.policy_errors.append(
            "RECORDER_METRIC_LABEL_KEYS must define the fixed recorder coordinates exactly"
        )
    if not set(recorder_labels.values()) <= sets["METRIC_LABEL_KEYS"]:
        result.policy_errors.append(
            "RECORDER_METRIC_LABEL_KEYS targets must be canonical METRIC_LABEL_KEYS"
        )
    return Vocabulary(sets, maps)


def _nonblank(value: Any, label: str, errors: list[str]) -> str:
    if not isinstance(value, str) or not value.strip():
        errors.append(f"{label} must be a non-empty string")
        return ""
    return value


def _string_array(
    value: Any,
    label: str,
    errors: list[str],
    *,
    required: bool = True,
) -> list[str]:
    if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
        errors.append(f"{label} must be an array of strings")
        return []
    if required and not value:
        errors.append(f"{label} must not be empty")
    if len(value) != len(set(value)):
        errors.append(f"{label} must not contain duplicates")
    return list(value)


def _evidence(
    value: Any,
    label: str,
    repo_root: Path,
    errors: list[str],
) -> list[str]:
    entries = _string_array(value, label, errors)
    for entry in entries:
        candidate = (repo_root / entry).resolve()
        if not candidate.is_relative_to(repo_root):
            errors.append(f"{label} path must be repository-relative: {entry}")
        elif not candidate.is_file():
            errors.append(f"{label} path does not exist: {entry}")
    return entries


def _validate_semantics(
    row: dict[str, Any],
    label: str,
    vocabulary: Vocabulary,
    repo_root: Path,
    errors: list[str],
    *,
    root_allowed: bool,
) -> None:
    signals = _string_array(row.get("signals"), f"{label}.signals", errors)
    unknown_signals = sorted(set(signals) - SIGNALS)
    if unknown_signals:
        errors.append(f"{label}.signals contains unknown values: {unknown_signals}")
    if "root" in signals and "child" in signals:
        errors.append(f"{label}.signals cannot contain both root and child")
    if "root" in signals and not root_allowed:
        errors.append(f"{label}.signals root is reserved for runtime or gateway ingress")
    if "none" in signals and len(signals) != 1:
        errors.append(f"{label}.signals none must be exclusive")

    outcomes = _string_array(row.get("outcomes"), f"{label}.outcomes", errors)
    unknown_outcomes = sorted(set(outcomes) - OUTCOME_DISPOSITIONS)
    if unknown_outcomes:
        errors.append(f"{label}.outcomes contains unknown values: {unknown_outcomes}")
    _nonblank(row.get("authority"), f"{label}.authority", errors)
    _evidence(row.get("evidence"), f"{label}.evidence", repo_root, errors)

    if "none" in signals or "advisory_suppression" in outcomes:
        _nonblank(row.get("rationale"), f"{label}.rationale", errors)

    span_names = _string_array(
        row.get("span_names", []), f"{label}.span_names", errors, required=False
    )
    span_signal = bool({"root", "child"} & set(signals))
    if span_signal and not span_names:
        errors.append(f"{label}.span_names is required for root/child signals")
    if span_names and not span_signal:
        errors.append(f"{label}.span_names requires a root or child signal")
    unknown_spans = sorted(set(span_names) - vocabulary.span_names)
    if unknown_spans:
        errors.append(f"{label}.span_names contains unknown names: {unknown_spans}")

    attribute_keys = _string_array(
        row.get("attribute_keys", []), f"{label}.attribute_keys", errors, required=False
    )
    unknown_attributes = sorted(set(attribute_keys) - vocabulary.attribute_keys)
    if unknown_attributes:
        errors.append(f"{label}.attribute_keys contains unknown keys: {unknown_attributes}")
    if attribute_keys and not span_signal:
        errors.append(f"{label}.attribute_keys requires a root or child signal")

    metric_names = _string_array(
        row.get("metric_names", []), f"{label}.metric_names", errors, required=False
    )
    metric_labels = _string_array(
        row.get("metric_label_keys", []),
        f"{label}.metric_label_keys",
        errors,
        required=False,
    )
    if "metric" in signals and not metric_names:
        errors.append(f"{label}.metric_names is required for metric signals")
    if metric_names and "metric" not in signals:
        errors.append(f"{label}.metric_names requires a metric signal")
    if metric_labels and "metric" not in signals:
        errors.append(f"{label}.metric_label_keys requires a metric signal")
    unknown_metrics = sorted(set(metric_names) - vocabulary.metric_names)
    if unknown_metrics:
        errors.append(f"{label}.metric_names contains unknown names: {unknown_metrics}")
    unbounded_labels = sorted(set(metric_labels) - vocabulary.metric_label_keys)
    if unbounded_labels:
        errors.append(f"{label}.metric_label_keys contains unbounded keys: {unbounded_labels}")


def _load_manifests(
    manifest_root: Path,
    repo_root: Path,
    vocabulary: Vocabulary,
    protocols: dict[str, set[str]],
    callable_paths: dict[str, str],
    result: AuditResult,
) -> ManifestState:
    state = ManifestState()
    paths = sorted(manifest_root.glob("*.toml")) if manifest_root.is_dir() else []
    if not paths:
        result.policy_errors.append(f"no observability manifests found under {manifest_root}")
        return state

    seen_owners: set[str] = set()
    for path in paths:
        relative = (
            path.relative_to(repo_root).as_posix() if path.is_relative_to(repo_root) else str(path)
        )
        try:
            document = tomllib.loads(path.read_text(encoding="utf-8"))
        except (OSError, tomllib.TOMLDecodeError) as error:
            result.policy_errors.append(f"cannot parse {relative}: {error}")
            continue
        local_errors: list[str] = []
        if document.get("version") != 1:
            local_errors.append(f"{relative}.version must equal 1")
        owner = _nonblank(document.get("owner"), f"{relative}.owner", local_errors)
        if owner in seen_owners:
            local_errors.append(f"duplicate observability owner {owner!r}")
        seen_owners.add(owner)
        if path.stem != owner:
            local_errors.append(f"{relative} filename must match owner {owner!r}")

        family_value = document.get("family")
        if owner in protocols:
            if family_value != owner:
                local_errors.append(f"{relative}.family must equal {owner!r}")
        elif family_value is not None:
            local_errors.append(f"{relative} is not a discovered protocol-family manifest")

        dispositions = document.get("disposition", [])
        if not isinstance(dispositions, list) or any(
            not isinstance(row, dict) for row in dispositions
        ):
            local_errors.append(f"{relative}.disposition must be an array of tables")
            dispositions = []
        if dispositions and owner not in protocols:
            local_errors.append(
                f"{relative}.disposition is allowed only for a discovered protocol family"
            )
        for index, row in enumerate(dispositions):
            label = f"{relative}.disposition[{index}]"
            operations = _string_array(row.get("operations"), f"{label}.operations", local_errors)
            _validate_semantics(
                row,
                label,
                vocabulary,
                repo_root,
                local_errors,
                root_allowed=owner in {"gateway", "runtime"},
            )
            signal_values = row.get("signals", [])
            signals = (
                {item for item in signal_values if isinstance(item, str)}
                if isinstance(signal_values, list)
                else set()
            )
            source_bound = bool({"root", "child", "metric"} & signals)
            emission_workflows = _string_array(
                row.get("emission_workflows", []),
                f"{label}.emission_workflows",
                local_errors,
                required=source_bound,
            )
            if source_bound and not emission_workflows:
                local_errors.append(
                    f"{label}.emission_workflows is required for root/child/metric signals"
                )
            if emission_workflows and not source_bound:
                local_errors.append(
                    f"{label}.emission_workflows requires a root, child, or metric signal"
                )
            for operation in operations:
                state.operations[owner][operation].append(label)
            state.dispositions.append(
                {
                    **row,
                    "owner": owner,
                    "_label": label,
                    "emission_workflows": emission_workflows,
                }
            )

        workflows = document.get("workflow", [])
        if not isinstance(workflows, list) or any(not isinstance(row, dict) for row in workflows):
            local_errors.append(f"{relative}.workflow must be an array of tables")
            workflows = []
        for index, row in enumerate(workflows):
            label = f"{relative}.workflow[{index}]"
            workflow_id = _nonblank(row.get("id"), f"{label}.id", local_errors)
            scope = _nonblank(row.get("qualified_scope"), f"{label}.qualified_scope", local_errors)
            _validate_semantics(
                row,
                label,
                vocabulary,
                repo_root,
                local_errors,
                root_allowed=owner in {"gateway", "runtime"},
            )
            if scope and scope not in callable_paths:
                local_errors.append(f"{label}.qualified_scope is not a shipped callable: {scope}")
            elif scope and not _owner_may_claim_source(owner, callable_paths[scope], scope):
                local_errors.append(
                    f"{label}.qualified_scope belongs to {_natural_owner(callable_paths[scope])!r}, "
                    f"not owner {owner!r}"
                )
            state.workflows.append({**row, "owner": owner, "_label": label, "id": workflow_id})

        hosts = document.get("host_callable", [])
        if not isinstance(hosts, list) or any(not isinstance(row, dict) for row in hosts):
            local_errors.append(f"{relative}.host_callable must be an array of tables")
            hosts = []
        for index, row in enumerate(hosts):
            label = f"{relative}.host_callable[{index}]"
            scope = _nonblank(row.get("qualified_scope"), f"{label}.qualified_scope", local_errors)
            capabilities = _string_array(
                row.get("capabilities"), f"{label}.capabilities", local_errors
            )
            unknown = sorted(set(capabilities) - HOST_CAPABILITIES)
            if unknown:
                local_errors.append(f"{label}.capabilities contains unknown values: {unknown}")
            _nonblank(row.get("rationale"), f"{label}.rationale", local_errors)
            _evidence(row.get("evidence"), f"{label}.evidence", repo_root, local_errors)
            if owner != "hosts":
                local_errors.append(f"{label} must be declared by quality/observability/hosts.toml")
            if scope and scope not in callable_paths:
                local_errors.append(f"{label}.qualified_scope is not a shipped callable: {scope}")
            for capability in capabilities:
                if scope and not _host_scope_allowed(scope, capability):
                    local_errors.append(
                        f"{label} cannot grant {capability!r} to non-host scope {scope!r}"
                    )
            if scope in state.hosts:
                local_errors.append(f"duplicate host callable: {scope}")
            state.hosts[scope] = frozenset(capabilities)

        legacy_rows = document.get("legacy", [])
        if not isinstance(legacy_rows, list) or any(
            not isinstance(row, dict) for row in legacy_rows
        ):
            local_errors.append(f"{relative}.legacy must be an array of tables")
            legacy_rows = []
        for index, row in enumerate(legacy_rows):
            label = f"{relative}.legacy[{index}]"
            rule = _nonblank(row.get("rule"), f"{label}.rule", local_errors)
            source_path = _nonblank(row.get("path"), f"{label}.path", local_errors)
            scope = _nonblank(row.get("qualified_scope"), f"{label}.qualified_scope", local_errors)
            target = _nonblank(row.get("target"), f"{label}.target", local_errors)
            if row.get("owner") != owner:
                local_errors.append(f"{label}.owner must equal manifest owner {owner!r}")
            issue = row.get("issue")
            if not isinstance(issue, int) or isinstance(issue, bool) or issue <= 0:
                local_errors.append(f"{label}.issue must be a positive integer")
            _nonblank(row.get("reason"), f"{label}.reason", local_errors)
            _nonblank(row.get("expiry_condition"), f"{label}.expiry_condition", local_errors)
            source_candidate = (repo_root / source_path).resolve() if source_path else repo_root
            if source_path and not source_candidate.is_relative_to(repo_root):
                local_errors.append(f"{label}.path must be repository-relative: {source_path}")
            elif source_path and not source_candidate.is_file():
                local_errors.append(f"{label}.path does not exist: {source_path}")
            elif source_path and not _owner_may_claim_source(owner, source_path, scope):
                local_errors.append(
                    f"{label}.path belongs to {_natural_owner(source_path)!r}, not owner {owner!r}"
                )
            key = (rule, source_path, scope, target)
            if key in state.legacy:
                local_errors.append(f"duplicate legacy exception key: {key!r}")
            state.legacy[key] = {**row, "_label": label}

        if owner not in protocols and not workflows and not hosts and not legacy_rows:
            local_errors.append(
                f"{relative} must own a real workflow, host callable, or legacy exception"
            )
        result.policy_errors.extend(local_errors)

    for family, expected in sorted(protocols.items()):
        declared = state.operations.get(family, {})
        if family not in seen_owners:
            result.policy_errors.append(f"missing observability manifest for family {family!r}")
        for operation in sorted(expected - set(declared)):
            result.policy_errors.append(f"missing disposition for {family}:{operation}")
        for operation in sorted(set(declared) - expected):
            result.policy_errors.append(f"phantom disposition for {family}:{operation}")
        for operation, locations in sorted(declared.items()):
            if len(locations) != 1:
                result.policy_errors.append(
                    f"duplicate disposition for {family}:{operation}: {', '.join(locations)}"
                )

    workflow_ids = Counter(str(row.get("id", "")) for row in state.workflows)
    for workflow_id, count in sorted(workflow_ids.items()):
        if workflow_id and count != 1:
            result.policy_errors.append(f"duplicate workflow id {workflow_id!r}")
    workflows_by_id = {
        str(row.get("id")): row
        for row in state.workflows
        if workflow_ids[str(row.get("id", ""))] == 1
    }
    for row in state.dispositions:
        label = str(row["_label"])
        owner = str(row["owner"])
        for workflow_id in row.get("emission_workflows", []):
            workflow = workflows_by_id.get(workflow_id)
            if workflow is None:
                result.policy_errors.append(
                    f"{label}.emission_workflows references unknown workflow {workflow_id!r}"
                )
            elif workflow.get("owner") != owner:
                result.policy_errors.append(
                    f"{label}.emission_workflows references workflow {workflow_id!r} "
                    f"owned by {workflow.get('owner')!r}, not {owner!r}"
                )
    workflow_scopes = Counter(str(row.get("qualified_scope", "")) for row in state.workflows)
    for scope, count in sorted(workflow_scopes.items()):
        if scope and count != 1:
            result.policy_errors.append(f"duplicate workflow qualified_scope {scope!r}")
    result.workflows_scanned = len(state.workflows)
    return state


def _literal_name(node: ast.AST | None) -> str | None:
    return node.value if isinstance(node, ast.Constant) and isinstance(node.value, str) else None


def _call_argument(node: ast.Call, position: int, keyword: str) -> ast.AST | None:
    for item in node.keywords:
        if item.arg == keyword:
            return item.value
    return node.args[position] if len(node.args) > position else None


def _obs_emitter(node: ast.Call, bindings: dict[str, str]) -> str | None:
    resolved = _resolved_name(node.func, bindings)
    if not resolved.startswith("archetype._obs."):
        return None
    name = resolved.rsplit(".", 1)[-1]
    return name if name in OBS_EMITTERS else None


def _looks_like_logger(node: ast.Call, bindings: dict[str, str]) -> bool:
    if not isinstance(node.func, ast.Attribute) or node.func.attr not in LOG_METHODS:
        return False
    if isinstance(node.func.value, ast.Call):
        factory = _resolved_name(node.func.value.func, bindings)
        if factory in {"logging.getLogger", "logging.Logger", "logging.LoggerAdapter"}:
            return True
    receiver = _resolved_name(node.func.value, bindings)
    tail = receiver.rsplit(".", 1)[-1].lower()
    return (
        receiver == AMBIGUOUS_OBSERVABILITY_BINDING
        or receiver.startswith("logging")
        or tail in {"log", "logger", "_logger"}
    )


def _is_eager_log_expression(node: ast.AST) -> bool:
    if isinstance(node, ast.JoinedStr):
        return True
    if isinstance(node, ast.BinOp) and isinstance(node.op, (ast.Add, ast.Mod)):
        return not (
            isinstance(node.left, ast.Constant)
            and isinstance(node.left.value, str)
            and isinstance(node.right, ast.Constant)
            and isinstance(node.right.value, str)
        )
    return (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "format"
    )


def _is_safe_exception_type_name(node: ast.AST, caught: set[str]) -> bool:
    if not isinstance(node, ast.Attribute) or node.attr != "__name__":
        return False
    value = node.value
    if isinstance(value, ast.Call) and isinstance(value.func, ast.Name) and value.func.id == "type":
        return (
            len(value.args) == 1
            and isinstance(value.args[0], ast.Name)
            and value.args[0].id in caught
        )
    if isinstance(value, ast.Attribute) and value.attr == "__class__":
        return isinstance(value.value, ast.Name) and value.value.id in caught
    return False


def _contains_raw_exception(node: ast.AST, caught: set[str]) -> bool:
    if _is_safe_exception_type_name(node, caught):
        return False
    if isinstance(node, ast.Name) and node.id in caught:
        return True
    return any(_contains_raw_exception(child, caught) for child in ast.iter_child_nodes(node))


def _contains_debug_coordinate(node: ast.AST) -> bool:
    return any(
        (isinstance(child, ast.Attribute) and child.attr == "debug")
        or (isinstance(child, ast.Name) and child.id in {"debug", "run_debug"})
        for child in ast.walk(node)
    )


def _broad_handler(node: ast.ExceptHandler) -> bool:
    if node.type is None:
        return True
    name = _dotted_name(node.type)
    return name in {"Exception", "BaseException", "builtins.Exception", "builtins.BaseException"}


def _handler_reraises(node: ast.ExceptHandler) -> bool:
    return len(node.body) == 1 and isinstance(node.body[0], ast.Raise) and node.body[0].exc is None


def _contains_sensitive_log_coordinate(node: ast.AST) -> bool:
    for child in ast.walk(node):
        if isinstance(child, ast.Name):
            value = child.id
        elif isinstance(child, ast.Attribute):
            value = child.attr
        elif isinstance(child, ast.Constant) and isinstance(child.value, str):
            value = child.value
        else:
            continue
        tokens = {token for token in re.split(r"[^a-z0-9]+", value.lower()) if token}
        if tokens & SENSITIVE_LOG_COORDINATES:
            return True
    return False


class _ReturnMapCollector(ast.NodeVisitor):
    def __init__(self, module: str) -> None:
        self.parts = [module]
        self.maps: dict[str, ast.Dict] = {}

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        self.parts.append(node.name)
        self.generic_visit(node)
        self.parts.pop()

    def _visit_function(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> None:
        self.parts.append(node.name)
        returns = [
            statement.value
            for statement in node.body
            if isinstance(statement, ast.Return) and isinstance(statement.value, ast.Dict)
        ]
        if len(returns) == 1:
            self.maps[".".join(self.parts)] = returns[0]
        self.parts.pop()

    visit_FunctionDef = _visit_function
    visit_AsyncFunctionDef = _visit_function


class _AssignmentCollector(ast.NodeVisitor):
    def __init__(self, root: ast.FunctionDef | ast.AsyncFunctionDef) -> None:
        self.root = root
        self.values: dict[str, list[ast.AST]] = defaultdict(list)

    def _visit_function(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> None:
        if node is self.root:
            self.generic_visit(node)

    visit_FunctionDef = _visit_function
    visit_AsyncFunctionDef = _visit_function

    def visit_Lambda(self, node: ast.Lambda) -> None:
        return

    def visit_Assign(self, node: ast.Assign) -> None:
        for target in node.targets:
            if isinstance(target, ast.Name):
                self.values[target.id].append(node.value)
        self.generic_visit(node)

    def visit_AnnAssign(self, node: ast.AnnAssign) -> None:
        if isinstance(node.target, ast.Name) and node.value is not None:
            self.values[node.target.id].append(node.value)
        self.generic_visit(node)


def _function_assignments(node: ast.FunctionDef | ast.AsyncFunctionDef) -> dict[str, ast.AST]:
    collector = _AssignmentCollector(node)
    collector.visit(node)
    return {name: values[0] for name, values in collector.values.items() if len(values) == 1}


class _SourceAnalyzer(ast.NodeVisitor):
    def __init__(
        self,
        unit: SourceUnit,
        vocabulary: Vocabulary,
        hosts: dict[str, frozenset[str]],
    ) -> None:
        self.unit = unit
        self.vocabulary = vocabulary
        self.hosts = hosts
        self.parts = [unit.module]
        self.caught: list[set[str]] = []
        self.assignments: list[dict[str, ast.AST]] = []
        self.binding_scopes: list[dict[str, str]] = [unit.bindings]
        self.violations: list[Violation] = []
        self.emissions: dict[str, ScopeEmissions] = defaultdict(ScopeEmissions)
        self.entered_context_calls = {
            id(item.context_expr)
            for candidate in ast.walk(unit.tree)
            if isinstance(candidate, ast.With)
            for item in candidate.items
            if isinstance(item.context_expr, ast.Call)
        }
        self.decorator_calls = {
            id(decorator)
            for candidate in ast.walk(unit.tree)
            if isinstance(candidate, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef))
            for decorator in candidate.decorator_list
            if isinstance(decorator, ast.Call)
        }
        collector = _ReturnMapCollector(unit.module)
        collector.visit(unit.tree)
        self.return_maps = collector.maps

    @property
    def scope(self) -> str:
        return ".".join(self.parts)

    @property
    def bindings(self) -> dict[str, str]:
        return self.binding_scopes[-1]

    def _add(self, rule: str, target: str, node: ast.AST, detail: str) -> None:
        self.violations.append(
            Violation(
                rule=rule,
                path=self.unit.relative_path,
                qualified_scope=self.scope,
                target=target,
                line=getattr(node, "lineno", 0),
                detail=detail,
            )
        )

    def _approved(self, capability: str) -> bool:
        return capability in self.hosts.get(self.scope, frozenset())

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        self.parts.append(node.name)
        definition_scope = self.scope
        self.binding_scopes.append(
            _class_bindings(node, self.bindings, self.unit.module, self.scope)
        )
        self.generic_visit(node)
        self.binding_scopes.pop()
        self.parts.pop()
        self.bindings[node.name] = definition_scope

    def _visit_function(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> None:
        self.parts.append(node.name)
        definition_scope = self.scope
        self.binding_scopes.append(
            _function_bindings(node, self.bindings, self.unit.module, self.scope)
        )
        self.assignments.append(_function_assignments(node))
        self.generic_visit(node)
        self.assignments.pop()
        self.binding_scopes.pop()
        self.parts.pop()
        self.bindings[node.name] = definition_scope

    visit_FunctionDef = _visit_function
    visit_AsyncFunctionDef = _visit_function

    def visit_Lambda(self, node: ast.Lambda) -> None:
        # Defaults execute in the enclosing scope; the lambda body is a distinct
        # callable boundary and must never donate emissions to its parent.
        for default in (*node.args.defaults, *node.args.kw_defaults):
            if default is not None:
                self.visit(default)

        self.parts.append(f"<lambda>@L{node.lineno}C{node.col_offset}")
        local_names = set(_parameter_names(node))
        local_names.update(
            child.target.id
            for child in _same_scope_nodes([node.body])
            if isinstance(child, ast.NamedExpr) and isinstance(child.target, ast.Name)
        )
        self.binding_scopes.append(
            {name: value for name, value in self.bindings.items() if name not in local_names}
        )
        self.assignments.append({})
        self.visit(node.body)
        self.assignments.pop()
        self.binding_scopes.pop()
        self.parts.pop()

    def visit_ExceptHandler(self, node: ast.ExceptHandler) -> None:
        names = {node.name} if isinstance(node.name, str) else set()
        self.caught.append(names)
        self.generic_visit(node)
        self.caught.pop()

    def _current_caught(self) -> set[str]:
        result: set[str] = set()
        for names in self.caught:
            result.update(names)
        return result

    def _resolve_local_expression(self, node: ast.AST, seen: set[str] | None = None) -> ast.AST:
        visited = set() if seen is None else seen
        if not isinstance(node, ast.Name) or node.id in visited:
            return node
        for assignments in reversed(self.assignments):
            if node.id in assignments:
                return self._resolve_local_expression(
                    assignments[node.id],
                    visited | {node.id},
                )
        return node

    def _vendor_import(self, imported: str, node: ast.AST) -> None:
        if not imported.startswith(VENDOR_PREFIXES):
            return
        if self.unit.module in PRIVATE_ADAPTER_MODULES or self.unit.module.startswith(
            "archetype.contrib."
        ):
            return
        if not self._approved("provider_configuration"):
            self._add(
                "vendor_observability_import",
                f"import:{imported}",
                node,
                "vendor telemetry imports are confined to an approved provider host",
            )

    def visit_Import(self, node: ast.Import) -> None:
        for alias in node.names:
            self._vendor_import(alias.name, node)
        self.bindings.update(_import_binding(node, self.unit.module))

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        base = _resolve_import_from(node, self.unit.module)
        for alias in node.names:
            imported = ".".join(part for part in (base, alias.name) if part)
            self._vendor_import(imported, node)
        self.bindings.update(_import_binding(node, self.unit.module))

    def _update_assignment_bindings(
        self,
        targets: list[ast.AST],
        value: ast.AST | None,
    ) -> None:
        binding = _assignment_binding(value, self.bindings)
        for target in targets:
            target_name = _dotted_name(target)
            if not target_name:
                continue
            if binding:
                self.bindings[target_name] = binding
            else:
                self.bindings.pop(target_name, None)

    def visit_Assign(self, node: ast.Assign) -> None:
        self.visit(node.value)
        for target in node.targets:
            self.visit(target)
        self._update_assignment_bindings(list(node.targets), node.value)

    def visit_AnnAssign(self, node: ast.AnnAssign) -> None:
        if node.value is not None:
            self.visit(node.value)
        self.visit(node.annotation)
        self.visit(node.target)
        if node.value is not None:
            self._update_assignment_bindings([node.target], node.value)

    def visit_AugAssign(self, node: ast.AugAssign) -> None:
        self.visit(node.target)
        self.visit(node.value)
        self._update_assignment_bindings([node.target], None)

    def visit_NamedExpr(self, node: ast.NamedExpr) -> None:
        self.visit(node.value)
        self.visit(node.target)
        self._update_assignment_bindings([node.target], node.value)

    def _visit_branch(
        self,
        statements: list[ast.stmt],
        initial: dict[str, str],
    ) -> dict[str, str]:
        outer = self.binding_scopes[-1]
        branch = dict(initial)
        self.binding_scopes[-1] = branch
        try:
            for statement in statements:
                self.visit(statement)
            return dict(branch)
        finally:
            self.binding_scopes[-1] = outer

    def _replace_bindings(self, state: dict[str, str]) -> None:
        self.bindings.clear()
        self.bindings.update(state)

    def visit_If(self, node: ast.If) -> None:
        self.visit(node.test)
        initial = dict(self.bindings)
        body = self._visit_branch(node.body, initial)
        otherwise = self._visit_branch(node.orelse, initial) if node.orelse else initial
        self._replace_bindings(_join_binding_states([body, otherwise]))

    def visit_Try(self, node: ast.Try | ast.TryStar) -> None:
        if self.unit.module not in PRIVATE_ADAPTER_MODULES:
            try_bindings = _scope_bindings(
                node.body,
                self.bindings,
                self.unit.module,
                self.scope,
            )
            has_emitter = any(
                isinstance(child, ast.Call) and _obs_emitter(child, try_bindings) is not None
                for statement in node.body
                for child in ast.walk(statement)
            )
            if has_emitter:
                for handler in node.handlers:
                    if _broad_handler(handler) and not _handler_reraises(handler):
                        exception_name = _dotted_name(handler.type) or "bare"
                        target = f"except:{exception_name}:telemetry-suppression"
                        self._add(
                            "broad_telemetry_suppression",
                            target,
                            handler,
                            "broad suppression around telemetry requires an exact migration entry",
                        )
        initial = dict(self.bindings)
        projected = _flow_bindings([node], initial, self.unit.module, self.scope)
        body = self._visit_branch(node.body, initial)
        success = self._visit_branch(node.orelse, body)
        outcomes = [success]
        outcomes.extend(self._visit_branch([handler], initial) for handler in node.handlers)
        joined = _join_binding_states(outcomes)
        self._visit_branch(node.finalbody, joined)
        self._replace_bindings(projected)

    visit_TryStar = visit_Try

    def _visit_loop(
        self,
        node: ast.For | ast.AsyncFor | ast.While,
    ) -> None:
        if isinstance(node, (ast.For, ast.AsyncFor)):
            self.visit(node.iter)
            self.visit(node.target)
        else:
            self.visit(node.test)
        initial = dict(self.bindings)
        projected = _flow_bindings([node], initial, self.unit.module, self.scope)
        iteration = dict(initial)
        if isinstance(node, (ast.For, ast.AsyncFor)):
            target_name = _dotted_name(node.target)
            if target_name:
                iteration.pop(target_name, None)
        body = self._visit_branch(node.body, iteration)
        loop = _join_binding_states([initial, body])
        if node.orelse:
            self._visit_branch(node.orelse, loop)
        self._replace_bindings(projected)

    visit_For = _visit_loop
    visit_AsyncFor = _visit_loop
    visit_While = _visit_loop

    def _visit_with(self, node: ast.With | ast.AsyncWith) -> None:
        initial = dict(self.bindings)
        for item in node.items:
            self.visit(item.context_expr)
            if item.optional_vars is not None:
                self.visit(item.optional_vars)
                target_name = _dotted_name(item.optional_vars)
                if target_name:
                    initial.pop(target_name, None)
        self._replace_bindings(self._visit_branch(node.body, initial))

    visit_With = _visit_with
    visit_AsyncWith = _visit_with

    def visit_Match(self, node: ast.Match) -> None:
        self.visit(node.subject)
        initial = dict(self.bindings)
        outcomes = [self._visit_branch(case.body, initial) for case in node.cases]
        outcomes.append(initial)
        self._replace_bindings(_join_binding_states(outcomes))

    def _configuration_capability(self, node: ast.Call, resolved: str) -> str | None:
        tail = resolved.rsplit(".", 1)[-1]
        local = f"{self.unit.module}.{resolved}" if "." not in resolved else resolved
        if local in INVOKE_CONFIGURATION_CALLS:
            return "invoke_configuration"
        if resolved == AMBIGUOUS_OBSERVABILITY_BINDING or tail in PROVIDER_MUTATOR_NAMES:
            return "provider_configuration"
        if resolved.startswith(VENDOR_PREFIXES) and tail in {
            "TracerProvider",
            "MeterProvider",
            "LoggerProvider",
            "OTLPSpanExporter",
            "OTLPMetricExporter",
            "OTLPLogExporter",
            "ConsoleSpanExporter",
            "ConsoleMetricExporter",
            "ConsoleLogExporter",
            "BatchSpanProcessor",
            "SimpleSpanProcessor",
            "BatchLogRecordProcessor",
            "SimpleLogRecordProcessor",
            "PeriodicExportingMetricReader",
        }:
            return "provider_configuration"
        if resolved in {
            "logging.basicConfig",
            "logging.config.dictConfig",
            "logging.config.fileConfig",
            "logging.setLogRecordFactory",
            "logging.captureWarnings",
            "logging.disable",
            "logging.config.listen",
            "logging.config.stopListening",
        }:
            return "logging_configuration"
        receiver = (
            _resolved_name(node.func.value, self.bindings)
            if isinstance(node.func, ast.Attribute)
            else ""
        )
        if tail in {
            "addFilter",
            "removeFilter",
            "addHandler",
            "removeHandler",
            "setFilter",
            "setFormatter",
            "setLevel",
        } and (
            receiver.startswith("logging.")
            or (self.unit.module == "archetype._logging" and receiver == "self")
        ):
            return "logging_configuration"
        return None

    def _check_configuration(self, node: ast.Call, resolved: str) -> None:
        capability = self._configuration_capability(node, resolved)
        if capability and not self._approved(capability):
            rule = (
                "logging_basic_config"
                if resolved.rsplit(".", 1)[-1] == "basicConfig"
                else "configuration_boundary"
            )
            self._add(
                rule,
                f"call:{resolved}:{ast.unparse(node)}",
                node,
                f"{capability} is not approved for this exact callable",
            )
        if resolved.endswith(".configure_tracing"):
            for keyword in node.keywords:
                if keyword.arg == "debug_console" and _contains_debug_coordinate(keyword.value):
                    self._add(
                        "debug_export_configuration",
                        f"configure_tracing:debug_console:{ast.unparse(keyword.value)}",
                        keyword.value,
                        "run/debug state cannot select telemetry export configuration",
                    )

    def _check_print(self, node: ast.Call, resolved: str) -> None:
        if resolved not in {"print", "builtins.print"}:
            return
        cli_prefix = "src/archetype/cli/"
        if not self.unit.relative_path.startswith(cli_prefix) and not self._approved(
            "console_export"
        ):
            self._add(
                "shipped_print",
                f"call:print:{ast.unparse(node)}",
                node,
                "shipped print calls are confined to CLI or an exact console exporter",
            )

    def _check_logging(self, node: ast.Call) -> None:
        if not _looks_like_logger(node, self.bindings):
            return
        method = node.func.attr if isinstance(node.func, ast.Attribute) else "log"
        message_position = 1 if method == "log" else 0
        message_node = _call_argument(node, message_position, "msg")
        message = ast.unparse(message_node) if message_node is not None else "<no-message>"
        site = f"log:{method}:{message}"
        resolved_arguments = [self._resolve_local_expression(argument) for argument in node.args]
        logged_arguments = resolved_arguments[1:] if method == "log" else resolved_arguments
        resolved_message = (
            self._resolve_local_expression(message_node) if message_node is not None else None
        )
        if resolved_message is not None and _is_eager_log_expression(resolved_message):
            self._add(
                "eager_log_interpolation",
                f"{site}:interpolation",
                node,
                "use parameterized stdlib logging instead of eager interpolation",
            )

        caught = self._current_caught()
        raw = method == "exception"
        raw = raw or any(
            keyword.arg in {"exc_info", "stack_info"}
            and not (
                isinstance(keyword.value, ast.Constant) and keyword.value.value in {False, None}
            )
            for keyword in node.keywords
        )
        raw = raw or any(_contains_raw_exception(argument, caught) for argument in logged_arguments)
        raw = raw or any(
            keyword.arg != "exc_info" and _contains_raw_exception(keyword.value, caught)
            for keyword in node.keywords
        )
        if raw:
            self._add(
                "raw_exception_logging",
                f"{site}:exception",
                node,
                "raw exceptions, messages, and stacks must not be exported",
            )

        sensitive_arguments = list(logged_arguments)
        if resolved_message is not None:
            sensitive_arguments.append(resolved_message)
        if any(_contains_sensitive_log_coordinate(argument) for argument in sensitive_arguments):
            self._add(
                "unsafe_log_value",
                f"{site}:sensitive-value",
                node,
                "logs must not export payload, prompt, secret, or path values",
            )

        for keyword in node.keywords:
            if keyword.arg != "extra":
                continue
            extra = self._resolve_local_expression(keyword.value)
            if not isinstance(extra, ast.Dict):
                self._add(
                    "dynamic_attribute_key",
                    f"{site}:extra:*",
                    keyword.value,
                    "structured log fields must use literal keys",
                )
                continue
            for key_node in extra.keys:
                key = _literal_name(key_node)
                if key is None:
                    self._add(
                        "dynamic_attribute_key",
                        f"{site}:extra:<dynamic>",
                        key_node or extra,
                        "structured log field keys must be literal",
                    )
                    continue
                tokens = {token for token in re.split(r"[^a-z0-9]+", key.lower()) if token}
                if tokens & SENSITIVE_LOG_COORDINATES:
                    self._add(
                        "unsafe_attribute_key",
                        f"{site}:extra:{key}",
                        key_node,
                        "structured log field is an obvious secret or content key",
                    )

    def _attribute_pairs(
        self,
        node: ast.Call,
        emitter: str,
        site: str,
    ) -> list[tuple[str | None, ast.AST, str]]:
        pairs: list[tuple[str | None, ast.AST, str]] = []
        for keyword in node.keywords:
            if keyword.arg is None:
                self._add(
                    "dynamic_attribute_key",
                    f"{site}:attributes:**:{ast.unparse(keyword.value)}",
                    keyword.value,
                    "telemetry calls cannot expand keyword mappings with unproven keys",
                )
        attributes = _call_argument(node, 1, "attributes") if emitter == "span" else None
        if emitter == "counter_add":
            attributes = _call_argument(node, 2, "attributes")
        elif emitter == "bind_context":
            attributes = _call_argument(node, 0, "attributes")
        if attributes is not None:
            if isinstance(attributes, ast.Constant) and attributes.value is None:
                pass
            else:
                mapping = self._resolve_attribute_mapping(attributes, set())
                if mapping is None:
                    self._add(
                        "dynamic_attribute_key",
                        f"{site}:attributes:*",
                        attributes,
                        "attribute mappings must expose literal keys at the emission site",
                    )
                else:
                    for key, value_node in mapping:
                        pairs.append((key, value_node, f"{site}:attribute:{key or '<dynamic>'}"))

        if emitter in {"span", "bind_context"}:
            for keyword in node.keywords:
                reserved = (
                    {None, "attributes", "name"} if emitter == "span" else {None, "attributes"}
                )
                if keyword.arg not in reserved:
                    pairs.append((keyword.arg, keyword.value, f"{site}:attribute:{keyword.arg}"))
        return pairs

    def _resolve_attribute_mapping(
        self,
        node: ast.AST,
        seen: set[str],
    ) -> list[tuple[str | None, ast.AST]] | None:
        if isinstance(node, ast.Name):
            if node.id in seen:
                return None
            for assignments in reversed(self.assignments):
                if node.id in assignments:
                    return self._resolve_attribute_mapping(
                        assignments[node.id],
                        seen | {node.id},
                    )
            return None
        if isinstance(node, ast.Call):
            dotted = _dotted_name(node.func)
            if dotted.startswith("self.") and len(self.parts) >= 2:
                scope = ".".join((*self.parts[:-1], dotted.removeprefix("self.")))
            else:
                scope = _resolved_name(node.func, self.bindings)
            returned = self.return_maps.get(scope)
            if returned is None or scope in seen:
                return None
            return self._resolve_attribute_mapping(returned, seen | {scope})
        if not isinstance(node, ast.Dict):
            return None

        pairs: list[tuple[str | None, ast.AST]] = []
        for key_node, value_node in zip(node.keys, node.values, strict=True):
            if key_node is None:
                unpacked = self._resolve_attribute_mapping(value_node, seen)
                if unpacked is None:
                    return None
                pairs.extend(unpacked)
            else:
                pairs.append((_literal_name(key_node), value_node))
        return pairs

    def _check_attributes(self, node: ast.Call, emitter: str, site: str) -> set[str]:
        caught = self._current_caught()
        observed: set[str] = set()
        for key, value, target in self._attribute_pairs(node, emitter, site):
            if key is None:
                self._add(
                    "dynamic_attribute_key",
                    target,
                    node,
                    "telemetry attribute keys must be literal",
                )
                continue
            if key in self.vocabulary.maps.get("TRACE_ATTRIBUTE_ALIASES", {}):
                observed.add(self.vocabulary.maps["TRACE_ATTRIBUTE_ALIASES"][key])
                self._add(
                    "legacy_attribute_key",
                    target,
                    node,
                    "legacy attribute aliases require an exact migration entry",
                )
            elif key not in self.vocabulary.attribute_keys:
                self._add(
                    "unsafe_attribute_key",
                    target,
                    node,
                    "attribute key is outside the safe literal vocabulary",
                )
            else:
                observed.add(key)
                if emitter == "counter_add" and key not in self.vocabulary.metric_label_keys:
                    self._add(
                        "high_cardinality_metric_label",
                        target,
                        node,
                        "metric labels must use the bounded label vocabulary",
                    )
            if _contains_raw_exception(value, caught):
                self._add(
                    "raw_exception_attribute",
                    f"{target}:exception",
                    value,
                    "raw exception values must not be exported as telemetry attributes",
                )
        return observed

    def _check_emitter(self, node: ast.Call, emitter: str) -> None:
        if self.unit.module in PRIVATE_ADAPTER_MODULES:
            return
        emission: ScopeEmissions | None = self.emissions[self.scope]
        if emitter == "instrument" and id(node) not in self.decorator_calls:
            emission = None
        elif emitter in {"bind_context", "span"} and id(node) not in (
            self.entered_context_calls | self.decorator_calls
        ):
            emission = None

        if emitter == "bind_context":
            attributes = self._check_attributes(node, emitter, "context:bind")
            if emission is not None:
                emission.attribute_keys.update(attributes)
            return
        if emitter in RECORDER_EMITTERS:
            if emission is None:
                return
            metric_name = self.vocabulary.recorder_metric_name(emitter)
            if metric_name is not None:
                emission.metric_names.add(metric_name)
            labels = self.vocabulary.recorder_metric_labels(emitter)
            if emitter == "record_outcome":
                operation = _call_argument(node, 1, "operation")
                if operation is None or (
                    isinstance(operation, ast.Constant) and operation.value is None
                ):
                    labels.pop("operation", None)
            emission.metric_label_keys.update(labels.values())
            return
        if emitter in {"span", "instrument"}:
            kind = "span"
            allowed = self.vocabulary.span_names
            legacy = self.vocabulary.legacy_span_names
        elif emitter == "counter_add":
            kind = "metric"
            allowed = self.vocabulary.metric_names
            legacy = frozenset()
        else:
            return

        name_node = _call_argument(node, 0, "name")
        name = _literal_name(name_node)
        canonical_name = name
        if kind == "span" and name is not None:
            canonical_name = self.vocabulary.maps.get("SPAN_NAME_ALIASES", {}).get(name, name)
        category = f"{kind}:{name if name is not None else '<dynamic>'}"
        site = category
        if name is None:
            self._add(
                "dynamic_signal_name",
                site,
                name_node or node,
                f"{kind} names must be literal",
            )
        elif canonical_name not in allowed:
            self._add(
                "unknown_signal_name",
                site,
                name_node or node,
                f"unknown {kind} name {name!r}",
            )
        elif name in self.vocabulary.maps.get("SPAN_NAME_ALIASES", {}):
            self._add(
                "legacy_span_name",
                site,
                name_node or node,
                f"legacy {kind} alias {name!r} requires an exact migration entry",
            )
        elif canonical_name in legacy:
            self._add(
                "legacy_span_name",
                site,
                name_node or node,
                f"legacy {kind} name {name!r} requires an exact migration entry",
            )
        if canonical_name in allowed and emission is not None:
            if kind == "span":
                emission.span_names.add(canonical_name)
            else:
                emission.metric_names.add(canonical_name)
        attributes = self._check_attributes(node, emitter, site)
        if emission is not None:
            if kind == "span":
                emission.attribute_keys.update(attributes)
            else:
                emission.metric_label_keys.update(attributes)

    def visit_Call(self, node: ast.Call) -> None:
        resolved = _resolved_name(node.func, self.bindings)
        self._check_configuration(node, resolved)
        self._check_print(node, resolved)
        self._check_logging(node)
        emitter = _obs_emitter(node, self.bindings)
        if emitter is not None:
            self._check_emitter(node, emitter)
        self.generic_visit(node)


def _analyze_sources(
    units: list[SourceUnit],
    vocabulary: Vocabulary,
    hosts: dict[str, frozenset[str]],
) -> tuple[list[Violation], dict[str, ScopeEmissions]]:
    violations: list[Violation] = []
    emissions: dict[str, ScopeEmissions] = defaultdict(ScopeEmissions)
    for unit in units:
        analyzer = _SourceAnalyzer(unit, vocabulary, hosts)
        analyzer.visit(unit.tree)
        violations.extend(analyzer.violations)
        for scope, observed in analyzer.emissions.items():
            emissions[scope].update(observed)
    return violations, dict(emissions)


_EMISSION_FIELDS = (
    "span_names",
    "attribute_keys",
    "metric_names",
    "metric_label_keys",
)


def _manifest_string_set(row: dict[str, Any], field_name: str) -> set[str]:
    value = row.get(field_name, [])
    if not isinstance(value, list):
        return set()
    return {item for item in value if isinstance(item, str)}


def _observed_emissions(
    scopes: list[str],
    emissions: dict[str, ScopeEmissions],
) -> ScopeEmissions:
    observed = ScopeEmissions()
    for scope in scopes:
        current = emissions.get(scope)
        if current is not None:
            observed.update(current)
    return observed


def _validate_emission_fields(
    row: dict[str, Any],
    scopes: list[str],
    emissions: dict[str, ScopeEmissions],
    result: AuditResult,
) -> None:
    if not scopes:
        return
    label = str(row["_label"])
    observed = _observed_emissions(scopes, emissions)
    for field_name in _EMISSION_FIELDS:
        declared = _manifest_string_set(row, field_name)
        actual = set(getattr(observed, field_name))
        if declared == actual:
            continue
        result.policy_errors.append(
            f"{label}.{field_name} does not match source emissions for {scopes!r}: "
            f"declared-only={sorted(declared - actual)!r}, "
            f"observed-only={sorted(actual - declared)!r}"
        )


def _validate_manifest_emissions(
    state: ManifestState,
    emissions: dict[str, ScopeEmissions],
    result: AuditResult,
) -> None:
    """Bind optional workflow claims and positive dispositions to exact source."""

    workflow_counts = Counter(str(row.get("id", "")) for row in state.workflows)
    workflows_by_id = {
        str(row.get("id")): row
        for row in state.workflows
        if isinstance(row.get("id"), str)
        and row.get("id")
        and workflow_counts[str(row.get("id"))] == 1
    }
    for workflow in state.workflows:
        scope = workflow.get("qualified_scope")
        _validate_emission_fields(
            workflow,
            [scope] if isinstance(scope, str) and scope else [],
            emissions,
            result,
        )

    for disposition in state.dispositions:
        workflow_ids = disposition.get("emission_workflows", [])
        if not isinstance(workflow_ids, list) or not workflow_ids:
            continue
        workflows = [
            workflows_by_id[workflow_id]
            for workflow_id in workflow_ids
            if isinstance(workflow_id, str)
            and workflow_id in workflows_by_id
            and workflows_by_id[workflow_id].get("owner") == disposition.get("owner")
        ]
        if len(workflows) != len(workflow_ids):
            continue
        scopes = [
            scope
            for workflow in workflows
            if isinstance((scope := workflow.get("qualified_scope")), str) and scope
        ]
        _validate_emission_fields(disposition, scopes, emissions, result)

        declared_signals = _manifest_string_set(disposition, "signals") & {
            "root",
            "child",
            "metric",
        }
        workflow_signals: set[str] = set()
        for workflow in workflows:
            workflow_signals.update(
                _manifest_string_set(workflow, "signals") & {"root", "child", "metric"}
            )
        if declared_signals != workflow_signals:
            result.policy_errors.append(
                f"{disposition['_label']}.signals does not match referenced workflows: "
                f"declared-only={sorted(declared_signals - workflow_signals)!r}, "
                f"workflow-only={sorted(workflow_signals - declared_signals)!r}"
            )


def _reconcile_legacy(
    findings: list[Violation],
    legacy: dict[tuple[str, str, str, str], dict[str, Any]],
    result: AuditResult,
) -> None:
    by_key: dict[tuple[str, str, str, str], list[Violation]] = defaultdict(list)
    for finding in findings:
        by_key[finding.key].append(finding)

    for key, matching in sorted(by_key.items()):
        if len(matching) > 1:
            locations = ", ".join(f"{item.path}:{item.line}" for item in matching)
            result.policy_errors.append(
                f"source findings collide on exact legacy key {key!r}: {locations}"
            )

    for key, finding_list in sorted(by_key.items()):
        if key in legacy and len(finding_list) == 1:
            result.exempted.append(finding_list[0])
        else:
            result.violations.extend(finding_list)

    for key, row in sorted(legacy.items()):
        matching = by_key.get(key, [])
        if len(matching) == 0:
            result.policy_errors.append(f"stale legacy exception {row['_label']}: {key!r}")
        elif len(matching) > 1:
            result.policy_errors.append(
                f"legacy exception {row['_label']} matches {len(matching)} findings: {key!r}"
            )

    result.violations.sort(
        key=lambda item: (item.path, item.line, item.rule, item.qualified_scope, item.target)
    )
    result.exempted.sort(
        key=lambda item: (item.path, item.line, item.rule, item.qualified_scope, item.target)
    )
    result.policy_errors.sort()


def audit_repository(
    manifest_root: Path = DEFAULT_MANIFEST_ROOT,
    *,
    repo_root: Path = ROOT,
) -> AuditResult:
    """Audit one repository without importing any shipped module."""

    root = repo_root.resolve()
    manifests = manifest_root.resolve()
    result = AuditResult()
    source_root, units = _parse_sources(root, result)
    vocabulary = _load_vocabulary(root, result)
    registered_family_scopes = _registered_top_level_family_scopes(root, result)
    protocols = _protocol_operations(
        units,
        source_root,
        registered_family_scopes,
        result,
    )
    callable_paths = _callable_scope_paths(units)
    state = _load_manifests(
        manifests,
        root,
        vocabulary,
        protocols,
        callable_paths,
        result,
    )
    findings, emissions = _analyze_sources(units, vocabulary, state.hosts)
    _validate_manifest_emissions(state, emissions, result)
    _reconcile_legacy(findings, state.legacy, result)
    return result


def _print_result(result: AuditResult) -> None:
    if result.ok:
        print(
            "Observability audit passed: "
            f"{result.files_scanned} files, "
            f"{result.protocols_scanned} protocols, "
            f"{result.operations_scanned} operations, "
            f"{result.workflows_scanned} workflows, "
            f"{len(result.exempted)} owned legacy exceptions."
        )
        return

    print("Observability audit failed.", file=sys.stderr)
    for error in result.policy_errors:
        print(f"POLICY: {error}", file=sys.stderr)
    for violation in result.violations:
        print(
            f"{violation.path}:{violation.line}: {violation.rule}: {violation.detail} "
            f"[scope={violation.qualified_scope}; target={violation.target}]",
            file=sys.stderr,
        )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--manifests",
        type=Path,
        default=DEFAULT_MANIFEST_ROOT,
        help="observability manifest directory",
    )
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=ROOT,
        help="repository root",
    )
    args = parser.parse_args(argv)
    result = audit_repository(args.manifests, repo_root=args.repo_root)
    _print_result(result)
    return 0 if result.ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
