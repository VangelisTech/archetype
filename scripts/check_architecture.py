# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Enforce the application architecture declared in quality/architecture.toml."""

from __future__ import annotations

import argparse
import ast
import re
import sys
import tomllib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_POLICY = ROOT / "quality" / "architecture.toml"
TOP_LEVEL_FAMILY_OUTWARD_PACKAGES = frozenset(
    {
        "archetype.app",
        "archetype.runtime",
        "archetype.api",
        "archetype.cli",
    }
)
REQUIRED_TOP_LEVEL_INFRASTRUCTURE = TOP_LEVEL_FAMILY_OUTWARD_PACKAGES | {"archetype.core"}
ROOT_EXPORT_POLICY_ERROR = (
    "unable to statically parse archetype._EXPORTS; root-facade import enforcement is degraded"
)
COMPONENT_BASES = frozenset(
    {
        "archetype.Component",
        "archetype.core.Component",
        "archetype.core.component.Component",
    }
)


@dataclass(frozen=True)
class Violation:
    """One exact architecture-policy violation."""

    rule: str
    consumer: str
    target: str
    path: Path
    line: int
    detail: str

    @property
    def key(self) -> tuple[str, str, str]:
        return (self.rule, self.consumer, self.target)


@dataclass
class AuditResult:
    """Architecture findings after applying declared migration exceptions."""

    violations: list[Violation] = field(default_factory=list)
    exempted: list[Violation] = field(default_factory=list)
    policy_errors: list[str] = field(default_factory=list)
    files_scanned: int = 0

    @property
    def ok(self) -> bool:
        return not self.violations and not self.policy_errors


def _matches_prefix(value: str, prefix: str) -> bool:
    return value == prefix or value.startswith(prefix + ".")


def _module_name(path: Path, source_root: Path) -> str:
    parts = list(path.relative_to(source_root).with_suffix("").parts)
    if parts[-1] == "__init__":
        parts.pop()
    return ".".join(parts)


def _resolve_import_from(node: ast.ImportFrom, consumer: str, is_package: bool) -> str:
    if node.level == 0:
        return node.module or ""

    package = consumer if is_package else consumer.rpartition(".")[0]
    parts = package.split(".") if package else []
    ascend = node.level - 1
    if ascend:
        parts = parts[:-ascend] if ascend <= len(parts) else []
    if node.module:
        parts.extend(node.module.split("."))
    return ".".join(parts)


def _imports(
    tree: ast.AST,
    consumer: str,
    is_package: bool,
    *,
    root_export_owners: dict[str, str],
    root_scopes: frozenset[str],
) -> list[tuple[str, int]]:
    found: list[tuple[str, int]] = []
    root_bindings = {
        alias.asname or "archetype"
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
        if alias.name == "archetype"
        or (alias.name.startswith("archetype.") and alias.asname is None)
    }
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            found.extend(
                (alias.name, node.lineno) for alias in node.names if alias.name != "archetype"
            )
        elif isinstance(node, ast.ImportFrom):
            module = _resolve_import_from(node, consumer, is_package)
            if module != "archetype":
                if module:
                    found.append((module, node.lineno))
                continue

            # Root-facade imports must receive the same disposition as their
            # owning modules. Python also permits ``from archetype import app``
            # for package attributes, so resolve both lazy public exports and
            # first-party root modules/packages here for every architecture
            # rule, not only the top-level-family rules.
            for alias in node.names:
                owner = root_export_owners.get(alias.name)
                candidate = f"archetype.{alias.name}"
                if owner:
                    found.append((owner, node.lineno))
                elif candidate in root_scopes:
                    found.append((candidate, node.lineno))
                else:
                    # Unknown root names remain first-party dependencies and
                    # are denied by registered-family default-deny handling.
                    found.append((candidate, node.lineno))
        elif isinstance(node, ast.Attribute) and root_bindings:
            dotted = _dotted_name(node)
            binding, separator, attribute_path = dotted.partition(".")
            if not separator or binding not in root_bindings:
                continue
            root_name = attribute_path.split(".", 1)[0]
            owner = root_export_owners.get(root_name)
            candidate = f"archetype.{root_name}"
            if owner:
                found.append((owner, node.lineno))
            elif candidate in root_scopes:
                found.append((candidate, node.lineno))
            else:
                # Attribute access after a bare root import is another root-
                # facade dependency form. Keep unknown names first-party so
                # registered-family default deny remains exhaustive.
                found.append((candidate, node.lineno))
    return found


def _interface_imports(
    tree: ast.AST,
    consumer: str,
    is_package: bool,
) -> list[tuple[str, str, int]]:
    """Return symbols imported from any family-owned ``interfaces`` module."""
    found: list[tuple[str, str, int]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name.endswith(".interfaces"):
                    found.append((alias.name, "*", node.lineno))
        elif isinstance(node, ast.ImportFrom):
            module = _resolve_import_from(node, consumer, is_package)
            if module.endswith(".interfaces"):
                found.extend((module, alias.name, node.lineno) for alias in node.names)
    return found


def _dotted_name(node: ast.AST) -> str:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        parent = _dotted_name(node.value)
        return f"{parent}.{node.attr}" if parent else node.attr
    return ""


def _bindings(tree: ast.AST, consumer: str, is_package: bool) -> dict[str, str]:
    bindings: dict[str, str] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                local = alias.asname or alias.name.split(".", 1)[0]
                bindings[local] = alias.name if alias.asname else local
        elif isinstance(node, ast.ImportFrom):
            module = _resolve_import_from(node, consumer, is_package)
            for alias in node.names:
                if alias.name == "*":
                    continue
                local = alias.asname or alias.name
                bindings[local] = f"{module}.{alias.name}" if module else alias.name
    return bindings


def _resolved_name(node: ast.AST, bindings: dict[str, str]) -> str:
    dotted = _dotted_name(node)
    if not dotted:
        return ""
    head, separator, tail = dotted.partition(".")
    resolved = bindings.get(head, head)
    return f"{resolved}.{tail}" if separator else resolved


def _source_files(source_root: Path) -> list[Path]:
    return sorted(path for path in source_root.rglob("*.py") if path.is_file())


def _top_level_first_party_scopes(source_root: Path) -> frozenset[str]:
    package_root = source_root / "archetype"
    if not package_root.is_dir():
        return frozenset()
    scopes: set[str] = set()
    for path in package_root.iterdir():
        if path.is_dir():
            if not path.name.startswith(".") and any(
                candidate.is_file() for candidate in path.rglob("*.py")
            ):
                scopes.add(f"archetype.{path.name}")
        elif path.is_file() and path.suffix == ".py" and path.name != "__init__.py":
            scopes.add(f"archetype.{path.stem}")
    return frozenset(scopes)


def _root_export_owners(tree: ast.AST | None) -> tuple[dict[str, str], str | None]:
    """Read the lazy root-facade export map without importing the package."""

    if tree is None:
        return {}, None
    for node in getattr(tree, "body", []):
        value: ast.AST | None = None
        if (
            isinstance(node, ast.AnnAssign)
            and isinstance(node.target, ast.Name)
            and node.target.id == "_EXPORTS"
        ):
            value = node.value
        elif isinstance(node, ast.Assign) and any(
            isinstance(target, ast.Name) and target.id == "_EXPORTS" for target in node.targets
        ):
            value = node.value
        if value is None:
            continue
        try:
            exports = ast.literal_eval(value)
        except (TypeError, ValueError):
            return {}, ROOT_EXPORT_POLICY_ERROR
        if not isinstance(exports, dict):
            return {}, ROOT_EXPORT_POLICY_ERROR
        owners: dict[str, str] = {}
        for name, export in exports.items():
            if not (
                isinstance(name, str)
                and bool(name)
                and isinstance(export, tuple)
                and len(export) == 2
                and all(isinstance(value, str) and bool(value) for value in export)
            ):
                return {}, ROOT_EXPORT_POLICY_ERROR
            owners[name] = export[0]
        return owners, None
    return {}, ROOT_EXPORT_POLICY_ERROR


FRAGMENT_SECTIONS = frozenset(
    {
        "capability_rule",
        "exception",
        "family_rule",
        "module_rule",
        "package_rule",
        "top_level_family_rule",
    }
)
_UNIQUE_RULE_KEYS = (
    ("capability_rule", "name"),
    ("family_rule", "name"),
    ("top_level_family_rule", "name"),
    ("package_rule", "name"),
    ("module_rule", "module"),
)


def _fragment_dir(policy_path: Path) -> Path:
    return policy_path.parent / f"{policy_path.stem}.d"


def _load_policy(policy_path: Path) -> dict[str, Any]:
    with policy_path.open("rb") as handle:
        policy = tomllib.load(handle)
    if policy.get("version") not in {1, 2, 3}:
        raise ValueError("architecture policy version must be 1, 2, or 3")
    fragment_dir = _fragment_dir(policy_path)
    fragment_paths = sorted(fragment_dir.glob("*.toml")) if fragment_dir.is_dir() else []
    for fragment_path in fragment_paths:
        with fragment_path.open("rb") as handle:
            fragment = tomllib.load(handle)
        for section, rows in fragment.items():
            if section not in FRAGMENT_SECTIONS:
                raise ValueError(
                    f"architecture fragment {fragment_path.name} declares {section!r}; "
                    f"fragments may declare only {sorted(FRAGMENT_SECTIONS)}"
                )
            if not isinstance(rows, list):
                raise ValueError(
                    f"architecture fragment {fragment_path.name} section {section!r} "
                    "must be an array of tables"
                )
            base = policy.setdefault(section, [])
            if not isinstance(base, list):
                raise ValueError(
                    f"architecture policy section {section!r} must be an array of tables"
                )
            base.extend(rows)
    for section, key in _UNIQUE_RULE_KEYS:
        seen: set[str] = set()
        for row in policy.get(section, []):
            value = row.get(key) if isinstance(row, dict) else None
            if not isinstance(value, str):
                continue
            if value in seen:
                raise ValueError(
                    f"duplicate {section} {key} {value!r} across the architecture "
                    "policy and its fragments"
                )
            seen.add(value)
    return policy


def _release_version(value: str) -> tuple[int, int, int] | None:
    match = re.fullmatch(r"v?(\d+)(?:\.(\d+))?(?:\.(\d+))?", value.strip())
    if match is None:
        return None
    major, minor, patch = match.groups()
    return (int(major), int(minor or 0), int(patch or 0))


def _dependency_cycle(
    graph: dict[str, frozenset[str]],
) -> tuple[str, ...] | None:
    """Return one deterministic cycle from a directed dependency graph."""

    state: dict[str, int] = {}
    stack: list[str] = []
    stack_indexes: dict[str, int] = {}

    def visit(node: str) -> tuple[str, ...] | None:
        state[node] = 1
        stack_indexes[node] = len(stack)
        stack.append(node)
        for dependency in sorted(graph.get(node, frozenset())):
            dependency_state = state.get(dependency, 0)
            if dependency_state == 0:
                cycle = visit(dependency)
                if cycle is not None:
                    return cycle
            elif dependency_state == 1:
                start = stack_indexes[dependency]
                return (*stack[start:], dependency)
        stack.pop()
        stack_indexes.pop(node)
        state[node] = 2
        return None

    for node in sorted(graph):
        if state.get(node, 0) != 0:
            continue
        cycle = visit(node)
        if cycle is not None:
            return cycle
    return None


def _project_version(repo_root: Path) -> tuple[int, int, int] | None:
    project_file = repo_root / "pyproject.toml"
    if not project_file.is_file():
        return None
    with project_file.open("rb") as handle:
        value = tomllib.load(handle).get("project", {}).get("version")
    return _release_version(str(value)) if value is not None else None


def _add_once(values: list[Violation], seen: set[tuple[str, str, str]], value: Violation) -> None:
    if value.key not in seen:
        seen.add(value.key)
        values.append(value)


def _enclosing_function(
    tree: ast.AST, node: ast.AST
) -> ast.FunctionDef | ast.AsyncFunctionDef | None:
    """Return the innermost lexical function containing ``node``."""

    line = getattr(node, "lineno", 0)
    end_line = getattr(node, "end_lineno", line)
    candidates = [
        candidate
        for candidate in ast.walk(tree)
        if isinstance(candidate, ast.FunctionDef | ast.AsyncFunctionDef)
        and candidate.lineno <= line
        and (candidate.end_lineno or candidate.lineno) >= end_line
    ]
    return max(candidates, key=lambda candidate: candidate.lineno, default=None)


def _lexical_nodes(scope: ast.FunctionDef | ast.AsyncFunctionDef) -> list[ast.AST]:
    """Walk one function body without borrowing bindings from nested scopes."""

    found: list[ast.AST] = []

    def visit(node: ast.AST) -> None:
        found.append(node)
        for child in ast.iter_child_nodes(node):
            if child is not scope and isinstance(
                child,
                ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef | ast.Lambda,
            ):
                continue
            visit(child)

    visit(scope)
    return found


def _assigned_names(node: ast.AST) -> set[str]:
    """Return simple local names overwritten by one assignment-like node."""

    targets: list[ast.AST] = []
    if isinstance(node, ast.Assign):
        targets.extend(node.targets)
    elif isinstance(node, ast.AnnAssign | ast.AugAssign | ast.NamedExpr):
        targets.append(node.target)
    elif isinstance(node, ast.For | ast.AsyncFor):
        targets.append(node.target)
    elif isinstance(node, ast.comprehension):
        targets.append(node.target)
    elif isinstance(node, ast.withitem) and node.optional_vars is not None:
        targets.append(node.optional_vars)

    names: set[str] = set()
    for target in targets:
        for candidate in ast.walk(target):
            if isinstance(candidate, ast.Name):
                names.add(candidate.id)
    if isinstance(node, ast.ExceptHandler) and node.name:
        names.add(node.name)
    return names


def _assignment_value(node: ast.AST) -> ast.AST | None:
    if isinstance(node, ast.Assign | ast.AnnAssign | ast.NamedExpr):
        return node.value
    return None


def _is_awaited_method(node: ast.AST | None, method: str) -> bool:
    return (
        isinstance(node, ast.Await)
        and isinstance(node.value, ast.Call)
        and isinstance(node.value.func, ast.Attribute)
        and node.value.func.attr == method
    )


def _is_mediated_receiver(
    tree: ast.AST,
    attribute: ast.Attribute,
    mediator: str,
) -> bool:
    """Prove that a terminal receiver came from an awaited mediator call.

    An inline terminal such as
    ``(await storage.materialize(df)).to_pydict()`` is accepted. A local
    receiver is accepted only when its latest earlier binding in the same
    lexical function is that awaited call; any intervening assignment revokes
    the proof.
    """

    if _is_awaited_method(attribute.value, mediator):
        return True
    if not isinstance(attribute.value, ast.Name):
        return False
    scope = _enclosing_function(tree, attribute)
    if scope is None:
        return False

    receiver = attribute.value.id
    latest: ast.AST | None = None
    latest_line = -1
    for candidate in _lexical_nodes(scope):
        line = getattr(candidate, "lineno", -1)
        if line < 0 or line >= attribute.lineno or receiver not in _assigned_names(candidate):
            continue
        if line >= latest_line:
            latest = candidate
            latest_line = line
    return _is_awaited_method(_assignment_value(latest) if latest is not None else None, mediator)


def audit_repository(
    policy_path: Path = DEFAULT_POLICY,
    *,
    repo_root: Path = ROOT,
) -> AuditResult:
    """Audit one repository against an architecture policy."""

    result = AuditResult()
    policy = _load_policy(policy_path)
    source_root = repo_root / str(policy.get("source_root", "src"))
    if not source_root.is_dir():
        result.policy_errors.append(f"source_root does not exist: {source_root}")
        return result

    files = _source_files(source_root)
    result.files_scanned = len(files)
    if not files:
        result.policy_errors.append(f"source_root contains no Python files: {source_root}")
        return result

    parsed: dict[str, tuple[Path, ast.AST, bool]] = {}
    for path in files:
        module = _module_name(path, source_root)
        parsed[module] = (
            path,
            ast.parse(path.read_text(encoding="utf-8"), filename=str(path)),
            path.name == "__init__.py",
        )

    actual_top_level_scopes = _top_level_first_party_scopes(source_root)
    root_tree = parsed.get("archetype", (None, None, False))[1]
    root_export_owners, root_export_error = _root_export_owners(root_tree)
    if root_export_error is not None:
        result.policy_errors.append(root_export_error)
    first_party_root_scopes = actual_top_level_scopes | frozenset(
        module for module in parsed if module.startswith("archetype.") and module.count(".") == 1
    )

    policy_version = int(policy["version"])
    package_rules = policy.get("package_rule", [])
    family_rules = policy.get("family_rule", [])
    capability_rules = policy.get("capability_rule", [])
    if not isinstance(capability_rules, list):
        result.policy_errors.append("capability_rule must be an array of tables")
        capability_rules = []
    top_level_family_rules = policy.get("top_level_family_rule", [])
    if not isinstance(top_level_family_rules, list):
        result.policy_errors.append("top_level_family_rule must be an array of tables")
        top_level_family_rules = []
    module_rules = policy.get("module_rule", [])
    concrete = policy.get("concrete_services", {})
    concrete_values = [str(value) for value in concrete.get("types", [])]
    concrete_types = set(concrete_values)
    composition_roots = set(concrete.get("composition_roots", []))
    if len(concrete_values) != len(concrete_types):
        result.policy_errors.append("concrete_services.types contains duplicate entries")

    top_level_family_config = policy.get("top_level_family_policy", {})
    if not isinstance(top_level_family_config, dict):
        result.policy_errors.append("top_level_family_policy must be a table")
        top_level_family_config = {}
    configured_outward = top_level_family_config.get("forbidden_outward", [])
    if not isinstance(configured_outward, list):
        result.policy_errors.append("top_level_family_policy.forbidden_outward must be a list")
        configured_outward = []
    outward_values = [str(value).strip() for value in configured_outward]
    outward_packages = frozenset(value for value in outward_values if value)
    if len(outward_values) != len(outward_packages):
        result.policy_errors.append(
            "top_level_family_policy.forbidden_outward contains empty or duplicate entries"
        )
    invalid_outward = sorted(
        value
        for value in outward_packages
        if re.fullmatch(r"archetype\.[A-Za-z_][A-Za-z0-9_]*", value) is None
    )
    if invalid_outward:
        result.policy_errors.append(
            "top_level_family_policy.forbidden_outward has non-top-level scopes: "
            + ", ".join(invalid_outward)
        )

    configured_infrastructure = top_level_family_config.get("reserved_infrastructure", [])
    if not isinstance(configured_infrastructure, list):
        result.policy_errors.append(
            "top_level_family_policy.reserved_infrastructure must be a list"
        )
        configured_infrastructure = []
    infrastructure_values = [str(value).strip() for value in configured_infrastructure]
    reserved_infrastructure = frozenset(value for value in infrastructure_values if value)
    if len(infrastructure_values) != len(reserved_infrastructure):
        result.policy_errors.append(
            "top_level_family_policy.reserved_infrastructure contains empty or duplicate entries"
        )
    invalid_infrastructure = sorted(
        value
        for value in reserved_infrastructure
        if re.fullmatch(r"archetype\.[A-Za-z_][A-Za-z0-9_]*", value) is None
    )
    if invalid_infrastructure:
        result.policy_errors.append(
            "top_level_family_policy.reserved_infrastructure has non-top-level scopes: "
            + ", ".join(invalid_infrastructure)
        )
    if policy_version >= 3:
        missing_outward = sorted(TOP_LEVEL_FAMILY_OUTWARD_PACKAGES - outward_packages)
        if missing_outward:
            result.policy_errors.append(
                "top_level_family_policy.forbidden_outward omits required packages: "
                + ", ".join(missing_outward)
            )
        missing_infrastructure = sorted(REQUIRED_TOP_LEVEL_INFRASTRUCTURE - reserved_infrastructure)
        if missing_infrastructure:
            result.policy_errors.append(
                "top_level_family_policy.reserved_infrastructure omits required packages: "
                + ", ".join(missing_infrastructure)
            )
        outward_not_reserved = sorted(outward_packages - reserved_infrastructure)
        if outward_not_reserved:
            result.policy_errors.append(
                "top_level_family_policy.forbidden_outward has unclassified packages: "
                + ", ".join(outward_not_reserved)
            )
        if not top_level_family_rules:
            result.policy_errors.append("architecture policy registers no top-level family scopes")
    if not outward_packages:
        outward_packages = TOP_LEVEL_FAMILY_OUTWARD_PACKAGES

    top_level_family_names: set[str] = set()
    top_level_family_scopes: dict[str, frozenset[str]] = {}
    pending_allowed_families: dict[str, list[str]] = {}
    reserved_family_scopes = reserved_infrastructure | REQUIRED_TOP_LEVEL_INFRASTRUCTURE
    for index, rule in enumerate(top_level_family_rules):
        name = str(rule.get("name", "")).strip()
        label = repr(name) if name else f"at index {index}"
        if not name:
            result.policy_errors.append(f"top-level family rule {label} has an empty name")
        elif name in top_level_family_names:
            result.policy_errors.append(f"duplicate top-level family rule name: {name}")
        else:
            top_level_family_names.add(name)

        if "consumer" not in rule:
            result.policy_errors.append(
                f"top-level family rule {label} is missing its consumer scope"
            )
            continue
        consumer_scope = str(rule.get("consumer", "")).strip()
        if not consumer_scope:
            result.policy_errors.append(
                f"top-level family rule {label} has an empty consumer scope"
            )
            continue
        if re.fullmatch(r"archetype\.[A-Za-z_][A-Za-z0-9_]*", consumer_scope) is None:
            result.policy_errors.append(
                f"top-level family rule {label} has non-top-level scope: {consumer_scope}"
            )
            continue
        if consumer_scope in reserved_family_scopes:
            result.policy_errors.append(
                f"top-level family rule {label} uses reserved scope: {consumer_scope}"
            )
            continue
        if consumer_scope in top_level_family_scopes:
            result.policy_errors.append(f"duplicate top-level family scope: {consumer_scope}")
            continue

        matched = [
            tree
            for module, (_path, tree, _is_package) in parsed.items()
            if _matches_prefix(module, consumer_scope)
        ]
        if not matched:
            result.policy_errors.append(
                f"top-level family rule {label} references stale scope: {consumer_scope}"
            )
        elif not any(getattr(tree, "body", []) for tree in matched):
            result.policy_errors.append(
                f"top-level family rule {label} matched an empty source scope: {consumer_scope}"
            )

        if "allowed_families" not in rule:
            result.policy_errors.append(
                f"top-level family rule {label} lacks an exact allowed_families disposition"
            )
            allowed_values: list[Any] = []
        else:
            configured_allowed = rule.get("allowed_families")
            if not isinstance(configured_allowed, list):
                result.policy_errors.append(
                    f"top-level family rule {label} allowed_families must be a list"
                )
                allowed_values = []
            else:
                allowed_values = configured_allowed
        allowed_families = [str(value).strip() for value in allowed_values]
        if any(not value for value in allowed_families):
            result.policy_errors.append(
                f"top-level family rule {label} has an empty allowed family scope"
            )
        if len(allowed_families) != len(set(allowed_families)):
            result.policy_errors.append(
                f"top-level family rule {label} has duplicate allowed family scopes"
            )
        top_level_family_scopes[consumer_scope] = frozenset()
        pending_allowed_families[consumer_scope] = allowed_families

    registered_family_scopes = frozenset(top_level_family_scopes)
    for consumer_scope, allowed_values in pending_allowed_families.items():
        label = next(
            (
                str(rule.get("name", consumer_scope))
                for rule in top_level_family_rules
                if str(rule.get("consumer", "")).strip() == consumer_scope
            ),
            consumer_scope,
        )
        allowed = frozenset(value for value in allowed_values if value)
        unknown = sorted(allowed - registered_family_scopes)
        if unknown:
            result.policy_errors.append(
                f"top-level family rule {label!r} allows unregistered family scopes: "
                + ", ".join(unknown)
            )
        if consumer_scope in allowed:
            result.policy_errors.append(
                f"top-level family rule {label!r} redundantly allows its own scope"
            )
        top_level_family_scopes[consumer_scope] = allowed

    if policy_version >= 3:
        overlapping_scopes = sorted(registered_family_scopes & reserved_infrastructure)
        if overlapping_scopes:
            result.policy_errors.append(
                "top-level scopes classified as both family and reserved infrastructure: "
                + ", ".join(overlapping_scopes)
            )
        unclassified_scopes = sorted(
            actual_top_level_scopes - registered_family_scopes - reserved_infrastructure
        )
        if unclassified_scopes:
            result.policy_errors.append(
                "unclassified first-party top-level scopes: " + ", ".join(unclassified_scopes)
            )

        graph = {
            consumer_scope: frozenset(
                dependency
                for dependency in allowed
                if dependency in registered_family_scopes and dependency != consumer_scope
            )
            for consumer_scope, allowed in top_level_family_scopes.items()
        }
        cycle = _dependency_cycle(graph)
        if cycle is not None:
            result.policy_errors.append("top-level family dependency cycle: " + " -> ".join(cycle))

    governed_root_scopes = (
        first_party_root_scopes
        | registered_family_scopes
        | reserved_infrastructure
        | outward_packages
    )

    for rule in package_rules:
        consumer_prefix = str(rule["consumer"])
        if not any(_matches_prefix(module, consumer_prefix) for module in parsed):
            result.policy_errors.append(
                f"package rule {rule.get('name', consumer_prefix)!r} matched no source modules"
            )
    for rule in family_rules:
        consumer_prefix = str(rule["consumer"])
        if not any(_matches_prefix(module, consumer_prefix) for module in parsed):
            result.policy_errors.append(
                f"family rule {rule.get('name', consumer_prefix)!r} matched no source modules"
            )

    known_interfaces: set[str] = set()
    for module, (_path, tree, _is_package) in parsed.items():
        if not module.endswith(".interfaces"):
            continue
        for node in tree.body:  # type: ignore[attr-defined]
            if isinstance(node, ast.ClassDef | ast.FunctionDef | ast.AsyncFunctionDef):
                known_interfaces.add(f"{module}.{node.name}")
            elif isinstance(node, ast.Assign):
                known_interfaces.update(
                    f"{module}.{target.id}"
                    for target in node.targets
                    if isinstance(target, ast.Name)
                )
            elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
                known_interfaces.add(f"{module}.{node.target.id}")

    module_policy: dict[str, tuple[tuple[str, ...], frozenset[str]]] = {}
    for rule in module_rules:
        module = str(rule["module"])
        if module in module_policy:
            result.policy_errors.append(f"duplicate module rule: {module}")
            continue
        allowed_app = tuple(str(value) for value in rule.get("allowed_app", []))
        allowed_interfaces = frozenset(str(value) for value in rule.get("allowed_interfaces", []))
        module_policy[module] = (allowed_app, allowed_interfaces)
        if module not in parsed:
            result.policy_errors.append(f"module rule references missing module: {module}")
        unknown_interfaces = sorted(allowed_interfaces - known_interfaces)
        if unknown_interfaces:
            result.policy_errors.append(
                f"module rule {module} allows unknown interfaces: " + ", ".join(unknown_interfaces)
            )

    capability_policy: list[
        tuple[str, str, str, frozenset[str], frozenset[str], dict[str, str]]
    ] = []
    capability_names: set[str] = set()
    for index, rule in enumerate(capability_rules):
        name = str(rule.get("name", "")).strip()
        label = repr(name) if name else f"at index {index}"
        consumer_scope = str(rule.get("consumer", "")).strip()
        owner = str(rule.get("owner", "")).strip()
        owned_attributes = frozenset(
            str(value).strip() for value in rule.get("owned_attributes", [])
        )
        owned_symbols = frozenset(str(value).strip() for value in rule.get("owned_symbols", []))
        raw_mediated = rule.get("mediated_attributes", {})
        mediated_attributes = (
            {str(key).strip(): str(value).strip() for key, value in raw_mediated.items()}
            if isinstance(raw_mediated, dict)
            else {}
        )

        if not name:
            result.policy_errors.append(f"capability rule {label} has an empty name")
        elif name in capability_names:
            result.policy_errors.append(f"duplicate capability rule name: {name}")
        capability_names.add(name)
        if not consumer_scope:
            result.policy_errors.append(f"capability rule {label} has an empty consumer scope")
        elif not any(_matches_prefix(module, consumer_scope) for module in parsed):
            result.policy_errors.append(
                f"capability rule {label} references stale consumer scope: {consumer_scope}"
            )
        if not owner:
            result.policy_errors.append(f"capability rule {label} has an empty owner")
        elif owner not in parsed:
            result.policy_errors.append(
                f"capability rule {label} references missing owner module: {owner}"
            )
        elif consumer_scope and not _matches_prefix(owner, consumer_scope):
            result.policy_errors.append(
                f"capability rule {label} owner {owner} is outside {consumer_scope}"
            )
        if not isinstance(raw_mediated, dict):
            result.policy_errors.append(
                f"capability rule {label} mediated_attributes must be a table"
            )
        empty_values = sorted(
            value
            for value in (
                *owned_attributes,
                *owned_symbols,
                *mediated_attributes.keys(),
                *mediated_attributes.values(),
            )
            if not value
        )
        if empty_values:
            result.policy_errors.append(f"capability rule {label} contains empty capability names")
        if not owned_attributes and not owned_symbols and not mediated_attributes:
            result.policy_errors.append(f"capability rule {label} owns no capabilities")
        capability_policy.append(
            (
                name,
                consumer_scope,
                owner,
                owned_attributes,
                owned_symbols,
                mediated_attributes,
            )
        )

    for module, (_path, tree, _is_package) in parsed.items():
        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef) and node.name in concrete_types:
                if module not in composition_roots and module not in module_policy:
                    result.policy_errors.append(
                        f"concrete service {node.name} in {module} has no module rule"
                    )

    findings: list[Violation] = []
    seen: set[tuple[str, str, str]] = set()
    for consumer, (path, tree, is_package) in parsed.items():
        imports = _imports(
            tree,
            consumer,
            is_package,
            root_export_owners=root_export_owners,
            root_scopes=governed_root_scopes,
        )

        consumer_family_scope = next(
            (scope for scope in registered_family_scopes if _matches_prefix(consumer, scope)),
            None,
        )
        if consumer_family_scope is not None:
            allowed_families = top_level_family_scopes[consumer_family_scope]
            for dependency, line in imports:
                if any(_matches_prefix(dependency, prefix) for prefix in outward_packages):
                    _add_once(
                        findings,
                        seen,
                        Violation(
                            rule="top_level_family_outward_dependency",
                            consumer=consumer,
                            target=dependency,
                            path=path,
                            line=line,
                            detail=(
                                f"{consumer} imports outward package {dependency}; "
                                "top-level domain families may depend only on core, "
                                "themselves, and declared lower family contracts"
                            ),
                        ),
                    )
                    continue

                if _matches_prefix(dependency, "archetype.core") or _matches_prefix(
                    dependency, consumer_family_scope
                ):
                    continue

                dependency_family_scope = next(
                    (
                        scope
                        for scope in registered_family_scopes
                        if _matches_prefix(dependency, scope)
                    ),
                    None,
                )
                if (
                    dependency_family_scope is not None
                    and dependency_family_scope in allowed_families
                ):
                    continue
                if not _matches_prefix(dependency, "archetype"):
                    continue
                _add_once(
                    findings,
                    seen,
                    Violation(
                        rule="top_level_family_dependency",
                        consumer=consumer,
                        target=dependency,
                        path=path,
                        line=line,
                        detail=(
                            f"{consumer} imports undeclared first-party dependency "
                            f"{dependency}; classify every top-level package and declare "
                            "each reviewed lower-family contract edge in "
                            "quality/architecture.toml"
                        ),
                    ),
                )

        for rule in package_rules:
            if not _matches_prefix(consumer, str(rule["consumer"])):
                continue
            forbidden = {str(prefix) for prefix in rule["forbidden"]}
            if policy_version >= 3 and str(rule["consumer"]) == "archetype.core":
                # The domain-family registry is the source of truth for the
                # foundation's reverse-dependency ban. A newly registered
                # family must be forbidden without a second manifest edit.
                forbidden.update(registered_family_scopes)
            for dependency, line in imports:
                if any(_matches_prefix(dependency, prefix) for prefix in forbidden):
                    _add_once(
                        findings,
                        seen,
                        Violation(
                            rule="package_dependency",
                            consumer=consumer,
                            target=dependency,
                            path=path,
                            line=line,
                            detail=(
                                f"{consumer} must not import outward dependency {dependency}; "
                                "move shared types downward or inject a lower-layer port"
                            ),
                        ),
                    )

        for rule in family_rules:
            if not _matches_prefix(consumer, str(rule["consumer"])):
                continue
            allowed = tuple(str(prefix) for prefix in rule.get("allowed_app", []))
            for dependency, line in imports:
                if not _matches_prefix(dependency, "archetype.app"):
                    continue
                if any(_matches_prefix(dependency, prefix) for prefix in allowed):
                    continue
                _add_once(
                    findings,
                    seen,
                    Violation(
                        rule="family_dependency",
                        consumer=consumer,
                        target=dependency,
                        path=path,
                        line=line,
                        detail=(
                            f"{consumer} crosses the declared family DAG to {dependency}; "
                            "move the contract to an allowed family or inject its port"
                        ),
                    ),
                )

        module_permissions = module_policy.get(consumer)
        if module_permissions is not None:
            allowed_app, allowed_interfaces = module_permissions
            for dependency, line in imports:
                if not _matches_prefix(dependency, "archetype.app") or dependency == consumer:
                    continue
                if dependency.endswith(".interfaces"):
                    continue
                if any(_matches_prefix(dependency, prefix) for prefix in allowed_app):
                    continue
                _add_once(
                    findings,
                    seen,
                    Violation(
                        rule="module_dependency",
                        consumer=consumer,
                        target=dependency,
                        path=path,
                        line=line,
                        detail=(
                            f"{consumer} imports unapproved app dependency {dependency}; "
                            "depend on the declared port instead"
                        ),
                    ),
                )

            for interface_module, interface, line in _interface_imports(tree, consumer, is_package):
                target = f"{interface_module}.{interface}"
                if target in allowed_interfaces:
                    continue
                _add_once(
                    findings,
                    seen,
                    Violation(
                        rule="interface_dependency",
                        consumer=consumer,
                        target=target,
                        path=path,
                        line=line,
                        detail=(
                            f"{consumer} imports unapproved interface {target}; "
                            "use only the consumer's declared ports"
                        ),
                    ),
                )

        bindings = _bindings(tree, consumer, is_package)

        for (
            capability_name,
            capability_consumer,
            capability_owner,
            owned_attributes,
            owned_symbols,
            mediated_attributes,
        ) in capability_policy:
            if consumer == capability_owner or not _matches_prefix(consumer, capability_consumer):
                continue
            for node in ast.walk(tree):
                if isinstance(node, ast.Attribute):
                    if node.attr in owned_attributes:
                        _add_once(
                            findings,
                            seen,
                            Violation(
                                rule="capability_ownership",
                                consumer=consumer,
                                target=node.attr,
                                path=path,
                                line=node.lineno,
                                detail=(
                                    f"{consumer} references storage-owned execution capability "
                                    f"{node.attr}; route it through {capability_owner} "
                                    f"({capability_name})"
                                ),
                            ),
                        )
                    mediator = mediated_attributes.get(node.attr)
                    if mediator is not None and not _is_mediated_receiver(tree, node, mediator):
                        _add_once(
                            findings,
                            seen,
                            Violation(
                                rule="capability_mediation",
                                consumer=consumer,
                                target=node.attr,
                                path=path,
                                line=node.lineno,
                                detail=(
                                    f"{consumer} references {node.attr} without first awaiting "
                                    f"{capability_owner}.{mediator}; application terminal reads "
                                    "must enter Python only after storage admits execution"
                                ),
                            ),
                        )

                if not isinstance(node, ast.Name | ast.Attribute):
                    continue
                resolved = _resolved_name(node, bindings)
                if resolved not in owned_symbols:
                    continue
                _add_once(
                    findings,
                    seen,
                    Violation(
                        rule="capability_ownership",
                        consumer=consumer,
                        target=resolved,
                        path=path,
                        line=node.lineno,
                        detail=(
                            f"{consumer} references storage-owned execution capability "
                            f"{resolved}; route it through {capability_owner} "
                            f"({capability_name})"
                        ),
                    ),
                )

        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                target = _resolved_name(node.func, bindings).rsplit(".", 1)[-1]
                if target in concrete_types and consumer not in composition_roots:
                    _add_once(
                        findings,
                        seen,
                        Violation(
                            rule="concrete_construction",
                            consumer=consumer,
                            target=target,
                            path=path,
                            line=node.lineno,
                            detail=(
                                f"{consumer} constructs concrete {target} outside a composition "
                                "root; inject a declared port instead"
                            ),
                        ),
                    )

            elif isinstance(node, ast.ClassDef):
                if _matches_prefix(consumer, "archetype.app"):
                    for base in node.bases:
                        if _resolved_name(base, bindings) not in COMPONENT_BASES:
                            continue
                        _add_once(
                            findings,
                            seen,
                            Violation(
                                rule="app_component_model",
                                consumer=consumer,
                                target=f"{consumer}.{node.name}",
                                path=path,
                                line=node.lineno,
                                detail=(
                                    f"{consumer}.{node.name} is a persistent Component "
                                    "declared in the application layer; move reusable ECS "
                                    "schema to archetype.<family>.components"
                                ),
                            ),
                        )

                for base in node.bases:
                    target = _resolved_name(base, bindings).rsplit(".", 1)[-1]
                    if target in concrete_types:
                        _add_once(
                            findings,
                            seen,
                            Violation(
                                rule="concrete_inheritance",
                                consumer=consumer,
                                target=target,
                                path=path,
                                line=node.lineno,
                                detail=(
                                    f"{consumer}.{node.name} inherits concrete service {target}; "
                                    "compose the collaborator instead"
                                ),
                            ),
                        )

                if node.name in concrete_types:
                    for statement in node.body:
                        if not isinstance(statement, ast.FunctionDef | ast.AsyncFunctionDef):
                            continue
                        if statement.name != "__init__":
                            continue
                        arguments = [*statement.args.posonlyargs, *statement.args.args]
                        if statement.args.vararg is not None:
                            arguments.append(statement.args.vararg)
                        if statement.args.kwarg is not None:
                            arguments.append(statement.args.kwarg)
                        arguments.extend(statement.args.kwonlyargs)
                        for argument in arguments:
                            if argument.arg in {"self", "cls"} or argument.annotation is None:
                                continue
                            target = _resolved_name(argument.annotation, bindings).rsplit(".", 1)[
                                -1
                            ]
                            if target not in concrete_types:
                                continue
                            _add_once(
                                findings,
                                seen,
                                Violation(
                                    rule="concrete_annotation",
                                    consumer=consumer,
                                    target=target,
                                    path=path,
                                    line=argument.lineno,
                                    detail=(
                                        f"{consumer}.{node.name} types constructor dependency "
                                        f"{argument.arg} as concrete {target}; annotate the "
                                        "family-owned protocol instead"
                                    ),
                                ),
                            )

    current_version = _project_version(repo_root)
    declared: dict[tuple[str, str, str], dict[str, Any]] = {}
    for exception in policy.get("exception", []):
        key = (
            str(exception.get("rule", "")),
            str(exception.get("consumer", "")),
            str(exception.get("target", "")),
        )
        if key in declared:
            result.policy_errors.append(f"duplicate architecture exception: {key}")
            continue
        if policy_version >= 3 and any("*" in coordinate for coordinate in key):
            result.policy_errors.append(
                f"architecture exception {key} uses a wildcard instead of an exact edge"
            )
        required_metadata = ["owner", "reason", "expires"]
        if policy_version >= 3:
            required_metadata.extend(["issue", "expiry_condition"])
        missing_metadata = [
            field_name
            for field_name in required_metadata
            if not str(exception.get(field_name, "")).strip()
        ]
        if missing_metadata:
            result.policy_errors.append(
                f"architecture exception {key} lacks {', '.join(missing_metadata)}"
            )
        if policy_version >= 3 and "issue" in exception:
            issue = exception.get("issue")
            if isinstance(issue, bool) or not isinstance(issue, int) or issue <= 0:
                result.policy_errors.append(
                    f"architecture exception {key} has invalid tracking issue: {issue!r}"
                )
        expires = _release_version(str(exception.get("expires", "")))
        if expires is None:
            result.policy_errors.append(
                f"architecture exception {key} has invalid release expiry: "
                f"{exception.get('expires')!r}"
            )
        elif current_version is None:
            result.policy_errors.append(
                f"cannot evaluate architecture exception {key}: project version is unavailable"
            )
        elif current_version >= expires:
            result.policy_errors.append(
                f"architecture exception {key} expired at "
                f"{exception.get('expires')} (project is "
                f"{'.'.join(str(part) for part in current_version)})"
            )
        declared[key] = exception

    used: set[tuple[str, str, str]] = set()
    for finding in findings:
        if finding.key in declared:
            result.exempted.append(finding)
            used.add(finding.key)
        else:
            result.violations.append(finding)

    for key in sorted(set(declared) - used):
        result.policy_errors.append(
            "stale architecture exception matched no violation: " + " | ".join(key)
        )

    return result


def _print_result(result: AuditResult, repo_root: Path) -> None:
    if result.policy_errors:
        print("Architecture policy errors:")
        for error in result.policy_errors:
            print(f"  - {error}")
    if result.violations:
        print("Architecture violations:")
        for violation in result.violations:
            path = violation.path.relative_to(repo_root)
            print(
                f"  - {path}:{violation.line}: {violation.detail} "
                f"[{violation.rule}; target={violation.target}]"
            )

    if result.ok:
        print(
            "Architecture audit passed "
            f"({result.files_scanned} files, {len(result.exempted)} owned migration exceptions)."
        )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--policy", type=Path, default=DEFAULT_POLICY)
    args = parser.parse_args(argv)
    result = audit_repository(args.policy.resolve(), repo_root=ROOT)
    _print_result(result, ROOT)
    return 0 if result.ok else 1


if __name__ == "__main__":
    sys.exit(main())
