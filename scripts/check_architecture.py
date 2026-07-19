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
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            found.extend((alias.name, node.lineno) for alias in node.names)
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


def _top_level_package_scopes(source_root: Path) -> frozenset[str]:
    package_root = source_root / "archetype"
    if not package_root.is_dir():
        return frozenset()
    return frozenset(
        f"archetype.{path.name}"
        for path in package_root.iterdir()
        if path.is_dir()
        and not path.name.startswith(".")
        and any(candidate.is_file() for candidate in path.rglob("*.py"))
    )


def _root_export_owners(tree: ast.AST | None) -> dict[str, str]:
    """Read the lazy root-facade export map without importing the package."""

    if tree is None:
        return {}
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
            return {}
        if not isinstance(exports, dict):
            return {}
        owners: dict[str, str] = {}
        for name, export in exports.items():
            if (
                isinstance(name, str)
                and isinstance(export, tuple)
                and len(export) == 2
                and isinstance(export[0], str)
            ):
                owners[name] = export[0]
        return owners
    return {}


def _load_policy(policy_path: Path) -> dict[str, Any]:
    with policy_path.open("rb") as handle:
        policy = tomllib.load(handle)
    if policy.get("version") not in {1, 2, 3}:
        raise ValueError("architecture policy version must be 1, 2, or 3")
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

    actual_top_level_packages = _top_level_package_scopes(source_root)
    root_tree = parsed.get("archetype", (None, None, False))[1]
    root_export_owners = _root_export_owners(root_tree)
    first_party_root_scopes = actual_top_level_packages | frozenset(
        module for module in parsed if module.startswith("archetype.") and module.count(".") == 1
    )

    policy_version = int(policy["version"])
    package_rules = policy.get("package_rule", [])
    family_rules = policy.get("family_rule", [])
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
        unclassified_packages = sorted(
            actual_top_level_packages - registered_family_scopes - reserved_infrastructure
        )
        if unclassified_packages:
            result.policy_errors.append(
                "unclassified first-party top-level packages: " + ", ".join(unclassified_packages)
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
            for dependency, line in imports:
                if any(_matches_prefix(dependency, str(prefix)) for prefix in rule["forbidden"]):
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
