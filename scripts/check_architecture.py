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


def _imports(tree: ast.AST, consumer: str, is_package: bool) -> list[tuple[str, int]]:
    found: list[tuple[str, int]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            found.extend((alias.name, node.lineno) for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            module = _resolve_import_from(node, consumer, is_package)
            if module:
                found.append((module, node.lineno))
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


def _load_policy(policy_path: Path) -> dict[str, Any]:
    with policy_path.open("rb") as handle:
        policy = tomllib.load(handle)
    if policy.get("version") not in {1, 2}:
        raise ValueError("architecture policy version must be 1 or 2")
    return policy


def _release_version(value: str) -> tuple[int, int, int] | None:
    match = re.fullmatch(r"v?(\d+)(?:\.(\d+))?(?:\.(\d+))?", value.strip())
    if match is None:
        return None
    major, minor, patch = match.groups()
    return (int(major), int(minor or 0), int(patch or 0))


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

    package_rules = policy.get("package_rule", [])
    family_rules = policy.get("family_rule", [])
    module_rules = policy.get("module_rule", [])
    concrete = policy.get("concrete_services", {})
    concrete_values = [str(value) for value in concrete.get("types", [])]
    concrete_types = set(concrete_values)
    composition_roots = set(concrete.get("composition_roots", []))
    if len(concrete_values) != len(concrete_types):
        result.policy_errors.append("concrete_services.types contains duplicate entries")

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
        imports = _imports(tree, consumer, is_package)

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
        missing_metadata = [
            field_name
            for field_name in ("owner", "reason", "expires")
            if not str(exception.get(field_name, "")).strip()
        ]
        if missing_metadata:
            result.policy_errors.append(
                f"architecture exception {key} lacks {', '.join(missing_metadata)}"
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
