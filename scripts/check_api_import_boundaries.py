#!/usr/bin/env python3
# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Enforce configured public-surface imports and ``@public_api`` signatures.

The boundary policy lives in ``quality/api_import_boundaries.toml``. Keeping
surface dependencies and concrete owner shapes in data lets architecture moves
repoint the policy without teaching this checker each package topology.

Two rules share one principle: concrete capabilities stay behind supported
facades.

1. Configured import surfaces may import only declared dependencies from each
   governed package root.
2. A ``@public_api`` callable may not accept a configured concrete owner type
   or owner-shaped parameter name.

Deprecated migration bridges remain next to the checker until one exists. Each
entry must carry a removal deadline in its comment; delete it with the bridge.
"""

from __future__ import annotations

import ast
import re
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_POLICY = ROOT / "quality" / "api_import_boundaries.toml"
SUPPORTED_POLICY_VERSION = 1


class BoundaryPolicyError(ValueError):
    """The boundary policy is malformed or points at stale source targets."""


@dataclass(frozen=True)
class ImportSurface:
    """One source surface and the dependency roots it may consume."""

    name: str
    targets: tuple[str, ...]
    dependency_roots: frozenset[str]
    allowed_dependencies: frozenset[str]
    forbidden_dependencies: frozenset[str]
    rationale: str


@dataclass(frozen=True)
class PublicApiPolicy:
    """Configured raw-owner shapes forbidden in supported signatures."""

    targets: tuple[str, ...]
    owner_type_source: str
    forbidden_owner_types: frozenset[str]
    forbidden_parameter_names: frozenset[str]


@dataclass(frozen=True)
class BoundaryPolicy:
    """Complete import and public-signature policy."""

    import_surfaces: tuple[ImportSurface, ...]
    public_api: PublicApiPolicy


# Deprecated owner-shaped bridge parameters, keyed "relpath::qualname".
# Add no entry without a removal deadline in the adjacent comment.
PUBLIC_API_BRIDGE_PARAMS: dict[str, set[str]] = {}

_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_MODULE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*")
_TOP_LEVEL_KEYS = frozenset({"version", "import_surface", "public_api"})
_IMPORT_SURFACE_KEYS = frozenset(
    {
        "name",
        "targets",
        "dependency_roots",
        "allowed_dependencies",
        "forbidden_dependencies",
        "rationale",
    }
)
_PUBLIC_API_KEYS = frozenset(
    {
        "targets",
        "forbidden_owner_types_from",
        "forbidden_parameter_names",
    }
)


def _reject_unknown_keys(
    table: dict[str, Any],
    allowed: frozenset[str],
    coordinate: str,
) -> None:
    unknown = sorted(set(table) - allowed)
    if unknown:
        raise BoundaryPolicyError(f"{coordinate} contains unknown keys: {', '.join(unknown)}")


def _string_list(
    table: dict[str, Any],
    key: str,
    coordinate: str,
    *,
    required: bool = True,
) -> tuple[str, ...]:
    raw = table.get(key)
    if raw is None and not required:
        return ()
    if not isinstance(raw, list) or not all(isinstance(value, str) for value in raw):
        raise BoundaryPolicyError(f"{coordinate}.{key} must be a list of strings")
    values = tuple(raw)
    if not values and required:
        raise BoundaryPolicyError(f"{coordinate}.{key} must not be empty")
    invalid = [value for value in values if not value or value.strip() != value]
    if invalid:
        raise BoundaryPolicyError(f"{coordinate}.{key} contains empty or whitespace-padded values")
    if len(values) != len(set(values)):
        raise BoundaryPolicyError(f"{coordinate}.{key} contains duplicate values")
    return values


def _validate_patterns(patterns: tuple[str, ...], coordinate: str) -> None:
    for pattern in patterns:
        path = Path(pattern)
        if path.is_absolute() or ".." in path.parts:
            raise BoundaryPolicyError(
                f"{coordinate} target patterns must stay relative to the repository root"
            )


def _validate_modules(modules: frozenset[str], coordinate: str) -> None:
    invalid = sorted(module for module in modules if _MODULE.fullmatch(module) is None)
    if invalid:
        raise BoundaryPolicyError(
            f"{coordinate} contains invalid module names: {', '.join(invalid)}"
        )


def _matches_root(module: str, root: str) -> bool:
    return module == root or module.startswith(root + ".")


def _relative_policy_path(value: Any, coordinate: str) -> str:
    if not isinstance(value, str) or not value or value.strip() != value:
        raise BoundaryPolicyError(f"{coordinate} must be a non-empty relative path")
    path = Path(value)
    if path.is_absolute() or ".." in path.parts:
        raise BoundaryPolicyError(f"{coordinate} must stay relative to the repository root")
    return value


def _load_concrete_owner_types(repo_root: Path, relative_path: str) -> frozenset[str]:
    path = repo_root / relative_path
    try:
        document = tomllib.loads(path.read_text(encoding="utf-8"))
    except (OSError, tomllib.TOMLDecodeError) as exc:
        raise BoundaryPolicyError(f"cannot read concrete owner registry {path}: {exc}") from exc
    concrete_services = document.get("concrete_services")
    if not isinstance(concrete_services, dict):
        raise BoundaryPolicyError(
            f"{relative_path}.concrete_services must be a table containing types"
        )
    types = frozenset(
        _string_list(
            concrete_services,
            "types",
            f"{relative_path}.concrete_services",
        )
    )
    invalid = sorted(name for name in types if _IDENTIFIER.fullmatch(name) is None)
    if invalid:
        raise BoundaryPolicyError(
            f"{relative_path}.concrete_services.types contains invalid identifiers: "
            + ", ".join(invalid)
        )
    return types


def _snake_case_owner(name: str) -> str:
    first = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", name)
    return re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", first).lower()


def _parse_policy(
    document: dict[str, Any],
    *,
    repo_root: Path = ROOT,
) -> BoundaryPolicy:
    _reject_unknown_keys(document, _TOP_LEVEL_KEYS, "policy")
    version = document.get("version")
    if version != SUPPORTED_POLICY_VERSION:
        raise BoundaryPolicyError(f"version must be {SUPPORTED_POLICY_VERSION}, got {version!r}")

    raw_surfaces = document.get("import_surface")
    if not isinstance(raw_surfaces, list) or not raw_surfaces:
        raise BoundaryPolicyError("import_surface must be a non-empty array of tables")

    surfaces: list[ImportSurface] = []
    names: set[str] = set()
    for index, raw_surface in enumerate(raw_surfaces):
        coordinate = f"import_surface[{index}]"
        if not isinstance(raw_surface, dict):
            raise BoundaryPolicyError(f"{coordinate} must be a table")
        _reject_unknown_keys(raw_surface, _IMPORT_SURFACE_KEYS, coordinate)

        name = raw_surface.get("name")
        rationale = raw_surface.get("rationale")
        if not isinstance(name, str) or not name or name.strip() != name:
            raise BoundaryPolicyError(f"{coordinate}.name must be a non-empty string")
        if name in names:
            raise BoundaryPolicyError(f"duplicate import surface name: {name}")
        names.add(name)
        if not isinstance(rationale, str) or not rationale.strip():
            raise BoundaryPolicyError(f"{coordinate}.rationale must be a non-empty string")

        targets = _string_list(raw_surface, "targets", coordinate)
        roots = frozenset(_string_list(raw_surface, "dependency_roots", coordinate))
        allowed = frozenset(
            _string_list(raw_surface, "allowed_dependencies", coordinate, required=False)
        )
        forbidden = frozenset(
            _string_list(raw_surface, "forbidden_dependencies", coordinate, required=False)
        )
        _validate_patterns(targets, f"{coordinate}.targets")
        _validate_modules(roots, f"{coordinate}.dependency_roots")
        _validate_modules(allowed, f"{coordinate}.allowed_dependencies")
        _validate_modules(forbidden, f"{coordinate}.forbidden_dependencies")

        overlap = sorted(allowed & forbidden)
        if overlap:
            raise BoundaryPolicyError(
                f"{coordinate} allows and forbids the same dependencies: " + ", ".join(overlap)
            )
        outside_roots = sorted(
            module
            for module in allowed | forbidden
            if not any(_matches_root(module, root) for root in roots)
        )
        if outside_roots:
            raise BoundaryPolicyError(
                f"{coordinate} dependencies fall outside dependency_roots: "
                + ", ".join(outside_roots)
            )

        surfaces.append(
            ImportSurface(
                name=name,
                targets=targets,
                dependency_roots=roots,
                allowed_dependencies=allowed,
                forbidden_dependencies=forbidden,
                rationale=rationale.strip(),
            )
        )

    raw_public_api = document.get("public_api")
    if not isinstance(raw_public_api, dict):
        raise BoundaryPolicyError("public_api must be a table")
    _reject_unknown_keys(raw_public_api, _PUBLIC_API_KEYS, "public_api")
    public_targets = _string_list(raw_public_api, "targets", "public_api")
    owner_type_source = _relative_policy_path(
        raw_public_api.get("forbidden_owner_types_from"),
        "public_api.forbidden_owner_types_from",
    )
    forbidden_types = _load_concrete_owner_types(repo_root, owner_type_source)
    configured_forbidden_names = frozenset(
        _string_list(raw_public_api, "forbidden_parameter_names", "public_api")
    )
    forbidden_names = configured_forbidden_names | {
        _snake_case_owner(name) for name in forbidden_types
    }
    _validate_patterns(public_targets, "public_api.targets")
    invalid_names = sorted(name for name in forbidden_names if _IDENTIFIER.fullmatch(name) is None)
    if invalid_names:
        raise BoundaryPolicyError(
            "public_api.forbidden_parameter_names contains invalid identifiers: "
            + ", ".join(invalid_names)
        )

    return BoundaryPolicy(
        import_surfaces=tuple(surfaces),
        public_api=PublicApiPolicy(
            targets=public_targets,
            owner_type_source=owner_type_source,
            forbidden_owner_types=forbidden_types,
            forbidden_parameter_names=forbidden_names,
        ),
    )


def load_policy(path: Path = DEFAULT_POLICY) -> BoundaryPolicy:
    """Load and validate the declarative boundary policy."""

    resolved = path.resolve()
    try:
        document = tomllib.loads(resolved.read_text(encoding="utf-8"))
    except (OSError, tomllib.TOMLDecodeError) as exc:
        raise BoundaryPolicyError(f"cannot read {resolved}: {exc}") from exc
    return _parse_policy(document, repo_root=resolved.parents[1])


def _expand_targets(root: Path, patterns: tuple[str, ...], coordinate: str) -> list[Path]:
    targets: set[Path] = set()
    for pattern in patterns:
        matches = {path for path in root.glob(pattern) if path.is_file()}
        if not matches:
            raise BoundaryPolicyError(f"{coordinate} target pattern matched no files: {pattern}")
        targets.update(matches)
    return sorted(targets)


def _source_module(path: Path, root: Path) -> tuple[str, bool]:
    try:
        relative = path.relative_to(root / "src").with_suffix("")
    except ValueError as exc:
        raise BoundaryPolicyError(f"source target is outside {root / 'src'}: {path}") from exc
    parts = list(relative.parts)
    is_package = bool(parts and parts[-1] == "__init__")
    if is_package:
        parts.pop()
    if not parts:
        raise BoundaryPolicyError(f"source target has no importable module name: {path}")
    return ".".join(parts), is_package


def _resolve_import_from(node: ast.ImportFrom, consumer: str, is_package: bool) -> str:
    if node.level == 0:
        return node.module or ""
    package = consumer.split(".") if is_package else consumer.split(".")[:-1]
    ascend = node.level - 1
    if ascend > len(package):
        return ""
    prefix = package[: len(package) - ascend] if ascend else package
    if node.module:
        prefix.extend(node.module.split("."))
    return ".".join(prefix)


def _imported_modules(
    node: ast.AST,
    dependency_roots: frozenset[str],
    declared_dependencies: frozenset[str],
    *,
    consumer: str,
    is_package: bool,
) -> list[str]:
    if isinstance(node, ast.Import):
        return [alias.name for alias in node.names]
    if isinstance(node, ast.ImportFrom):
        module = _resolve_import_from(node, consumer, is_package)
        if not module:
            return []
        # A leaf policy module commonly imports symbols
        # (``from ...interfaces import iCommandGateway``); keep that leaf
        # intact. A parent-package import instead names the dependency in its
        # alias (``from ...gateway import interfaces``), so expand aliases
        # whenever either side is an ancestor of a governed root.
        if module in declared_dependencies:
            return [module]
        if any(
            _matches_root(module, root) or _matches_root(root, module) for root in dependency_roots
        ):
            expanded: set[str] = set()
            for alias in node.names:
                if alias.name == "*":
                    governed_descendants = {
                        root for root in dependency_roots if _matches_root(root, module)
                    }
                    expanded.update(governed_descendants or {module})
                else:
                    expanded.add(f"{module}.{alias.name}")
            return sorted(
                candidate
                for candidate in expanded
                if any(
                    _matches_root(candidate, root) or _matches_root(root, candidate)
                    for root in dependency_roots
                )
            )
        return [module]
    return []


def _import_violations(path: Path, surface: ImportSurface, *, root: Path = ROOT) -> list[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    consumer, is_package = _source_module(path, root)
    violations: list[str] = []
    for node in ast.walk(tree):
        for module in _imported_modules(
            node,
            surface.dependency_roots,
            surface.allowed_dependencies | surface.forbidden_dependencies,
            consumer=consumer,
            is_package=is_package,
        ):
            if not any(_matches_root(module, prefix) for prefix in surface.dependency_roots):
                continue
            if module in surface.forbidden_dependencies:
                violations.append(
                    f"{path.relative_to(root)} imports forbidden {module} "
                    f"({surface.name}: {surface.rationale})"
                )
                continue
            if module not in surface.allowed_dependencies:
                violations.append(
                    f"{path.relative_to(root)} imports {module}; allowed governed imports "
                    f"for {surface.name} are "
                    f"{', '.join(sorted(surface.allowed_dependencies))}"
                )
    return violations


def _is_public_api_decorated(
    node: ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef,
) -> bool:
    for dec in node.decorator_list:
        target = dec.func if isinstance(dec, ast.Call) else dec
        name = target.attr if isinstance(target, ast.Attribute) else getattr(target, "id", "")
        if name == "public_api":
            return True
    return False


def _owner_param_violation(param: ast.arg, policy: PublicApiPolicy) -> bool:
    """Match whole annotation tokens plus configured owner-shaped names.

    Tokenizing avoids substring hits (``ArtifactServiceConfig`` is not
    ``ArtifactService``). A parameter-name rule remains the backstop for
    import aliases, whose type token cannot be resolved without import walking.
    """

    annotation = ast.unparse(param.annotation) if param.annotation else ""
    tokens = set(_IDENTIFIER.findall(annotation))
    return bool(tokens & policy.forbidden_owner_types) or (
        param.arg in policy.forbidden_parameter_names
    )


def _signature_violations(
    rel: str,
    qualname: str,
    fn: ast.FunctionDef | ast.AsyncFunctionDef,
    policy: PublicApiPolicy,
) -> list[str]:
    bridge = PUBLIC_API_BRIDGE_PARAMS.get(f"{rel}::{qualname}", set())
    params = [*fn.args.posonlyargs, *fn.args.args, *fn.args.kwonlyargs]
    if fn.args.vararg is not None:
        params.append(fn.args.vararg)
    if fn.args.kwarg is not None:
        params.append(fn.args.kwarg)
    violations: list[str] = []
    for param in params:
        if param.arg in bridge or param.arg in ("self", "cls"):
            continue
        if _owner_param_violation(param, policy):
            violations.append(
                f"{rel}::{qualname} — @public_api callable accepts raw service "
                f"parameter `{param.arg}`; public capability must be expressible "
                f"through the supported runtime or gateway boundary, as appropriate. "
                f"Deprecated bridges "
                f"belong in PUBLIC_API_BRIDGE_PARAMS with a removal deadline."
            )
    return violations


def _public_api_violations(
    path: Path,
    policy: PublicApiPolicy,
    *,
    root: Path = ROOT,
) -> list[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    rel = str(path.relative_to(root))
    violations: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
            if _is_public_api_decorated(node):
                violations += _signature_violations(rel, node.name, node, policy)
        elif isinstance(node, ast.ClassDef) and _is_public_api_decorated(node):
            # The marker covers classes too: constructors are their supported
            # signature surface.
            for member in node.body:
                if isinstance(member, ast.FunctionDef | ast.AsyncFunctionDef) and member.name in (
                    "__init__",
                    "__new__",
                ):
                    violations += _signature_violations(
                        rel, f"{node.name}.{member.name}", member, policy
                    )
    return violations


def audit(root: Path, policy: BoundaryPolicy) -> list[str]:
    """Apply a loaded policy to one repository root."""

    violations: list[str] = []
    for surface in policy.import_surfaces:
        targets = _expand_targets(root, surface.targets, f"import surface {surface.name}")
        violations.extend(
            violation
            for path in targets
            for violation in _import_violations(path, surface, root=root)
        )
    public_targets = _expand_targets(root, policy.public_api.targets, "public_api")
    violations.extend(
        violation
        for path in public_targets
        for violation in _public_api_violations(path, policy.public_api, root=root)
    )
    return violations


def main() -> int:
    try:
        policy = load_policy()
        violations = audit(ROOT, policy)
    except BoundaryPolicyError as exc:
        print(f"Invalid outward-boundary policy: {exc}")
        return 1

    if violations:
        print("Outward-boundary violations:")
        for violation in violations:
            print(f"  - {violation}")
        return 1
    print("Outward import boundary audit passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
