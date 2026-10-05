"""Audit the separately installed, stdlib-only DDlog preview infrastructure."""

from __future__ import annotations

import ast
import sys
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def check(root: Path = ROOT) -> list[str]:
    metadata = tomllib.loads((root / "quality/ddlog-preview.toml").read_text())
    errors: list[str] = []
    expected = {
        "classification": "isolated-runtime-preview",
        "distribution": "archetype-ddlog-preview",
        "module": "archetype_ddlog_preview",
        "package": "packages/archetype-ddlog-preview",
        "native_crate": "crates/archetype-ddlog-python",
        "contract": "docs/guide/ddlog-python-preview.md",
    }
    if metadata != expected:
        return ["DDlog preview metadata must match its exact infrastructure reservation"]
    package = root / metadata["package"]
    source = package / "src"
    if not (source / metadata["module"] / "__init__.py").is_file():
        return ["Missing declared DDlog preview module"]
    generated = {"__pycache__", metadata["module"] + ".egg-info"}
    if {p.name for p in source.iterdir() if p.name not in generated} != {metadata["module"]}:
        errors.append("Undeclared top-level preview package/module")
    for required in (root / metadata["native_crate"] / "Cargo.toml", root / metadata["contract"]):
        if not required.is_file():
            errors.append(f"Missing preview contract/native crate: {required}")
    project = tomllib.loads((package / "pyproject.toml").read_text())["project"]
    if project["name"] != metadata["distribution"] or project.get("dependencies") != []:
        errors.append("DDlog preview must remain its exact isolated, dependency-free distribution")
    allowed = sys.stdlib_module_names | {metadata["module"]}
    for path in sorted((package / "src").rglob("*.py")):
        for node in ast.walk(ast.parse(path.read_text())):
            names = []
            if isinstance(node, ast.Import):
                names = [item.name for item in node.names]
            elif isinstance(node, ast.ImportFrom) and node.level == 0:
                names = [node.module or ""]
            for name in names:
                if name.split(".")[0] not in allowed or name.split(".")[0] in {
                    "importlib",
                    "runpy",
                    "builtins",
                }:
                    errors.append(
                        f"{path.relative_to(root)}:{node.lineno}: forbidden import {name}"
                    )
            if isinstance(node, ast.Call) and (
                isinstance(node.func, ast.Name)
                and node.func.id in {"__import__", "exec", "eval"}
                or isinstance(node.func, ast.Attribute)
                and node.func.attr in {"import_module", "exec_module", "__import__", "exec", "eval"}
            ):
                errors.append(f"{path.relative_to(root)}:{node.lineno}: dynamic import/evaluation")
    return errors


if __name__ == "__main__":
    failures = check()
    if failures:
        raise SystemExit("\n".join(failures))
    print("DDlog preview infrastructure: stdlib/self imports only; no Python dependencies")
