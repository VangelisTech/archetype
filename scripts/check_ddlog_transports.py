"""Audit the optional adapter's exact import and distribution reservation."""

from __future__ import annotations

import ast
import sys
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def check(root: Path = ROOT) -> list[str]:
    expected = {
        "classification": "native-transport-adapter",
        "distribution": "archetype-transports",
        "module": "archetype_transports",
        "package": "packages/archetype-transports",
        "contract": "docs/guide/transports.md",
    }
    metadata = tomllib.loads((root / "quality/ddlog-transports.toml").read_text())
    if metadata != expected:
        return ["DDlog transport metadata must match its exact infrastructure reservation"]
    package = root / metadata["package"]
    source = package / "src"
    errors: list[str] = []
    if not (source / metadata["module"] / "__init__.py").is_file():
        return ["Missing declared DDlog transport module"]
    generated = {"__pycache__", metadata["module"] + ".egg-info"}
    if {p.name for p in source.iterdir() if p.name not in generated} != {metadata["module"]}:
        errors.append("Undeclared top-level transport package/module")
    if not (root / metadata["contract"]).is_file():
        errors.append("Missing transport contract")
    project = tomllib.loads((package / "pyproject.toml").read_text())["project"]
    if project["name"] != metadata["distribution"] or project.get("dependencies") != [
        "archetype-native==0.7.0",
        "mcp==2.3.0",
        "pyjwt[crypto]==2.15.1",
        "cryptography==50.0.1",
        "starlette==1.3.1",
        "httpx2==2.13.1",
    ]:
        errors.append("Transport dependency set must remain explicitly pinned and isolated")
    allowed = sys.stdlib_module_names | {
        metadata["module"],
        "archetype_native",
        "mcp",
        "mcp_types",
        "starlette",
    }
    for path in sorted(source.rglob("*.py")):
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
    print("DDlog local transports: pinned SDK/Starlette/preview imports; no retained runtime")
