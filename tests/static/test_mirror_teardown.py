# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Contract for PR-4 composition-mirror teardown."""

from __future__ import annotations

import ast
import re
from pathlib import Path

_REPOSITORY_ROOT = Path(__file__).parents[2]
_SOURCE_ROOT = _REPOSITORY_ROOT / "src" / "archetype"
_DELETED_PATHS = (
    "app/application",
    "app/container.py",
    "app/gateway",
    "app/models.py",
    "/".join(("app", "redaction")),
)
_DELETED_SYMBOLS = frozenset(
    {
        "CommandBroker",
        "CommandGateway",
        "CommandService",
        "RuntimeApplication",
        "ServiceContainer",
        "iCommandGateway",
        "iRuntimeApplication",
    }
)
_DELETED_STATE = frozenset(
    {
        "_ACTIVE_APPLICATION",
        "_ADMITTED_STATE",
        "_HELD_WORLD_LANES",
        "_RUNTIME_CLEANUP_STATE",
    }
)
_DELETED_SETTERS = frozenset(
    {
        "set_container",
        "set_outbox_source",
        "set_quota_reset",
    }
)
_CONTEXTVAR_FORBIDDEN_ROOTS = (
    "api",
    "runtime",
    "runtime_resources.py",
    "wiring.py",
)
_GUIDANCE_PATTERNS = (
    ("CommandBroker", re.compile(r"\bCommandBroker\b")),
    ("CommandGateway", re.compile(r"\bCommandGateway\b")),
    ("CommandService", re.compile(r"\bCommandService\b")),
    ("RuntimeApplication", re.compile(r"\bRuntimeApplication\b")),
    ("ServiceContainer", re.compile(r"\bServiceContainer\b")),
    ("iCommandGateway", re.compile(r"\biCommandGateway\b")),
    ("iRuntimeApplication", re.compile(r"\biRuntimeApplication\b")),
    ("service container", re.compile(r"\bservice\s+container\b", re.IGNORECASE)),
)
# Current normative guidance keeps one explicit account of the deleted design.
# Archive/report/planning trees and executable negative fixtures are not current
# collaborator surfaces and therefore remain outside this scan.
_GUIDANCE_REFERENCE_ALLOWLIST = {
    "docs/guide/application-architecture.md": {
        "Historical note (superseded):": frozenset(
            {
                "CommandGateway",
                "RuntimeApplication",
                "ServiceContainer",
            }
        ),
    },
}


def _python_files(root: Path) -> tuple[Path, ...]:
    return tuple(sorted(path for path in root.rglob("*.py") if path.is_file()))


def _defined_names(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(), filename=str(path))
    return {
        node.name
        for node in ast.walk(tree)
        if isinstance(
            node,
            (
                ast.AsyncFunctionDef,
                ast.ClassDef,
                ast.FunctionDef,
            ),
        )
    }


def _loaded_names(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(), filename=str(path))
    return {node.id for node in ast.walk(tree) if isinstance(node, ast.Name)}


def _current_guidance_files(root: Path) -> tuple[Path, ...]:
    paths = set(root.glob("*.md"))
    paths.update((root / "docs" / "guide").rglob("*.md"))
    paths.update((root / ".claude" / "skills").rglob("SKILL.md"))
    paths.add(root / "src" / "archetype" / "__init__.py")
    return tuple(sorted(path for path in paths if path.is_file()))


def _paragraph_at(text: str, offset: int) -> str:
    start = text.rfind("\n\n", 0, offset)
    end = text.find("\n\n", offset)
    return text[0 if start < 0 else start + 2 : None if end < 0 else end]


def _allowlisted_guidance_reference(
    relative: str,
    label: str,
    paragraph: str,
) -> bool:
    for marker, labels in _GUIDANCE_REFERENCE_ALLOWLIST.get(relative, {}).items():
        if marker in paragraph and label in labels:
            return True
    return False


def _guidance_findings(root: Path) -> list[str]:
    findings: list[str] = []
    for path in _current_guidance_files(root):
        relative = path.relative_to(root).as_posix()
        text = path.read_text()
        for label, pattern in _GUIDANCE_PATTERNS:
            for match in pattern.finditer(text):
                paragraph = _paragraph_at(text, match.start())
                if _allowlisted_guidance_reference(relative, label, paragraph):
                    continue
                line = text.count("\n", 0, match.start()) + 1
                findings.append(f"guidance:{relative}:{line}:{label}")
    return sorted(findings)


def _mirror_findings(root: Path) -> list[str]:
    findings: list[str] = []
    for relative in _DELETED_PATHS:
        if (root / relative).exists():
            findings.append(f"path:{relative}")

    for path in _python_files(root):
        relative = path.relative_to(root).as_posix()
        names = _defined_names(path)
        for symbol in sorted((_DELETED_SYMBOLS | _DELETED_SETTERS) & names):
            findings.append(f"definition:{relative}:{symbol}")

        loaded = _loaded_names(path)
        for symbol in sorted(_DELETED_STATE & loaded):
            findings.append(f"state:{relative}:{symbol}")

    for relative in _CONTEXTVAR_FORBIDDEN_ROOTS:
        candidate = root / relative
        paths = _python_files(candidate) if candidate.is_dir() else (candidate,)
        for path in paths:
            if path.is_file() and "ContextVar" in _loaded_names(path):
                findings.append(
                    f"contextvar:{path.relative_to(root).as_posix()}",
                )
    return findings


def test_runtime_application_gateway_container_and_context_mirrors_are_absent(
    tmp_path: Path,
) -> None:
    counterfactual = tmp_path / "archetype"
    mirror = counterfactual / "runtime" / "mirror.py"
    mirror.parent.mkdir(parents=True)
    mirror.write_text(
        "from contextvars import ContextVar\n"
        "_ADMITTED_STATE = ContextVar('admitted')\n"
        "class RuntimeApplication: pass\n"
        "def set_container(value): return value\n",
    )
    assert _mirror_findings(counterfactual) == [
        "definition:runtime/mirror.py:RuntimeApplication",
        "definition:runtime/mirror.py:set_container",
        "state:runtime/mirror.py:_ADMITTED_STATE",
        "contextvar:runtime/mirror.py",
    ]

    assert _mirror_findings(_SOURCE_ROOT) == []


def test_deleted_composition_mirrors_are_absent_from_current_guidance(
    tmp_path: Path,
) -> None:
    counterfactual = tmp_path / "repository"
    skill = counterfactual / ".claude" / "skills" / "processor" / "SKILL.md"
    skill.parent.mkdir(parents=True)
    skill.write_text("Inject a CommandBroker into every world.\n")
    guide = counterfactual / "docs" / "guide" / "application-architecture.md"
    guide.parent.mkdir(parents=True)
    guide.write_text(
        "Historical note (superseded): RuntimeApplication, CommandGateway, and "
        "ServiceContainer were deleted.\n\n"
        "Current code should construct a RuntimeApplication.\n"
    )
    source_root = counterfactual / "src" / "archetype"
    source_root.mkdir(parents=True)
    (source_root / "__init__.py").write_text(
        '"""Import iCommandGateway from the package root."""\n'
    )
    (counterfactual / "CLAUDE.md").write_text(
        "Call CommandService through the service container.\n"
    )
    negative = counterfactual / "tests" / "static" / "test_negative.py"
    negative.parent.mkdir(parents=True)
    negative.write_text("assert 'ServiceContainer' not in public_names\n")

    assert _guidance_findings(counterfactual) == [
        "guidance:.claude/skills/processor/SKILL.md:1:CommandBroker",
        "guidance:CLAUDE.md:1:CommandService",
        "guidance:CLAUDE.md:1:service container",
        "guidance:docs/guide/application-architecture.md:3:RuntimeApplication",
        "guidance:src/archetype/__init__.py:1:iCommandGateway",
    ]

    assert _guidance_findings(_REPOSITORY_ROOT) == []
