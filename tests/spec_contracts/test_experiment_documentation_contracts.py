# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Executable contracts for claims made by experiment documentation."""

from __future__ import annotations

import re
import tomllib
from pathlib import Path

_EXPERIMENT_ROOT = Path("experiments")
_PYTHON_LOWER_BOUND = re.compile(r"(?:^|,)\s*>=\s*(\d+\.\d+)")
_DOCUMENTED_REQUIREMENT = re.compile(
    r"\bRequires:\s*Python\s+(\d+\.\d+)\+",
    re.IGNORECASE,
)


def test_experiment_python_requirements_match_package_lower_bound() -> None:
    """Experiment prerequisites must move with the package's Python floor."""
    project = tomllib.loads(Path("pyproject.toml").read_text())
    requires_python = project["project"]["requires-python"]
    lower_bound = _PYTHON_LOWER_BOUND.search(requires_python)
    assert lower_bound is not None, (
        f"cannot derive a Python floor from project.requires-python={requires_python!r}"
    )
    expected = lower_bound.group(1)

    documented: list[tuple[Path, int, str]] = []
    for path in sorted(_EXPERIMENT_ROOT.rglob("*.md")):
        text = path.read_text()
        for match in _DOCUMENTED_REQUIREMENT.finditer(text):
            line = text.count("\n", 0, match.start()) + 1
            documented.append((path, line, match.group(1)))

    assert documented, "experiment docs no longer state a Python prerequisite"
    stale = [
        f"{path}:{line} requires Python {version}+; package floor is {expected}"
        for path, line, version in documented
        if version != expected
    ]
    assert not stale, "stale experiment Python requirements:\n" + "\n".join(stale)
