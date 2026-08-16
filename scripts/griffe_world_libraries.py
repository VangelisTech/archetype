# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Expose split ``archetype`` world-library packages to Griffe.

Griffe resolves one physical root for a regular Python package. Archetype's
framework wheel owns ``archetype/__init__.py`` while three separate wheels add
namespace subpackages, so static collection otherwise stops at the framework
root. This documentation-only extension loads each library from its own source
root and attaches that tree to the framework package before mkdocstrings looks
up public objects.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from griffe import Extension, GriffeLoader, Module

_REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
_LIBRARY_SOURCE_ROOTS = {
    "missions": _REPOSITORY_ROOT / "packages/archetype-missions/src",
    "physical_ai": _REPOSITORY_ROOT / "packages/archetype-physical-ai/src",
    "research": _REPOSITORY_ROOT / "packages/archetype-research/src",
}


class WorldLibraryPackages(Extension):
    """Merge separately distributed world-library trees for documentation."""

    def __init__(self) -> None:
        self._attached = False

    def on_module(self, *, mod: Module, loader: GriffeLoader, **kwargs: Any) -> None:
        del loader, kwargs
        if self._attached or mod.path != "archetype":
            return

        self._attached = True
        for package, source_root in _LIBRARY_SOURCE_ROOTS.items():
            library_loader = GriffeLoader(search_paths=[source_root])
            library = library_loader.load(f"archetype.{package}")
            mod.set_member(package, library)


__all__ = ["WorldLibraryPackages"]
